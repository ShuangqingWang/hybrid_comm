/**
 * @file comm_test.cpp
 * @brief hybrid_comm 两进程测试。不带参数运行时依次执行全部用例；用例通过 /proc/self/exe 启动子进程扮演另一方。
 */

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "hybrid_comm/hybrid_comm.h"
#include "test_msgs.pb.h"
#include "test_types.h"

namespace hc = snow::hybrid_comm;
using snow::hybrid_comm_test::OtherBlob;
using snow::hybrid_comm_test::Ping;
using snow::hybrid_comm_test::Pong;
using snow::hybrid_comm_test::TestBlob;
using Clock = std::chrono::steady_clock;

namespace
{
constexpr int kChildTimeoutMs = 10000;
constexpr uint32_t kBlobLength = 1024 * 1024 + 7; ///< 有效长度，不取整，便于发现截断
constexpr int kExpectedCount = 5;
constexpr int kExitOk = 0;
constexpr int kExitFail = 1;

// ============================ 工具 ============================

void Sleep(int ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

int64_t ElapsedMs(Clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count();
}

/// @brief 每次运行使用不同的 topic，避免与上次运行的残留互相影响
std::string Topic(const std::string& name)
{
    return "test/p" + std::to_string(getpid()) + "/" + name;
}

void FillBlob(TestBlob& blob, uint64_t seq)
{
    blob.seq = seq;
    blob.length = kBlobLength;
    for (uint32_t i = 0; i < kBlobLength; ++i)
    {
        blob.data[i] = static_cast<uint8_t>((seq + i) & 0xff);
    }
}

bool CheckBlob(const TestBlob& blob)
{
    if (blob.length != kBlobLength)
    {
        return false;
    }
    for (uint32_t i = 0; i < kBlobLength; ++i)
    {
        if (blob.data[i] != static_cast<uint8_t>((blob.seq + i) & 0xff))
        {
            return false;
        }
    }
    return true;
}

pid_t Spawn(const std::vector<std::string>& args)
{
    const pid_t pid = fork();
    if (pid == 0)
    {
        std::vector<char*> argv;
        static char self[] = "hybrid_comm_test";
        argv.push_back(self);
        for (const auto& arg : args)
        {
            argv.push_back(const_cast<char*>(arg.c_str()));
        }
        argv.push_back(nullptr);
        execv("/proc/self/exe", argv.data());
        _exit(127);
    }
    return pid;
}

/// @brief 等待子进程结束，返回退出码；超时则杀掉并返回 -1，被信号杀死返回 -信号值
int WaitChild(pid_t pid, int timeout_ms = kChildTimeoutMs)
{
    const auto start = Clock::now();
    while (ElapsedMs(start) < timeout_ms)
    {
        int status = 0;
        if (waitpid(pid, &status, WNOHANG) == pid)
        {
            if (WIFEXITED(status))
            {
                return WEXITSTATUS(status);
            }
            return WIFSIGNALED(status) ? -WTERMSIG(status) : -1;
        }
        Sleep(10);
    }
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    return -1;
}

void KillChild(pid_t pid)
{
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
}

/// @brief 子进程的 waitpid 状态只能取一次，因此持续发送直到子进程退出时直接取得退出码
int PublishUntilChildExits(pid_t pid, const std::function<bool(uint64_t)>& publish)
{
    const auto start = Clock::now();
    uint64_t seq = 0;
    while (ElapsedMs(start) < kChildTimeoutMs)
    {
        int status = 0;
        if (waitpid(pid, &status, WNOHANG) == pid)
        {
            return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        }
        publish(seq++);
        Sleep(20);
    }
    KillChild(pid);
    return -1;
}

// ============================ 子进程角色 ============================

/// @brief 共享内存订阅方：收到 kExpectedCount 条不同且内容正确的数据后退出
int RoleShmReader(const std::string& topic)
{
    std::mutex mutex;
    std::set<uint64_t> received;
    std::atomic<bool> corrupted{false};
    std::string error;
    auto reader = hc::Reader<TestBlob>::Create(
        topic,
        [&](const TestBlob& blob) {
            if (!CheckBlob(blob))
            {
                corrupted = true;
            }
            std::lock_guard<std::mutex> lock(mutex);
            received.insert(blob.seq);
        },
        &error);
    if (reader == nullptr)
    {
        std::cerr << "RoleShmReader: " << error << std::endl;
        return kExitFail;
    }
    const auto start = Clock::now();
    while (ElapsedMs(start) < kChildTimeoutMs)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (static_cast<int>(received.size()) >= kExpectedCount)
            {
                return corrupted ? kExitFail : kExitOk;
            }
        }
        Sleep(10);
    }
    return kExitFail;
}

/// @brief Protobuf 订阅方
int RoleProtoReader(const std::string& topic)
{
    std::mutex mutex;
    std::set<uint64_t> received;
    std::atomic<bool> corrupted{false};
    std::string error;
    auto reader = hc::Reader<Ping>::Create(
        topic,
        [&](const Ping& ping) {
            if (ping.text() != "ping" + std::to_string(ping.seq()))
            {
                corrupted = true;
            }
            std::lock_guard<std::mutex> lock(mutex);
            received.insert(ping.seq());
        },
        &error);
    if (reader == nullptr)
    {
        std::cerr << "RoleProtoReader: " << error << std::endl;
        return kExitFail;
    }
    const auto start = Clock::now();
    while (ElapsedMs(start) < kChildTimeoutMs)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (static_cast<int>(received.size()) >= kExpectedCount)
            {
                return corrupted ? kExitFail : kExitOk;
            }
        }
        Sleep(10);
    }
    return kExitFail;
}

/// @brief 应答方：seq 加一返回；text 为 "throw" 时抛出异常。一直运行到被杀死
int RoleServer(const std::string& topic)
{
    std::string error;
    auto server = hc::Server<Ping, Pong>::Create(
        topic,
        [](const Ping& ping, Pong& pong) {
            if (ping.text() == "throw")
            {
                throw std::runtime_error("测试异常");
            }
            pong.set_seq(ping.seq() + 1);
            pong.set_text(ping.text());
        },
        &error);
    if (server == nullptr)
    {
        std::cerr << "RoleServer: " << error << std::endl;
        return kExitFail;
    }
    Sleep(kChildTimeoutMs * 3);
    return kExitOk;
}

/// @brief 期望创建失败：kind 为 shm / proto / server；创建失败返回 kExitOk
int RoleExpectCreateFail(const std::string& kind, const std::string& topic)
{
    std::string error;
    bool created = false;
    if (kind == "shm")
    {
        created = hc::Writer<TestBlob>::Create(topic, &error) != nullptr;
    }
    else if (kind == "proto")
    {
        created = hc::Writer<Ping>::Create(topic, &error) != nullptr;
    }
    else if (kind == "server")
    {
        created = hc::Server<Ping, Pong>::Create(
                      topic, [](const Ping&, Pong&) {}, &error)
                  != nullptr;
    }
    else if (kind == "mismatch")
    {
        created = hc::Reader<OtherBlob>::Create(
                      topic, [](const OtherBlob&) {}, &error)
                  != nullptr;
    }
    if (!created && !error.empty())
    {
        std::cerr << "RoleExpectCreateFail(" << kind << ") 按预期失败: " << error << std::endl;
        return kExitOk;
    }
    return kExitFail;
}

/// @brief 创建发布方与应答方后被 SIGKILL 杀死，模拟崩溃
int RoleCrash(const std::string& shm_topic, const std::string& proto_topic, const std::string& server_topic)
{
    auto shm_writer = hc::Writer<TestBlob>::Create(shm_topic);
    auto proto_writer = hc::Writer<Ping>::Create(proto_topic);
    auto server = hc::Server<Ping, Pong>::Create(server_topic, [](const Ping&, Pong&) {});
    if (shm_writer == nullptr || proto_writer == nullptr || server == nullptr)
    {
        return kExitFail;
    }
    auto blob = std::make_unique<TestBlob>();
    FillBlob(*blob, 0);
    shm_writer->Publish(*blob);
    raise(SIGKILL);
    return kExitFail;
}

// ============================ 用例 ============================

bool TestShmPublish()
{
    const std::string topic = Topic("shm_publish");
    auto writer = hc::Writer<TestBlob>::Create(topic);
    if (writer == nullptr)
    {
        return false;
    }
    auto blob = std::make_unique<TestBlob>();
    const pid_t child = Spawn({"shm_reader", topic});
    return PublishUntilChildExits(child,
                                  [&](uint64_t seq) {
                                      FillBlob(*blob, seq);
                                      return writer->Publish(*blob);
                                  })
           == kExitOk;
}

bool TestShmLoan()
{
    const std::string topic = Topic("shm_loan");
    auto writer = hc::Writer<TestBlob>::Create(topic);
    if (writer == nullptr)
    {
        return false;
    }
    const pid_t child = Spawn({"shm_reader", topic});
    return PublishUntilChildExits(child,
                                  [&](uint64_t seq) {
                                      auto sample = writer->Loan();
                                      if (!sample.has_value())
                                      {
                                          return false;
                                      }
                                      FillBlob(**sample, seq);
                                      return writer->Send(std::move(*sample));
                                  })
           == kExitOk;
}

bool TestProtoPubSub()
{
    const std::string topic = Topic("proto_pubsub");
    auto writer = hc::Writer<Ping>::Create(topic);
    if (writer == nullptr)
    {
        return false;
    }
    const pid_t child = Spawn({"proto_reader", topic});
    return PublishUntilChildExits(child,
                                  [&](uint64_t seq) {
                                      Ping ping;
                                      ping.set_seq(seq);
                                      ping.set_text("ping" + std::to_string(seq));
                                      return writer->Publish(ping);
                                  })
           == kExitOk;
}

/// @brief 重复调用直到成功或超时，用于等待应答方启动
hc::CallStatus CallUntilOk(hc::Client<Ping, Pong>& client, const Ping& ping, Pong& pong)
{
    hc::CallStatus status = hc::CallStatus::TIMEOUT;
    const auto start = Clock::now();
    while (ElapsedMs(start) < kChildTimeoutMs)
    {
        status = client.Call(ping, pong, 300);
        if (status == hc::CallStatus::OK)
        {
            break;
        }
    }
    return status;
}

bool TestRequestResponse()
{
    const std::string topic = Topic("rpc");
    auto client = hc::Client<Ping, Pong>::Create(topic);
    if (client == nullptr)
    {
        return false;
    }
    Ping ping;
    ping.set_seq(42);
    ping.set_text("hello");
    Pong pong;

    // 应答方不存在
    const auto start = Clock::now();
    if (client->Call(ping, pong, 300) != hc::CallStatus::TIMEOUT || ElapsedMs(start) > 1000)
    {
        std::cerr << "  应答方不存在时未按超时返回" << std::endl;
        return false;
    }

    pid_t server = Spawn({"server", topic});
    bool ok = CallUntilOk(*client, ping, pong) == hc::CallStatus::OK && pong.seq() == 43 && pong.text() == "hello";
    if (!ok)
    {
        std::cerr << "  正常调用失败" << std::endl;
    }

    Ping throwing;
    throwing.set_text("throw");
    if (ok && client->Call(throwing, pong, 1000) != hc::CallStatus::SERVER_ERROR)
    {
        std::cerr << "  处理函数异常未返回 SERVER_ERROR" << std::endl;
        ok = false;
    }
    if (ok && client->Call(ping, pong, 1000) != hc::CallStatus::OK)
    {
        std::cerr << "  处理函数异常后再次调用失败" << std::endl;
        ok = false;
    }

    // 应答方崩溃后超时，重启后同一个 Client 恢复
    KillChild(server);
    if (ok && client->Call(ping, pong, 300) != hc::CallStatus::TIMEOUT)
    {
        std::cerr << "  应答方退出后未返回 TIMEOUT" << std::endl;
        ok = false;
    }
    server = Spawn({"server", topic});
    pong.Clear();
    if (ok && (CallUntilOk(*client, ping, pong) != hc::CallStatus::OK || pong.seq() != 43))
    {
        std::cerr << "  应答方重启后未恢复" << std::endl;
        ok = false;
    }
    KillChild(server);
    return ok;
}

bool TestSinglePublisher()
{
    const std::string shm_topic = Topic("single_shm");
    const std::string proto_topic = Topic("single_proto");
    const std::string server_topic = Topic("single_server");
    auto shm_writer = hc::Writer<TestBlob>::Create(shm_topic);
    auto proto_writer = hc::Writer<Ping>::Create(proto_topic);
    auto server = hc::Server<Ping, Pong>::Create(server_topic, [](const Ping&, Pong&) {});
    if (shm_writer == nullptr || proto_writer == nullptr || server == nullptr)
    {
        return false;
    }
    bool ok = true;
    // 其他进程
    ok = WaitChild(Spawn({"expect_fail", "shm", shm_topic})) == kExitOk && ok;
    ok = WaitChild(Spawn({"expect_fail", "proto", proto_topic})) == kExitOk && ok;
    ok = WaitChild(Spawn({"expect_fail", "server", server_topic})) == kExitOk && ok;
    // 同一进程
    std::string error;
    ok = hc::Writer<TestBlob>::Create(shm_topic, &error) == nullptr && ok;
    ok = hc::Writer<Ping>::Create(proto_topic, &error) == nullptr && ok;
    ok = hc::Server<Ping, Pong>::Create(
             server_topic, [](const Ping&, Pong&) {}, &error)
             == nullptr
         && ok;
    // 释放后可以重新创建
    shm_writer.reset();
    proto_writer.reset();
    server.reset();
    ok = hc::Writer<TestBlob>::Create(shm_topic) != nullptr && ok;
    ok = hc::Writer<Ping>::Create(proto_topic) != nullptr && ok;
    ok = hc::Server<Ping, Pong>::Create(server_topic, [](const Ping&, Pong&) {}) != nullptr && ok;
    return ok;
}

bool TestTypeMismatch()
{
    const std::string topic = Topic("mismatch");
    auto writer = hc::Writer<TestBlob>::Create(topic);
    if (writer == nullptr)
    {
        return false;
    }
    return WaitChild(Spawn({"expect_fail", "mismatch", topic})) == kExitOk;
}

bool TestInvalidTopic()
{
    const std::vector<std::string> invalid = {"",
                                              "Camera/image",
                                              "camera//image",
                                              "/camera",
                                              "camera/",
                                              "camera image",
                                              "camera-image",
                                              std::string(65, 'a')};
    const std::vector<std::string> valid = {"camera", "camera/image", "a/b_c/d0", std::string(64, 'a')};
    for (const auto& topic : invalid)
    {
        if (hc::IsValidTopic(topic))
        {
            std::cerr << "  误判为合法: \"" << topic << "\"" << std::endl;
            return false;
        }
    }
    for (const auto& topic : valid)
    {
        if (!hc::IsValidTopic(topic))
        {
            std::cerr << "  误判为不合法: \"" << topic << "\"" << std::endl;
            return false;
        }
    }
    std::string error;
    return hc::Writer<Ping>::Create("Bad/Topic", &error) == nullptr && !error.empty()
           && hc::Reader<TestBlob>::Create(
                  "bad topic", [](const TestBlob&) {}, &error)
                  == nullptr
           && hc::Client<Ping, Pong>::Create("", &error) == nullptr;
}

bool TestCrashRecovery()
{
    const std::string shm_topic = Topic("crash_shm");
    const std::string proto_topic = Topic("crash_proto");
    const std::string server_topic = Topic("crash_server");
    if (WaitChild(Spawn({"crash", shm_topic, proto_topic, server_topic})) != -SIGKILL)
    {
        std::cerr << "  崩溃子进程未按预期退出" << std::endl;
        return false;
    }
    auto shm_writer = hc::Writer<TestBlob>::Create(shm_topic);
    auto proto_writer = hc::Writer<Ping>::Create(proto_topic);
    auto server = hc::Server<Ping, Pong>::Create(server_topic, [](const Ping&, Pong&) {});
    if (shm_writer == nullptr || proto_writer == nullptr || server == nullptr)
    {
        std::cerr << "  崩溃后无法重新创建发布方或应答方" << std::endl;
        return false;
    }
    // 重新创建后通信正常
    auto blob = std::make_unique<TestBlob>();
    const pid_t child = Spawn({"shm_reader", shm_topic});
    return PublishUntilChildExits(child,
                                  [&](uint64_t seq) {
                                      FillBlob(*blob, seq);
                                      return shm_writer->Publish(*blob);
                                  })
           == kExitOk;
}

bool TestStopLatency()
{
    auto shm_reader = hc::Reader<TestBlob>::Create(Topic("stop_shm"), [](const TestBlob&) {});
    auto proto_reader = hc::Reader<Ping>::Create(Topic("stop_proto"), [](const Ping&) {});
    auto server = hc::Server<Ping, Pong>::Create(Topic("stop_server"), [](const Ping&, Pong&) {});
    if (shm_reader == nullptr || proto_reader == nullptr || server == nullptr)
    {
        return false;
    }
    const auto start = Clock::now();
    shm_reader.reset();
    proto_reader.reset();
    server.reset();
    const int64_t elapsed = ElapsedMs(start);
    std::cerr << "  销毁 3 个对象耗时 " << elapsed << " ms" << std::endl;
    return elapsed < 1000;
}

bool TestCallbackException()
{
    const std::string topic = Topic("callback_throw");
    std::atomic<int> calls{0};
    auto reader = hc::Reader<Ping>::Create(topic, [&](const Ping&) {
        ++calls;
        throw std::runtime_error("测试异常");
    });
    auto writer = hc::Writer<Ping>::Create(topic);
    if (reader == nullptr || writer == nullptr)
    {
        return false;
    }
    const auto start = Clock::now();
    while (calls < 3 && ElapsedMs(start) < kChildTimeoutMs)
    {
        writer->Publish(Ping());
        Sleep(20);
    }
    // 回调抛出异常后接收线程继续工作
    return calls >= 3;
}

bool TestLogHandler()
{
    std::vector<std::string> logs;
    hc::SetLogHandler([&](hc::LogLevel level, const std::string& text) {
        if (level == hc::LogLevel::ERR)
        {
            logs.push_back(text);
        }
    });
    const bool created = hc::Writer<Ping>::Create("Bad") != nullptr;
    hc::SetLogHandler(nullptr);
    return !created && logs.size() == 1;
}

struct TestCase
{
    const char* name;
    bool (*run)();
};

int RunAll()
{
    const std::vector<TestCase> cases = {
        {"共享内存发布订阅（Publish）", TestShmPublish},
        {"共享内存发布订阅（Loan/Send）", TestShmLoan},
        {"Protobuf 发布订阅", TestProtoPubSub},
        {"请求应答（超时、异常、重启恢复）", TestRequestResponse},
        {"每个 topic 只允许一个发布方", TestSinglePublisher},
        {"收发双方类型不一致", TestTypeMismatch},
        {"topic 校验", TestInvalidTopic},
        {"进程崩溃后重新创建", TestCrashRecovery},
        {"销毁时线程及时退出", TestStopLatency},
        {"回调抛出异常", TestCallbackException},
        {"日志回调", TestLogHandler},
    };
    int failed = 0;
    for (const auto& test : cases)
    {
        const auto start = Clock::now();
        const bool ok = test.run();
        std::cout << (ok ? "[PASS] " : "[FAIL] ") << test.name << "（" << ElapsedMs(start) << " ms）" << std::endl;
        failed += ok ? 0 : 1;
    }
    std::cout << "通过 " << cases.size() - failed << " / " << cases.size() << std::endl;
    return failed == 0 ? kExitOk : kExitFail;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        return RunAll();
    }
    const std::string role = argv[1];
    if (role == "shm_reader" && argc == 3)
    {
        return RoleShmReader(argv[2]);
    }
    if (role == "proto_reader" && argc == 3)
    {
        return RoleProtoReader(argv[2]);
    }
    if (role == "server" && argc == 3)
    {
        return RoleServer(argv[2]);
    }
    if (role == "expect_fail" && argc == 4)
    {
        return RoleExpectCreateFail(argv[2], argv[3]);
    }
    if (role == "crash" && argc == 5)
    {
        return RoleCrash(argv[2], argv[3], argv[4]);
    }
    std::cerr << "未知参数" << std::endl;
    return kExitFail;
}
