#include "hybrid_comm/detail/msg_channel.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

#include "zmq.hpp"

namespace snow::hybrid_comm::detail
{
namespace
{
// 本机通信路径：<根目录><类别>/<topic>.sock，topic 中的 '/' 保留为子目录
constexpr const char* kMsgRootPath = "/tmp/snow/hybrid_comm/zmq/";
constexpr const char* kPublishKind = "pub";
constexpr const char* kServiceKind = "rpc";
constexpr int kPollTimeoutMs = 100; ///< 接收线程的等待超时，决定退出响应时间

/// @brief 进程内唯一的 ZeroMQ 上下文；有意不析构，避免进程退出时等待未关闭的 socket
zmq::context_t& Context()
{
    static auto* context = new zmq::context_t();
    return *context;
}

std::string SocketPath(const char* kind, const std::string& topic)
{
    return std::string(kMsgRootPath) + kind + "/" + topic + ".sock";
}

std::string Endpoint(const std::string& socket_path)
{
    return "ipc://" + socket_path;
}

bool CreateParentDirectory(const std::string& path, const std::string& topic, std::string* error)
{
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    if (ec)
    {
        ReportError(error, "[" + topic + "] 创建通信目录失败: " + path + ": " + ec.message());
        return false;
    }
    return true;
}

/// @brief topic 独占锁：保证同一 topic 只有一个发布方或应答方，进程退出时由系统释放
class TopicLock
{
  public:
    static std::optional<TopicLock>
    Acquire(const std::string& socket_path, const std::string& topic, std::string* error)
    {
        const std::string lock_path = socket_path + ".lock";
        const int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0)
        {
            ReportError(error, "[" + topic + "] 打开锁文件失败: " + lock_path + ": " + std::strerror(errno));
            return std::nullopt;
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0)
        {
            const int lock_errno = errno;
            ::close(fd);
            if (lock_errno == EWOULDBLOCK)
            {
                ReportError(error, "[" + topic + "] 该 topic 已有发布方或应答方");
            }
            else
            {
                ReportError(error, "[" + topic + "] 加锁失败: " + std::strerror(lock_errno));
            }
            return std::nullopt;
        }
        return TopicLock(fd);
    }

    TopicLock(TopicLock&& other) noexcept
        : fd_(std::exchange(other.fd_, -1))
    {
    }
    TopicLock& operator=(TopicLock&&) = delete;
    TopicLock(const TopicLock&) = delete;
    TopicLock& operator=(const TopicLock&) = delete;

    ~TopicLock()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
    }

  private:
    explicit TopicLock(int fd)
        : fd_(fd)
    {
    }

    int fd_;
};

/// @brief 创建 socket：关闭时不等待未发出的消息，接收超时用于定期检查退出标志
std::optional<zmq::socket_t> MakeSocket(zmq::socket_type type, const std::string& topic, std::string* error)
{
    try
    {
        zmq::socket_t socket(Context(), type);
        socket.set(zmq::sockopt::linger, 0);
        return socket;
    }
    catch (const zmq::error_t& e)
    {
        ReportError(error, "[" + topic + "] 创建 socket 失败: " + e.what());
        return std::nullopt;
    }
}

bool Bind(zmq::socket_t& socket, const std::string& socket_path, const std::string& topic, std::string* error)
{
    try
    {
        socket.bind(Endpoint(socket_path));
        return true;
    }
    catch (const zmq::error_t& e)
    {
        ReportError(error, "[" + topic + "] 绑定失败: " + socket_path + ": " + e.what());
        return false;
    }
}

bool Connect(zmq::socket_t& socket, const std::string& socket_path, const std::string& topic, std::string* error)
{
    try
    {
        socket.connect(Endpoint(socket_path));
        return true;
    }
    catch (const zmq::error_t& e)
    {
        ReportError(error, "[" + topic + "] 连接失败: " + socket_path + ": " + e.what());
        return false;
    }
}

/// @brief 带超时接收一帧；超时或被信号中断返回 false
bool ReceiveFrame(zmq::socket_t& socket, zmq::message_t& frame, const std::string& topic)
{
    try
    {
        return socket.recv(frame, zmq::recv_flags::none).has_value();
    }
    catch (const zmq::error_t& e)
    {
        if (e.num() != EINTR)
        {
            Log(LogLevel::ERR, "[" + topic + "] 接收失败: " + e.what());
        }
        return false;
    }
}

/// @brief 丢弃同一条消息中剩余的帧
void DiscardRemainingFrames(zmq::socket_t& socket)
{
    while (socket.get(zmq::sockopt::rcvmore))
    {
        zmq::message_t frame;
        if (!socket.recv(frame, zmq::recv_flags::none).has_value())
        {
            return;
        }
    }
}

} // namespace

// ============================ MsgPublisher ============================

struct MsgPublisher::Impl
{
    std::string topic;
    TopicLock lock;
    zmq::socket_t socket;
    std::mutex mutex;
};

std::unique_ptr<MsgPublisher> MsgPublisher::Create(const std::string& topic, std::string* error)
{
    const std::string path = SocketPath(kPublishKind, topic);
    if (!CreateParentDirectory(path, topic, error))
    {
        return nullptr;
    }
    auto lock = TopicLock::Acquire(path, topic, error);
    if (!lock.has_value())
    {
        return nullptr;
    }
    auto socket = MakeSocket(zmq::socket_type::pub, topic, error);
    if (!socket.has_value() || !Bind(*socket, path, topic, error))
    {
        return nullptr;
    }
    return std::unique_ptr<MsgPublisher>(
        new MsgPublisher(std::unique_ptr<Impl>(new Impl{topic, std::move(*lock), std::move(*socket), {}})));
}

MsgPublisher::MsgPublisher(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

MsgPublisher::~MsgPublisher() = default;

bool MsgPublisher::Send(const std::string& payload)
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    try
    {
        // PUB 在订阅方缓存满时丢弃消息，不会阻塞
        return impl_->socket.send(zmq::buffer(payload), zmq::send_flags::none).has_value();
    }
    catch (const zmq::error_t& e)
    {
        Log(LogLevel::ERR, "[" + impl_->topic + "] 发送失败: " + e.what());
        return false;
    }
}

// ============================ MsgSubscriber ============================

struct MsgSubscriber::Impl
{
    std::string topic;
    zmq::socket_t socket;
    BytesHandler handler;
    std::atomic<bool> stop{false};
    std::thread worker;

    void Run()
    {
        while (!stop.load())
        {
            zmq::message_t frame;
            if (!ReceiveFrame(socket, frame, topic))
            {
                continue;
            }
            InvokeGuarded(topic, [&] { handler(frame.data(), frame.size()); });
        }
    }
};

std::unique_ptr<MsgSubscriber> MsgSubscriber::Create(const std::string& topic, BytesHandler handler, std::string* error)
{
    const std::string path = SocketPath(kPublishKind, topic);
    if (!CreateParentDirectory(path, topic, error))
    {
        return nullptr;
    }
    auto socket = MakeSocket(zmq::socket_type::sub, topic, error);
    if (!socket.has_value())
    {
        return nullptr;
    }
    socket->set(zmq::sockopt::rcvtimeo, kPollTimeoutMs);
    socket->set(zmq::sockopt::subscribe, "");
    // 发布方尚未启动时 ZeroMQ 在后台自动重连
    if (!Connect(*socket, path, topic, error))
    {
        return nullptr;
    }
    auto impl = std::unique_ptr<Impl>(new Impl{topic, std::move(*socket), std::move(handler), {false}, {}});
    // socket 创建后只在工作线程中使用
    impl->worker = std::thread(&Impl::Run, impl.get());
    return std::unique_ptr<MsgSubscriber>(new MsgSubscriber(std::move(impl)));
}

MsgSubscriber::MsgSubscriber(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

MsgSubscriber::~MsgSubscriber()
{
    StopWorker(impl_->stop, impl_->worker);
}

// ============================ MsgServer ============================

// 应答由两帧组成：第一帧为 1 字节 CallStatus，第二帧为应答内容

struct MsgServer::Impl
{
    std::string topic;
    TopicLock lock;
    zmq::socket_t socket;
    ReplyHandler handler;
    std::atomic<bool> stop{false};
    std::thread worker;

    void Run()
    {
        while (!stop.load())
        {
            zmq::message_t request;
            if (!ReceiveFrame(socket, request, topic))
            {
                continue;
            }
            DiscardRemainingFrames(socket);
            std::string reply;
            CallStatus status = CallStatus::SERVER_ERROR;
            if (!InvokeGuarded(topic, [&] { status = handler(request.data(), request.size(), reply); }))
            {
                status = CallStatus::SERVER_ERROR;
            }
            if (status != CallStatus::OK)
            {
                reply.clear();
            }
            SendReply(status, reply);
        }
    }

    void SendReply(CallStatus status, const std::string& reply)
    {
        const auto status_byte = static_cast<uint8_t>(status);
        try
        {
            socket.send(zmq::buffer(&status_byte, sizeof(status_byte)), zmq::send_flags::sndmore);
            socket.send(zmq::buffer(reply), zmq::send_flags::none);
        }
        catch (const zmq::error_t& e)
        {
            Log(LogLevel::ERR, "[" + topic + "] 发送应答失败: " + e.what());
        }
    }
};

std::unique_ptr<MsgServer> MsgServer::Create(const std::string& topic, ReplyHandler handler, std::string* error)
{
    const std::string path = SocketPath(kServiceKind, topic);
    if (!CreateParentDirectory(path, topic, error))
    {
        return nullptr;
    }
    auto lock = TopicLock::Acquire(path, topic, error);
    if (!lock.has_value())
    {
        return nullptr;
    }
    auto socket = MakeSocket(zmq::socket_type::rep, topic, error);
    if (!socket.has_value())
    {
        return nullptr;
    }
    socket->set(zmq::sockopt::rcvtimeo, kPollTimeoutMs);
    if (!Bind(*socket, path, topic, error))
    {
        return nullptr;
    }
    auto impl =
        std::unique_ptr<Impl>(new Impl{topic, std::move(*lock), std::move(*socket), std::move(handler), {false}, {}});
    // socket 创建后只在工作线程中使用
    impl->worker = std::thread(&Impl::Run, impl.get());
    return std::unique_ptr<MsgServer>(new MsgServer(std::move(impl)));
}

MsgServer::MsgServer(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

MsgServer::~MsgServer()
{
    StopWorker(impl_->stop, impl_->worker);
}

// ============================ MsgClient ============================

struct MsgClient::Impl
{
    std::string topic;
    std::string path;
    std::optional<zmq::socket_t> socket;
    std::mutex mutex;

    /// @brief REQ 在一次请求未完成时不能再发送，因此超时或出错后丢弃并重建
    bool EnsureSocket()
    {
        if (socket.has_value())
        {
            return true;
        }
        auto created = MakeSocket(zmq::socket_type::req, topic, nullptr);
        if (!created.has_value() || !Connect(*created, path, topic, nullptr))
        {
            return false;
        }
        socket.emplace(std::move(*created));
        return true;
    }

    CallStatus Fail(CallStatus status)
    {
        socket.reset();
        return status;
    }
};

std::unique_ptr<MsgClient> MsgClient::Create(const std::string& topic, std::string* error)
{
    const std::string path = SocketPath(kServiceKind, topic);
    if (!CreateParentDirectory(path, topic, error))
    {
        return nullptr;
    }
    auto impl = std::unique_ptr<Impl>(new Impl{topic, path, std::nullopt, {}});
    auto socket = MakeSocket(zmq::socket_type::req, topic, error);
    if (!socket.has_value() || !Connect(*socket, path, topic, error))
    {
        return nullptr;
    }
    impl->socket.emplace(std::move(*socket));
    return std::unique_ptr<MsgClient>(new MsgClient(std::move(impl)));
}

MsgClient::MsgClient(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

MsgClient::~MsgClient() = default;

CallStatus MsgClient::Call(const std::string& request, std::string& reply, int timeout_ms)
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->EnsureSocket())
    {
        return CallStatus::SEND_FAILED;
    }
    zmq::socket_t& socket = *impl_->socket;
    try
    {
        socket.set(zmq::sockopt::sndtimeo, timeout_ms);
        socket.set(zmq::sockopt::rcvtimeo, timeout_ms);
        if (!socket.send(zmq::buffer(request), zmq::send_flags::none).has_value())
        {
            return impl_->Fail(CallStatus::SEND_FAILED);
        }
        zmq::message_t status_frame;
        if (!socket.recv(status_frame, zmq::recv_flags::none).has_value())
        {
            return impl_->Fail(CallStatus::TIMEOUT);
        }
        if (status_frame.size() != 1 || !socket.get(zmq::sockopt::rcvmore))
        {
            return impl_->Fail(CallStatus::PARSE_FAILED);
        }
        zmq::message_t body_frame;
        if (!socket.recv(body_frame, zmq::recv_flags::none).has_value())
        {
            return impl_->Fail(CallStatus::TIMEOUT);
        }
        DiscardRemainingFrames(socket);
        const uint8_t status_byte = *status_frame.data<uint8_t>();
        if (status_byte > static_cast<uint8_t>(CallStatus::SERVER_ERROR))
        {
            return CallStatus::PARSE_FAILED;
        }
        const auto status = static_cast<CallStatus>(status_byte);
        if (status != CallStatus::OK)
        {
            return status;
        }
        reply.assign(body_frame.data<char>(), body_frame.size());
        return CallStatus::OK;
    }
    catch (const zmq::error_t& e)
    {
        Log(LogLevel::ERR, "[" + impl_->topic + "] 请求失败: " + e.what());
        return impl_->Fail(CallStatus::SEND_FAILED);
    }
}

} // namespace snow::hybrid_comm::detail
