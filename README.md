# hybrid_comm

hybrid_comm 是供 C++ 程序链接使用的本机进程间通信库，由头文件和 `.so` 动态库组成，安装后无需单独启动任何进程。

## 功能

| 用法 | 消息类型 | 底层 |
| --- | --- | --- |
| 发布订阅 | `shared_msg` 中的 C++ 结构体（定义了 `IOX2_TYPE_NAME` 且可按字节拷贝） | iceoryx2 共享内存，零拷贝 |
| 发布订阅 | Protobuf 消息 | ZeroMQ |
| 请求应答 | Protobuf 消息 | ZeroMQ |

底层通道由消息类型在编译期自动选择，两者都不是的类型编译报错；当前只支持本机通信。

## 依赖

| 库 | 版本 | 许可证 | 位置 |
| --- | --- | --- | --- |
| iceoryx2 | 随包提供，版本号未标注；通信双方必须使用同一份 | Apache-2.0 或 MIT | `third_party/x86_64/iceoryx2/` |
| libzmq | 4.3.2 | LGPL-3.0（附静态链接例外条款），以动态库方式使用 | `third_party/x86_64/zeromq/` |
| cppzmq | 4.9.0（仅头文件） | MIT | `third_party/x86_64/zeromq/include/` |
| Protobuf | 系统包，当前在 3.6.1 上验证 | BSD-3-Clause | `sudo apt install libprotobuf-dev protobuf-compiler` |

- iceoryx2 与 libzmq 的 `.so` 随发布包安装到 `lib/`，目标机器无需另装；Protobuf 必须在目标机器安装，且与编译时版本一致。
- 更换架构（如 `aarch64`）时，需为该架构准备同版本的第三方库，放到 `third_party/<架构>/`，并新增 `cmake/arch/<架构>.cmake`。

## 编译、测试与打包

```bash
bash scripts/build_tar.sh                                     # 编译到 build/
bash scripts/build_tar.sh --clean                             # 清理后重新编译
bash scripts/build_tar.sh --package -DCMAKE_BUILD_TYPE=Release   # 编译并生成发布包
cd build && ctest --output-on-failure                         # 运行测试
```

发布包为 `build/hybrid_comm-<版本>-<架构>-<时间>.tar.gz`，版本取自 `VERSION`。

| 目录 | 内容 |
| --- | --- |
| `include/` | hybrid_comm 头文件与 iceoryx2 头文件 |
| `lib/` | `libhybrid_comm.so` 与 iceoryx2、libzmq 的 `.so` |
| `lib/cmake/hybrid_comm/` | 供 `find_package` 使用的 CMake 配置 |

`tests/` 下的测试程序以子进程扮演通信另一方，共 11 个用例，覆盖：共享内存发布订阅（拷贝与零拷贝两种写法，4 MB 结构体）、Protobuf 发布订阅、请求应答（超时、处理函数异常、应答方重启后恢复）、单一发布方限制、收发类型不一致、topic 校验、进程崩溃后重新创建、销毁时线程退出、回调异常、日志回调。测试使用 `tests/iceoryx2.toml` 指定独立的 iceoryx2 资源目录，与本机其他 iceoryx2 程序隔离。

可用 `-DBUILD_TESTS=OFF` 关闭测试编译。

## 安装

```bash
hybrid_pk install build/hybrid_comm-<...>.tar.gz
```

安装后解压到 `/opt/snow/hybrid_packages/<包名>/`，软链接 `/opt/snow/hybrid/hybrid_comm` 指向当前版本。

## 在项目中使用

```cmake
find_package(hybrid_comm REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE hybrid_comm::hybrid_comm)
```

配置时指定 `-DCMAKE_PREFIX_PATH=/opt/snow/hybrid/hybrid_comm`；同时使用 `shared_msg` 时，将两者的安装路径用分号分隔，一起写入 `CMAKE_PREFIX_PATH`。

`hybrid_comm::hybrid_comm` 自动带上头文件路径、iceoryx2 与 Protobuf 库，并给引用方的链接加上 `--disable-new-dtags`，使用 RPATH 而非 RUNPATH。原因是 RUNPATH 对间接依赖无效，`libiceoryx2_cxx.so` 依赖的 `libiceoryx2_ffi_c.so` 会找不到。程序安装后运行时，其 `INSTALL_RPATH` 必须包含 `/opt/snow/hybrid/hybrid_comm/lib`。

项目只包含 `#include "hybrid_comm/hybrid_comm.h"`，禁止直接包含 `detail/` 下的头文件以及 iceoryx2、ZeroMQ 的头文件。命名空间为 `snow::hybrid_comm`，下文示例使用别名 `namespace hc = snow::hybrid_comm;`。请求应答示例中的 `DetectWorkpieceRequest`、`DetectWorkpieceResponse` 为 shared_msg 中 `vision/vision.proto` 定义的消息，位于命名空间 `snow::vision`。

## 接口

### 发布

`Writer<T>::Create(topic, &error)` 失败时返回空指针，原因写入 `error`；`error` 为空指针时写日志。`Publish(const T&)` 对结构体与 Protobuf 消息都可用，结构体会拷贝一次。

`Loan()` / `Send()` 只用于结构体，实现零拷贝。`Loan()` 返回的结构体成员按结构体定义默认初始化，不整体清零；借出的内存不发送时，丢弃即归还。各成员函数线程安全。

```cpp
std::string error;
auto writer = hc::Writer<snow::msg::ImageSDType>::Create("camera/image", &error);
if (writer == nullptr)
{
    // 失败原因见 error
}

auto sample = writer->Loan();               // 借出共享内存
if (sample.has_value())
{
    (*sample)->width = 640;                 // 原地填写
    (*sample)->height = 480;
    writer->Send(std::move(*sample));       // 发送，不拷贝数据
}
```

### 订阅

使用 `Reader<T>::Create(topic, 回调, &error)` 创建订阅方。每个 `Reader` 有一个内部接收线程，回调在该线程中按到达顺序逐条执行，不同 `Reader` 的回调可能并发。结构体回调参数直接指向共享内存，只在回调期间有效，需要保留时拷出。回调抛出的异常被捕获并写日志，接收线程继续工作。

```cpp
auto reader = hc::Reader<snow::msg::ImageSDType>::Create(
    "camera/image",
    [](const snow::msg::ImageSDType& image) {
        // 在接收线程中执行；image 指向共享内存，只在回调期间有效
    },
    &error);
```

### 请求应答

`Server<Req, Rsp>` 每个 topic 只允许一个，请求在其内部线程中逐个处理。`Client<Req, Rsp>` 不要求应答方已启动，`Call` 线程安全，同一 `Client` 同一时间只处理一个请求；超时后内部重建连接，下次调用照常可用。

`CallStatus` 取值如下：

| 值 | 含义 |
| --- | --- |
| `OK` | 成功 |
| `TIMEOUT` | 超时未收到应答（含应答方不存在） |
| `SEND_FAILED` | 请求发送失败 |
| `PARSE_FAILED` | 请求或应答无法解析 |
| `SERVER_ERROR` | 应答方处理函数抛出异常 |

```cpp
auto server = hc::Server<DetectWorkpieceRequest, DetectWorkpieceResponse>::Create(
    "vision/detect",
    [](const DetectWorkpieceRequest& request, DetectWorkpieceResponse& response) {
        // 处理 request，填写 response
    },
    &error);

auto client = hc::Client<DetectWorkpieceRequest, DetectWorkpieceResponse>::Create("vision/detect", &error);
DetectWorkpieceRequest request;
request.set_task_id(1);
DetectWorkpieceResponse response;
if (client->Call(request, response, 3000) == hc::CallStatus::OK) {   // 超时 3000 ms
    // 使用 response
}
```

### 日志

hybrid_comm 默认把日志写到 `stderr`；用 `SetLogHandler` 接到项目日志模块。回调可能在内部线程中调用，传入空回调恢复默认。

```cpp
hc::SetLogHandler([](hc::LogLevel level, const std::string& text) {
    // 转到项目日志模块，例如 LOG_STREAM(comm, err) << text;
});
```

## 行为约定

- topic 由项目自己定义，由 `/` 分隔的若干段组成，每段只含小写字母、数字、下划线，总长不超过 64 个字符，例如 `camera/image`；不合法时 `Create` 失败，可用 `IsValidTopic` 预先检查。
- 每个 topic 只允许一个发布方（`Writer`）或应答方（`Server`），第二个在 `Create` 时失败；进程退出或崩溃后，可重新创建。订阅方、请求方数量不限，共享内存每个 topic 的订阅方最多 4 个。
- 同一 topic 的收发双方必须使用同一消息类型；结构体类型不一致时 `Reader` / `Writer` 创建失败。修改结构体布局后，所有收发方必须一起重新编译。
- 订阅方处理不及时时丢弃最旧的数据（共享内存每个订阅方缓存 2 条）；订阅方创建之前发布的数据收不到。必须送达的数据使用请求应答。
- 销毁 `Reader` / `Server` 时等待其接收线程退出，耗时不超过 100 ms；禁止在回调或处理函数中销毁所属对象（会终止进程）。
- 所有接口不抛出异常。

## 运行环境

- 共享内存资源使用 iceoryx2 默认目录 `/tmp/iceoryx2` 与 `/dev/shm`；通信双方必须以同一系统用户运行。
- 本机已有其他用户运行的 iceoryx2 程序占用 `/tmp/iceoryx2` 时，用 iceoryx2 配置文件指定其他目录。iceoryx2 依次查找：工作目录下的 `config/iceoryx2.toml`、用户配置目录下的 `iceoryx2/iceoryx2.toml`、`/etc/iceoryx2/iceoryx2.toml`。示例：

  ```toml
  [global]
  root-path = "/tmp/snow/iceoryx2/"
  prefix = "snow_"
  ```

  通信双方必须使用同一配置。不在代码中为节点单独指定配置，原因：当前 iceoryx2 版本只有使用全局配置时，才能正确清理崩溃进程遗留的资源。

- `/dev/shm` 容量须大于各 topic 结构体大小乘以缓存条数之和。
- ZeroMQ 通信文件位于 `/tmp/snow/hybrid_comm/zmq/`，由 hybrid_comm 按 topic 自动生成，无需配置。
- hybrid_comm 不注册信号处理，进程的信号由项目自行处理。
- 调试 iceoryx2 时可设置环境变量 `IOX2_LOG_LEVEL`（如 `Debug`），默认只输出错误。
