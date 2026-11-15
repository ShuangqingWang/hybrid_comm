#ifndef SNOW_HYBRID_COMM_MSG_CHANNEL_H
#define SNOW_HYBRID_COMM_MSG_CHANNEL_H

/**
 * @file msg_channel.h
 * @brief ZeroMQ 通道：按字节收发，不依赖消息类型。ZeroMQ 只在实现文件中使用，不对外暴露。
 */

#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include "hybrid_comm/common.h"

namespace snow::hybrid_comm::detail
{
/// @brief 收到一条消息时的回调
using BytesHandler = std::function<void(const void* data, std::size_t size)>;

/// @brief 应答处理回调：返回值为应答状态，reply 为应答内容
using ReplyHandler = std::function<CallStatus(const void* data, std::size_t size, std::string& reply)>;

/// @brief 发布方（PUB），每个 topic 只允许一个
class MsgPublisher
{
  public:
    static std::unique_ptr<MsgPublisher> Create(const std::string& topic, std::string* error);
    ~MsgPublisher();

    MsgPublisher(const MsgPublisher&) = delete;
    MsgPublisher& operator=(const MsgPublisher&) = delete;

    /// @brief 发送一条消息；线程安全
    bool Send(const std::string& payload);

  private:
    struct Impl;
    explicit MsgPublisher(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

/// @brief 订阅方（SUB），在内部线程中回调
class MsgSubscriber
{
  public:
    static std::unique_ptr<MsgSubscriber> Create(const std::string& topic, BytesHandler handler, std::string* error);
    ~MsgSubscriber();

    MsgSubscriber(const MsgSubscriber&) = delete;
    MsgSubscriber& operator=(const MsgSubscriber&) = delete;

  private:
    struct Impl;
    explicit MsgSubscriber(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

/// @brief 应答方（REP），每个 topic 只允许一个，在内部线程中回调
class MsgServer
{
  public:
    static std::unique_ptr<MsgServer> Create(const std::string& topic, ReplyHandler handler, std::string* error);
    ~MsgServer();

    MsgServer(const MsgServer&) = delete;
    MsgServer& operator=(const MsgServer&) = delete;

  private:
    struct Impl;
    explicit MsgServer(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

/// @brief 请求方（REQ）
class MsgClient
{
  public:
    static std::unique_ptr<MsgClient> Create(const std::string& topic, std::string* error);
    ~MsgClient();

    MsgClient(const MsgClient&) = delete;
    MsgClient& operator=(const MsgClient&) = delete;

    /**
     * @brief 发送请求并等待应答；线程安全，同一时间只处理一个请求。
     * @param timeout_ms 发送与等待应答的超时，单位 ms
     */
    CallStatus Call(const std::string& request, std::string& reply, int timeout_ms);

  private:
    struct Impl;
    explicit MsgClient(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

} // namespace snow::hybrid_comm::detail

#endif // SNOW_HYBRID_COMM_MSG_CHANNEL_H
