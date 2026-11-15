#ifndef SNOW_HYBRID_COMM_PROTO_CHANNEL_H
#define SNOW_HYBRID_COMM_PROTO_CHANNEL_H

/**
 * @file proto_channel.h
 * @brief Protobuf 消息与 ZeroMQ 字节通道之间的转换：发送前序列化，收到后解析。
 */

#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "hybrid_comm/common.h"
#include "hybrid_comm/detail/msg_channel.h"

namespace snow::hybrid_comm::detail
{
/// @brief 按字节解析 Protobuf 消息
template <typename T>
bool ParseProto(const void* data, std::size_t size, T& message)
{
    if (size > static_cast<std::size_t>(std::numeric_limits<int>::max()))
    {
        return false;
    }
    return message.ParseFromArray(data, static_cast<int>(size));
}

/// @brief Protobuf 发布方
template <typename T>
class ProtoWriter
{
  public:
    static std::unique_ptr<ProtoWriter> Create(const std::string& topic, std::string* error)
    {
        auto publisher = MsgPublisher::Create(topic, error);
        if (publisher == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<ProtoWriter>(new ProtoWriter(topic, std::move(publisher)));
    }

    bool Publish(const T& message)
    {
        std::string payload;
        if (!message.SerializeToString(&payload))
        {
            Log(LogLevel::ERR, "[" + topic_ + "] 消息序列化失败");
            return false;
        }
        return publisher_->Send(payload);
    }

  private:
    ProtoWriter(std::string topic, std::unique_ptr<MsgPublisher> publisher)
        : topic_(std::move(topic))
        , publisher_(std::move(publisher))
    {
    }

    std::string topic_;
    std::unique_ptr<MsgPublisher> publisher_;
};

/// @brief Protobuf 订阅方，在内部线程中回调
template <typename T>
class ProtoReader
{
  public:
    using Handler = std::function<void(const T&)>;

    static std::unique_ptr<ProtoReader> Create(const std::string& topic, Handler handler, std::string* error)
    {
        auto on_bytes = [topic, handler = std::move(handler)](const void* data, std::size_t size) {
            T message;
            if (!ParseProto(data, size, message))
            {
                Log(LogLevel::WARN, "[" + topic + "] 消息解析失败，已丢弃");
                return;
            }
            handler(message);
        };
        auto subscriber = MsgSubscriber::Create(topic, std::move(on_bytes), error);
        if (subscriber == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<ProtoReader>(new ProtoReader(std::move(subscriber)));
    }

  private:
    explicit ProtoReader(std::unique_ptr<MsgSubscriber> subscriber)
        : subscriber_(std::move(subscriber))
    {
    }

    std::unique_ptr<MsgSubscriber> subscriber_;
};

} // namespace snow::hybrid_comm::detail

#endif // SNOW_HYBRID_COMM_PROTO_CHANNEL_H
