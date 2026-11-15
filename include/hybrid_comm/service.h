#ifndef SNOW_HYBRID_COMM_SERVICE_H
#define SNOW_HYBRID_COMM_SERVICE_H

/**
 * @file service.h
 * @brief 请求应答：请求与应答均为 Protobuf 消息，走 ZeroMQ。
 */

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "hybrid_comm/common.h"
#include "hybrid_comm/detail/msg_channel.h"
#include "hybrid_comm/detail/proto_channel.h"

namespace snow::hybrid_comm
{
/**
 * @brief 应答方，每个 topic 只允许一个。
 * @details 内部有一个接收线程，请求在该线程中逐个处理；处理函数抛出异常时请求方得到 CallStatus::SERVER_ERROR。
 * @note 禁止在处理函数中销毁所属的 Server。
 */
template <typename Request, typename Response>
class Server
{
    static_assert(detail::kIsProtoMessage<Request> && detail::kIsProtoMessage<Response>,
                  "请求与应答必须是 Protobuf 消息");

  public:
    using Handler = std::function<void(const Request&, Response&)>;

    /**
     * @brief 创建应答方并启动接收线程。
     * @param error 非空时写入失败原因，为空时失败原因写日志
     * @return 失败返回空指针：topic 不合法、处理函数为空、该 topic 已有应答方、底层资源创建失败
     */
    static std::unique_ptr<Server> Create(const std::string& topic, Handler handler, std::string* error = nullptr)
    {
        if (!IsValidTopic(topic))
        {
            detail::ReportError(error, "topic 不合法: \"" + topic + "\"");
            return nullptr;
        }
        if (!handler)
        {
            detail::ReportError(error, "[" + topic + "] 处理函数为空");
            return nullptr;
        }
        auto on_request = [handler = std::move(handler)](const void* data, std::size_t size, std::string& reply) {
            Request request;
            if (!detail::ParseProto(data, size, request))
            {
                return CallStatus::PARSE_FAILED;
            }
            Response response;
            handler(request, response);
            if (!response.SerializeToString(&reply))
            {
                return CallStatus::SERVER_ERROR;
            }
            return CallStatus::OK;
        };
        auto server = detail::MsgServer::Create(topic, std::move(on_request), error);
        if (server == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<Server>(new Server(std::move(server)));
    }

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

  private:
    explicit Server(std::unique_ptr<detail::MsgServer> server)
        : server_(std::move(server))
    {
    }

    std::unique_ptr<detail::MsgServer> server_;
};

/**
 * @brief 请求方，同一 topic 的请求方数量不限。
 * @note Call 线程安全，同一 Client 同一时间只处理一个请求；超时后内部重建连接，下次调用照常可用。
 */
template <typename Request, typename Response>
class Client
{
    static_assert(detail::kIsProtoMessage<Request> && detail::kIsProtoMessage<Response>,
                  "请求与应答必须是 Protobuf 消息");

  public:
    /**
     * @brief 创建请求方；不要求应答方已启动。
     * @param error 非空时写入失败原因，为空时失败原因写日志
     * @return 失败返回空指针：topic 不合法、底层资源创建失败
     */
    static std::unique_ptr<Client> Create(const std::string& topic, std::string* error = nullptr)
    {
        if (!IsValidTopic(topic))
        {
            detail::ReportError(error, "topic 不合法: \"" + topic + "\"");
            return nullptr;
        }
        auto client = detail::MsgClient::Create(topic, error);
        if (client == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<Client>(new Client(std::move(client)));
    }

    /**
     * @brief 发送请求并等待应答。
     * @param timeout_ms 超时，单位 ms；应答方不存在时同样在超时后返回 CallStatus::TIMEOUT
     */
    CallStatus Call(const Request& request, Response& response, int timeout_ms)
    {
        std::string payload;
        if (!request.SerializeToString(&payload))
        {
            return CallStatus::SEND_FAILED;
        }
        std::string reply;
        const CallStatus status = client_->Call(payload, reply, timeout_ms);
        if (status != CallStatus::OK)
        {
            return status;
        }
        if (!detail::ParseProto(reply.data(), reply.size(), response))
        {
            return CallStatus::PARSE_FAILED;
        }
        return CallStatus::OK;
    }

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

  private:
    explicit Client(std::unique_ptr<detail::MsgClient> client)
        : client_(std::move(client))
    {
    }

    std::unique_ptr<detail::MsgClient> client_;
};

} // namespace snow::hybrid_comm

#endif // SNOW_HYBRID_COMM_SERVICE_H
