#ifndef SNOW_HYBRID_COMM_READER_H
#define SNOW_HYBRID_COMM_READER_H

/**
 * @file reader.h
 * @brief 订阅方：共享内存结构体走 iceoryx2，Protobuf 消息走 ZeroMQ，由消息类型在编译期决定。
 */

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "hybrid_comm/common.h"
#include "hybrid_comm/detail/proto_channel.h"
#include "hybrid_comm/detail/shm_channel.h"

namespace snow::hybrid_comm
{
/**
 * @brief 订阅方，订阅方数量不限（共享内存每个 topic 最多 detail::kShmMaxSubscribers 个）。
 * @details 内部有一个接收线程，回调在该线程中按到达顺序逐条执行。
 *          共享内存结构体的回调参数直接指向共享内存，只在回调期间有效。
 *          订阅方处理不及时时丢弃最旧的数据；订阅方创建之前发布的数据收不到。
 * @note 禁止在回调中销毁所属的 Reader。
 */
template <typename T>
class Reader
{
    static_assert(detail::kIsShmMessage<T> || detail::kIsProtoMessage<T>,
                  "消息类型必须是 Protobuf 消息，或定义了 IOX2_TYPE_NAME 且可按字节拷贝的结构体");

    using Impl = std::conditional_t<detail::kIsShmMessage<T>, detail::ShmReader<T>, detail::ProtoReader<T>>;

  public:
    using Handler = std::function<void(const T&)>;

    /**
     * @brief 创建订阅方并启动接收线程。
     * @param error 非空时写入失败原因，为空时失败原因写日志
     * @return 失败返回空指针：topic 不合法、回调为空、收发双方类型不一致、底层资源创建失败
     */
    static std::unique_ptr<Reader> Create(const std::string& topic, Handler handler, std::string* error = nullptr)
    {
        if (!IsValidTopic(topic))
        {
            detail::ReportError(error, "topic 不合法: \"" + topic + "\"");
            return nullptr;
        }
        if (!handler)
        {
            detail::ReportError(error, "[" + topic + "] 回调为空");
            return nullptr;
        }
        auto impl = Impl::Create(topic, std::move(handler), error);
        if (impl == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<Reader>(new Reader(std::move(impl)));
    }

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

  private:
    explicit Reader(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl))
    {
    }

    std::unique_ptr<Impl> impl_;
};

} // namespace snow::hybrid_comm

#endif // SNOW_HYBRID_COMM_READER_H
