#ifndef SNOW_HYBRID_COMM_WRITER_H
#define SNOW_HYBRID_COMM_WRITER_H

/**
 * @file writer.h
 * @brief 发布方：共享内存结构体走 iceoryx2，Protobuf 消息走 ZeroMQ，由消息类型在编译期决定。
 */

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "hybrid_comm/common.h"
#include "hybrid_comm/detail/proto_channel.h"
#include "hybrid_comm/detail/shm_channel.h"

namespace snow::hybrid_comm
{
template <typename T>
class Writer;

/**
 * @brief 借出的共享内存，只用于共享内存结构体。
 * @details 通过 Writer::Loan 获得，原地填写后交给 Writer::Send 发送；不发送时析构即归还。
 */
template <typename T>
class LoanedSample
{
  public:
    T& operator*()
    {
        return sample_.payload_mut();
    }
    T* operator->()
    {
        return &sample_.payload_mut();
    }

  private:
    friend class Writer<T>;
    explicit LoanedSample(typename detail::ShmWriter<T>::Sample sample)
        : sample_(std::move(sample))
    {
    }

    typename detail::ShmWriter<T>::Sample sample_;
};

/**
 * @brief 发布方，每个 topic 只允许一个。
 * @note 各成员函数线程安全。
 */
template <typename T>
class Writer
{
    static_assert(detail::kIsShmMessage<T> || detail::kIsProtoMessage<T>,
                  "消息类型必须是 Protobuf 消息，或定义了 IOX2_TYPE_NAME 且可按字节拷贝的结构体");

    using Impl = std::conditional_t<detail::kIsShmMessage<T>, detail::ShmWriter<T>, detail::ProtoWriter<T>>;

  public:
    /**
     * @brief 创建发布方。
     * @param error 非空时写入失败原因，为空时失败原因写日志
     * @return 失败返回空指针：topic 不合法、该 topic 已有发布方、底层资源创建失败
     */
    static std::unique_ptr<Writer> Create(const std::string& topic, std::string* error = nullptr)
    {
        if (!IsValidTopic(topic))
        {
            detail::ReportError(error, "topic 不合法: \"" + topic + "\"");
            return nullptr;
        }
        auto impl = Impl::Create(topic, error);
        if (impl == nullptr)
        {
            return nullptr;
        }
        return std::unique_ptr<Writer>(new Writer(std::move(impl)));
    }

    /// @brief 发布一条消息；共享内存结构体会拷贝一次
    bool Publish(const T& message)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return impl_->Publish(message);
    }

    /// @brief 借出共享内存（零拷贝写法），只用于共享内存结构体；失败返回空
    template <typename U = T, std::enable_if_t<detail::kIsShmMessage<U>, int> = 0>
    std::optional<LoanedSample<U>> Loan()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto sample = impl_->Loan();
        if (!sample.has_value())
        {
            return std::nullopt;
        }
        return LoanedSample<U>(std::move(*sample));
    }

    /// @brief 发送借出的共享内存
    template <typename U = T, std::enable_if_t<detail::kIsShmMessage<U>, int> = 0>
    bool Send(LoanedSample<U>&& sample)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return impl_->Send(std::move(sample.sample_));
    }

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

  private:
    explicit Writer(std::unique_ptr<Impl> impl)
        : impl_(std::move(impl))
    {
    }

    std::mutex mutex_;
    std::unique_ptr<Impl> impl_;
};

} // namespace snow::hybrid_comm

#endif // SNOW_HYBRID_COMM_WRITER_H
