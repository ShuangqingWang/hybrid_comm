#ifndef SNOW_HYBRID_COMM_SHM_CHANNEL_H
#define SNOW_HYBRID_COMM_SHM_CHANNEL_H

/**
 * @file shm_channel.h
 * @brief iceoryx2 共享内存通道：发布方借出共享内存写入后发送，订阅方在内部线程中直接读取共享内存。
 *        每个 topic 对应一个发布订阅服务和一个同名事件服务，事件用于发送后唤醒订阅线程。
 */

#include <atomic>
#include <functional>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "hybrid_comm/common.h"
#include "hybrid_comm/detail/shm_common.h"

namespace snow::hybrid_comm::detail
{
template <typename T>
using ShmService = iox2::PortFactoryPublishSubscribe<iox2::ServiceType::Ipc, T, void>;
using ShmEvent = iox2::PortFactoryEvent<iox2::ServiceType::Ipc>;

/// @brief 打开或创建 topic 对应的发布订阅服务与事件服务
template <typename T>
bool OpenShmServices(ShmNode& node,
                     const std::string& topic,
                     std::optional<ShmService<T>>& service,
                     std::optional<ShmEvent>& event,
                     std::string* error)
{
    auto name = MakeServiceName(topic, error);
    if (!name.has_value())
    {
        return false;
    }
    auto opened = node.service_builder(*name)
                      .publish_subscribe<T>()
                      .max_publishers(kShmMaxPublishers)
                      .max_subscribers(kShmMaxSubscribers)
                      .subscriber_max_buffer_size(kShmBufferSize)
                      .subscriber_max_borrowed_samples(kShmMaxBorrowed)
                      .history_size(kShmHistorySize)
                      .enable_safe_overflow(true)
                      .open_or_create();
    if (!opened.has_value())
    {
        ReportError(error,
                    "[" + topic + "] 打开共享内存服务失败（错误码 " + std::to_string(static_cast<int>(opened.error()))
                        + "），常见原因：收发双方结构体类型不一致");
        return false;
    }
    auto opened_event = node.service_builder(*name)
                            .event()
                            .max_notifiers(kShmMaxPublishers)
                            .max_listeners(kShmMaxSubscribers)
                            .open_or_create();
    if (!opened_event.has_value())
    {
        ReportError(error,
                    "[" + topic + "] 打开共享内存事件服务失败（错误码 "
                        + std::to_string(static_cast<int>(opened_event.error())) + "）");
        return false;
    }
    service.emplace(std::move(opened.value()));
    event.emplace(std::move(opened_event.value()));
    return true;
}

/// @brief 共享内存发布方
template <typename T>
class ShmWriter
{
  public:
    using Sample = iox2::SampleMut<iox2::ServiceType::Ipc, T, void>;

    static std::unique_ptr<ShmWriter> Create(const std::string& topic, std::string* error)
    {
        std::unique_ptr<ShmWriter> writer(new ShmWriter(topic));
        auto node = CreateShmNode(error);
        if (!node.has_value())
        {
            return nullptr;
        }
        writer->node_.emplace(std::move(*node));
        if (!OpenShmServices<T>(*writer->node_, topic, writer->service_, writer->event_, error))
        {
            return nullptr;
        }
        auto publisher = writer->service_->publisher_builder().create();
        if (!publisher.has_value())
        {
            ReportError(error,
                        "[" + topic + "] 创建共享内存发布方失败（错误码 "
                            + std::to_string(static_cast<int>(publisher.error()))
                            + "），常见原因：该 topic 已有发布方");
            return nullptr;
        }
        writer->publisher_.emplace(std::move(publisher.value()));
        auto notifier = writer->event_->notifier_builder().create();
        if (!notifier.has_value())
        {
            ReportError(error,
                        "[" + topic + "] 创建共享内存通知方失败（错误码 "
                            + std::to_string(static_cast<int>(notifier.error())) + "）");
            return nullptr;
        }
        writer->notifier_.emplace(std::move(notifier.value()));
        return writer;
    }

    /// @brief 拷贝一次到共享内存后发送
    bool Publish(const T& message)
    {
        auto uninit = publisher_->loan_uninit();
        if (!uninit.has_value())
        {
            Log(LogLevel::ERR,
                "[" + topic_ + "] 借出共享内存失败（错误码 " + std::to_string(static_cast<int>(uninit.error())) + "）");
            return false;
        }
        return Send(uninit->write_payload(message));
    }

    /// @brief 借出共享内存，成员按结构体定义默认初始化，不整体清零
    std::optional<Sample> Loan()
    {
        auto uninit = publisher_->loan_uninit();
        if (!uninit.has_value())
        {
            Log(LogLevel::ERR,
                "[" + topic_ + "] 借出共享内存失败（错误码 " + std::to_string(static_cast<int>(uninit.error())) + "）");
            return std::nullopt;
        }
        new (&uninit->payload_mut()) T;
        return iox2::assume_init(std::move(*uninit));
    }

    /// @brief 发送已填写的共享内存，并唤醒订阅方
    bool Send(Sample&& sample)
    {
        auto sent = iox2::send(std::move(sample));
        if (!sent.has_value())
        {
            Log(LogLevel::ERR,
                "[" + topic_ + "] 发送失败（错误码 " + std::to_string(static_cast<int>(sent.error())) + "）");
            return false;
        }
        auto notified = notifier_->notify();
        if (!notified.has_value())
        {
            Log(LogLevel::WARN,
                "[" + topic_ + "] 唤醒订阅方失败（错误码 " + std::to_string(static_cast<int>(notified.error()))
                    + "），订阅方将在等待超时后收到");
        }
        return true;
    }

  private:
    explicit ShmWriter(std::string topic)
        : topic_(std::move(topic))
    {
    }

    std::string topic_;
    // 声明顺序即依赖顺序，析构时逆序释放
    std::optional<ShmNode> node_;
    std::optional<ShmService<T>> service_;
    std::optional<ShmEvent> event_;
    std::optional<iox2::Publisher<iox2::ServiceType::Ipc, T, void>> publisher_;
    std::optional<iox2::Notifier<iox2::ServiceType::Ipc>> notifier_;
};

/// @brief 共享内存订阅方，在内部线程中回调
template <typename T>
class ShmReader
{
  public:
    using Handler = std::function<void(const T&)>;

    static std::unique_ptr<ShmReader> Create(const std::string& topic, Handler handler, std::string* error)
    {
        std::unique_ptr<ShmReader> reader(new ShmReader(topic, std::move(handler)));
        auto node = CreateShmNode(error);
        if (!node.has_value())
        {
            return nullptr;
        }
        reader->node_.emplace(std::move(*node));
        if (!OpenShmServices<T>(*reader->node_, topic, reader->service_, reader->event_, error))
        {
            return nullptr;
        }
        auto subscriber = reader->service_->subscriber_builder().create();
        if (!subscriber.has_value())
        {
            ReportError(error,
                        "[" + topic + "] 创建共享内存订阅方失败（错误码 "
                            + std::to_string(static_cast<int>(subscriber.error())) + "），常见原因：订阅方数量超过 "
                            + std::to_string(kShmMaxSubscribers));
            return nullptr;
        }
        reader->subscriber_.emplace(std::move(subscriber.value()));
        auto listener = reader->event_->listener_builder().create();
        if (!listener.has_value())
        {
            ReportError(error,
                        "[" + topic + "] 创建共享内存监听方失败（错误码 "
                            + std::to_string(static_cast<int>(listener.error())) + "）");
            return nullptr;
        }
        reader->listener_.emplace(std::move(listener.value()));
        // iceoryx2 端口创建后只在工作线程中使用
        reader->worker_ = std::thread(&ShmReader::Run, reader.get());
        return reader;
    }

    ~ShmReader()
    {
        StopWorker(stop_, worker_);
    }

    ShmReader(const ShmReader&) = delete;
    ShmReader& operator=(const ShmReader&) = delete;

  private:
    ShmReader(std::string topic, Handler handler)
        : topic_(std::move(topic))
        , handler_(std::move(handler))
    {
    }

    void Run()
    {
        while (!stop_.load())
        {
            // 超时或出错都只是重新检查一遍数据，事件只用于及时唤醒
            auto event = listener_->timed_wait_one(iox2::bb::Duration::from_millis(kShmWaitMs));
            (void)event;
            Drain();
        }
    }

    /// @brief 逐条取出并处理已到达的数据，每条处理完立即释放
    void Drain()
    {
        while (!stop_.load())
        {
            auto received = subscriber_->receive();
            if (!received.has_value())
            {
                Log(LogLevel::ERR,
                    "[" + topic_ + "] 接收失败（错误码 " + std::to_string(static_cast<int>(received.error())) + "）");
                return;
            }
            auto& sample = received.value();
            if (!sample.has_value())
            {
                return;
            }
            InvokeGuarded(topic_, [&] { handler_(sample->payload()); });
        }
    }

    std::string topic_;
    Handler handler_;
    std::atomic<bool> stop_{false};
    // 声明顺序即依赖顺序，析构时逆序释放；worker_ 在析构函数中先行结束
    std::optional<ShmNode> node_;
    std::optional<ShmService<T>> service_;
    std::optional<ShmEvent> event_;
    std::optional<iox2::Subscriber<iox2::ServiceType::Ipc, T, void>> subscriber_;
    std::optional<iox2::Listener<iox2::ServiceType::Ipc>> listener_;
    std::thread worker_;
};

} // namespace snow::hybrid_comm::detail

#endif // SNOW_HYBRID_COMM_SHM_CHANNEL_H
