#ifndef SNOW_HYBRID_COMM_COMMON_H
#define SNOW_HYBRID_COMM_COMMON_H

/**
 * @file common.h
 * @brief hybrid_comm 公共定义：日志接口、调用状态、topic 校验与消息类型判定。
 */

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <string>
#include <thread>
#include <type_traits>

#include "google/protobuf/message_lite.h"

namespace snow::hybrid_comm
{
/// @brief 日志级别
enum class LogLevel : uint8_t
{
    INFO,
    WARN,
    ERR,
};

/// @brief 日志回调，项目可接到自己的日志模块
using LogHandler = std::function<void(LogLevel level, const std::string& text)>;

/**
 * @brief 设置日志回调，未设置或传入空回调时写到 stderr。
 * @note 线程安全；回调可能在 hybrid_comm 内部线程中调用。
 */
void SetLogHandler(LogHandler handler);

/// @brief 请求应答的调用结果
enum class CallStatus : uint8_t
{
    OK,           ///< 成功
    TIMEOUT,      ///< 超时未收到应答（含应答方不存在）
    SEND_FAILED,  ///< 请求发送失败
    PARSE_FAILED, ///< 请求或应答无法解析
    SERVER_ERROR, ///< 应答方处理函数抛出异常
};

/// @brief topic 最大长度
constexpr std::size_t kMaxTopicLength = 64;

/**
 * @brief 检查 topic 是否合法。
 * @details 合法 topic 由 '/' 分隔的若干段组成，每段只含小写字母、数字、下划线，总长不超过 kMaxTopicLength。
 */
bool IsValidTopic(const std::string& topic);

namespace detail
{
/// @brief 写日志（经 SetLogHandler 设置的回调或 stderr）
void Log(LogLevel level, const std::string& text);

/// @brief 报告创建失败：error 非空时写入 error，否则写日志
void ReportError(std::string* error, const std::string& text);

/**
 * @brief 通知内部线程退出并等待其结束。
 * @note 在该线程自身中调用（即在回调中销毁所属对象）属于用法错误，记录日志后终止进程。
 */
void StopWorker(std::atomic<bool>& stop, std::thread& worker);

/// @brief 执行用户回调，捕获并记录回调抛出的异常；返回回调是否正常结束
template <typename Function>
bool InvokeGuarded(const std::string& topic, Function&& function)
{
    try
    {
        function();
        return true;
    }
    catch (const std::exception& e)
    {
        Log(LogLevel::WARN, "[" + topic + "] 回调抛出异常: " + e.what());
    }
    catch (...)
    {
        Log(LogLevel::WARN, "[" + topic + "] 回调抛出未知异常");
    }
    return false;
}

template <typename T, typename = void>
struct HasIox2TypeName : std::false_type
{
};

template <typename T>
struct HasIox2TypeName<T, std::void_t<decltype(T::IOX2_TYPE_NAME)>> : std::true_type
{
};

/// @brief Protobuf 消息，走 ZeroMQ
template <typename T>
constexpr bool kIsProtoMessage = std::is_base_of_v<google::protobuf::MessageLite, T>;

/// @brief 共享内存消息（可按字节拷贝且定义了 IOX2_TYPE_NAME 的结构体），走 iceoryx2
template <typename T>
constexpr bool kIsShmMessage = !kIsProtoMessage<T> && std::is_trivially_copyable_v<T> && HasIox2TypeName<T>::value;

} // namespace detail

} // namespace snow::hybrid_comm

#endif // SNOW_HYBRID_COMM_COMMON_H
