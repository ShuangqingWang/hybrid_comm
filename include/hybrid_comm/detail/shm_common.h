#ifndef SNOW_HYBRID_COMM_SHM_COMMON_H
#define SNOW_HYBRID_COMM_SHM_COMMON_H

/**
 * @file shm_common.h
 * @brief iceoryx2 公共部分：节点创建、服务参数与服务名。
 */

#include <cstdint>
#include <optional>
#include <string>

#include "iox2/iceoryx2.hpp"

namespace snow::hybrid_comm::detail
{
using ShmNode = iox2::Node<iox2::ServiceType::Ipc>;

// 共享内存服务参数。同一 topic 的收发双方必须使用相同参数，因此固定为常量。
constexpr uint64_t kShmMaxPublishers = 1;  ///< 每个 topic 只允许一个发布方
constexpr uint64_t kShmMaxSubscribers = 4; ///< 每个 topic 的订阅方上限
constexpr uint64_t kShmBufferSize = 2;     ///< 订阅方缓存条数，满时丢弃最旧的
constexpr uint64_t kShmMaxBorrowed = 2;    ///< 订阅方同时持有的条数上限
constexpr uint64_t kShmHistorySize = 0;    ///< 不向后加入的订阅方补发历史数据
constexpr int kShmWaitMs = 100;            ///< 订阅线程等待事件的超时，决定退出响应时间

/**
 * @brief 创建 iceoryx2 节点。
 * @details 使用 iceoryx2 全局配置，不注册信号处理；创建时 iceoryx2 自动清理已退出进程遗留的资源。
 */
std::optional<ShmNode> CreateShmNode(std::string* error);

/// @brief topic 对应的 iceoryx2 服务名
std::optional<iox2::ServiceName> MakeServiceName(const std::string& topic, std::string* error);

} // namespace snow::hybrid_comm::detail

#endif // SNOW_HYBRID_COMM_SHM_COMMON_H
