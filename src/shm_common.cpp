#include "hybrid_comm/detail/shm_common.h"

#include <mutex>

#include "hybrid_comm/common.h"

namespace snow::hybrid_comm::detail
{
std::optional<ShmNode> CreateShmNode(std::string* error)
{
    // iceoryx2 自身只输出错误；调试时可用环境变量 IOX2_LOG_LEVEL 调整
    static std::once_flag log_level_once;
    std::call_once(log_level_once, [] { iox2::set_log_level_from_env_or(iox2::LogLevel::Error); });

    // 使用 iceoryx2 全局配置（默认资源目录 /tmp/iceoryx2，可由 iceoryx2.toml 修改）。
    // 不为节点单独指定配置：当前版本只有使用全局配置时，节点创建才能正确清理已退出进程遗留的资源。
    auto node =
        iox2::NodeBuilder().signal_handling_mode(iox2::SignalHandlingMode::Disabled).create<iox2::ServiceType::Ipc>();
    if (!node.has_value())
    {
        ReportError(error,
                    "创建共享内存节点失败（错误码 " + std::to_string(static_cast<int>(node.error()))
                        + "），常见原因：iceoryx2 资源目录属于其他系统用户");
        return std::nullopt;
    }
    return std::optional<ShmNode>(std::move(node.value()));
}

std::optional<iox2::ServiceName> MakeServiceName(const std::string& topic, std::string* error)
{
    auto name = iox2::ServiceName::create(topic.c_str());
    if (!name.has_value())
    {
        ReportError(error, "[" + topic + "] 无法作为共享内存服务名");
        return std::nullopt;
    }
    return std::optional<iox2::ServiceName>(std::move(name.value()));
}

} // namespace snow::hybrid_comm::detail
