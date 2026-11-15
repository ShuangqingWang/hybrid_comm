#include "hybrid_comm/common.h"

#include <cstdlib>
#include <iostream>
#include <mutex>

namespace snow::hybrid_comm
{
namespace
{
std::mutex& LogMutex()
{
    static std::mutex mutex;
    return mutex;
}

LogHandler& CurrentLogHandler()
{
    static LogHandler handler;
    return handler;
}

const char* LevelName(LogLevel level)
{
    switch (level)
    {
    case LogLevel::INFO:
        return "INFO";
    case LogLevel::WARN:
        return "WARN";
    case LogLevel::ERR:
        return "ERR";
    }
    return "UNKNOWN";
}

bool IsTopicChar(char c)
{
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
}

} // namespace

void SetLogHandler(LogHandler handler)
{
    std::lock_guard<std::mutex> lock(LogMutex());
    CurrentLogHandler() = std::move(handler);
}

bool IsValidTopic(const std::string& topic)
{
    if (topic.empty() || topic.size() > kMaxTopicLength)
    {
        return false;
    }
    bool segment_empty = true;
    for (const char c : topic)
    {
        if (c == '/')
        {
            if (segment_empty)
            {
                return false;
            }
            segment_empty = true;
        }
        else if (IsTopicChar(c))
        {
            segment_empty = false;
        }
        else
        {
            return false;
        }
    }
    return !segment_empty;
}

namespace detail
{
void Log(LogLevel level, const std::string& text)
{
    LogHandler handler;
    {
        std::lock_guard<std::mutex> lock(LogMutex());
        handler = CurrentLogHandler();
    }
    // 在锁外调用，避免回调中再次写日志时死锁
    if (handler)
    {
        handler(level, text);
        return;
    }
    std::cerr << "[hybrid_comm][" << LevelName(level) << "] " << text << std::endl;
}

void ReportError(std::string* error, const std::string& text)
{
    if (error != nullptr)
    {
        *error = text;
        return;
    }
    Log(LogLevel::ERR, text);
}

void StopWorker(std::atomic<bool>& stop, std::thread& worker)
{
    stop.store(true);
    if (!worker.joinable())
    {
        return;
    }
    if (worker.get_id() == std::this_thread::get_id())
    {
        Log(LogLevel::ERR, "在回调中销毁了所属的 Reader 或 Server，属于用法错误，进程终止");
        std::abort();
    }
    worker.join();
}

} // namespace detail

} // namespace snow::hybrid_comm
