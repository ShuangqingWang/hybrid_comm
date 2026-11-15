#ifndef SNOW_HYBRID_COMM_TEST_TYPES_H
#define SNOW_HYBRID_COMM_TEST_TYPES_H

/**
 * @file test_types.h
 * @brief 测试用共享内存结构体。
 */

#include <array>
#include <cstdint>
#include <type_traits>

namespace snow::hybrid_comm_test
{
constexpr uint32_t kBlobSize = 4 * 1024 * 1024;

/// @brief 4 MB 大块数据，data[i] = (seq + i) & 0xff
struct TestBlob
{
    uint64_t seq{0};
    uint32_t length{0};
    std::array<uint8_t, kBlobSize> data;
    static constexpr const char* IOX2_TYPE_NAME = "snow::hybrid_comm_test::TestBlob";
};
static_assert(std::is_trivially_copyable_v<TestBlob>);

/// @brief 与 TestBlob 布局不同的结构体，用于类型不一致测试
struct OtherBlob
{
    uint64_t value{0};
    static constexpr const char* IOX2_TYPE_NAME = "snow::hybrid_comm_test::OtherBlob";
};
static_assert(std::is_trivially_copyable_v<OtherBlob>);

} // namespace snow::hybrid_comm_test

#endif // SNOW_HYBRID_COMM_TEST_TYPES_H
