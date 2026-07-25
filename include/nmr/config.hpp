#pragma once

#include <cstddef>
#include <cstdint>

namespace nmr {

inline constexpr std::uint32_t kMagic = 0x4E4D5254; // 'NMRT'
inline constexpr std::size_t kMaxFrame = 1u << 20;
inline constexpr std::size_t kDefaultPoolBlock = 4096;
inline constexpr std::size_t kWriteHighWatermark = 1u << 20;
inline constexpr std::size_t kWriteLowWatermark = 256u << 10;

}  // namespace nmr
