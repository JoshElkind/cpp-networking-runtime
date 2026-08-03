#pragma once

#include <cstddef>
#include <cstdint>

namespace nmr {

// Apple M-series parts use 128-byte cache lines; x86 and most other aarch64
// cores use 64 (with an adjacent-line prefetcher on Intel, but 64 is enough to
// stop true false-sharing).
#if defined(__APPLE__) && defined(__aarch64__)
inline constexpr std::size_t kCacheLine = 128;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

inline constexpr std::uint32_t kMagic = 0x4E4D5254; // 'NMRT'
inline constexpr std::size_t kMaxFrame = 1u << 20;
inline constexpr std::size_t kDefaultPoolBlock = 4096;
inline constexpr std::size_t kWriteHighWatermark = 1u << 20;
inline constexpr std::size_t kWriteLowWatermark = 256u << 10;

}  // namespace nmr
