#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace nmr {

enum class MsgType : std::uint8_t {
  Publish = 1,
  Subscribe = 2,
  Unsubscribe = 3,
  Heartbeat = 4,
};

#pragma pack(push, 1)
struct FrameHeader {
  std::uint32_t magic;
  MsgType type;
  std::uint8_t flags;
  std::uint16_t topic_len;
  std::uint32_t payload_len;
};
#pragma pack(pop)

static_assert(sizeof(FrameHeader) == 12);

inline constexpr std::size_t kHeaderSize = sizeof(FrameHeader);

std::vector<std::byte> encode_frame(MsgType type, std::string_view topic,
                                    std::span<const std::byte> payload);

// Returns bytes consumed, or 0 if incomplete, or SIZE_MAX on error.
std::size_t decode_frame(std::span<const std::byte> buf, FrameHeader& hdr,
                         std::string_view& topic, std::span<const std::byte>& payload);

}  // namespace nmr
