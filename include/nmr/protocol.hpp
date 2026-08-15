#pragma once

#include "nmr/config.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace nmr {

// Wire format. All integers are little-endian regardless of host order.
//
//   0        4      5       6           8               12
//   +--------+------+-------+-----------+---------------+---------+-----------+
//   | magic  | type | flags | topic_len |  payload_len  |  topic  |  payload  |
//   +--------+------+-------+-----------+---------------+---------+-----------+
//
// A PUBLISH is forwarded to subscribers byte-for-byte, so the broker never
// re-encodes on the fan-out path.
enum class MsgType : std::uint8_t {
  kPublish = 1,
  kSubscribe = 2,
  kUnsubscribe = 3,
  kHeartbeat = 4,
};

inline constexpr std::size_t kHeaderSize = 12;
inline constexpr std::size_t kMaxFrameSize = kHeaderSize + kMaxTopicLen + kMaxPayloadLen;

struct FrameHeader {
  MsgType type{};
  std::uint8_t flags = 0;
  std::uint16_t topic_len = 0;
  std::uint32_t payload_len = 0;

  std::size_t frame_size() const noexcept { return kHeaderSize + topic_len + payload_len; }
};

struct FrameView {
  FrameHeader header;
  std::string_view topic;
  std::span<const std::byte> payload;
  std::span<const std::byte> wire;  // the whole frame, header included
};

enum class DecodeStatus : std::uint8_t {
  kOk,
  kNeedMore,
  kBadMagic,
  kBadType,
  kTooLarge,
};

const char* to_string(DecodeStatus s) noexcept;

// Validates the fixed header. Never reads past buf.
DecodeStatus decode_header(std::span<const std::byte> buf, FrameHeader& out) noexcept;

// Decodes the first frame in buf. On kOk the caller may consume out.wire.size()
// bytes; the views point into buf.
DecodeStatus decode_frame(std::span<const std::byte> buf, FrameView& out) noexcept;

void encode_header(const FrameHeader& h, std::byte* dst) noexcept;

// Writes a frame into dst and returns its size, or 0 if the topic/payload are
// over the protocol limits or dst is too small.
std::size_t encode_frame(std::span<std::byte> dst, MsgType type, std::string_view topic,
                         std::span<const std::byte> payload) noexcept;

// Allocating convenience wrapper for clients and tests. Throws std::length_error
// if the frame would exceed protocol limits.
std::vector<std::byte> encode_frame(MsgType type, std::string_view topic,
                                    std::span<const std::byte> payload = {});

}  // namespace nmr
