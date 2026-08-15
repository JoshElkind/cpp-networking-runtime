#include "nmr/protocol.hpp"

#include <cstring>
#include <stdexcept>

namespace nmr {
namespace {

std::uint16_t load_u16(const std::byte* p) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<unsigned>(p[0]) |
                                    std::to_integer<unsigned>(p[1]) << 8);
}

std::uint32_t load_u32(const std::byte* p) noexcept {
  return std::to_integer<std::uint32_t>(p[0]) | std::to_integer<std::uint32_t>(p[1]) << 8 |
         std::to_integer<std::uint32_t>(p[2]) << 16 | std::to_integer<std::uint32_t>(p[3]) << 24;
}

void store_u16(std::byte* p, std::uint16_t v) noexcept {
  p[0] = static_cast<std::byte>(v);
  p[1] = static_cast<std::byte>(v >> 8);
}

void store_u32(std::byte* p, std::uint32_t v) noexcept {
  p[0] = static_cast<std::byte>(v);
  p[1] = static_cast<std::byte>(v >> 8);
  p[2] = static_cast<std::byte>(v >> 16);
  p[3] = static_cast<std::byte>(v >> 24);
}

bool valid_type(std::uint8_t t) noexcept {
  return t >= static_cast<std::uint8_t>(MsgType::kPublish) &&
         t <= static_cast<std::uint8_t>(MsgType::kHeartbeat);
}

}  // namespace

const char* to_string(DecodeStatus s) noexcept {
  switch (s) {
    case DecodeStatus::kOk: return "ok";
    case DecodeStatus::kNeedMore: return "need-more";
    case DecodeStatus::kBadMagic: return "bad-magic";
    case DecodeStatus::kBadType: return "bad-type";
    case DecodeStatus::kTooLarge: return "too-large";
  }
  return "unknown";
}

DecodeStatus decode_header(std::span<const std::byte> buf, FrameHeader& out) noexcept {
  if (buf.size() < kHeaderSize) return DecodeStatus::kNeedMore;
  const std::byte* p = buf.data();
  if (load_u32(p) != kMagic) return DecodeStatus::kBadMagic;
  const auto type = std::to_integer<std::uint8_t>(p[4]);
  if (!valid_type(type)) return DecodeStatus::kBadType;

  out.type = static_cast<MsgType>(type);
  out.flags = std::to_integer<std::uint8_t>(p[5]);
  out.topic_len = load_u16(p + 6);
  out.payload_len = load_u32(p + 8);
  if (out.topic_len > kMaxTopicLen || out.payload_len > kMaxPayloadLen) {
    return DecodeStatus::kTooLarge;
  }
  return DecodeStatus::kOk;
}

DecodeStatus decode_frame(std::span<const std::byte> buf, FrameView& out) noexcept {
  FrameHeader h;
  if (const DecodeStatus st = decode_header(buf, h); st != DecodeStatus::kOk) return st;
  const std::size_t size = h.frame_size();
  if (buf.size() < size) return DecodeStatus::kNeedMore;

  out.header = h;
  out.wire = buf.first(size);
  out.topic = std::string_view(reinterpret_cast<const char*>(buf.data() + kHeaderSize),
                               h.topic_len);
  out.payload = buf.subspan(kHeaderSize + h.topic_len, h.payload_len);
  return DecodeStatus::kOk;
}

void encode_header(const FrameHeader& h, std::byte* dst) noexcept {
  store_u32(dst, kMagic);
  dst[4] = static_cast<std::byte>(h.type);
  dst[5] = static_cast<std::byte>(h.flags);
  store_u16(dst + 6, h.topic_len);
  store_u32(dst + 8, h.payload_len);
}

std::size_t encode_frame(std::span<std::byte> dst, MsgType type, std::string_view topic,
                         std::span<const std::byte> payload) noexcept {
  if (topic.size() > kMaxTopicLen || payload.size() > kMaxPayloadLen) return 0;
  const std::size_t size = kHeaderSize + topic.size() + payload.size();
  if (dst.size() < size) return 0;

  FrameHeader h;
  h.type = type;
  h.topic_len = static_cast<std::uint16_t>(topic.size());
  h.payload_len = static_cast<std::uint32_t>(payload.size());
  encode_header(h, dst.data());
  // memcpy with a null source is UB even for zero bytes, and an empty
  // string_view is allowed to have a null data().
  if (!topic.empty()) std::memcpy(dst.data() + kHeaderSize, topic.data(), topic.size());
  if (!payload.empty()) {
    std::memcpy(dst.data() + kHeaderSize + topic.size(), payload.data(), payload.size());
  }
  return size;
}

std::vector<std::byte> encode_frame(MsgType type, std::string_view topic,
                                    std::span<const std::byte> payload) {
  std::vector<std::byte> out(kHeaderSize + topic.size() + payload.size());
  if (encode_frame(out, type, topic, payload) == 0) {
    throw std::length_error("nmr: frame exceeds protocol limits");
  }
  return out;
}

}  // namespace nmr
