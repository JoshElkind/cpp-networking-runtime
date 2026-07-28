#include "nmr/protocol.hpp"

#include "nmr/config.hpp"

#include <cstring>

namespace nmr {

std::vector<std::byte> encode_frame(MsgType type, std::string_view topic,
                                    std::span<const std::byte> payload) {
  FrameHeader hdr{};
  hdr.magic = kMagic;
  hdr.type = type;
  hdr.flags = 0;
  hdr.topic_len = static_cast<std::uint16_t>(topic.size());
  hdr.payload_len = static_cast<std::uint32_t>(payload.size());

  std::vector<std::byte> out(kHeaderSize + topic.size() + payload.size());
  std::memcpy(out.data(), &hdr, kHeaderSize);
  std::memcpy(out.data() + kHeaderSize, topic.data(), topic.size());
  if (!payload.empty()) {
    std::memcpy(out.data() + kHeaderSize + topic.size(), payload.data(), payload.size());
  }
  return out;
}

std::size_t decode_frame(std::span<const std::byte> buf, FrameHeader& hdr,
                         std::string_view& topic, std::span<const std::byte>& payload) {
  if (buf.size() < kHeaderSize) return 0;
  std::memcpy(&hdr, buf.data(), kHeaderSize);
  if (hdr.magic != kMagic) return static_cast<std::size_t>(-1);
  const std::size_t need = kHeaderSize + hdr.topic_len + hdr.payload_len;
  if (need > kMaxFrame) return static_cast<std::size_t>(-1);
  if (buf.size() < need) return 0;
  topic = std::string_view(reinterpret_cast<const char*>(buf.data() + kHeaderSize),
                           hdr.topic_len);
  payload = buf.subspan(kHeaderSize + hdr.topic_len, hdr.payload_len);
  return need;
}

}  // namespace nmr
