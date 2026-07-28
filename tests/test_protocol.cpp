#include "test_macros.hpp"

#include "nmr/protocol.hpp"

#include <string>
#include <vector>

TEST(protocol_roundtrip) {
  std::string topic = "orders";
  std::vector<std::byte> payload(16, std::byte{0xAB});
  auto frame = nmr::encode_frame(nmr::MsgType::Publish, topic, payload);
  nmr::FrameHeader hdr{};
  std::string_view t;
  std::span<const std::byte> p;
  auto n = nmr::decode_frame(frame, hdr, t, p);
  CHECK(n == frame.size());
  CHECK(hdr.type == nmr::MsgType::Publish);
  CHECK(std::string(t) == topic);
  CHECK(p.size() == payload.size());
  CHECK(p[0] == std::byte{0xAB});
}

TEST(protocol_incomplete) {
  auto frame = nmr::encode_frame(nmr::MsgType::Subscribe, "t", {});
  nmr::FrameHeader hdr{};
  std::string_view t;
  std::span<const std::byte> p;
  auto n = nmr::decode_frame(std::span<const std::byte>(frame.data(), 4), hdr, t, p);
  CHECK(n == 0);
}
