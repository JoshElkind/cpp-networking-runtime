#include "test.hpp"

#include "nmr/protocol.hpp"

#include <array>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

using nmr::DecodeStatus;
using nmr::MsgType;

namespace {
std::vector<std::byte> bytes(std::initializer_list<int> v) {
  std::vector<std::byte> out;
  for (int b : v) out.push_back(static_cast<std::byte>(b));
  return out;
}
}  // namespace

TEST(protocol_roundtrip) {
  const std::vector<std::byte> payload(16, std::byte{0xAB});
  const auto frame = nmr::encode_frame(MsgType::kPublish, "orders", payload);
  CHECK_EQ(frame.size(), nmr::kHeaderSize + 6 + 16);

  nmr::FrameView v;
  REQUIRE(nmr::decode_frame(frame, v) == DecodeStatus::kOk);
  CHECK(v.header.type == MsgType::kPublish);
  CHECK_EQ(std::string(v.topic), std::string("orders"));
  CHECK_EQ(v.payload.size(), 16u);
  CHECK(v.payload[15] == std::byte{0xAB});
  CHECK_EQ(v.wire.size(), frame.size());
}

TEST(protocol_wire_layout_is_little_endian) {
  // Golden bytes: changing these breaks every deployed client.
  const auto frame = nmr::encode_frame(MsgType::kSubscribe, "ab", bytes({1, 2, 3}));
  const auto expect = bytes({0x54, 0x52, 0x4D, 0x4E,  // magic
                             0x02, 0x00,              // type, flags
                             0x02, 0x00,              // topic_len
                             0x03, 0x00, 0x00, 0x00,  // payload_len
                             'a', 'b', 1, 2, 3});
  CHECK(frame == expect);
}

TEST(protocol_every_prefix_needs_more) {
  const auto frame = nmr::encode_frame(MsgType::kPublish, "t", bytes({9, 9, 9, 9}));
  for (std::size_t n = 0; n < frame.size(); ++n) {
    nmr::FrameView v;
    CHECK(nmr::decode_frame(std::span(frame).first(n), v) == DecodeStatus::kNeedMore);
  }
}

TEST(protocol_decodes_back_to_back_frames) {
  std::vector<std::byte> stream;
  for (int i = 0; i < 3; ++i) {
    const auto f = nmr::encode_frame(MsgType::kPublish, "t" + std::to_string(i), bytes({i}));
    stream.insert(stream.end(), f.begin(), f.end());
  }
  std::size_t off = 0;
  for (int i = 0; i < 3; ++i) {
    nmr::FrameView v;
    REQUIRE(nmr::decode_frame(std::span(stream).subspan(off), v) == DecodeStatus::kOk);
    CHECK_EQ(std::string(v.topic), "t" + std::to_string(i));
    off += v.wire.size();
  }
  CHECK_EQ(off, stream.size());
}

TEST(protocol_rejects_bad_magic_type_and_size) {
  auto frame = nmr::encode_frame(MsgType::kPublish, "t", {});
  nmr::FrameView v;

  auto bad = frame;
  bad[0] = std::byte{0};
  CHECK(nmr::decode_frame(bad, v) == DecodeStatus::kBadMagic);

  bad = frame;
  bad[4] = std::byte{0};
  CHECK(nmr::decode_frame(bad, v) == DecodeStatus::kBadType);
  bad[4] = std::byte{5};
  CHECK(nmr::decode_frame(bad, v) == DecodeStatus::kBadType);

  bad = frame;
  bad[8] = std::byte{0xFF};
  bad[9] = std::byte{0xFF};
  bad[10] = std::byte{0xFF};  // payload_len ~16M
  CHECK(nmr::decode_frame(bad, v) == DecodeStatus::kTooLarge);

  bad = frame;
  bad[6] = std::byte{0x00};
  bad[7] = std::byte{0x01};  // topic_len 256
  CHECK(nmr::decode_frame(bad, v) == DecodeStatus::kTooLarge);
}

TEST(protocol_encode_enforces_limits) {
  std::array<std::byte, 64> small{};
  CHECK_EQ(nmr::encode_frame(small, MsgType::kPublish, std::string(100, 'x'), {}), 0u);
  const std::string long_topic(nmr::kMaxTopicLen + 1, 'x');
  std::vector<std::byte> big(1024);
  CHECK_EQ(nmr::encode_frame(big, MsgType::kPublish, long_topic, {}), 0u);

  bool threw = false;
  try {
    nmr::encode_frame(MsgType::kPublish, long_topic);
  } catch (const std::length_error&) {
    threw = true;
  }
  CHECK(threw);
}

TEST(protocol_fuzz_random_input_never_overreads) {
  // decode_frame must either reject or return views that lie inside the input.
  // Run under ASan in CI, where an overread is a hard failure.
  std::mt19937_64 rng(1234);
  std::vector<std::byte> buf;
  for (int iter = 0; iter < 200'000; ++iter) {
    buf.resize(rng() % 64);
    for (auto& b : buf) b = static_cast<std::byte>(rng());
    if (buf.size() >= 5 && (rng() & 1)) {
      // Give half the cases a valid magic and type so we get past the first checks.
      const std::uint32_t m = nmr::kMagic;
      for (int i = 0; i < 4; ++i) buf[static_cast<std::size_t>(i)] = static_cast<std::byte>(m >> (8 * i));
      buf[4] = static_cast<std::byte>(1 + rng() % 4);
    }
    nmr::FrameView v;
    if (nmr::decode_frame(buf, v) == DecodeStatus::kOk) {
      REQUIRE(v.wire.data() == buf.data());
      REQUIRE(v.wire.size() <= buf.size());
      REQUIRE(v.wire.size() == nmr::kHeaderSize + v.topic.size() + v.payload.size());
    }
  }
}
