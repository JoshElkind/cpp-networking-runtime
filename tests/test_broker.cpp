#include "test.hpp"

#include "nmr/broker.hpp"

#include <algorithm>
#include <cstring>
#include <vector>

namespace {

// Owns a pool and builds frames, so tests can drive a TopicShard directly.
struct Fixture {
  nmr::FramePool pool;
  nmr::TopicShard shard;
  std::vector<nmr::Egress> out;

  nmr::Frame* frame(nmr::MsgType type, std::string_view topic) {
    const auto wire = nmr::encode_frame(type, topic);
    nmr::Frame* f = pool.allocate(wire.size());
    std::memcpy(f->data(), wire.data(), wire.size());
    return f;
  }

  // Applies a command and plays the IO thread's part: frees the frame on its
  // release. Returns the connections that would have been delivered to.
  std::vector<nmr::ConnId> apply(nmr::Command cmd, std::string_view topic, nmr::ConnId conn) {
    out.clear();
    nmr::Frame* f = cmd == nmr::Command::kDisconnect ? nullptr : frame(to_type(cmd), topic);
    shard.apply(nmr::Ingress{f, conn, cmd}, [&](const nmr::Egress& e) { out.push_back(e); });

    std::vector<nmr::ConnId> delivered;
    bool released = f == nullptr;
    for (const auto& e : out) {
      CHECK(e.frame == f);
      if (e.conn == nmr::kNoConn) {
        CHECK(!released);
        released = true;
        pool.release(e.frame);
      } else {
        CHECK(!released);  // deliveries must precede the release
        delivered.push_back(e.conn);
      }
    }
    CHECK(released);
    std::sort(delivered.begin(), delivered.end());
    return delivered;
  }

  static nmr::MsgType to_type(nmr::Command c) {
    switch (c) {
      case nmr::Command::kPublish: return nmr::MsgType::kPublish;
      case nmr::Command::kSubscribe: return nmr::MsgType::kSubscribe;
      default: return nmr::MsgType::kUnsubscribe;
    }
  }
};

using nmr::Command;
using Ids = std::vector<nmr::ConnId>;

}  // namespace

TEST(shard_publish_without_subscribers_only_releases) {
  Fixture fx;
  CHECK(fx.apply(Command::kPublish, "x", 100).empty());
  CHECK_EQ(fx.out.size(), 1u);
  CHECK_EQ(fx.pool.live(), 0u);
}

TEST(shard_fans_out_to_every_subscriber) {
  Fixture fx;
  fx.apply(Command::kSubscribe, "md", 20);
  fx.apply(Command::kSubscribe, "md", 21);
  fx.apply(Command::kSubscribe, "other", 22);
  CHECK(fx.apply(Command::kPublish, "md", 99) == (Ids{20, 21}));
  CHECK(fx.apply(Command::kPublish, "other", 99) == (Ids{22}));
  CHECK_EQ(fx.pool.live(), 0u);
}

TEST(shard_duplicate_subscribe_is_idempotent) {
  Fixture fx;
  fx.apply(Command::kSubscribe, "md", 20);
  fx.apply(Command::kSubscribe, "md", 20);
  CHECK_EQ(fx.shard.subscriber_count("md"), 1u);
  CHECK(fx.apply(Command::kPublish, "md", 1) == (Ids{20}));
}

TEST(shard_unsubscribe) {
  Fixture fx;
  fx.apply(Command::kSubscribe, "md", 20);
  fx.apply(Command::kSubscribe, "md", 21);
  fx.apply(Command::kUnsubscribe, "md", 20);
  CHECK(fx.apply(Command::kPublish, "md", 1) == (Ids{21}));
  fx.apply(Command::kUnsubscribe, "md", 21);
  CHECK_EQ(fx.shard.topic_count(), 0u);
  fx.apply(Command::kUnsubscribe, "never", 21);  // harmless
}

TEST(shard_disconnect_drops_all_subscriptions) {
  Fixture fx;
  for (const char* t : {"a", "b", "c"}) fx.apply(Command::kSubscribe, t, 30);
  fx.apply(Command::kSubscribe, "b", 31);
  fx.apply(Command::kDisconnect, {}, 30);
  CHECK(fx.out.empty());
  CHECK(fx.apply(Command::kPublish, "a", 1).empty());
  CHECK(fx.apply(Command::kPublish, "b", 1) == (Ids{31}));
  CHECK_EQ(fx.shard.topic_count(), 1u);
  fx.apply(Command::kDisconnect, {}, 30);  // second disconnect is a no-op
}

TEST(topic_shard_hash_is_stable_and_spread) {
  CHECK_EQ(nmr::topic_shard("md.AAPL", 1), 0u);
  CHECK_EQ(nmr::topic_shard("md.AAPL", 8), nmr::topic_shard("md.AAPL", 8));
  std::vector<int> hits(4, 0);
  for (int i = 0; i < 4000; ++i) ++hits[nmr::topic_shard("sym." + std::to_string(i), 4)];
  for (int h : hits) CHECK(h > 800 && h < 1200);
}
