#include "test.hpp"

#include "nmr/client.hpp"
#include "nmr/server.hpp"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

nmr::ServerConfig test_config() {
  nmr::ServerConfig cfg;
  cfg.port = 0;
  cfg.workers = 2;
  return cfg;
}

std::vector<std::byte> u64_payload(std::uint64_t v) {
  std::vector<std::byte> p(sizeof(v));
  std::memcpy(p.data(), &v, sizeof(v));
  return p;
}

std::uint64_t u64_of(const nmr::FrameView& f) {
  std::uint64_t v = 0;
  std::memcpy(&v, f.payload.data(), sizeof(v));
  return v;
}

template <typename Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 5s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return pred();
}

}  // namespace

TEST(server_delivers_publish_to_subscriber) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());

  sub.subscribe("md.AAPL");
  REQUIRE(sub.ping(2s));
  pub.publish("md.AAPL", u64_payload(42));

  const auto f = sub.receive(2s);
  REQUIRE(f.has_value());
  CHECK(f->header.type == nmr::MsgType::kPublish);
  CHECK(f->topic == "md.AAPL");
  CHECK_EQ(u64_of(*f), 42u);

  server.stop();
  CHECK_EQ(server.frames_in_flight(), 0u);
}

TEST(server_fan_out_preserves_per_publisher_order) {
  nmr::Server server(test_config());
  server.start();
  std::vector<nmr::Client> subs;
  for (int i = 0; i < 3; ++i) {
    subs.emplace_back("127.0.0.1", server.port());
    subs.back().subscribe("orders");
    REQUIRE(subs.back().ping(2s));
  }
  nmr::Client pub("127.0.0.1", server.port());
  constexpr std::uint64_t kN = 2000;
  for (std::uint64_t i = 0; i < kN; ++i) pub.publish("orders", u64_payload(i));

  for (auto& s : subs) {
    for (std::uint64_t i = 0; i < kN; ++i) {
      const auto f = s.receive(2s);
      REQUIRE(f.has_value());
      REQUIRE(u64_of(*f) == i);
    }
  }
  const auto st = server.stats();
  CHECK_EQ(st.publishes, kN);
  CHECK_EQ(st.deliveries, 3 * kN);
  server.stop();
  CHECK_EQ(server.frames_in_flight(), 0u);
}

TEST(server_topics_are_isolated) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("a");
  REQUIRE(sub.ping(2s));

  pub.publish("b", u64_payload(1));
  pub.publish("a", u64_payload(2));
  const auto f = sub.receive(2s);
  REQUIRE(f.has_value());
  CHECK(f->topic == "a");
  CHECK_EQ(u64_of(*f), 2u);
}

TEST(server_unsubscribe_stops_delivery) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("x");
  sub.subscribe("sentinel");
  REQUIRE(sub.ping(2s));
  sub.unsubscribe("x");
  REQUIRE(sub.ping(2s));

  // "x" and "sentinel" may live on different workers, so give the "x"
  // publish time to be (not) delivered before the sentinel goes out.
  pub.publish("x", u64_payload(1));
  REQUIRE(pub.ping(2s));
  std::this_thread::sleep_for(50ms);
  pub.publish("sentinel", u64_payload(2));

  const auto f = sub.receive(2s);
  REQUIRE(f.has_value());
  CHECK(f->topic == "sentinel");
}

TEST(server_disconnect_cleans_up_subscriptions) {
  nmr::Server server(test_config());
  server.start();
  {
    nmr::Client gone("127.0.0.1", server.port());
    gone.subscribe("t");
    REQUIRE(gone.ping(2s));
  }
  REQUIRE(eventually([&] { return server.stats().connections_closed == 1; }));

  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("t");
  REQUIRE(sub.ping(2s));
  pub.publish("t", u64_payload(7));
  REQUIRE(sub.receive(2s).has_value());
  REQUIRE(eventually([&] { return server.stats().deliveries == 1; }));
}

TEST(server_heartbeat_echo) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client c("127.0.0.1", server.port());
  for (int i = 0; i < 10; ++i) CHECK(c.ping(2s));
}

TEST(server_closes_connection_on_malformed_frame) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client bad("127.0.0.1", server.port());
  const std::vector<std::byte> junk(32, std::byte{0x42});
  bad.send_raw(junk);
  CHECK(!bad.receive(2s).has_value());
  CHECK(bad.closed());
  CHECK_EQ(server.stats().protocol_errors, 1u);

  // The broker itself is unaffected.
  nmr::Client good("127.0.0.1", server.port());
  CHECK(good.ping(2s));
}

TEST(server_handles_frames_split_across_reads) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("slow");
  REQUIRE(sub.ping(2s));

  const auto frame = nmr::encode_frame(nmr::MsgType::kPublish, "slow", u64_payload(99));
  for (const std::byte& b : frame) {
    pub.send_raw(std::span(&b, 1));
    std::this_thread::sleep_for(1ms);
  }
  const auto f = sub.receive(2s);
  REQUIRE(f.has_value());
  CHECK_EQ(u64_of(*f), 99u);
}

TEST(server_large_payload_roundtrip) {
  nmr::Server server(test_config());
  server.start();
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("big");
  REQUIRE(sub.ping(2s));

  std::vector<std::byte> payload(nmr::kMaxPayloadLen);
  for (std::size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<std::byte>(i * 31);
  std::thread sender([&] { pub.publish("big", payload); });
  const auto f = sub.receive(5s);
  sender.join();
  REQUIRE(f.has_value());
  REQUIRE(f->payload.size() == payload.size());
  CHECK(std::memcmp(f->payload.data(), payload.data(), payload.size()) == 0);
  server.stop();
  CHECK_EQ(server.frames_in_flight(), 0u);
}

namespace {

// A subscriber that never reads, flooded until the broker has to act.
void flood_slow_consumer(nmr::Server& server, nmr::Client& slow) {
  slow.set_receive_buffer(4096);
  slow.subscribe("firehose");
  REQUIRE(slow.ping(2s));

  nmr::Client pub("127.0.0.1", server.port());
  const std::vector<std::byte> payload(1024);
  for (int i = 0; i < 20'000; ++i) pub.publish("firehose", payload);
  REQUIRE(pub.ping(5s));
}

}  // namespace

TEST(server_slow_consumer_drop_policy) {
  auto cfg = test_config();
  cfg.write_high_watermark = 64 * 1024;
  nmr::Server server(cfg);
  server.start();
  nmr::Client slow("127.0.0.1", server.port());
  flood_slow_consumer(server, slow);

  REQUIRE(eventually([&] { return server.stats().slow_consumer_drops > 0; }));
  CHECK_EQ(server.stats().slow_consumer_disconnects, 0u);

  // Other clients still get service.
  nmr::Client sub("127.0.0.1", server.port());
  nmr::Client pub("127.0.0.1", server.port());
  sub.subscribe("ok");
  REQUIRE(sub.ping(2s));
  pub.publish("ok", u64_payload(5));
  CHECK(sub.receive(2s).has_value());

  server.stop();
  CHECK_EQ(server.frames_in_flight(), 0u);
}

TEST(server_slow_consumer_disconnect_policy) {
  auto cfg = test_config();
  cfg.write_high_watermark = 64 * 1024;
  cfg.slow_consumer = nmr::SlowConsumerPolicy::kDisconnect;
  nmr::Server server(cfg);
  server.start();
  nmr::Client slow("127.0.0.1", server.port());
  flood_slow_consumer(server, slow);

  REQUIRE(eventually([&] { return server.stats().slow_consumer_disconnects == 1; }));
  REQUIRE(eventually([&] { return server.stats().connections_closed >= 1; }));
  server.stop();
  CHECK_EQ(server.frames_in_flight(), 0u);
}

TEST(server_stop_with_traffic_in_flight_frees_everything) {
  auto cfg = test_config();
  cfg.workers = 4;
  nmr::Server server(cfg);
  server.start();

  std::vector<nmr::Client> subs;
  for (int i = 0; i < 4; ++i) {
    subs.emplace_back("127.0.0.1", server.port());
    for (int t = 0; t < 8; ++t) subs.back().subscribe("t" + std::to_string(t));
    REQUIRE(subs.back().ping(2s));
  }
  std::vector<std::thread> pubs;
  for (int p = 0; p < 4; ++p) {
    pubs.emplace_back([&, p] {
      try {
        nmr::Client c("127.0.0.1", server.port());
        const std::vector<std::byte> payload(64);
        for (int i = 0; i < 20'000; ++i) c.publish("t" + std::to_string((p + i) % 8), payload);
      } catch (const std::exception&) {
        // Expected once the server stops underneath us.
      }
    });
  }
  std::this_thread::sleep_for(20ms);
  server.stop();
  for (auto& t : pubs) t.join();
  CHECK_EQ(server.frames_in_flight(), 0u);
}
