#include "nmr/client.hpp"
#include "nmr/server.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

using clock_ = std::chrono::steady_clock;

static std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          clock_::now().time_since_epoch())
          .count());
}

struct Stats {
  std::uint64_t recv = 0;
  double secs = 0;
  double p50 = 0, p95 = 0, p99 = 0;
};

static Stats run_pubsub(std::uint16_t port, int pubs, int subs, int payload, int seconds,
                        int delay_us) {
  std::atomic<bool> run{true};
  std::atomic<std::uint64_t> received{0};
  std::mutex lat_mu;
  std::vector<std::uint64_t> lats;
  lats.reserve(300000);

  std::vector<std::thread> sub_threads;
  for (int i = 0; i < subs; ++i) {
    sub_threads.emplace_back([&] {
      int fd = nmr::connect_tcp(port);
      nmr::send_all(fd, nmr::encode_frame(nmr::MsgType::Subscribe, "bench", {}));
      std::this_thread::sleep_for(std::chrono::milliseconds(80));
      std::vector<std::uint64_t> local;
      local.reserve(100000);
      while (run.load(std::memory_order_relaxed)) {
        try {
          auto raw = nmr::recv_frame(fd);
          nmr::FrameHeader hdr{};
          std::string_view topic;
          std::span<const std::byte> pl;
          nmr::decode_frame(raw, hdr, topic, pl);
          if (pl.size() >= 8) {
            std::uint64_t sent = 0;
            std::memcpy(&sent, pl.data(), 8);
            local.push_back(now_ns() - sent);
          }
          received.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
          break;
        }
      }
      std::lock_guard<std::mutex> g(lat_mu);
      lats.insert(lats.end(), local.begin(), local.end());
      ::close(fd);
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  std::vector<std::thread> pub_threads;
  auto t0 = clock_::now();
  for (int i = 0; i < pubs; ++i) {
    pub_threads.emplace_back([&] {
      int fd = nmr::connect_tcp(port);
      std::vector<std::byte> body(static_cast<std::size_t>(payload));
      while (run.load(std::memory_order_relaxed)) {
        auto ns = now_ns();
        std::memcpy(body.data(), &ns, 8);
        try {
          nmr::send_all(fd, nmr::encode_frame(nmr::MsgType::Publish, "bench", body));
        } catch (...) {
          break;
        }
        if (delay_us > 0) std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
      }
      ::close(fd);
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  run = false;
  for (auto& t : pub_threads) t.join();
  Stats s;
  s.secs = std::chrono::duration<double>(clock_::now() - t0).count();
  s.recv = received.load();
  std::sort(lats.begin(), lats.end());
  auto pct = [&](double p) {
    if (lats.empty()) return 0.0;
    return static_cast<double>(lats[static_cast<std::size_t>(p * (lats.size() - 1))]) / 1000.0;
  };
  s.p50 = pct(0.50);
  s.p95 = pct(0.95);
  s.p99 = pct(0.99);
  for (auto& t : sub_threads) t.join();
  return s;
}

void run_e2e_bench() {
  std::printf("== end-to-end pub/sub (loopback) ==\n");

  {
    nmr::Server server(0, 2);
    server.start();
    auto st = run_pubsub(server.port(), 4, 2, 64, 2, 0);
    std::printf("throughput  4 pub / 2 sub / 64B: recv=%llu  %.0f msgs/s\n",
                static_cast<unsigned long long>(st.recv), st.recv / st.secs);
    std::printf("  server in=%llu out=%llu dropped=%llu\n",
                static_cast<unsigned long long>(server.messages_in()),
                static_cast<unsigned long long>(server.messages_out()),
                static_cast<unsigned long long>(server.broker().dropped()));
    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  {
    nmr::Server server(0, 2);
    server.start();
    auto st = run_pubsub(server.port(), 1, 1, 64, 2, 20);
    std::printf("latency     1 pub / 1 sub / 64B / 20us pacing: recv=%llu  p50=%.1f us p95=%.1f us p99=%.1f us\n",
                static_cast<unsigned long long>(st.recv), st.p50, st.p95, st.p99);
    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

int main() {
  extern void run_queue_benches();
  extern void run_pool_benches();
  run_queue_benches();
  run_pool_benches();
  run_e2e_bench();
  return 0;
}
