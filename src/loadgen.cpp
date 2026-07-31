#include "nmr/client.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

using namespace nmr;
using clock_ = std::chrono::steady_clock;

int main(int argc, char** argv) {
  std::uint16_t port = 9000;
  int pubs = 4;
  int subs = 4;
  int seconds = 3;
  int payload = 64;
  if (argc > 1) port = static_cast<std::uint16_t>(std::atoi(argv[1]));
  if (argc > 2) pubs = std::atoi(argv[2]);
  if (argc > 3) subs = std::atoi(argv[3]);
  if (argc > 4) seconds = std::atoi(argv[4]);
  if (argc > 5) payload = std::atoi(argv[5]);

  std::atomic<bool> run{true};
  std::atomic<std::uint64_t> received{0};
  std::vector<std::uint64_t> lats;
  lats.reserve(1 << 20);
  std::mutex lat_mu;

  std::vector<std::thread> sub_threads;
  for (int i = 0; i < subs; ++i) {
    sub_threads.emplace_back([&] {
      int fd = connect_tcp(port);
      auto frame = encode_frame(MsgType::Subscribe, "bench", {});
      send_all(fd, frame);
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      while (run.load()) {
        try {
          auto raw = recv_frame(fd);
          FrameHeader hdr{};
          std::string_view topic;
          std::span<const std::byte> payload_v;
          decode_frame(raw, hdr, topic, payload_v);
          if (payload_v.size() >= 8) {
            std::uint64_t sent = 0;
            std::memcpy(&sent, payload_v.data(), 8);
            auto now = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    clock_::now().time_since_epoch())
                    .count());
            std::lock_guard<std::mutex> g(lat_mu);
            lats.push_back(now - sent);
          }
          received.fetch_add(1);
        } catch (...) {
          break;
        }
      }
      ::close(fd);
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  std::atomic<std::uint64_t> sent_n{0};
  std::vector<std::thread> pub_threads;
  auto t0 = clock_::now();
  for (int i = 0; i < pubs; ++i) {
    pub_threads.emplace_back([&] {
      int fd = connect_tcp(port);
      std::vector<std::byte> body(static_cast<std::size_t>(payload));
      while (run.load()) {
        auto ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                clock_::now().time_since_epoch())
                .count());
        std::memcpy(body.data(), &ns, sizeof(ns));
        auto frame = encode_frame(MsgType::Publish, "bench", body);
        try {
          send_all(fd, frame);
          sent_n.fetch_add(1);
        } catch (...) {
          break;
        }
      }
      ::close(fd);
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  run = false;
  for (auto& t : pub_threads) t.join();
  for (auto& t : sub_threads) t.join();
  auto secs = std::chrono::duration<double>(clock_::now() - t0).count();

  std::sort(lats.begin(), lats.end());
  auto pct = [&](double p) -> double {
    if (lats.empty()) return 0;
    auto idx = static_cast<std::size_t>(p * (lats.size() - 1));
    return static_cast<double>(lats[idx]) / 1000.0;
  };
  const double msgs = static_cast<double>(received.load()) / secs;
  const double gbps = msgs * static_cast<double>(payload) * 8.0 / 1e9;
  std::cout << "pubs=" << pubs << " subs=" << subs << " payload=" << payload
            << "B duration=" << secs << "s\n";
  std::cout << "sent=" << sent_n.load() << " recv=" << received.load() << "\n";
  std::cout << "throughput=" << msgs << " msgs/s  " << gbps << " Gbps payload\n";
  std::cout << "latency_us p50=" << pct(0.50) << " p95=" << pct(0.95)
            << " p99=" << pct(0.99) << "\n";
  return 0;
}
