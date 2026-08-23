#include "workload.hpp"

#include "nmr/client.hpp"
#include "nmr/cpu.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace nmr::tools {
namespace {

using std::chrono::milliseconds;

std::string topic_name(int i) { return "topic." + std::to_string(i); }

void stamp(std::byte* payload, std::uint64_t ns) { std::memcpy(payload, &ns, sizeof(ns)); }

void wait_until(std::uint64_t deadline_ns) {
  for (;;) {
    const std::uint64_t now = now_ns();
    if (now >= deadline_ns) return;
    // Sleep for the bulk of a long gap, spin the last stretch: OS sleeps
    // routinely overshoot by tens of microseconds.
    if (deadline_ns - now > 200'000) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(deadline_ns - now - 100'000));
    } else {
      cpu_relax();
    }
  }
}

}  // namespace

WorkloadResult run_workload(const WorkloadConfig& cfg) {
  if (cfg.payload < sizeof(std::uint64_t)) throw std::invalid_argument("payload must be >= 8");
  if (cfg.topics < 1 || cfg.publishers < 0 || cfg.subscribers < 0) {
    throw std::invalid_argument("bad workload shape");
  }

  std::atomic<bool> publishing{true};
  std::atomic<std::uint64_t> sent{0};
  std::atomic<std::uint64_t> received{0};
  std::mutex merge_mu;
  WorkloadResult result;

  // Subscribers first, and confirmed with a ping, so nothing published below
  // can race ahead of a subscription.
  std::vector<Client> sub_clients;
  sub_clients.reserve(static_cast<std::size_t>(cfg.subscribers));
  for (int i = 0; i < cfg.subscribers; ++i) {
    Client& c = sub_clients.emplace_back(cfg.host, cfg.port);
    for (int t = 0; t < cfg.topics; ++t) c.subscribe(topic_name(t));
    if (!c.ping(milliseconds(2000))) throw std::runtime_error("subscriber ping timed out");
  }

  std::vector<std::thread> subs;
  for (auto& client : sub_clients) {
    subs.emplace_back([&, c = &client] {
      LatencyHistogram local;
      std::uint64_t count = 0;
      int idle_polls = 0;
      // Keep reading until publishers are done and the pipe has gone quiet.
      while (publishing.load(std::memory_order_relaxed) || idle_polls < 5) {
        const auto frame = c->receive(milliseconds(50));
        if (!frame) {
          if (c->closed()) break;
          if (!publishing.load(std::memory_order_relaxed)) ++idle_polls;
          continue;
        }
        idle_polls = 0;
        if (frame->header.type != MsgType::kPublish) continue;
        std::uint64_t sent_at = 0;
        std::memcpy(&sent_at, frame->payload.data(), sizeof(sent_at));
        const std::uint64_t now = now_ns();
        local.record(now > sent_at ? now - sent_at : 0);
        ++count;
      }
      received.fetch_add(count, std::memory_order_relaxed);
      std::lock_guard<std::mutex> lock(merge_mu);
      result.latency_ns.merge(local);
    });
  }

  const auto t0 = std::chrono::steady_clock::now();
  const auto run_ns = static_cast<std::uint64_t>(cfg.seconds * 1e9);
  std::vector<std::thread> pubs;
  for (int i = 0; i < cfg.publishers; ++i) {
    pubs.emplace_back([&, i] {
      Client c(cfg.host, cfg.port);
      const std::string topic = topic_name(i % cfg.topics);
      const std::size_t frame_size = kHeaderSize + topic.size() + cfg.payload;
      const int batch = cfg.rate > 0 ? 1 : std::max(cfg.batch, 1);

      // Encode the frames once; only the timestamp changes per message.
      std::vector<std::byte> buf(frame_size * static_cast<std::size_t>(batch));
      const std::vector<std::byte> body(cfg.payload);
      for (int b = 0; b < batch; ++b) {
        encode_frame(std::span(buf).subspan(frame_size * static_cast<std::size_t>(b)),
                     MsgType::kPublish, topic, body);
      }
      const std::size_t stamp_offset = kHeaderSize + topic.size();

      const std::uint64_t start = now_ns();
      const std::uint64_t end = start + run_ns;
      const double interval = cfg.rate > 0 ? 1e9 / cfg.rate : 0.0;
      std::uint64_t n = 0;
      try {
        for (;;) {
          std::uint64_t ts;
          if (interval > 0) {
            ts = start + static_cast<std::uint64_t>(static_cast<double>(n) * interval);
            if (ts >= end) break;
            wait_until(ts);
          } else {
            ts = now_ns();
            if (ts >= end) break;
          }
          for (int b = 0; b < batch; ++b) {
            stamp(buf.data() + frame_size * static_cast<std::size_t>(b) + stamp_offset, ts);
          }
          c.send_raw(buf);
          n += static_cast<std::uint64_t>(batch);
        }
      } catch (const std::exception&) {
        // Broker went away; report what we got.
      }
      sent.fetch_add(n, std::memory_order_relaxed);
    });
  }

  for (auto& t : pubs) t.join();
  result.elapsed_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  publishing.store(false, std::memory_order_relaxed);
  for (auto& t : subs) t.join();

  result.sent = sent.load();
  result.received = received.load();
  // Every subscriber takes every topic, so each publish fans out to all.
  result.expected = result.sent * static_cast<std::uint64_t>(cfg.subscribers);
  return result;
}

void print_workload(const WorkloadConfig& cfg, const WorkloadResult& r, std::FILE* out) {
  const double secs = r.elapsed_s > 0 ? r.elapsed_s : 1.0;
  const double loss =
      r.expected == 0 ? 0.0 : 100.0 * (1.0 - static_cast<double>(r.received) / static_cast<double>(r.expected));
  const auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
  const auto& h = r.latency_ns;

  std::fprintf(out, "pubs=%d subs=%d topics=%d payload=%zuB rate=%s duration=%.2fs\n",
               cfg.publishers, cfg.subscribers, cfg.topics, cfg.payload,
               cfg.rate > 0 ? (std::to_string(static_cast<long long>(cfg.rate)) + "/s/pub").c_str()
                            : "max",
               secs);
  std::fprintf(out, "  sent       %12llu  (%.0f msgs/s)\n", static_cast<unsigned long long>(r.sent),
               static_cast<double>(r.sent) / secs);
  std::fprintf(out, "  delivered  %12llu  (%.0f msgs/s, %.1f MB/s payload)  loss %.2f%%\n",
               static_cast<unsigned long long>(r.received), static_cast<double>(r.received) / secs,
               static_cast<double>(r.received) * static_cast<double>(cfg.payload) / secs / 1e6,
               loss);
  std::fprintf(out,
               "  latency us min %.1f  p50 %.1f  p90 %.1f  p99 %.1f  p99.9 %.1f  max %.1f  "
               "(n=%llu)\n",
               us(h.min()), us(h.percentile(50)), us(h.percentile(90)), us(h.percentile(99)),
               us(h.percentile(99.9)), us(h.max()), static_cast<unsigned long long>(h.count()));
}

}  // namespace nmr::tools
