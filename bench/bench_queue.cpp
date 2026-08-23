#include "nmr/cpu.hpp"
#include "nmr/histogram.hpp"
#include "nmr/spsc_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

namespace {

// One element per cache line, to show the cost of a line transfer per message.
struct alignas(nmr::kCacheLine) PaddedU64 {
  std::uint64_t v;
};

template <typename Queue, typename T = std::uint64_t>
double throughput_mops(std::size_t capacity, std::uint64_t n) {
  Queue q(capacity);
  std::atomic<bool> go{false};
  std::thread producer([&] {
    while (!go.load(std::memory_order_acquire)) {
    }
    for (std::uint64_t i = 0; i < n; ++i) {
      while (!q.try_push(T{i})) nmr::cpu_relax();
    }
  });

  const auto t0 = std::chrono::steady_clock::now();
  go.store(true, std::memory_order_release);
  for (std::uint64_t got = 0; got < n;) {
    T v;
    if (q.try_pop(v)) {
      ++got;
    } else {
      nmr::cpu_relax();
    }
  }
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  producer.join();
  return static_cast<double>(n) / secs / 1e6;
}

// Round trip through two queues between two threads: the cross-core
// handoff cost, which a message pays twice crossing the broker.
nmr::LatencyHistogram ping_pong(std::uint64_t n) {
  nmr::SpscQueue<std::uint64_t> to(64);
  nmr::SpscQueue<std::uint64_t> from(64);
  std::thread echo([&] {
    for (std::uint64_t i = 0; i < n; ++i) {
      std::uint64_t v;
      while (!to.try_pop(v)) nmr::cpu_relax();
      while (!from.try_push(v)) nmr::cpu_relax();
    }
  });

  nmr::LatencyHistogram h;
  for (std::uint64_t i = 0; i < n; ++i) {
    const std::uint64_t t0 = nmr::now_ns();
    while (!to.try_push(i)) nmr::cpu_relax();
    std::uint64_t v;
    while (!from.try_pop(v)) nmr::cpu_relax();
    h.record(nmr::now_ns() - t0);
  }
  echo.join();
  return h;
}

}  // namespace

void run_queue_benches() {
  constexpr std::uint64_t kN = 20'000'000;
  std::printf("== queues: 1 producer -> 1 consumer, Mops/s ==\n");
  std::printf("  %-10s %12s %16s %12s\n", "capacity", "spsc u64", "spsc padded u64", "mutex u64");
  for (const std::size_t cap : {256u, 1024u, 4096u, 65536u}) {
    std::printf("  %-10zu %12.1f %16.1f %12.1f\n", cap,
                throughput_mops<nmr::SpscQueue<std::uint64_t>>(cap, kN),
                throughput_mops<nmr::SpscQueue<PaddedU64>, PaddedU64>(cap, kN / 4),
                throughput_mops<nmr::MutexQueue<std::uint64_t>>(cap, kN / 10));
  }

  const auto h = ping_pong(1'000'000);
  std::printf("  spsc round trip ns: p50 %llu  p99 %llu  p99.9 %llu\n\n",
              static_cast<unsigned long long>(h.percentile(50)),
              static_cast<unsigned long long>(h.percentile(99)),
              static_cast<unsigned long long>(h.percentile(99.9)));
}
