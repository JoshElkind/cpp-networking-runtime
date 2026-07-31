#include "nmr/spsc_queue.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>

using clock_ = std::chrono::steady_clock;

static void bench_spsc(std::uint64_t n) {
  nmr::SpscQueue<std::uint64_t> q(1 << 16);
  auto t0 = clock_::now();
  std::thread prod([&] {
    for (std::uint64_t i = 0; i < n; ++i) {
      while (!q.try_push(i)) {
      }
    }
  });
  std::uint64_t sum = 0;
  std::thread cons([&] {
    std::uint64_t got = 0;
    std::uint64_t v;
    while (got < n) {
      if (q.try_pop(v)) {
        sum += v;
        ++got;
      }
    }
  });
  prod.join();
  cons.join();
  auto ms = std::chrono::duration<double>(clock_::now() - t0).count();
  std::printf("spsc_lockfree  n=%llu  %.2f Mops/s  checksum=%llu\n",
              static_cast<unsigned long long>(n), (n / ms) / 1e6,
              static_cast<unsigned long long>(sum));
}

static void bench_mutex(std::uint64_t n) {
  nmr::MutexQueue<std::uint64_t> q(1 << 16);
  auto t0 = clock_::now();
  std::thread prod([&] {
    for (std::uint64_t i = 0; i < n; ++i) {
      while (!q.try_push(i)) {
      }
    }
  });
  std::uint64_t sum = 0;
  std::thread cons([&] {
    std::uint64_t got = 0;
    std::uint64_t v;
    while (got < n) {
      if (q.try_pop(v)) {
        sum += v;
        ++got;
      }
    }
  });
  prod.join();
  cons.join();
  auto ms = std::chrono::duration<double>(clock_::now() - t0).count();
  std::printf("mutex_queue    n=%llu  %.2f Mops/s  checksum=%llu\n",
              static_cast<unsigned long long>(n), (n / ms) / 1e6,
              static_cast<unsigned long long>(sum));
}

void run_queue_benches() {
  std::printf("== queues ==\n");
  bench_spsc(2'000'000);
  bench_mutex(2'000'000);
}
