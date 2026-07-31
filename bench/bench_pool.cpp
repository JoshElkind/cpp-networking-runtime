#include "nmr/memory_pool.hpp"

#include <chrono>
#include <cstdio>
#include <new>
#include <vector>

using clock_ = std::chrono::steady_clock;

void run_pool_benches() {
  constexpr int n = 1'000'000;
  std::printf("== memory pool vs new ==\n");

  {
    nmr::MemoryPool pool(64, 1024);
    auto t0 = clock_::now();
    for (int i = 0; i < n; ++i) {
      void* p = pool.allocate();
      pool.deallocate(p);
    }
    auto s = std::chrono::duration<double>(clock_::now() - t0).count();
    std::printf("pool alloc/free  %.2f Mops/s\n", (n / s) / 1e6);
  }
  {
    auto t0 = clock_::now();
    for (int i = 0; i < n; ++i) {
      void* p = ::operator new(64);
      ::operator delete(p);
    }
    auto s = std::chrono::duration<double>(clock_::now() - t0).count();
    std::printf("global new/delete %.2f Mops/s\n", (n / s) / 1e6);
  }
}
