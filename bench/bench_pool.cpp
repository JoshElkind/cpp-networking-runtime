#include "nmr/frame_pool.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <new>

namespace {

// Keeps a window of live frames and frees in FIFO order, which is how the
// broker actually uses memory (frames die roughly in the order they arrive),
// and is harder on an allocator than alloc/free of a single block.
constexpr std::size_t kWindow = 1024;

template <typename Alloc, typename Free>
double mops(std::size_t size, std::uint64_t n, Alloc alloc, Free release) {
  std::array<void*, kWindow> live{};
  for (auto& p : live) p = alloc(size);
  const auto t0 = std::chrono::steady_clock::now();
  for (std::uint64_t i = 0; i < n; ++i) {
    void*& slot = live[i % kWindow];
    release(slot);
    slot = alloc(size);
    // Touch the payload (past the Frame header) so neither side gets a free pass.
    std::memset(static_cast<std::byte*>(slot) + sizeof(nmr::Frame), 0, 16);
  }
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  for (auto* p : live) release(p);
  return static_cast<double>(n) / secs / 1e6;
}

}  // namespace

void run_pool_benches() {
  constexpr std::uint64_t kN = 20'000'000;
  std::printf("== frame allocation (window of %zu live, FIFO free) ==\n", kWindow);
  for (const std::size_t size : {64u, 1024u}) {
    nmr::FramePool pool;
    const double p = mops(
        size, kN, [&](std::size_t s) -> void* { return pool.allocate(s); },
        [&](void* f) { pool.release(static_cast<nmr::Frame*>(f)); });
    const double h = mops(
        size, kN, [](std::size_t s) { return ::operator new(s + sizeof(nmr::Frame)); },
        [](void* f) { ::operator delete(f); });
    std::printf("  %5zuB  FramePool %7.1f Mops/s   new/delete %7.1f Mops/s   (%.1fx)\n", size, p,
                h, p / h);
  }
  std::printf("\n");
}
