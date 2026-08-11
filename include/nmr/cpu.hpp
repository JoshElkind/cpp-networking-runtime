#pragma once

#include <chrono>
#include <cstdint>
#include <thread>

namespace nmr {

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#endif
}

inline std::uint64_t now_ns() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

// Spin-then-yield idle strategy for polling threads. Spinning keeps wakeup
// latency in the tens of nanoseconds while traffic is flowing; yielding stops
// an idle worker from monopolising a core it shares with something else.
// spin_only never yields, for threads that own a dedicated core.
class Backoff {
 public:
  explicit Backoff(bool spin_only = false) noexcept : spin_only_(spin_only) {}

  void idle() noexcept {
    if (spin_only_ || spins_ < kSpinLimit) {
      ++spins_;
      cpu_relax();
    } else {
      std::this_thread::yield();
    }
  }

  void reset() noexcept { spins_ = 0; }

 private:
  static constexpr unsigned kSpinLimit = 1u << 14;
  unsigned spins_ = 0;
  bool spin_only_;
};

// Returns false where affinity isn't supported (macOS has no hard pinning).
bool pin_current_thread(int cpu) noexcept;

// Name shows up in top -H / perf / lldb. Linux truncates to 15 chars.
void set_current_thread_name(const char* name) noexcept;

}  // namespace nmr
