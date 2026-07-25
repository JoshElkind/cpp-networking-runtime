#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <deque>
#include <mutex>
#include <utility>
#include <vector>

namespace nmr {

// Bounded lock-free SPSC ring. Capacity must be a power of two.
template <typename T>
class SpscQueue {
 public:
  explicit SpscQueue(std::size_t capacity)
      : cap_(next_pow2(capacity)), mask_(cap_ - 1), buf_(new Slot[cap_]) {
    for (std::size_t i = 0; i < cap_; ++i) {
      buf_[i].seq.store(i, std::memory_order_relaxed);
    }
  }

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  bool try_push(T v) {
    const std::size_t pos = head_.load(std::memory_order_relaxed);
    Slot& slot = buf_[pos & mask_];
    const std::size_t seq = slot.seq.load(std::memory_order_acquire);
    if (seq != pos) {
      return false;
    }
    slot.val = std::move(v);
    slot.seq.store(pos + 1, std::memory_order_release);
    head_.store(pos + 1, std::memory_order_relaxed);
    return true;
  }

  bool try_pop(T& out) {
    const std::size_t pos = tail_.load(std::memory_order_relaxed);
    Slot& slot = buf_[pos & mask_];
    const std::size_t seq = slot.seq.load(std::memory_order_acquire);
    if (seq != pos + 1) {
      return false;
    }
    out = std::move(slot.val);
    slot.seq.store(pos + cap_, std::memory_order_release);
    tail_.store(pos + 1, std::memory_order_relaxed);
    return true;
  }

  std::size_t capacity() const { return cap_; }

  std::size_t approx_size() const {
    const auto h = head_.load(std::memory_order_acquire);
    const auto t = tail_.load(std::memory_order_acquire);
    return h - t;
  }

 private:
  struct Slot {
    alignas(64) std::atomic<std::size_t> seq;
    T val{};
  };

  static std::size_t next_pow2(std::size_t n) {
    std::size_t p = 1;
    while (p < n) p <<= 1;
    return p;
  }

  const std::size_t cap_;
  const std::size_t mask_;
  std::unique_ptr<Slot[]> buf_;
  alignas(64) std::atomic<std::size_t> head_{0};
  alignas(64) std::atomic<std::size_t> tail_{0};
};

template <typename T>
class MutexQueue {
 public:
  explicit MutexQueue(std::size_t capacity) : cap_(capacity) {}

  bool try_push(T v) {
    std::lock_guard<std::mutex> g(mu_);
    if (q_.size() >= cap_) return false;
    q_.push_back(std::move(v));
    return true;
  }

  bool try_pop(T& out) {
    std::lock_guard<std::mutex> g(mu_);
    if (q_.empty()) return false;
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }

 private:
  std::size_t cap_;
  std::mutex mu_;
  std::deque<T> q_;
};

}  // namespace nmr
