#pragma once

#include "nmr/config.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <new>
#include <type_traits>
#include <utility>

namespace nmr {

// Bounded single-producer / single-consumer ring.
//
// Indices grow monotonically and are masked on access, so full vs. empty is
// just head - tail == capacity vs. head == tail, with no wasted slot.
//
// Each side keeps a private copy of the other side's index and only re-reads
// the shared one when the ring looks full (producer) or empty (consumer). In
// steady state a push touches only the producer's cache line and the slot, and
// a pop touches only the consumer's line and the slot.
template <typename T>
class SpscQueue {
  static_assert(std::is_nothrow_move_constructible_v<T>);
  static_assert(std::is_nothrow_destructible_v<T>);

 public:
  explicit SpscQueue(std::size_t capacity)
      : capacity_(std::bit_ceil(std::max<std::size_t>(capacity, 2))),
        mask_(capacity_ - 1),
        slots_(std::make_unique<Slot[]>(capacity_)) {}

  ~SpscQueue() {
    const std::size_t head = producer_.head.load(std::memory_order_relaxed);
    for (std::size_t i = consumer_.tail.load(std::memory_order_relaxed); i != head; ++i) {
      slot(i)->~T();
    }
  }

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  // Producer only.
  template <typename... Args>
  bool try_emplace(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args&&...>) {
    const std::size_t head = producer_.head.load(std::memory_order_relaxed);
    if (head - producer_.cached_tail == capacity_) {
      producer_.cached_tail = consumer_.tail.load(std::memory_order_acquire);
      if (head - producer_.cached_tail == capacity_) return false;
    }
    ::new (static_cast<void*>(slot(head))) T(std::forward<Args>(args)...);
    producer_.head.store(head + 1, std::memory_order_release);
    return true;
  }

  bool try_push(T&& v) noexcept { return try_emplace(std::move(v)); }
  bool try_push(const T& v) noexcept(std::is_nothrow_copy_constructible_v<T>) {
    return try_emplace(v);
  }

  // Consumer only. Returns the oldest element without removing it, or nullptr
  // if the ring is empty. The pointer stays valid until pop().
  T* front() noexcept {
    const std::size_t tail = consumer_.tail.load(std::memory_order_relaxed);
    if (tail == consumer_.cached_head) {
      consumer_.cached_head = producer_.head.load(std::memory_order_acquire);
      if (tail == consumer_.cached_head) return nullptr;
    }
    return slot(tail);
  }

  // Consumer only. Precondition: front() returned non-null.
  void pop() noexcept {
    const std::size_t tail = consumer_.tail.load(std::memory_order_relaxed);
    slot(tail)->~T();
    consumer_.tail.store(tail + 1, std::memory_order_release);
  }

  bool try_pop(T& out) noexcept(std::is_nothrow_move_assignable_v<T>) {
    T* p = front();
    if (p == nullptr) return false;
    out = std::move(*p);
    pop();
    return true;
  }

  std::size_t capacity() const noexcept { return capacity_; }

  // Safe from either side, but stale the moment it returns. Tail is loaded
  // first so the difference can't go negative.
  std::size_t size_approx() const noexcept {
    const std::size_t tail = consumer_.tail.load(std::memory_order_acquire);
    const std::size_t head = producer_.head.load(std::memory_order_acquire);
    return head - tail;
  }

  bool empty_approx() const noexcept { return size_approx() == 0; }

 private:
  struct Slot {
    alignas(T) std::byte storage[sizeof(T)];
  };

  struct alignas(kCacheLine) ProducerSide {
    std::atomic<std::size_t> head{0};
    std::size_t cached_tail = 0;
  };

  struct alignas(kCacheLine) ConsumerSide {
    std::atomic<std::size_t> tail{0};
    std::size_t cached_head = 0;
  };

  T* slot(std::size_t i) const noexcept {
    return std::launder(reinterpret_cast<T*>(slots_[i & mask_].storage));
  }

  const std::size_t capacity_;
  const std::size_t mask_;
  const std::unique_ptr<Slot[]> slots_;
  ProducerSide producer_;
  ConsumerSide consumer_;
};

// Baseline for the benchmarks: the obvious mutex + deque queue.
template <typename T>
class MutexQueue {
 public:
  explicit MutexQueue(std::size_t capacity) : capacity_(capacity) {}

  bool try_push(T v) {
    std::lock_guard<std::mutex> lock(mu_);
    if (q_.size() >= capacity_) return false;
    q_.push_back(std::move(v));
    return true;
  }

  bool try_pop(T& out) {
    std::lock_guard<std::mutex> lock(mu_);
    if (q_.empty()) return false;
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }

 private:
  const std::size_t capacity_;
  std::mutex mu_;
  std::deque<T> q_;
};

}  // namespace nmr
