#include "test.hpp"

#include "nmr/spsc_queue.hpp"

#include <cstdint>
#include <memory>
#include <thread>

TEST(spsc_push_pop_fifo) {
  nmr::SpscQueue<int> q(8);
  CHECK(q.try_push(1));
  CHECK(q.try_push(2));
  int v = 0;
  CHECK(q.try_pop(v) && v == 1);
  CHECK(q.try_pop(v) && v == 2);
  CHECK(!q.try_pop(v));
}

TEST(spsc_capacity_rounds_up_to_power_of_two) {
  CHECK_EQ(nmr::SpscQueue<int>(5).capacity(), 8u);
  CHECK_EQ(nmr::SpscQueue<int>(8).capacity(), 8u);
  CHECK_EQ(nmr::SpscQueue<int>(0).capacity(), 2u);
}

TEST(spsc_uses_every_slot_and_reports_full) {
  nmr::SpscQueue<int> q(4);
  for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
  CHECK(!q.try_push(99));
  CHECK_EQ(q.size_approx(), 4u);
  int v = -1;
  CHECK(q.try_pop(v) && v == 0);
  CHECK(q.try_push(4));
  for (int expect = 1; expect <= 4; ++expect) CHECK(q.try_pop(v) && v == expect);
  CHECK(q.empty_approx());
}

TEST(spsc_wraps_many_times) {
  nmr::SpscQueue<std::uint64_t> q(4);
  std::uint64_t v = 0;
  for (std::uint64_t i = 0; i < 10'000; ++i) {
    REQUIRE(q.try_push(i));
    REQUIRE(q.try_pop(v));
    REQUIRE(v == i);
  }
}

TEST(spsc_front_pop_peek) {
  nmr::SpscQueue<int> q(4);
  CHECK(q.front() == nullptr);
  q.try_push(7);
  int* p = q.front();
  REQUIRE(p != nullptr);
  CHECK_EQ(*p, 7);
  CHECK(q.front() == p);  // peek doesn't consume
  q.pop();
  CHECK(q.front() == nullptr);
}

TEST(spsc_move_only_type) {
  nmr::SpscQueue<std::unique_ptr<int>> q(4);
  CHECK(q.try_push(std::make_unique<int>(42)));
  CHECK(q.try_emplace(new int(43)));
  std::unique_ptr<int> out;
  CHECK(q.try_pop(out) && *out == 42);
  CHECK(q.try_pop(out) && *out == 43);
}

namespace {
struct Tracked {
  static inline int alive = 0;
  Tracked() noexcept { ++alive; }
  Tracked(Tracked&&) noexcept { ++alive; }
  Tracked& operator=(Tracked&&) noexcept = default;
  ~Tracked() { --alive; }
};
}  // namespace

TEST(spsc_destroys_remaining_elements) {
  Tracked::alive = 0;
  {
    nmr::SpscQueue<Tracked> q(8);
    for (int i = 0; i < 5; ++i) q.try_emplace();
    Tracked out;
    q.try_pop(out);
    CHECK_EQ(Tracked::alive, 5);  // 4 queued + out
  }
  CHECK_EQ(Tracked::alive, 0);
}

TEST(spsc_two_threads_preserve_order) {
  constexpr std::uint64_t kN = 2'000'000;
  nmr::SpscQueue<std::uint64_t> q(1024);
  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kN; ++i) {
      while (!q.try_push(i)) {
      }
    }
  });

  std::uint64_t expect = 0;
  bool in_order = true;
  while (expect < kN) {
    std::uint64_t v;
    if (!q.try_pop(v)) continue;
    if (v != expect) in_order = false;
    ++expect;
  }
  producer.join();
  CHECK(in_order);
  CHECK(q.empty_approx());
}

TEST(mutex_queue_basic) {
  nmr::MutexQueue<int> q(2);
  CHECK(q.try_push(1));
  CHECK(q.try_push(2));
  CHECK(!q.try_push(3));
  int v = 0;
  CHECK(q.try_pop(v) && v == 1);
}
