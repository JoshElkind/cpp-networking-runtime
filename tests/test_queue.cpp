#include "test_macros.hpp"

#include "nmr/spsc_queue.hpp"

#include <thread>

TEST(spsc_basic) {
  nmr::SpscQueue<int> q(8);
  CHECK(q.try_push(1));
  CHECK(q.try_push(2));
  int v = 0;
  CHECK(q.try_pop(v) && v == 1);
  CHECK(q.try_pop(v) && v == 2);
  CHECK(!q.try_pop(v));
}

TEST(spsc_full) {
  nmr::SpscQueue<int> q(4);
  CHECK(q.try_push(1));
  CHECK(q.try_push(2));
  CHECK(q.try_push(3));
  CHECK(q.try_push(4));
  CHECK(!q.try_push(5));
  int v;
  CHECK(q.try_pop(v));
  CHECK(q.try_push(5));
}

TEST(spsc_threaded) {
  nmr::SpscQueue<int> q(1024);
  constexpr int n = 100000;
  std::thread prod([&] {
    for (int i = 0; i < n; ++i) {
      while (!q.try_push(i)) {
      }
    }
  });
  int sum = 0;
  std::thread cons([&] {
    int got = 0;
    int v;
    while (got < n) {
      if (q.try_pop(v)) {
        sum += v;
        ++got;
      }
    }
  });
  prod.join();
  cons.join();
  CHECK(sum == n * (n - 1) / 2);
}
