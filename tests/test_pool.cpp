#include "test.hpp"

#include "nmr/memory_pool.hpp"

#include <set>
#include <vector>

TEST(pool_alloc_free_roundtrip) {
  nmr::MemoryPool pool(64, 8);
  std::vector<void*> ptrs;
  for (int i = 0; i < 40; ++i) ptrs.push_back(pool.allocate());
  std::set<void*> uniq(ptrs.begin(), ptrs.end());
  CHECK(uniq.size() == ptrs.size());
  for (void* p : ptrs) pool.deallocate(p);
  CHECK(pool.allocated_blocks() == 0);
}

TEST(pool_reuse) {
  nmr::MemoryPool pool(32, 4);
  void* a = pool.allocate();
  pool.deallocate(a);
  void* b = pool.allocate();
  CHECK(a == b);
  pool.deallocate(b);
}
