#include "test.hpp"

#include "nmr/frame_pool.hpp"

#include <cstdint>
#include <cstring>
#include <set>
#include <vector>

TEST(frame_pool_picks_smallest_fitting_class) {
  using P = nmr::FramePool;
  CHECK_EQ(int(P::size_class_for(1)), 0);
  CHECK_EQ(int(P::size_class_for(256)), 0);
  CHECK_EQ(int(P::size_class_for(257)), 1);
  CHECK_EQ(int(P::size_class_for(65536)), 4);
  CHECK_EQ(int(P::size_class_for(65537)), int(P::kHeapClass));
}

TEST(frame_pool_allocations_are_distinct_and_aligned) {
  nmr::FramePool pool(4096);
  std::vector<nmr::Frame*> frames;
  std::set<nmr::Frame*> uniq;
  for (int i = 0; i < 100; ++i) {
    nmr::Frame* f = pool.allocate(100);
    CHECK_EQ(f->refs, 1u);
    CHECK_EQ(f->size, 100u);
    CHECK(reinterpret_cast<std::uintptr_t>(f) % 16 == 0);
    std::memset(f->data(), i, f->size);  // ASan catches a block that's too small
    frames.push_back(f);
    uniq.insert(f);
  }
  CHECK_EQ(uniq.size(), frames.size());
  CHECK_EQ(pool.live(), 100u);
  for (auto* f : frames) pool.release(f);
  CHECK_EQ(pool.live(), 0u);
}

TEST(frame_pool_reuses_freed_blocks_lifo) {
  nmr::FramePool pool;
  nmr::Frame* a = pool.allocate(10);
  pool.release(a);
  nmr::Frame* b = pool.allocate(20);
  CHECK(a == b);
  pool.release(b);
  CHECK_EQ(pool.slab_count(), 1u);
}

TEST(frame_pool_refcount) {
  nmr::FramePool pool;
  nmr::Frame* f = pool.allocate(32);
  pool.retain(f);
  pool.retain(f);
  CHECK_EQ(f->refs, 3u);
  pool.release(f);
  pool.release(f);
  CHECK_EQ(pool.live(), 1u);
  pool.release(f);
  CHECK_EQ(pool.live(), 0u);
}

TEST(frame_pool_large_frames_use_heap) {
  nmr::FramePool pool;
  nmr::Frame* f = pool.allocate(200'000);
  CHECK(f->size_class == nmr::FramePool::kHeapClass);
  CHECK_EQ(pool.heap_allocations(), 1u);
  std::memset(f->data(), 0x5A, f->size);
  pool.release(f);
  CHECK_EQ(pool.live(), 0u);
}

TEST(frame_pool_grows_by_slab) {
  nmr::FramePool pool(65536);  // 256 blocks of the smallest class per slab
  std::vector<nmr::Frame*> frames;
  for (int i = 0; i < 1000; ++i) frames.push_back(pool.allocate(64));
  CHECK_EQ(pool.slab_count(), 4u);
  for (auto* f : frames) pool.release(f);
  for (int i = 0; i < 1000; ++i) frames[static_cast<std::size_t>(i)] = pool.allocate(64);
  CHECK_EQ(pool.slab_count(), 4u);  // recycled, no new slabs
  for (auto* f : frames) pool.release(f);
}

TEST(frame_topic_reads_header) {
  nmr::FramePool pool;
  const std::vector<std::byte> payload(3, std::byte{1});
  const auto wire = nmr::encode_frame(nmr::MsgType::kPublish, "md.AAPL", payload);
  nmr::Frame* f = pool.allocate(wire.size());
  std::memcpy(f->data(), wire.data(), wire.size());
  CHECK(f->topic() == "md.AAPL");
  pool.release(f);
}
