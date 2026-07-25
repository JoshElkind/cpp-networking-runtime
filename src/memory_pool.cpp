#include "nmr/memory_pool.hpp"

namespace nmr {

MemoryPool::MemoryPool(std::size_t block_size, std::size_t blocks_per_slab)
    : block_size_(block_size), blocks_per_slab_(blocks_per_slab) {
  std::lock_guard<std::mutex> g(mu_);
  refill_locked();
}

MemoryPool::~MemoryPool() = default;

void MemoryPool::refill_locked() {
  auto slab = std::make_unique<std::byte[]>(block_size_ * blocks_per_slab_);
  std::byte* base = slab.get();
  for (std::size_t i = 0; i < blocks_per_slab_; ++i) {
    free_list_.push_back(base + i * block_size_);
  }
  slabs_.push_back(std::move(slab));
}

void* MemoryPool::allocate() {
  std::lock_guard<std::mutex> g(mu_);
  if (free_list_.empty()) refill_locked();
  void* p = free_list_.back();
  free_list_.pop_back();
  ++allocated_;
  return p;
}

void MemoryPool::deallocate(void* p) {
  if (!p) return;
  std::lock_guard<std::mutex> g(mu_);
  free_list_.push_back(p);
  --allocated_;
}

std::size_t MemoryPool::allocated_blocks() const {
  std::lock_guard<std::mutex> g(mu_);
  return allocated_;
}

std::size_t MemoryPool::free_blocks() const {
  std::lock_guard<std::mutex> g(mu_);
  return free_list_.size();
}

}  // namespace nmr
