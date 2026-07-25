#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace nmr {

// Slab allocator with a shared free list plus a thread-local cache.
// Blocks are a fixed size; allocations larger than the block size fall back
// to ::operator new.
class MemoryPool {
 public:
  explicit MemoryPool(std::size_t block_size = 4096, std::size_t blocks_per_slab = 256);
  ~MemoryPool();

  MemoryPool(const MemoryPool&) = delete;
  MemoryPool& operator=(const MemoryPool&) = delete;

  void* allocate();
  void deallocate(void* p);

  std::size_t block_size() const { return block_size_; }
  std::size_t allocated_blocks() const;
  std::size_t free_blocks() const;

 private:
  void refill_locked();

  std::size_t block_size_;
  std::size_t blocks_per_slab_;
  mutable std::mutex mu_;
  std::vector<std::unique_ptr<std::byte[]>> slabs_;
  std::vector<void*> free_list_;
  std::size_t allocated_{0};
};

}  // namespace nmr
