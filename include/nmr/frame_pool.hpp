#pragma once

#include "nmr/protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace nmr {

// One wire frame in pooled storage. The bytes live directly after this header
// in the same block, so a frame is a single allocation and a single pointer,
// and the same Frame* can sit in many subscribers' write queues at once.
struct alignas(16) Frame {
  std::uint32_t refs;
  std::uint32_t size;
  std::uint8_t size_class;

  std::byte* data() noexcept { return reinterpret_cast<std::byte*>(this + 1); }
  const std::byte* data() const noexcept { return reinterpret_cast<const std::byte*>(this + 1); }
  std::span<const std::byte> wire() const noexcept { return {data(), size}; }

  // Only meaningful once data() holds a frame that passed decode_frame().
  std::string_view topic() const noexcept;
};

static_assert(sizeof(Frame) == 16);

// Size-class slab allocator for Frames.
//
// Deliberately single-threaded. The IO thread allocates every frame and is the
// only thread that touches refcounts: workers never release a frame directly,
// they hand it back through their egress queue. That keeps refcounting to a
// plain increment and the free lists to intrusive singly-linked stacks.
class FramePool {
 public:
  // Block sizes include the 16-byte Frame header. Anything larger than the
  // last class goes to the heap.
  static constexpr std::array<std::size_t, 5> kBlockSizes = {256, 1024, 4096, 16384, 65536};
  static constexpr std::uint8_t kHeapClass = 0xff;

  explicit FramePool(std::size_t slab_bytes = 1u << 20);
  ~FramePool();

  FramePool(const FramePool&) = delete;
  FramePool& operator=(const FramePool&) = delete;

  // Returns a frame with refs == 1 and room for `size` bytes.
  Frame* allocate(std::size_t size);

  void retain(Frame* f) noexcept { ++f->refs; }
  void release(Frame* f) noexcept;

  std::size_t live() const noexcept { return live_; }
  std::size_t slab_count() const noexcept { return slabs_.size(); }
  std::size_t heap_allocations() const noexcept { return heap_allocations_; }

  static std::uint8_t size_class_for(std::size_t bytes) noexcept;

 private:
  struct FreeBlock {
    FreeBlock* next;
  };

  struct Slab {
    std::byte* mem;
    std::size_t bytes;
  };

  void refill(std::uint8_t cls);

  const std::size_t slab_bytes_;
  std::array<FreeBlock*, kBlockSizes.size()> free_{};
  std::vector<Slab> slabs_;
  std::size_t live_ = 0;
  std::size_t heap_allocations_ = 0;
};

}  // namespace nmr
