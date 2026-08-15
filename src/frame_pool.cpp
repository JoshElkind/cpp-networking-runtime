#include "nmr/frame_pool.hpp"

#include <algorithm>
#include <cassert>
#include <new>

namespace nmr {
namespace {

constexpr std::align_val_t kSlabAlign{kCacheLine};

}  // namespace

std::string_view Frame::topic() const noexcept {
  FrameHeader h;
  if (decode_header(wire(), h) != DecodeStatus::kOk) return {};
  return {reinterpret_cast<const char*>(data() + kHeaderSize), h.topic_len};
}

FramePool::FramePool(std::size_t slab_bytes)
    : slab_bytes_(std::max(slab_bytes, kBlockSizes.back())) {}

FramePool::~FramePool() {
  for (const Slab& s : slabs_) ::operator delete(s.mem, kSlabAlign);
}

std::uint8_t FramePool::size_class_for(std::size_t bytes) noexcept {
  for (std::size_t i = 0; i < kBlockSizes.size(); ++i) {
    if (bytes <= kBlockSizes[i]) return static_cast<std::uint8_t>(i);
  }
  return kHeapClass;
}

void FramePool::refill(std::uint8_t cls) {
  const std::size_t block = kBlockSizes[cls];
  const std::size_t count = slab_bytes_ / block;
  auto* mem = static_cast<std::byte*>(::operator new(count * block, kSlabAlign));
  slabs_.push_back({mem, count * block});

  // Thread in reverse so the first allocations walk the slab front to back.
  FreeBlock* head = free_[cls];
  for (std::size_t i = count; i-- > 0;) {
    auto* b = reinterpret_cast<FreeBlock*>(mem + i * block);
    b->next = head;
    head = b;
  }
  free_[cls] = head;
}

Frame* FramePool::allocate(std::size_t size) {
  const std::size_t total = sizeof(Frame) + size;
  const std::uint8_t cls = size_class_for(total);

  void* mem;
  if (cls == kHeapClass) {
    mem = ::operator new(total, kSlabAlign);
    ++heap_allocations_;
  } else {
    if (free_[cls] == nullptr) refill(cls);
    FreeBlock* b = free_[cls];
    free_[cls] = b->next;
    mem = b;
  }

  ++live_;
  return ::new (mem) Frame{1, static_cast<std::uint32_t>(size), cls};
}

void FramePool::release(Frame* f) noexcept {
  assert(f->refs > 0);
  if (--f->refs != 0) return;

  --live_;
  const std::uint8_t cls = f->size_class;
  f->~Frame();
  if (cls == kHeapClass) {
    ::operator delete(static_cast<void*>(f), kSlabAlign);
    return;
  }
  auto* b = reinterpret_cast<FreeBlock*>(f);
  b->next = free_[cls];
  free_[cls] = b;
}

}  // namespace nmr
