#pragma once

#include "nmr/broker.hpp"
#include "nmr/frame_pool.hpp"
#include "nmr/socket.hpp"

#include <cstddef>
#include <deque>
#include <span>
#include <vector>

namespace nmr {

enum class IoStatus : std::uint8_t {
  kOk,
  kWouldBlock,
  kClosed,
  kError,
};

// A client socket plus its buffers. Owned and touched only by the IO thread,
// so there's no locking anywhere in here.
class Connection {
 public:
  Connection(ConnId id, Fd fd);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  ConnId id() const noexcept { return id_; }
  int fd() const noexcept { return fd_.get(); }

  // ---- input ----

  // One recv() straight into the input buffer. Grows the buffer if a single
  // frame doesn't fit.
  IoStatus read_some(std::size_t& bytes_read);
  std::span<const std::byte> pending_input() const noexcept {
    return {rbuf_.data() + rbegin_, rend_ - rbegin_};
  }
  void consume_input(std::size_t n) noexcept;

  // ---- output ----

  // Takes over one reference to f.
  void enqueue(Frame* f) noexcept;
  bool has_output() const noexcept { return !outq_.empty(); }
  std::size_t queued_bytes() const noexcept { return queued_bytes_; }

  // Gathers queued frames into sendmsg() calls until the queue drains or the
  // socket would block. Releases each frame once it's fully written.
  IoStatus flush(FramePool& pool, std::size_t& bytes_written);
  void discard_output(FramePool& pool) noexcept;

  // ---- IO-loop bookkeeping ----

  bool closing() const noexcept { return closing_; }
  void mark_closing() noexcept { closing_ = true; }
  bool dirty() const noexcept { return dirty_; }
  void set_dirty(bool v) noexcept { dirty_ = v; }
  bool read_pending() const noexcept { return read_pending_; }
  void set_read_pending(bool v) noexcept { read_pending_ = v; }

 private:
  static constexpr std::size_t kInitialReadBuffer = 64 * 1024;
  static constexpr std::size_t kMinReadSpace = 4 * 1024;
  static constexpr std::size_t kMaxIov = 64;

  void advance_output(FramePool& pool, std::size_t n) noexcept;

  const ConnId id_;
  Fd fd_;

  std::vector<std::byte> rbuf_;
  std::size_t rbegin_ = 0;
  std::size_t rend_ = 0;

  std::deque<Frame*> outq_;
  std::size_t head_offset_ = 0;  // bytes of outq_.front() already sent
  std::size_t queued_bytes_ = 0;

  bool closing_ = false;
  bool dirty_ = false;
  bool read_pending_ = false;
};

}  // namespace nmr
