#include "nmr/connection.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cstring>

#include <sys/socket.h>
#include <sys/uio.h>

namespace nmr {

Connection::Connection(ConnId id, Fd fd)
    : id_(id), fd_(std::move(fd)), rbuf_(kInitialReadBuffer) {}

Connection::~Connection() { assert(outq_.empty() && "discard_output() must run before destruction"); }

IoStatus Connection::read_some(std::size_t& bytes_read) {
  bytes_read = 0;
  if (rbuf_.size() - rend_ < kMinReadSpace) {
    const std::size_t pending = rend_ - rbegin_;
    if (rbegin_ > 0) {
      std::memmove(rbuf_.data(), rbuf_.data() + rbegin_, pending);
      rbegin_ = 0;
      rend_ = pending;
    }
    // Still short on space means one frame is bigger than the buffer.
    if (rbuf_.size() - rend_ < kMinReadSpace) {
      rbuf_.resize(std::min(rbuf_.size() * 2, kMaxFrameSize + kMinReadSpace));
    }
  }

  for (;;) {
    const ssize_t n = ::recv(fd_.get(), rbuf_.data() + rend_, rbuf_.size() - rend_, 0);
    if (n > 0) {
      rend_ += static_cast<std::size_t>(n);
      bytes_read = static_cast<std::size_t>(n);
      return IoStatus::kOk;
    }
    if (n == 0) return IoStatus::kClosed;
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return IoStatus::kWouldBlock;
    return IoStatus::kError;
  }
}

void Connection::consume_input(std::size_t n) noexcept {
  rbegin_ += n;
  if (rbegin_ == rend_) rbegin_ = rend_ = 0;
}

void Connection::enqueue(Frame* f) noexcept {
  outq_.push_back(f);
  queued_bytes_ += f->size;
}

IoStatus Connection::flush(FramePool& pool, std::size_t& bytes_written) {
  bytes_written = 0;
  std::array<iovec, kMaxIov> iov;

  while (!outq_.empty()) {
    std::size_t n = 0;
    for (auto it = outq_.begin(); it != outq_.end() && n < kMaxIov; ++it, ++n) {
      const std::size_t skip = n == 0 ? head_offset_ : 0;
      iov[n].iov_base = const_cast<std::byte*>((*it)->data() + skip);
      iov[n].iov_len = (*it)->size - skip;
    }

    msghdr msg{};
    msg.msg_iov = iov.data();
    msg.msg_iovlen = static_cast<decltype(msg.msg_iovlen)>(n);
    const ssize_t sent = ::sendmsg(fd_.get(), &msg, kSendFlags);
    if (sent < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) return IoStatus::kWouldBlock;
      return IoStatus::kError;
    }
    bytes_written += static_cast<std::size_t>(sent);
    advance_output(pool, static_cast<std::size_t>(sent));
  }
  return IoStatus::kOk;
}

void Connection::advance_output(FramePool& pool, std::size_t n) noexcept {
  queued_bytes_ -= n;
  while (n > 0) {
    Frame* f = outq_.front();
    const std::size_t left = f->size - head_offset_;
    if (n < left) {
      head_offset_ += n;
      return;
    }
    n -= left;
    head_offset_ = 0;
    outq_.pop_front();
    pool.release(f);
  }
}

void Connection::discard_output(FramePool& pool) noexcept {
  for (Frame* f : outq_) pool.release(f);
  outq_.clear();
  head_offset_ = 0;
  queued_bytes_ = 0;
}

}  // namespace nmr
