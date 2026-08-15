#pragma once

#include <cstdint>
#include <span>
#include <vector>

#if defined(__linux__)
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif

namespace nmr {

struct PollEvent {
  std::uint64_t token = 0;
  bool readable = false;
  bool writable = false;
  bool closed = false;  // peer hung up or the socket is in error
};

// Edge-triggered readiness over epoll (Linux) or kqueue (macOS / BSD).
//
// Every fd is registered for read and write readiness once, up front. With
// edge-triggering, write readiness only fires when a full send buffer drains,
// so the IO loop never has to toggle EPOLLOUT interest on the hot path. The
// flip side: callers must read and write until EAGAIN, or they lose the edge.
//
// Events carry a caller-chosen token rather than a pointer, so a late event
// for a connection that's already gone just fails a map lookup.
class Poller {
 public:
  explicit Poller(int max_events = 256);
  ~Poller();

  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  void add(int fd, std::uint64_t token, bool want_write = true);
  void remove(int fd) noexcept;

  // timeout_ms < 0 blocks indefinitely. The span is valid until the next call.
  std::span<const PollEvent> wait(int timeout_ms);

 private:
  int fd_ = -1;
#if defined(__linux__)
  std::vector<epoll_event> raw_;
#else
  std::vector<struct kevent> raw_;
#endif
  std::vector<PollEvent> events_;
};

// Lets other threads interrupt Poller::wait(). eventfd on Linux, a pipe
// elsewhere. wake() is async-signal-safe and cheap to call redundantly.
class Waker {
 public:
  Waker();
  ~Waker();

  Waker(const Waker&) = delete;
  Waker& operator=(const Waker&) = delete;

  int fd() const noexcept { return read_fd_; }
  void wake() noexcept;
  void drain() noexcept;

 private:
  int read_fd_ = -1;
  int write_fd_ = -1;
};

}  // namespace nmr
