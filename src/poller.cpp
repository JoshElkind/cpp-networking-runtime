#include "nmr/poller.hpp"

#include "nmr/socket.hpp"

#include <cerrno>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

namespace nmr {
namespace {

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

}  // namespace

#if defined(__linux__)

Poller::Poller(int max_events)
    : fd_(::epoll_create1(EPOLL_CLOEXEC)),
      raw_(static_cast<std::size_t>(max_events)),
      events_(static_cast<std::size_t>(max_events)) {
  if (fd_ < 0) throw_errno("epoll_create1");
}

Poller::~Poller() { ::close(fd_); }

void Poller::add(int fd, std::uint64_t token, bool want_write) {
  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
  if (want_write) ev.events |= EPOLLOUT;
  ev.data.u64 = token;
  if (::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev) < 0) throw_errno("epoll_ctl(ADD)");
}

void Poller::remove(int fd) noexcept { ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr); }

std::span<const PollEvent> Poller::wait(int timeout_ms) {
  const int n = ::epoll_wait(fd_, raw_.data(), static_cast<int>(raw_.size()), timeout_ms);
  if (n < 0) {
    if (errno == EINTR) return {};
    throw_errno("epoll_wait");
  }
  for (int i = 0; i < n; ++i) {
    const epoll_event& r = raw_[static_cast<std::size_t>(i)];
    PollEvent& e = events_[static_cast<std::size_t>(i)];
    e.token = r.data.u64;
    e.readable = (r.events & (EPOLLIN | EPOLLRDHUP)) != 0;
    e.writable = (r.events & EPOLLOUT) != 0;
    e.closed = (r.events & (EPOLLHUP | EPOLLERR)) != 0;
  }
  return {events_.data(), static_cast<std::size_t>(n)};
}

Waker::Waker() {
  read_fd_ = write_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (read_fd_ < 0) throw_errno("eventfd");
}

Waker::~Waker() { ::close(read_fd_); }

void Waker::wake() noexcept {
  const std::uint64_t one = 1;
  [[maybe_unused]] auto rc = ::write(write_fd_, &one, sizeof(one));
}

void Waker::drain() noexcept {
  std::uint64_t v;
  while (::read(read_fd_, &v, sizeof(v)) > 0) {
  }
}

#else  // kqueue

Poller::Poller(int max_events)
    : fd_(::kqueue()),
      raw_(static_cast<std::size_t>(max_events)),
      events_(static_cast<std::size_t>(max_events)) {
  if (fd_ < 0) throw_errno("kqueue");
}

Poller::~Poller() { ::close(fd_); }

void Poller::add(int fd, std::uint64_t token, bool want_write) {
  struct kevent changes[2];
  void* udata = reinterpret_cast<void*>(static_cast<std::uintptr_t>(token));
  EV_SET(&changes[0], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, udata);
  EV_SET(&changes[1], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, udata);
  if (::kevent(fd_, changes, want_write ? 2 : 1, nullptr, 0, nullptr) < 0) {
    throw_errno("kevent(EV_ADD)");
  }
}

void Poller::remove(int fd) noexcept {
  // The write filter goes last because it may never have been added; kevent
  // stops at the first failing change, and by then the read filter is gone.
  struct kevent changes[2];
  EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, changes, 2, nullptr, 0, nullptr);
}

std::span<const PollEvent> Poller::wait(int timeout_ms) {
  timespec ts{};
  timespec* tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1'000'000L;
    tsp = &ts;
  }
  const int n = ::kevent(fd_, nullptr, 0, raw_.data(), static_cast<int>(raw_.size()), tsp);
  if (n < 0) {
    if (errno == EINTR) return {};
    throw_errno("kevent");
  }
  for (int i = 0; i < n; ++i) {
    const struct kevent& r = raw_[static_cast<std::size_t>(i)];
    PollEvent& e = events_[static_cast<std::size_t>(i)];
    e.token = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(r.udata));
    e.readable = r.filter == EVFILT_READ;
    e.writable = r.filter == EVFILT_WRITE;
    e.closed = (r.flags & EV_ERROR) != 0 || (r.filter == EVFILT_WRITE && (r.flags & EV_EOF));
  }
  return {events_.data(), static_cast<std::size_t>(n)};
}

Waker::Waker() {
  int fds[2];
  if (::pipe(fds) < 0) throw_errno("pipe");
  read_fd_ = fds[0];
  write_fd_ = fds[1];
  for (int fd : fds) {
    set_nonblocking(fd);
    ::fcntl(fd, F_SETFD, FD_CLOEXEC);
  }
}

Waker::~Waker() {
  ::close(read_fd_);
  ::close(write_fd_);
}

void Waker::wake() noexcept {
  const char b = 1;
  [[maybe_unused]] auto rc = ::write(write_fd_, &b, 1);
}

void Waker::drain() noexcept {
  char buf[64];
  while (::read(read_fd_, buf, sizeof(buf)) > 0) {
  }
}

#endif

}  // namespace nmr
