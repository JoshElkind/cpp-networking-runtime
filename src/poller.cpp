#include "nmr/poller.hpp"

#include <cerrno>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace nmr {

Poller::Poller() {
#ifdef __linux__
  fd_ = ::epoll_create1(0);
#else
  fd_ = ::kqueue();
#endif
  if (fd_ < 0) throw std::runtime_error("failed to create poller");
}

Poller::~Poller() {
  if (fd_ >= 0) ::close(fd_);
}

void Poller::add(int fd, void* data, bool want_read, bool want_write) {
#ifdef __linux__
  epoll_event ev{};
  ev.events = EPOLLET;
  if (want_read) ev.events |= EPOLLIN;
  if (want_write) ev.events |= EPOLLOUT;
  ev.data.ptr = data;
  if (::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
    throw std::runtime_error("epoll_ctl add failed");
  }
#else
  struct kevent ke[2];
  int n = 0;
  if (want_read) {
    EV_SET(&ke[n++], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, data);
  }
  if (want_write) {
    EV_SET(&ke[n++], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, data);
  }
  if (n && ::kevent(fd_, ke, n, nullptr, 0, nullptr) < 0) {
    throw std::runtime_error("kevent add failed");
  }
#endif
}

void Poller::mod(int fd, void* data, bool want_read, bool want_write) {
#ifdef __linux__
  epoll_event ev{};
  ev.events = EPOLLET;
  if (want_read) ev.events |= EPOLLIN;
  if (want_write) ev.events |= EPOLLOUT;
  ev.data.ptr = data;
  if (::epoll_ctl(fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
    throw std::runtime_error("epoll_ctl mod failed");
  }
#else
  struct kevent ke[4];
  int n = 0;
  EV_SET(&ke[n++], fd, EVFILT_READ, want_read ? (EV_ADD | EV_CLEAR) : EV_DELETE, 0, 0, data);
  EV_SET(&ke[n++], fd, EVFILT_WRITE, want_write ? (EV_ADD | EV_CLEAR) : EV_DELETE, 0, 0, data);
  ::kevent(fd_, ke, n, nullptr, 0, nullptr);
  (void)fd;
#endif
}

void Poller::del(int fd) {
#ifdef __linux__
  ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
#else
  struct kevent ke[2];
  EV_SET(&ke[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  EV_SET(&ke[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, ke, 2, nullptr, 0, nullptr);
#endif
}

int Poller::wait(PollEvent* out, int max_events, int timeout_ms) {
#ifdef __linux__
  std::vector<epoll_event> ev(static_cast<std::size_t>(max_events));
  int n = ::epoll_wait(fd_, ev.data(), max_events, timeout_ms);
  if (n < 0) {
    if (errno == EINTR) return 0;
    throw std::runtime_error("epoll_wait failed");
  }
  for (int i = 0; i < n; ++i) {
    out[i].data = ev[i].data.ptr;
    out[i].readable = ev[i].events & (EPOLLIN | EPOLLRDHUP | EPOLLERR);
    out[i].writable = ev[i].events & EPOLLOUT;
    out[i].hangup = ev[i].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR);
  }
  return n;
#else
  std::vector<struct kevent> ev(static_cast<std::size_t>(max_events));
  timespec ts;
  timespec* tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
    tsp = &ts;
  }
  int n = ::kevent(fd_, nullptr, 0, ev.data(), max_events, tsp);
  if (n < 0) {
    if (errno == EINTR) return 0;
    throw std::runtime_error("kevent wait failed");
  }
  for (int i = 0; i < n; ++i) {
    out[i].data = ev[i].udata;
    out[i].readable = ev[i].filter == EVFILT_READ;
    out[i].writable = ev[i].filter == EVFILT_WRITE;
    out[i].hangup = ev[i].flags & (EV_EOF | EV_ERROR);
  }
  return n;
#endif
}

int set_nonblock(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int set_nodelay(int fd) {
  int yes = 1;
  return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
}

int set_reuseaddr(int fd) {
  int yes = 1;
  return ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
}

}  // namespace nmr
