#include "nmr/socket.hpp"

#include <cerrno>
#include <system_error>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

namespace nmr {
namespace {

[[noreturn]] void throw_errno(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

sockaddr_in make_addr(const std::string& host, std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    throw std::system_error(std::make_error_code(std::errc::invalid_argument),
                            "invalid IPv4 address: " + host);
  }
  return addr;
}

void set_int_opt(int fd, int level, int opt, int value, const char* what) {
  if (::setsockopt(fd, level, opt, &value, sizeof(value)) < 0) throw_errno(what);
}

}  // namespace

void Fd::reset(int fd) noexcept {
  if (fd_ >= 0) ::close(fd_);
  fd_ = fd;
}

Fd listen_tcp(const std::string& host, std::uint16_t port, int backlog) {
  const sockaddr_in addr = make_addr(host, port);
  Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
  if (!fd) throw_errno("socket");
  set_int_opt(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1, "setsockopt(SO_REUSEADDR)");
  if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
    throw_errno("bind");
  }
  if (::listen(fd.get(), backlog) < 0) throw_errno("listen");
  set_nonblocking(fd.get());
  return fd;
}

Fd connect_tcp(const std::string& host, std::uint16_t port) {
  const sockaddr_in addr = make_addr(host, port);
  Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
  if (!fd) throw_errno("socket");
  int rc;
  do {
    rc = ::connect(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) throw_errno("connect");
  set_nodelay(fd.get());
  set_nosigpipe(fd.get());
  return fd;
}

std::uint16_t local_port(int fd) {
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) throw_errno("getsockname");
  return ntohs(addr.sin_port);
}

void set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) throw_errno("fcntl(O_NONBLOCK)");
}

void set_nodelay(int fd) {
  set_int_opt(fd, IPPROTO_TCP, TCP_NODELAY, 1, "setsockopt(TCP_NODELAY)");
}

void set_nosigpipe(int fd) {
#if defined(SO_NOSIGPIPE)
  set_int_opt(fd, SOL_SOCKET, SO_NOSIGPIPE, 1, "setsockopt(SO_NOSIGPIPE)");
#else
  (void)fd;
#endif
}

}  // namespace nmr
