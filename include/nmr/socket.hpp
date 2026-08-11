#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include <sys/socket.h>

namespace nmr {

// Owning file descriptor.
class Fd {
 public:
  Fd() noexcept = default;
  explicit Fd(int fd) noexcept : fd_(fd) {}
  ~Fd() { reset(); }

  Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  Fd& operator=(Fd&& other) noexcept {
    if (this != &other) reset(std::exchange(other.fd_, -1));
    return *this;
  }
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;

  int get() const noexcept { return fd_; }
  explicit operator bool() const noexcept { return fd_ >= 0; }
  int release() noexcept { return std::exchange(fd_, -1); }
  void reset(int fd = -1) noexcept;

 private:
  int fd_ = -1;
};

// Linux has no SO_NOSIGPIPE, so suppress SIGPIPE per call instead.
#if defined(MSG_NOSIGNAL)
inline constexpr int kSendFlags = MSG_NOSIGNAL;
#else
inline constexpr int kSendFlags = 0;
#endif

// IPv4 only. Throw std::system_error on failure.
Fd listen_tcp(const std::string& host, std::uint16_t port, int backlog = 1024);
Fd connect_tcp(const std::string& host, std::uint16_t port);
std::uint16_t local_port(int fd);

void set_nonblocking(int fd);
void set_nodelay(int fd);
void set_nosigpipe(int fd);

}  // namespace nmr
