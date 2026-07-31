#pragma once

#include "nmr/config.hpp"
#include "nmr/protocol.hpp"

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace nmr {

inline int connect_tcp(std::uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket");
  int yes = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("connect");
  }
  timeval tv{};
  tv.tv_sec = 0;
  tv.tv_usec = 200000;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  return fd;
}

inline void send_all(int fd, const std::vector<std::byte>& buf) {
  std::size_t off = 0;
  while (off < buf.size()) {
    ssize_t n = ::send(fd, buf.data() + off, buf.size() - off, 0);
    if (n <= 0) throw std::runtime_error("send");
    off += static_cast<std::size_t>(n);
  }
}

inline std::vector<std::byte> recv_frame(int fd) {
  FrameHeader hdr{};
  std::size_t got = 0;
  auto* hp = reinterpret_cast<std::byte*>(&hdr);
  while (got < kHeaderSize) {
    ssize_t n = ::recv(fd, hp + got, kHeaderSize - got, 0);
    if (n <= 0) throw std::runtime_error("recv header");
    got += static_cast<std::size_t>(n);
  }
  if (hdr.magic != kMagic) throw std::runtime_error("bad magic");
  std::vector<std::byte> rest(hdr.topic_len + hdr.payload_len);
  got = 0;
  while (got < rest.size()) {
    ssize_t n = ::recv(fd, rest.data() + got, rest.size() - got, 0);
    if (n <= 0) throw std::runtime_error("recv body");
    got += static_cast<std::size_t>(n);
  }
  std::vector<std::byte> out(kHeaderSize + rest.size());
  std::memcpy(out.data(), &hdr, kHeaderSize);
  if (!rest.empty()) std::memcpy(out.data() + kHeaderSize, rest.data(), rest.size());
  return out;
}

}  // namespace nmr
