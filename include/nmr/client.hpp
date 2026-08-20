#pragma once

#include "nmr/protocol.hpp"
#include "nmr/socket.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace nmr {

// Blocking client for tests, tools and benchmarks. Not thread-safe; one
// thread may send while another receives only if they never share a call.
class Client {
 public:
  Client(const std::string& host, std::uint16_t port);

  void subscribe(std::string_view topic);
  void unsubscribe(std::string_view topic);
  void publish(std::string_view topic, std::span<const std::byte> payload);
  void send_raw(std::span<const std::byte> bytes);

  // Waits up to `timeout` for the next frame. The returned views point into
  // the client's buffer and are valid until the next receive()/ping().
  // Returns nullopt on timeout or when the server closes the connection; check
  // closed() to tell them apart. Throws if the server sends garbage.
  std::optional<FrameView> receive(std::chrono::milliseconds timeout);

  // Heartbeat round-trip. Any SUBSCRIBE/UNSUBSCRIBE sent before ping() is
  // routed ahead of PUBLISHes the broker reads after ping() returns.
  // Discards any other frames that arrive in the meantime.
  bool ping(std::chrono::milliseconds timeout);

  bool closed() const noexcept { return closed_; }
  int fd() const noexcept { return fd_.get(); }
  void set_receive_buffer(int bytes);

 private:
  void send_frame(MsgType type, std::string_view topic, std::span<const std::byte> payload);

  Fd fd_;
  std::vector<std::byte> wbuf_;
  std::vector<std::byte> rbuf_;
  std::size_t rbegin_ = 0;
  std::size_t rend_ = 0;
  std::uint64_t ping_seq_ = 0;
  bool closed_ = false;
};

}  // namespace nmr
