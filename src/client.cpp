#include "nmr/client.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <system_error>

#include <poll.h>
#include <sys/socket.h>

namespace nmr {

using std::chrono::milliseconds;
using steady = std::chrono::steady_clock;

Client::Client(const std::string& host, std::uint16_t port)
    : fd_(connect_tcp(host, port)), rbuf_(64 * 1024) {}

void Client::subscribe(std::string_view topic) { send_frame(MsgType::kSubscribe, topic, {}); }

void Client::unsubscribe(std::string_view topic) { send_frame(MsgType::kUnsubscribe, topic, {}); }

void Client::publish(std::string_view topic, std::span<const std::byte> payload) {
  send_frame(MsgType::kPublish, topic, payload);
}

void Client::send_frame(MsgType type, std::string_view topic, std::span<const std::byte> payload) {
  wbuf_.resize(kHeaderSize + topic.size() + payload.size());
  if (encode_frame(wbuf_, type, topic, payload) == 0) {
    throw std::length_error("nmr: frame exceeds protocol limits");
  }
  send_raw(wbuf_);
}

void Client::send_raw(std::span<const std::byte> bytes) {
  std::size_t off = 0;
  while (off < bytes.size()) {
    const ssize_t n = ::send(fd_.get(), bytes.data() + off, bytes.size() - off, kSendFlags);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::system_error(errno, std::generic_category(), "send");
    }
    off += static_cast<std::size_t>(n);
  }
}

std::optional<FrameView> Client::receive(milliseconds timeout) {
  const auto deadline = steady::now() + timeout;
  for (;;) {
    FrameView frame;
    const auto pending = std::span<const std::byte>(rbuf_.data() + rbegin_, rend_ - rbegin_);
    const DecodeStatus st = decode_frame(pending, frame);
    if (st == DecodeStatus::kOk) {
      rbegin_ += frame.wire.size();
      return frame;
    }
    if (st != DecodeStatus::kNeedMore) {
      throw std::runtime_error(std::string("nmr: bad frame from server: ") + to_string(st));
    }
    if (closed_) return std::nullopt;

    // Make room. Anything before rbegin_ belongs to frames already handed out,
    // which the contract says are dead now.
    if (rbegin_ > 0) {
      std::memmove(rbuf_.data(), rbuf_.data() + rbegin_, rend_ - rbegin_);
      rend_ -= rbegin_;
      rbegin_ = 0;
    }
    if (rend_ == rbuf_.size()) rbuf_.resize(rbuf_.size() * 2);

    const auto left = std::chrono::duration_cast<milliseconds>(deadline - steady::now());
    if (left.count() < 0) return std::nullopt;
    pollfd pfd{fd_.get(), POLLIN, 0};
    const int rc = ::poll(&pfd, 1, static_cast<int>(left.count()));
    if (rc < 0) {
      if (errno == EINTR) continue;
      throw std::system_error(errno, std::generic_category(), "poll");
    }
    if (rc == 0) return std::nullopt;

    const ssize_t n = ::recv(fd_.get(), rbuf_.data() + rend_, rbuf_.size() - rend_, 0);
    if (n > 0) {
      rend_ += static_cast<std::size_t>(n);
    } else if (n == 0 || errno == ECONNRESET) {
      closed_ = true;
    } else if (errno != EINTR && errno != EAGAIN) {
      throw std::system_error(errno, std::generic_category(), "recv");
    }
  }
}

bool Client::ping(milliseconds timeout) {
  const std::uint64_t seq = ++ping_seq_;
  std::byte nonce[sizeof(seq)];
  std::memcpy(nonce, &seq, sizeof(seq));
  send_frame(MsgType::kHeartbeat, {}, nonce);

  const auto deadline = steady::now() + timeout;
  for (;;) {
    const auto left = std::chrono::duration_cast<milliseconds>(deadline - steady::now());
    if (left.count() <= 0) return false;
    const auto frame = receive(left);
    if (!frame) return false;
    if (frame->header.type == MsgType::kHeartbeat && frame->payload.size() == sizeof(seq) &&
        std::memcmp(frame->payload.data(), nonce, sizeof(seq)) == 0) {
      return true;
    }
  }
}

void Client::set_receive_buffer(int bytes) {
  if (::setsockopt(fd_.get(), SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) < 0) {
    throw std::system_error(errno, std::generic_category(), "setsockopt(SO_RCVBUF)");
  }
}

}  // namespace nmr
