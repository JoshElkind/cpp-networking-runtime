#include "nmr/server.hpp"

#include "nmr/config.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nmr {
namespace {

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

int listen_tcp(std::uint16_t port, std::uint16_t& bound) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket");
  set_reuseaddr(fd);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("bind");
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
    bound = ntohs(addr.sin_port);
  } else {
    bound = port;
  }
  if (::listen(fd, 512) < 0) {
    ::close(fd);
    throw std::runtime_error("listen");
  }
  set_nonblock(fd);
  return fd;
}

}  // namespace

Connection::Connection(int fd) : fd_(fd) {
  in_buf_.reserve(8192);
  out_buf_.reserve(8192);
}

Connection::~Connection() {
  if (fd_ >= 0) ::close(fd_);
}

bool Connection::backpressured() const {
  std::lock_guard<std::mutex> g(out_mu_);
  return write_bytes_ >= kWriteHighWatermark;
}

std::vector<Connection::Parsed> Connection::take_messages() {
  auto out = std::move(parsed_);
  parsed_.clear();
  return out;
}

bool Connection::enqueue_out(std::vector<std::byte> frame) {
  std::lock_guard<std::mutex> g(out_mu_);
  if (write_bytes_ >= kWriteHighWatermark) return false;
  out_buf_.insert(out_buf_.end(), frame.begin(), frame.end());
  write_bytes_ += frame.size();
  want_write_ = true;
  return true;
}

bool Connection::flush_writes() {
  std::lock_guard<std::mutex> g(out_mu_);
  while (!out_buf_.empty()) {
    ssize_t n = ::send(fd_, out_buf_.data(), out_buf_.size(),
#ifdef MSG_NOSIGNAL
                       MSG_NOSIGNAL
#else
                       0
#endif
    );
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
      return false;
    }
    if (n == 0) return false;
    out_buf_.erase(out_buf_.begin(), out_buf_.begin() + n);
    write_bytes_ -= static_cast<std::size_t>(n);
  }
  want_write_ = false;
  return true;
}

bool Connection::read_frames() {
  std::byte tmp[4096];
  for (;;) {
    ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) break;
      return false;
    }
    if (n == 0) return false;
    in_buf_.insert(in_buf_.end(), tmp, tmp + n);
  }

  while (true) {
    std::span<const std::byte> view(in_buf_.data() + in_off_, in_buf_.size() - in_off_);
    FrameHeader hdr{};
    std::string_view topic;
    std::span<const std::byte> payload;
    std::size_t used = decode_frame(view, hdr, topic, payload);
    if (used == static_cast<std::size_t>(-1)) return false;
    if (used == 0) break;
    Parsed p;
    p.type = hdr.type;
    p.topic.assign(topic.data(), topic.size());
    p.payload.assign(payload.begin(), payload.end());
    parsed_.push_back(std::move(p));
    in_off_ += used;
  }
  if (in_off_ > 0) {
    in_buf_.erase(in_buf_.begin(), in_buf_.begin() + static_cast<std::ptrdiff_t>(in_off_));
    in_off_ = 0;
  }
  return true;
}

Server::Server(std::uint16_t port, int workers)
    : port_(port), workers_n_(workers < 1 ? 1 : workers), broker_(workers_n_) {}

Server::~Server() { stop(); }

void Server::start() {
  if (running_.exchange(true)) return;
  listen_fd_ = listen_tcp(port_, bound_port_);
  poller_.add(listen_fd_, nullptr, true, false);
  io_ = std::thread([this] { io_loop(); });
  workers_.reserve(static_cast<std::size_t>(workers_n_));
  for (int i = 0; i < workers_n_; ++i) {
    workers_.emplace_back([this, i] { worker_loop(i); });
  }
}

void Server::stop() {
  if (!running_.exchange(false)) return;
  if (io_.joinable()) io_.join();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
  workers_.clear();
  conns_.clear();
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
}

void Server::accept_one() {
  for (;;) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    if (fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) return;
      return;
    }
    set_nonblock(fd);
    set_nodelay(fd);
    auto c = std::make_unique<Connection>(fd);
    Connection* raw = c.get();
    conns_.emplace(fd, std::move(c));
    poller_.add(fd, raw, true, false);
  }
}

void Server::close_conn(Connection* c) {
  if (!c) return;
  int fd = c->fd();
  broker_.drop(c);
  poller_.del(fd);
  conns_.erase(fd);
}

void Server::on_read(Connection* c) {
  if (!c->read_frames()) {
    close_conn(c);
    return;
  }
  auto msgs = c->take_messages();
  for (auto& m : msgs) {
    if (m.type == MsgType::Subscribe) {
      broker_.subscribe(c, m.topic);
    } else if (m.type == MsgType::Unsubscribe) {
      broker_.unsubscribe(c, m.topic);
    } else if (m.type == MsgType::Publish) {
      messages_in_.fetch_add(1, std::memory_order_relaxed);
      if (c->backpressured()) {
        backpressure_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      PublishJob job;
      job.topic = std::move(m.topic);
      job.payload = std::move(m.payload);
      job.enqueued_ns = now_ns();
      broker_.enqueue_publish(std::move(job));
    }
  }
}

void Server::on_write(Connection* c) {
  if (!c->flush_writes()) {
    close_conn(c);
    return;
  }
  poller_.mod(c->fd(), c, true, c->want_write());
}

void Server::io_loop() {
  PollEvent ev[64];
  while (running_.load(std::memory_order_relaxed)) {
    int n = poller_.wait(ev, 64, 1);
    for (int i = 0; i < n; ++i) {
      if (ev[i].data == nullptr) {
        if (ev[i].readable) accept_one();
        continue;
      }
      auto* c = static_cast<Connection*>(ev[i].data);
      if (conns_.find(c->fd()) == conns_.end()) continue;
      if (ev[i].hangup && !ev[i].readable) {
        close_conn(c);
        continue;
      }
      if (ev[i].readable) on_read(c);
      if (conns_.find(c->fd()) == conns_.end()) continue;
      if (ev[i].writable) on_write(c);
    }
    // Opportunistic flush for connections that queued writes from workers.
    for (auto& [fd, c] : conns_) {
      if (c->want_write()) {
        if (!c->flush_writes()) {
          // defer close to avoid iterator invalidation; mark
          c->mark_close();
        } else if (c->want_write()) {
          poller_.mod(fd, c.get(), true, true);
        }
      }
    }
    for (auto it = conns_.begin(); it != conns_.end();) {
      if (it->second->closing()) {
        int fd = it->first;
        broker_.drop(it->second.get());
        poller_.del(fd);
        it = conns_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

void Server::worker_loop(int id) {
  PublishJob job;
  while (running_.load(std::memory_order_relaxed)) {
    if (!broker_.dequeue_publish(id, job)) {
      std::this_thread::yield();
      continue;
    }
    auto subs = broker_.subscribers(job.topic);
    auto frame = encode_frame(MsgType::Publish, job.topic, job.payload);
    for (Connection* c : subs) {
      if (!c->enqueue_out(frame)) {
        backpressure_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      messages_out_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

}  // namespace nmr
