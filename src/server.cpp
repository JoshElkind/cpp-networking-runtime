#include "nmr/server.hpp"

#include "nmr/cpu.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include <sys/socket.h>

namespace nmr {

Server::Server(ServerConfig config) : config_(std::move(config)) {
  if (config_.workers < 1) config_.workers = 1;
  workers_.reserve(static_cast<std::size_t>(config_.workers));
  for (int i = 0; i < config_.workers; ++i) {
    workers_.push_back(
        std::make_unique<Worker>(config_.ingress_capacity, config_.egress_capacity));
  }
}

Server::~Server() { stop(); }

void Server::start() {
  if (started_) return;
  listener_ = listen_tcp(config_.bind_address, config_.port);
  port_ = local_port(listener_.get());
  poller_.add(listener_.get(), kListenToken, /*want_write=*/false);
  poller_.add(waker_.fd(), kWakeToken, /*want_write=*/false);

  started_ = true;
  workers_running_.store(true, std::memory_order_release);
  io_running_.store(true, std::memory_order_release);
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    workers_[i]->thread = std::thread([this, i] { worker_loop(i); });
  }
  io_thread_ = std::thread([this] { io_loop(); });
}

void Server::stop() {
  if (!started_) return;
  started_ = false;

  // Workers first, while the IO thread is still draining egress: a worker
  // blocked on a full egress ring always gets to finish its current command,
  // so every frame's release reaches the IO side.
  workers_running_.store(false, std::memory_order_release);
  for (auto& w : workers_) {
    if (w->thread.joinable()) w->thread.join();
  }

  io_running_.store(false, std::memory_order_release);
  waker_.wake();
  if (io_thread_.joinable()) io_thread_.join();

  reclaim_after_stop();
}

void Server::reclaim_after_stop() {
  // Every thread has been joined, so this thread now owns the pool.
  while (egress_pending()) drain_egress();
  for (auto& w : workers_) {
    // Deliveries in egress don't own a reference; only releases do, and
    // drain_egress() handled those. Unprocessed ingress still owns one each.
    for (Ingress* in = w->ingress.front(); in != nullptr; in = w->ingress.front()) {
      if (in->frame != nullptr) pool_.release(in->frame);
      w->ingress.pop();
    }
    for (const Ingress& in : w->backlog) {
      if (in.frame != nullptr) pool_.release(in.frame);
    }
    w->backlog.clear();
  }
  for (auto& [id, conn] : conns_) {
    poller_.remove(conn->fd());
    conn->discard_output(pool_);
  }
  conns_.clear();
  dirty_.clear();
  read_pending_.clear();
  to_close_.clear();
  if (listener_) {
    poller_.remove(listener_.get());
    listener_.reset();
  }
}

ServerStats Server::stats() const noexcept {
  ServerStats s;
  s.connections_accepted = counters_.connections_accepted.get();
  s.connections_closed = counters_.connections_closed.get();
  s.frames_in = counters_.frames_in.get();
  s.publishes = counters_.publishes.get();
  s.deliveries = counters_.deliveries.get();
  s.bytes_out = counters_.bytes_out.get();
  s.ingress_drops = counters_.ingress_drops.get();
  s.slow_consumer_drops = counters_.slow_consumer_drops.get();
  s.slow_consumer_disconnects = counters_.slow_consumer_disconnects.get();
  s.protocol_errors = counters_.protocol_errors.get();
  return s;
}

// ---------------------------------------------------------------------------
// IO thread

void Server::io_loop() {
  set_current_thread_name("nmr-io");
  if (config_.io_cpu >= 0) pin_current_thread(config_.io_cpu);

  while (io_running_.load(std::memory_order_acquire)) {
    int timeout = 0;
    if (!config_.busy_poll) {
      // Sleep handshake with notify_io(). Each side stores, fences, then
      // loads the other side's variable, so at least one of them sees the
      // other's store: either a worker sees io_sleeping_ and wakes us, or we
      // see its egress entries here and poll without blocking.
      io_sleeping_.store(true, std::memory_order_relaxed);
      std::atomic_thread_fence(std::memory_order_seq_cst);
      timeout = egress_pending() || !read_pending_.empty() ? 0 : kIdleWaitMs;
    }

    const auto events = poller_.wait(timeout);
    io_sleeping_.store(false, std::memory_order_relaxed);

    service_pending_reads();
    for (const PollEvent& ev : events) dispatch(ev);
    drain_backlogs();
    drain_egress();
    flush_dirty();
    close_pending();
  }
}

bool Server::egress_pending() const noexcept {
  for (const auto& w : workers_) {
    if (!w->egress.empty_approx()) return true;
  }
  return false;
}

void Server::dispatch(const PollEvent& ev) {
  if (ev.token == kListenToken) {
    accept_all();
    return;
  }
  if (ev.token == kWakeToken) {
    waker_.drain();
    return;
  }

  const auto it = conns_.find(ev.token);
  if (it == conns_.end()) return;
  Connection& c = *it->second;
  if (c.closing()) return;

  if (ev.readable && !c.read_pending()) on_readable(c);
  if (ev.closed) schedule_close(c);
  if (ev.writable && !c.closing() && c.has_output()) mark_dirty(c);
}

void Server::accept_all() {
  for (;;) {
#if defined(__linux__)
    const int fd = ::accept4(listener_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const int fd = ::accept(listener_.get(), nullptr, nullptr);
#endif
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      // EAGAIN: backlog drained. EMFILE/ENFILE: out of descriptors; the
      // pending connections stay queued in the kernel until a slot frees up.
      return;
    }

    Fd owned(fd);
    try {
#if !defined(__linux__)
      set_nonblocking(fd);
#endif
      set_nodelay(fd);
      set_nosigpipe(fd);
      const ConnId id = next_conn_id_++;
      auto conn = std::make_unique<Connection>(id, std::move(owned));
      poller_.add(conn->fd(), id);
      conns_.emplace(id, std::move(conn));
      counters_.connections_accepted.add();
    } catch (const std::system_error&) {
      // Peer reset before we finished setting the socket up; `owned` closes it.
    }
  }
}

void Server::on_readable(Connection& c) {
  // Parsing after every recv keeps the input buffer bounded by one read plus
  // one partial frame, however fast the peer sends.
  std::size_t budget = kReadBudget;
  while (budget > 0) {
    std::size_t n = 0;
    switch (c.read_some(n)) {
      case IoStatus::kOk:
        break;
      case IoStatus::kWouldBlock:
        return;
      case IoStatus::kClosed:
      case IoStatus::kError:
        schedule_close(c);
        return;
    }
    if (!process_input(c)) {
      counters_.protocol_errors.add();
      schedule_close(c);
      return;
    }
    budget -= std::min(budget, n);
  }
  // Out of budget before EAGAIN. Edge-triggering won't report this socket
  // again until new data arrives, so remember it ourselves; otherwise one
  // firehose publisher would monopolise the loop and starve egress.
  c.set_read_pending(true);
  read_pending_.push_back(c.id());
}

void Server::service_pending_reads() {
  read_scratch_.swap(read_pending_);
  for (const ConnId id : read_scratch_) {
    const auto it = conns_.find(id);
    if (it == conns_.end()) continue;
    Connection& c = *it->second;
    c.set_read_pending(false);
    if (!c.closing()) on_readable(c);
  }
  read_scratch_.clear();
}

bool Server::process_input(Connection& c) {
  const auto input = c.pending_input();
  std::size_t used = 0;
  bool ok = true;
  while (ok) {
    FrameView frame;
    const DecodeStatus st = decode_frame(input.subspan(used), frame);
    if (st == DecodeStatus::kNeedMore) break;
    if (st != DecodeStatus::kOk) {
      ok = false;
      break;
    }
    used += frame.wire.size();
    counters_.frames_in.add();
    ok = handle_frame(c, frame);
  }
  c.consume_input(used);
  return ok;
}

bool Server::handle_frame(Connection& c, const FrameView& frame) {
  switch (frame.header.type) {
    case MsgType::kPublish:
      if (frame.topic.empty()) return false;
      route(Command::kPublish, c, frame);
      return true;
    case MsgType::kSubscribe:
      if (frame.topic.empty()) return false;
      route(Command::kSubscribe, c, frame);
      return true;
    case MsgType::kUnsubscribe:
      if (frame.topic.empty()) return false;
      route(Command::kUnsubscribe, c, frame);
      return true;
    case MsgType::kHeartbeat: {
      // Echoed straight back from the IO thread. Because it never goes
      // through a worker, a client that sends SUBSCRIBE then a heartbeat and
      // gets the echo knows its SUBSCRIBE is already queued ahead of any
      // PUBLISH the broker reads after that point.
      Frame* f = pool_.allocate(frame.wire.size());
      std::memcpy(f->data(), frame.wire.data(), frame.wire.size());
      c.enqueue(f);
      mark_dirty(c);
      return true;
    }
  }
  return false;
}

void Server::route(Command cmd, Connection& c, const FrameView& frame) {
  Worker& w = *workers_[topic_shard(frame.topic, workers_.size())];
  Frame* f = pool_.allocate(frame.wire.size());
  std::memcpy(f->data(), frame.wire.data(), frame.wire.size());
  const Ingress in{f, c.id(), cmd};

  if (cmd != Command::kPublish) {
    send_control(w, in);
    return;
  }
  // Publishes are droppable: if the worker is behind, shed load here rather
  // than buffer without bound. Also drop while control commands are
  // backlogged, so a publish can't overtake an earlier subscribe.
  if (w.backlog.empty() && w.ingress.try_push(in)) {
    counters_.publishes.add();
  } else {
    pool_.release(f);
    counters_.ingress_drops.add();
  }
}

void Server::send_control(Worker& w, const Ingress& in) {
  if (w.backlog.empty() && w.ingress.try_push(in)) return;
  w.backlog.push_back(in);
}

void Server::drain_backlogs() {
  for (auto& w : workers_) {
    while (!w->backlog.empty() && w->ingress.try_push(w->backlog.front())) {
      w->backlog.pop_front();
    }
  }
}

void Server::drain_egress() {
  for (auto& w : workers_) {
    // Bounded so one hot worker can't starve socket IO. Anything left over
    // makes the next poll non-blocking.
    for (std::size_t i = 0; i < kEgressBatch; ++i) {
      const Egress* e = w->egress.front();
      if (e == nullptr) break;
      const Egress item = *e;
      w->egress.pop();
      if (item.conn == kNoConn) {
        pool_.release(item.frame);
      } else {
        deliver(item.conn, item.frame);
      }
    }
  }
}

void Server::deliver(ConnId id, Frame* f) {
  const auto it = conns_.find(id);
  if (it == conns_.end()) return;
  Connection& c = *it->second;
  if (c.closing()) return;

  if (c.queued_bytes() + f->size > config_.write_high_watermark) {
    if (config_.slow_consumer == SlowConsumerPolicy::kDisconnect) {
      counters_.slow_consumer_disconnects.add();
      schedule_close(c);
    } else {
      counters_.slow_consumer_drops.add();
    }
    return;
  }

  pool_.retain(f);
  c.enqueue(f);
  counters_.deliveries.add();
  mark_dirty(c);
}

void Server::mark_dirty(Connection& c) {
  if (c.dirty()) return;
  c.set_dirty(true);
  dirty_.push_back(&c);
}

void Server::flush_dirty() {
  // Connections are only destroyed in close_pending(), after this, so the
  // raw pointers in dirty_ are all still valid here.
  for (Connection* c : dirty_) {
    c->set_dirty(false);
    if (c->closing()) continue;
    std::size_t written = 0;
    const IoStatus st = c->flush(pool_, written);
    counters_.bytes_out.add(written);
    // kWouldBlock is fine: the socket's write edge will fire once it drains.
    if (st == IoStatus::kError) schedule_close(*c);
  }
  dirty_.clear();
}

void Server::schedule_close(Connection& c) {
  if (c.closing()) return;
  c.mark_closing();
  to_close_.push_back(c.id());
}

void Server::close_pending() {
  for (const ConnId id : to_close_) {
    const auto it = conns_.find(id);
    if (it == conns_.end()) continue;
    Connection& c = *it->second;
    poller_.remove(c.fd());
    c.discard_output(pool_);
    // Every worker may hold subscriptions for this connection.
    for (auto& w : workers_) send_control(*w, Ingress{nullptr, id, Command::kDisconnect});
    conns_.erase(it);
    counters_.connections_closed.add();
  }
  to_close_.clear();
}

// ---------------------------------------------------------------------------
// Workers

void Server::worker_loop(std::size_t index) {
  Worker& w = *workers_[index];
  const std::string name = "nmr-worker-" + std::to_string(index);
  set_current_thread_name(name.c_str());
  if (config_.worker_cpu_base >= 0) {
    pin_current_thread(config_.worker_cpu_base + static_cast<int>(index));
  }

  const auto emit = [&](const Egress& e) {
    // If the IO thread is behind, wait for it rather than dropping: a lost
    // release would leak the frame. The IO thread never blocks on a worker,
    // so this always makes progress.
    while (!w.egress.try_push(e)) {
      notify_io();
      cpu_relax();
    }
  };

  Backoff backoff(config_.busy_poll);
  while (workers_running_.load(std::memory_order_acquire)) {
    std::size_t processed = 0;
    for (const Ingress* in = w.ingress.front(); in != nullptr && processed < kWorkerBatch;
         in = w.ingress.front()) {
      w.shard.apply(*in, emit);
      w.ingress.pop();
      ++processed;
    }
    if (processed == 0) {
      backoff.idle();
      continue;
    }
    backoff.reset();
    notify_io();
  }
}

void Server::notify_io() noexcept {
  // Pairs with the fence in io_loop(); see the comment there.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (io_sleeping_.load(std::memory_order_relaxed) &&
      io_sleeping_.exchange(false, std::memory_order_relaxed)) {
    waker_.wake();
  }
}

}  // namespace nmr
