#pragma once

#include "nmr/broker.hpp"
#include "nmr/connection.hpp"
#include "nmr/frame_pool.hpp"
#include "nmr/poller.hpp"
#include "nmr/socket.hpp"
#include "nmr/spsc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace nmr {

enum class SlowConsumerPolicy : std::uint8_t {
  kDropMessages,  // keep the subscriber, skip frames it has no room for
  kDisconnect,    // cut the subscriber off the first time it falls behind
};

struct ServerConfig {
  std::string bind_address = "127.0.0.1";
  std::uint16_t port = 9000;  // 0 picks an ephemeral port; see Server::port()
  int workers = 2;
  std::size_t ingress_capacity = 1u << 16;  // per worker, in commands
  std::size_t egress_capacity = 1u << 16;   // per worker, in deliveries
  std::size_t write_high_watermark = 8u << 20;  // bytes queued per subscriber
  SlowConsumerPolicy slow_consumer = SlowConsumerPolicy::kDropMessages;
  // IO thread polls without blocking and workers never yield. Lowest and
  // most stable latency, at the cost of 1 + workers cores at 100%.
  bool busy_poll = false;
  int io_cpu = -1;           // pin the IO thread (Linux only); -1 = don't pin
  int worker_cpu_base = -1;  // pin worker i to worker_cpu_base + i
};

struct ServerStats {
  std::uint64_t connections_accepted = 0;
  std::uint64_t connections_closed = 0;
  std::uint64_t frames_in = 0;
  std::uint64_t publishes = 0;
  std::uint64_t deliveries = 0;
  std::uint64_t bytes_out = 0;
  std::uint64_t ingress_drops = 0;
  std::uint64_t slow_consumer_drops = 0;
  std::uint64_t slow_consumer_disconnects = 0;
  std::uint64_t protocol_errors = 0;
};

// Pub/sub broker.
//
// Threading is shared-nothing:
//
//   * One IO thread owns the listener, every socket, every Connection and the
//     FramePool. It parses input, copies each PUBLISH/SUBSCRIBE frame once into
//     a pooled Frame, and routes it to the worker that owns the topic.
//   * N workers each own a TopicShard. They turn a publish into one Egress per
//     subscriber and never touch a socket.
//   * IO -> worker and worker -> IO are SPSC rings, one pair per worker. Those
//     rings are the only shared state on the data path; there are no mutexes.
//
// A publish is fanned out by reference: every subscriber's write queue holds
// the same Frame*, and the socket writes are batched with sendmsg() at the end
// of each IO-loop iteration.
class Server {
 public:
  explicit Server(ServerConfig config);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds, then starts the IO and worker threads. Throws on bind failure.
  void start();
  // Idempotent. Joins all threads and frees every in-flight frame.
  void stop();

  std::uint16_t port() const noexcept { return port_; }
  const ServerConfig& config() const noexcept { return config_; }

  // Safe to call from any thread while running.
  ServerStats stats() const noexcept;

  // Frames currently allocated from the pool. Only meaningful after stop();
  // it should be zero then, and the tests hold it to that.
  std::size_t frames_in_flight() const noexcept { return pool_.live(); }

 private:
  static constexpr std::uint64_t kListenToken = 0;
  static constexpr std::uint64_t kWakeToken = 1;
  static constexpr ConnId kFirstConnId = 16;
  static constexpr int kIdleWaitMs = 100;
  static constexpr std::size_t kEgressBatch = 4096;
  static constexpr std::size_t kWorkerBatch = 256;
  static constexpr std::size_t kReadBudget = 256 * 1024;  // bytes per connection per loop

  struct Worker {
    Worker(std::size_t ingress_cap, std::size_t egress_cap)
        : ingress(ingress_cap), egress(egress_cap) {}

    SpscQueue<Ingress> ingress;  // IO -> worker
    SpscQueue<Egress> egress;    // worker -> IO
    // IO thread only: control commands that didn't fit in `ingress`. They
    // can't be dropped, and must stay ordered ahead of anything newer.
    std::deque<Ingress> backlog;
    TopicShard shard;  // worker thread only
    std::thread thread;
  };

  // Written only by the IO thread, read by anyone. A single writer doesn't
  // need a locked read-modify-write, just a relaxed load and store.
  struct Counter {
    std::atomic<std::uint64_t> value{0};
    void add(std::uint64_t n = 1) noexcept {
      value.store(value.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
    }
    std::uint64_t get() const noexcept { return value.load(std::memory_order_relaxed); }
  };

  struct Counters {
    Counter connections_accepted, connections_closed, frames_in, publishes, deliveries, bytes_out,
        ingress_drops, slow_consumer_drops, slow_consumer_disconnects, protocol_errors;
  };

  // IO thread
  void io_loop();
  void dispatch(const PollEvent& ev);
  void accept_all();
  void on_readable(Connection& c);
  void service_pending_reads();
  bool process_input(Connection& c);
  bool handle_frame(Connection& c, const FrameView& frame);
  void route(Command cmd, Connection& c, const FrameView& frame);
  void send_control(Worker& w, const Ingress& in);
  void drain_backlogs();
  void drain_egress();
  void deliver(ConnId id, Frame* f);
  void mark_dirty(Connection& c);
  void flush_dirty();
  void schedule_close(Connection& c);
  void close_pending();
  bool egress_pending() const noexcept;

  // Workers
  void worker_loop(std::size_t index);
  void notify_io() noexcept;

  void reclaim_after_stop();

  ServerConfig config_;
  Fd listener_;
  std::uint16_t port_ = 0;
  Poller poller_;
  Waker waker_;
  FramePool pool_;
  std::vector<std::unique_ptr<Worker>> workers_;

  std::unordered_map<ConnId, std::unique_ptr<Connection>> conns_;
  std::vector<Connection*> dirty_;
  std::vector<ConnId> read_pending_;  // ran out of read budget before EAGAIN
  std::vector<ConnId> read_scratch_;
  std::vector<ConnId> to_close_;
  ConnId next_conn_id_ = kFirstConnId;

  std::thread io_thread_;
  std::atomic<bool> io_running_{false};
  std::atomic<bool> workers_running_{false};
  std::atomic<bool> io_sleeping_{false};
  bool started_ = false;

  Counters counters_;
};

}  // namespace nmr
