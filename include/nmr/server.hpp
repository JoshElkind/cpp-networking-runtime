#pragma once

#include "nmr/broker.hpp"
#include "nmr/poller.hpp"
#include "nmr/protocol.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace nmr {

class Connection {
 public:
  explicit Connection(int fd);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  int fd() const { return fd_; }
  bool read_frames();
  bool flush_writes();
  bool enqueue_out(std::vector<std::byte> frame);
  bool backpressured() const;
  bool closing() const { return closing_; }
  void mark_close() { closing_ = true; }
  void want_write(bool v) { want_write_ = v; }
  bool want_write() const { return want_write_; }

  struct Parsed {
    MsgType type;
    std::string topic;
    std::vector<std::byte> payload;
  };
  std::vector<Parsed> take_messages();

 private:
  int fd_ = -1;
  bool closing_ = false;
  bool want_write_ = false;
  std::vector<std::byte> in_buf_;
  std::size_t in_off_ = 0;
  std::vector<Parsed> parsed_;

  mutable std::mutex out_mu_;
  std::vector<std::byte> out_buf_;
  std::size_t write_bytes_ = 0;
};

class Server {
 public:
  explicit Server(std::uint16_t port, int workers = 2);
  ~Server();

  void start();
  void stop();

  std::uint16_t port() const { return bound_port_; }
  std::uint64_t messages_in() const { return messages_in_.load(); }
  std::uint64_t messages_out() const { return messages_out_.load(); }
  std::uint64_t backpressure_events() const { return backpressure_.load(); }
  Broker& broker() { return broker_; }

 private:
  void io_loop();
  void worker_loop(int id);
  void accept_one();
  void on_read(Connection* c);
  void on_write(Connection* c);
  void close_conn(Connection* c);

  std::uint16_t port_;
  std::uint16_t bound_port_ = 0;
  int listen_fd_ = -1;
  int workers_n_;
  Poller poller_;
  Broker broker_;
  std::unordered_map<int, std::unique_ptr<Connection>> conns_;
  std::thread io_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{false};
  std::atomic<std::uint64_t> messages_in_{0};
  std::atomic<std::uint64_t> messages_out_{0};
  std::atomic<std::uint64_t> backpressure_{0};
};

}  // namespace nmr
