#pragma once

#include "nmr/memory_pool.hpp"
#include "nmr/spsc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace nmr {

struct PublishJob {
  std::string topic;
  std::vector<std::byte> payload;
  std::uint64_t enqueued_ns = 0;
};

class Connection;

class Broker {
 public:
  explicit Broker(int workers, std::size_t queue_cap = 1 << 16);

  void subscribe(Connection* c, const std::string& topic);
  void unsubscribe(Connection* c, const std::string& topic);
  void drop(Connection* c);

  // IO thread: shard by topic onto a per-worker lock-free SPSC queue.
  bool enqueue_publish(PublishJob job);
  bool dequeue_publish(int worker_id, PublishJob& job);

  std::vector<Connection*> subscribers(const std::string& topic);

  std::uint64_t dropped() const { return dropped_.load(std::memory_order_relaxed); }
  std::uint64_t enqueued() const { return enqueued_.load(std::memory_order_relaxed); }
  int workers() const { return static_cast<int>(queues_.size()); }

  MemoryPool& pool() { return pool_; }

 private:
  std::size_t shard(const std::string& topic) const;

  MemoryPool pool_;
  std::vector<std::unique_ptr<SpscQueue<PublishJob>>> queues_;
  std::mutex mu_;
  std::unordered_map<std::string, std::unordered_set<Connection*>> topics_;
  std::atomic<std::uint64_t> dropped_{0};
  std::atomic<std::uint64_t> enqueued_{0};
};

}  // namespace nmr
