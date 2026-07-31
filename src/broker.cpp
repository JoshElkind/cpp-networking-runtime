#include "nmr/broker.hpp"
#include "nmr/config.hpp"

#include <functional>

namespace nmr {

Broker::Broker(int workers, std::size_t queue_cap) : pool_(kDefaultPoolBlock) {
  if (workers < 1) workers = 1;
  queues_.reserve(static_cast<std::size_t>(workers));
  for (int i = 0; i < workers; ++i) {
    queues_.push_back(std::make_unique<SpscQueue<PublishJob>>(queue_cap));
  }
}

std::size_t Broker::shard(const std::string& topic) const {
  return std::hash<std::string>{}(topic) % queues_.size();
}

void Broker::subscribe(Connection* c, const std::string& topic) {
  std::lock_guard<std::mutex> g(mu_);
  topics_[topic].insert(c);
}

void Broker::unsubscribe(Connection* c, const std::string& topic) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = topics_.find(topic);
  if (it == topics_.end()) return;
  it->second.erase(c);
  if (it->second.empty()) topics_.erase(it);
}

void Broker::drop(Connection* c) {
  std::lock_guard<std::mutex> g(mu_);
  for (auto it = topics_.begin(); it != topics_.end();) {
    it->second.erase(c);
    if (it->second.empty()) {
      it = topics_.erase(it);
    } else {
      ++it;
    }
  }
}

bool Broker::enqueue_publish(PublishJob job) {
  const std::size_t i = shard(job.topic);
  if (!queues_[i]->try_push(std::move(job))) {
    dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  enqueued_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool Broker::dequeue_publish(int worker_id, PublishJob& job) {
  return queues_[static_cast<std::size_t>(worker_id)]->try_pop(job);
}

std::vector<Connection*> Broker::subscribers(const std::string& topic) {
  std::lock_guard<std::mutex> g(mu_);
  auto it = topics_.find(topic);
  if (it == topics_.end()) return {};
  return {it->second.begin(), it->second.end()};
}

}  // namespace nmr
