#pragma once

#include "nmr/frame_pool.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace nmr {

// Connection ids are never reused, so a stale id held by a worker can't alias a
// newer connection; the IO thread just fails to find it and drops the delivery.
using ConnId = std::uint64_t;
inline constexpr ConnId kNoConn = 0;

enum class Command : std::uint8_t {
  kPublish,
  kSubscribe,
  kUnsubscribe,
  kDisconnect,
};

// IO thread -> worker. `frame` is the client's original wire frame (and so
// carries the topic) for every command except kDisconnect, where it's null.
// The worker owns one reference to it until it sends the matching release.
struct Ingress {
  Frame* frame = nullptr;
  ConnId conn = kNoConn;
  Command cmd = Command::kPublish;
};

// Worker -> IO thread. Deliver `frame` to `conn`; or, when conn == kNoConn,
// release the reference that came with the Ingress. Deliveries for a frame
// always precede its release in the same FIFO, so the frame can't be freed
// while deliveries are in flight.
struct Egress {
  Frame* frame = nullptr;
  ConnId conn = kNoConn;
};

// FNV-1a. Stable across platforms and runs, unlike std::hash, so a topic
// always lands on the same worker.
inline std::size_t topic_shard(std::string_view topic, std::size_t shards) noexcept {
  std::uint64_t h = 0xcbf29ce484222325ull;
  for (const char c : topic) {
    h ^= static_cast<unsigned char>(c);
    h *= 0x100000001b3ull;
  }
  return static_cast<std::size_t>(h % shards);
}

// Subscription table for one worker. Topics are partitioned across workers by
// topic_shard(), so each table is only ever touched by its own thread.
class TopicShard {
 public:
  template <typename Emit>
  void apply(const Ingress& in, Emit&& emit);

  std::size_t topic_count() const noexcept { return topics_.size(); }
  std::size_t subscriber_count(std::string_view topic) const;

 private:
  struct StringHash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept {
      return std::hash<std::string_view>{}(s);
    }
  };

  void subscribe(std::string_view topic, ConnId conn);
  void unsubscribe(std::string_view topic, ConnId conn);
  void disconnect(ConnId conn);
  const std::vector<ConnId>* find(std::string_view topic) const;

  std::unordered_map<std::string, std::vector<ConnId>, StringHash, std::equal_to<>> topics_;
  // Reverse index so a disconnect costs O(topics the connection had) rather
  // than a scan of every topic in the shard.
  std::unordered_map<ConnId, std::vector<std::string>> conn_topics_;
};

template <typename Emit>
void TopicShard::apply(const Ingress& in, Emit&& emit) {
  switch (in.cmd) {
    case Command::kPublish:
      if (const auto* subs = find(in.frame->topic())) {
        for (const ConnId c : *subs) emit(Egress{in.frame, c});
      }
      break;
    case Command::kSubscribe:
      subscribe(in.frame->topic(), in.conn);
      break;
    case Command::kUnsubscribe:
      unsubscribe(in.frame->topic(), in.conn);
      break;
    case Command::kDisconnect:
      disconnect(in.conn);
      return;
  }
  emit(Egress{in.frame, kNoConn});
}

}  // namespace nmr
