#include "nmr/broker.hpp"

#include <algorithm>

namespace nmr {
namespace {

// Order doesn't matter in either list, so erase is swap-and-pop.
template <typename T, typename U>
bool erase_unordered(std::vector<T>& v, const U& value) {
  const auto it = std::find(v.begin(), v.end(), value);
  if (it == v.end()) return false;
  if (it != v.end() - 1) *it = std::move(v.back());
  v.pop_back();
  return true;
}

}  // namespace

std::size_t TopicShard::subscriber_count(std::string_view topic) const {
  const auto* subs = find(topic);
  return subs == nullptr ? 0 : subs->size();
}

const std::vector<ConnId>* TopicShard::find(std::string_view topic) const {
  const auto it = topics_.find(topic);
  return it == topics_.end() ? nullptr : &it->second;
}

void TopicShard::subscribe(std::string_view topic, ConnId conn) {
  auto it = topics_.find(topic);
  if (it == topics_.end()) it = topics_.emplace(std::string(topic), std::vector<ConnId>{}).first;
  auto& subs = it->second;
  if (std::find(subs.begin(), subs.end(), conn) != subs.end()) return;
  subs.push_back(conn);
  conn_topics_[conn].emplace_back(topic);
}

void TopicShard::unsubscribe(std::string_view topic, ConnId conn) {
  const auto it = topics_.find(topic);
  if (it == topics_.end() || !erase_unordered(it->second, conn)) return;
  if (it->second.empty()) topics_.erase(it);

  const auto ct = conn_topics_.find(conn);
  if (ct == conn_topics_.end()) return;
  erase_unordered(ct->second, topic);
  if (ct->second.empty()) conn_topics_.erase(ct);
}

void TopicShard::disconnect(ConnId conn) {
  const auto ct = conn_topics_.find(conn);
  if (ct == conn_topics_.end()) return;
  for (const std::string& topic : ct->second) {
    const auto it = topics_.find(topic);
    if (it == topics_.end()) continue;
    erase_unordered(it->second, conn);
    if (it->second.empty()) topics_.erase(it);
  }
  conn_topics_.erase(ct);
}

}  // namespace nmr
