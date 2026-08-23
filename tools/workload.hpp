#pragma once

#include "nmr/histogram.hpp"

#include <cstdint>
#include <cstdio>
#include <string>

namespace nmr::tools {

struct WorkloadConfig {
  std::string host = "127.0.0.1";
  std::uint16_t port = 9000;
  int publishers = 1;
  int subscribers = 1;
  int topics = 1;        // publisher i sends on topic (i % topics); subscribers take all
  double seconds = 3.0;
  std::size_t payload = 64;  // bytes, >= 8 (first 8 carry the send timestamp)
  double rate = 0.0;         // msgs/s per publisher; 0 = as fast as possible
  int batch = 1;             // frames per send() when unpaced
};

struct WorkloadResult {
  std::uint64_t sent = 0;
  std::uint64_t received = 0;
  std::uint64_t expected = 0;
  double elapsed_s = 0.0;
  LatencyHistogram latency_ns;
};

// Runs publishers and subscribers against a broker that's already listening.
//
// With a fixed rate, each message is stamped with the time it was *scheduled*
// to go out, not when send() actually ran. If the publisher stalls (because
// the broker pushed back), the stall shows up in the latency numbers instead
// of quietly vanishing; that's the usual coordinated-omission correction.
WorkloadResult run_workload(const WorkloadConfig& cfg);

void print_workload(const WorkloadConfig& cfg, const WorkloadResult& r, std::FILE* out);

}  // namespace nmr::tools
