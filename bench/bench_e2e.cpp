#include "workload.hpp"

#include "nmr/server.hpp"

#include <cstdio>

namespace {

struct Broker {
  int workers = 1;
  bool busy_poll = false;
};

void run(const char* title, Broker b, nmr::tools::WorkloadConfig cfg) {
  nmr::ServerConfig server_cfg;
  server_cfg.port = 0;
  server_cfg.workers = b.workers;
  server_cfg.busy_poll = b.busy_poll;
  nmr::Server server(server_cfg);
  server.start();
  cfg.port = server.port();

  std::printf("-- %s  [workers=%d%s]\n", title, b.workers, b.busy_poll ? ", busy-poll" : "");
  const auto result = nmr::tools::run_workload(cfg);
  nmr::tools::print_workload(cfg, result, stdout);
  const auto st = server.stats();
  std::printf("  broker: ingress_drops=%llu slow_consumer_drops=%llu\n\n",
              static_cast<unsigned long long>(st.ingress_drops),
              static_cast<unsigned long long>(st.slow_consumer_drops));
  server.stop();
}

}  // namespace

void run_e2e_benches() {
  std::printf("== end-to-end over loopback TCP ==\n");
  std::printf("(client threads share the machine with the broker; on a laptop the\n"
              " tail percentiles mostly measure the OS scheduler)\n\n");

  nmr::tools::WorkloadConfig latency;
  latency.publishers = 1;
  latency.subscribers = 1;
  latency.payload = 64;
  latency.seconds = 3;
  latency.rate = 20'000;
  run("latency: 1 pub -> 1 sub, 20k msgs/s", {1, false}, latency);
  run("latency: 1 pub -> 1 sub, 20k msgs/s", {1, true}, latency);

  nmr::tools::WorkloadConfig fanout = latency;
  fanout.subscribers = 4;
  fanout.rate = 10'000;
  run("fan-out: 1 pub -> 4 subs, 10k msgs/s", {1, true}, fanout);

  nmr::tools::WorkloadConfig sustained;
  sustained.publishers = 4;
  sustained.subscribers = 2;
  sustained.topics = 4;
  sustained.payload = 64;
  sustained.seconds = 3;
  sustained.rate = 100'000;
  run("sustained: 4 pubs x 100k msgs/s -> 2 subs", {2, false}, sustained);

  nmr::tools::WorkloadConfig overload = sustained;
  overload.rate = 0;
  overload.batch = 32;
  run("overload: unpaced publishers, broker sheds load", {2, false}, overload);
}
