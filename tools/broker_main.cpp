#include "args.hpp"

#include "nmr/server.hpp"

#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <exception>
#include <mutex>
#include <thread>

#include <pthread.h>
#include <signal.h>

namespace {

constexpr const char* kUsage = R"(usage: nmr-broker [options]
  --bind ADDR            listen address (default 127.0.0.1)
  --port N               listen port (default 9000, 0 = ephemeral)
  --workers N            topic worker threads (default 2)
  --hwm BYTES            per-subscriber write high watermark (default 8 MiB)
  --slow-consumer MODE   drop | disconnect (default drop)
  --busy-poll            spin instead of sleeping (lower latency, burns cores)
  --pin-io CPU           pin the IO thread (Linux)
  --pin-workers CPU      pin worker i to CPU+i (Linux)
  --stats SECONDS        print counters every N seconds (default 0 = off)
)";

void print_stats(const nmr::ServerStats& s, std::FILE* out) {
  std::fprintf(out,
               "conns=%llu/%llu in=%llu pub=%llu deliver=%llu out=%.1fMB drops(ingress=%llu "
               "slow=%llu) slow_disconnects=%llu proto_err=%llu\n",
               static_cast<unsigned long long>(s.connections_accepted - s.connections_closed),
               static_cast<unsigned long long>(s.connections_accepted),
               static_cast<unsigned long long>(s.frames_in),
               static_cast<unsigned long long>(s.publishes),
               static_cast<unsigned long long>(s.deliveries),
               static_cast<double>(s.bytes_out) / 1e6,
               static_cast<unsigned long long>(s.ingress_drops),
               static_cast<unsigned long long>(s.slow_consumer_drops),
               static_cast<unsigned long long>(s.slow_consumer_disconnects),
               static_cast<unsigned long long>(s.protocol_errors));
  std::fflush(out);
}

}  // namespace

int main(int argc, char** argv) {
  nmr::ServerConfig cfg;
  int stats_every = 0;
  try {
    nmr::tools::Args args(argc, argv);
    if (args.has("help")) {
      std::fputs(kUsage, stdout);
      return 0;
    }
    cfg.bind_address = args.get("bind", cfg.bind_address);
    cfg.port = static_cast<std::uint16_t>(args.get_int("port", cfg.port));
    cfg.workers = static_cast<int>(args.get_int("workers", cfg.workers));
    cfg.write_high_watermark =
        static_cast<std::size_t>(args.get_int("hwm", static_cast<long long>(cfg.write_high_watermark)));
    const std::string slow = args.get("slow-consumer", "drop");
    if (slow == "drop") {
      cfg.slow_consumer = nmr::SlowConsumerPolicy::kDropMessages;
    } else if (slow == "disconnect") {
      cfg.slow_consumer = nmr::SlowConsumerPolicy::kDisconnect;
    } else {
      throw std::invalid_argument("--slow-consumer must be drop or disconnect");
    }
    cfg.busy_poll = args.has("busy-poll");
    cfg.io_cpu = static_cast<int>(args.get_int("pin-io", -1));
    cfg.worker_cpu_base = static_cast<int>(args.get_int("pin-workers", -1));
    stats_every = static_cast<int>(args.get_int("stats", 0));
    args.check_unused();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "nmr-broker: %s\n\n%s", e.what(), kUsage);
    return 2;
  }

  // Block the shutdown signals before any thread exists so every thread
  // inherits the mask, then take them synchronously with sigwait() below.
  // That keeps all shutdown work out of signal-handler context.
  // Shells start background jobs with SIGINT ignored, and an ignored signal
  // is discarded before sigwait() can see it, so restore the default first.
  sigset_t shutdown_signals;
  sigemptyset(&shutdown_signals);
  for (const int s : {SIGINT, SIGTERM}) {
    std::signal(s, SIG_DFL);
    sigaddset(&shutdown_signals, s);
  }
  pthread_sigmask(SIG_BLOCK, &shutdown_signals, nullptr);
  std::signal(SIGPIPE, SIG_IGN);

  nmr::Server server(cfg);
  try {
    server.start();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "nmr-broker: %s\n", e.what());
    return 1;
  }
  std::printf("nmr-broker listening on %s:%u (workers=%d)\n", cfg.bind_address.c_str(),
              static_cast<unsigned>(server.port()), cfg.workers);
  std::fflush(stdout);

  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  std::thread reporter;
  if (stats_every > 0) {
    reporter = std::thread([&] {
      std::unique_lock<std::mutex> lock(mu);
      while (!cv.wait_for(lock, std::chrono::seconds(stats_every), [&] { return done; })) {
        print_stats(server.stats(), stdout);
      }
    });
  }

  int sig = 0;
  sigwait(&shutdown_signals, &sig);
  std::printf("\nnmr-broker: caught signal %d, shutting down\n", sig);

  {
    std::lock_guard<std::mutex> lock(mu);
    done = true;
  }
  cv.notify_all();
  if (reporter.joinable()) reporter.join();

  server.stop();
  print_stats(server.stats(), stdout);
  return 0;
}
