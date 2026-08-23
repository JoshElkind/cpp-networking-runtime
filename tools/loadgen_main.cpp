#include "args.hpp"
#include "workload.hpp"

#include <csignal>
#include <cstdio>
#include <exception>

namespace {

constexpr const char* kUsage = R"(usage: nmr-loadgen [options]
  --host ADDR        broker address (default 127.0.0.1)
  --port N           broker port (default 9000)
  --pubs N           publisher connections (default 4)
  --subs N           subscriber connections (default 2)
  --topics N         topics; publisher i uses topic i % N (default 1)
  --seconds S        run time (default 3)
  --payload BYTES    payload size, >= 8 (default 64)
  --rate R           msgs/s per publisher; 0 = unpaced (default 0)
  --batch N          frames per send() when unpaced (default 16)
)";

}  // namespace

int main(int argc, char** argv) {
  nmr::tools::WorkloadConfig cfg;
  cfg.publishers = 4;
  cfg.subscribers = 2;
  cfg.batch = 16;
  try {
    nmr::tools::Args args(argc, argv);
    if (args.has("help")) {
      std::fputs(kUsage, stdout);
      return 0;
    }
    cfg.host = args.get("host", cfg.host);
    cfg.port = static_cast<std::uint16_t>(args.get_int("port", cfg.port));
    cfg.publishers = static_cast<int>(args.get_int("pubs", cfg.publishers));
    cfg.subscribers = static_cast<int>(args.get_int("subs", cfg.subscribers));
    cfg.topics = static_cast<int>(args.get_int("topics", cfg.topics));
    cfg.seconds = args.get_double("seconds", cfg.seconds);
    cfg.payload = static_cast<std::size_t>(args.get_int("payload", static_cast<long long>(cfg.payload)));
    cfg.rate = args.get_double("rate", cfg.rate);
    cfg.batch = static_cast<int>(args.get_int("batch", cfg.batch));
    args.check_unused();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "nmr-loadgen: %s\n\n%s", e.what(), kUsage);
    return 2;
  }

  std::signal(SIGPIPE, SIG_IGN);
  try {
    const auto result = nmr::tools::run_workload(cfg);
    nmr::tools::print_workload(cfg, result, stdout);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "nmr-loadgen: %s\n", e.what());
    return 1;
  }
  return 0;
}
