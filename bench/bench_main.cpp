#include <csignal>
#include <cstdio>
#include <cstring>

void run_queue_benches();
void run_pool_benches();
void run_e2e_benches();

// Usage: nmr-bench [queue|pool|e2e]...   (no arguments runs everything)
int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  const auto wanted = [&](const char* name) {
    if (argc == 1) return true;
    for (int i = 1; i < argc; ++i) {
      if (std::strcmp(argv[i], name) == 0) return true;
    }
    return false;
  };
  if (wanted("queue")) run_queue_benches();
  if (wanted("pool")) run_pool_benches();
  if (wanted("e2e")) run_e2e_benches();
  return 0;
}
