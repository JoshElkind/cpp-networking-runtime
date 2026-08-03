#include "test.hpp"

#include <chrono>
#include <csignal>
#include <cstring>
#include <exception>

// Usage: nmr-tests [substring]   runs only tests whose name contains it.
int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  const char* filter = argc > 1 ? argv[1] : nullptr;

  int run = 0;
  int failed = 0;
  for (const auto& c : nmr_test::registry()) {
    if (filter != nullptr && std::strstr(c.name, filter) == nullptr) continue;
    ++run;
    const int before = nmr_test::failures();
    const auto t0 = std::chrono::steady_clock::now();
    try {
      c.fn();
    } catch (const nmr_test::Abort&) {
    } catch (const std::exception& e) {
      std::fprintf(stderr, "    uncaught exception: %s\n", e.what());
      ++nmr_test::failures();
    }
    const auto ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0);
    const bool ok = nmr_test::failures() == before;
    if (!ok) ++failed;
    std::printf("[%s] %-54s %8.1f ms\n", ok ? "  OK  " : " FAIL ", c.name, ms.count());
  }
  std::printf("\n%d tests, %d failed\n", run, failed);
  return failed == 0 ? 0 : 1;
}
