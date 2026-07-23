#include "test_macros.hpp"

int main() {
  for (auto& c : nmr_test::registry()) {
    std::printf("%s\n", c.name);
    c.fn();
  }
  std::printf("%d passed, %d failed\n", nmr_test::passed(), nmr_test::failed());
  return nmr_test::failed() ? 1 : 0;
}
