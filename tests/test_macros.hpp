#pragma once

#include <cstdio>
#include <vector>

namespace nmr_test {
struct Case {
  const char* name;
  void (*fn)();
};
inline std::vector<Case>& registry() {
  static std::vector<Case> r;
  return r;
}
struct Add {
  Add(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};
inline int& failed() {
  static int v = 0;
  return v;
}
inline int& passed() {
  static int v = 0;
  return v;
}
}  // namespace nmr_test

#define TEST(name)                               \
  void name();                                   \
  static nmr_test::Add add_##name(#name, name);  \
  void name()

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      nmr_test::failed()++;                                                    \
    } else {                                                                   \
      nmr_test::passed()++;                                                    \
    }                                                                          \
  } while (0)
