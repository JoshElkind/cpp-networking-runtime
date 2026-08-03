#pragma once

// Tiny self-registering test harness. CHECK records a failure and keeps
// going; REQUIRE aborts the current test.

#include <cstdio>
#include <sstream>
#include <string>
#include <vector>

namespace nmr_test {

struct Case {
  const char* name;
  void (*fn)();
};

struct Abort {};

inline std::vector<Case>& registry() {
  static std::vector<Case> r;
  return r;
}

inline int& failures() {
  static int n = 0;
  return n;
}

struct Register {
  Register(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

inline void fail(const char* file, int line, const std::string& what) {
  std::fprintf(stderr, "    %s:%d: %s\n", file, line, what.c_str());
  ++failures();
}

template <typename A, typename B>
std::string describe_eq(const char* ea, const char* eb, const A& a, const B& b) {
  std::ostringstream os;
  os << ea << " == " << eb << "  (" << a << " vs " << b << ")";
  return os.str();
}

}  // namespace nmr_test

#define TEST(name)                                                   \
  static void test_##name();                                         \
  static const nmr_test::Register register_##name(#name, test_##name); \
  static void test_##name()

#define CHECK(cond)                                                        \
  do {                                                                     \
    if (!(cond)) nmr_test::fail(__FILE__, __LINE__, "CHECK(" #cond ")");   \
  } while (0)

#define REQUIRE(cond)                                                       \
  do {                                                                      \
    if (!(cond)) {                                                          \
      nmr_test::fail(__FILE__, __LINE__, "REQUIRE(" #cond ")");             \
      throw nmr_test::Abort{};                                              \
    }                                                                       \
  } while (0)

#define CHECK_EQ(a, b)                                                                     \
  do {                                                                                     \
    const auto& nmr_a_ = (a);                                                              \
    const auto& nmr_b_ = (b);                                                              \
    if (!(nmr_a_ == nmr_b_)) {                                                             \
      nmr_test::fail(__FILE__, __LINE__, nmr_test::describe_eq(#a, #b, nmr_a_, nmr_b_));   \
    }                                                                                      \
  } while (0)
