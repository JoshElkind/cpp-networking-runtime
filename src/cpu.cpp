#include "nmr/cpu.hpp"

#include <pthread.h>

#if defined(__linux__)
#include <sched.h>
#endif

namespace nmr {

bool pin_current_thread(int cpu) noexcept {
#if defined(__linux__)
  if (cpu < 0 || cpu >= CPU_SETSIZE) return false;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set) == 0;
#else
  (void)cpu;
  return false;
#endif
}

void set_current_thread_name(const char* name) noexcept {
#if defined(__APPLE__)
  ::pthread_setname_np(name);
#elif defined(__linux__)
  char buf[16] = {};
  for (int i = 0; i < 15 && name[i] != '\0'; ++i) buf[i] = name[i];
  ::pthread_setname_np(::pthread_self(), buf);
#else
  (void)name;
#endif
}

}  // namespace nmr
