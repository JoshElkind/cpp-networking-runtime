#pragma once

#include <cstddef>
#include <cstdint>

namespace nmr {

struct PollEvent {
  void* data = nullptr;
  bool readable = false;
  bool writable = false;
  bool hangup = false;
};

class Poller {
 public:
  Poller();
  ~Poller();

  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  void add(int fd, void* data, bool want_read, bool want_write);
  void mod(int fd, void* data, bool want_read, bool want_write);
  void del(int fd);

  // Returns number of events. timeout_ms < 0 waits forever.
  int wait(PollEvent* out, int max_events, int timeout_ms);

 private:
  int fd_ = -1;
};

int set_nonblock(int fd);
int set_nodelay(int fd);
int set_reuseaddr(int fd);

}  // namespace nmr
