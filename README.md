# cpp-networking-runtime

High-throughput publish/subscribe messaging runtime in C++20. A small broker sitting between a message bus and a low-latency RPC fabric: nonblocking TCP, an OS event loop (`epoll` on Linux, `kqueue` on macOS), sharded lock-free queues, pooled buffers, and subscriber backpressure.

```
TCP publishers / subscribers
            │
            ▼
   Network event loop
   nonblocking sockets, epoll/kqueue
            │
            ▼
   Topic-sharded SPSC queues
            │
     ┌──────┴──────┐
  worker 0      worker N     lock-free dequeue, fan-out
     └──────┬──────┘
            ▼
   Subscriber write buffers
   high-watermark backpressure
```

## Features

- Binary framed protocol (`PUBLISH` / `SUBSCRIBE` / `UNSUBSCRIBE`)
- Nonblocking TCP + edge-triggered I/O
- Per-topic lock-free SPSC rings into worker threads
- Slab memory pool (vs global `new`)
- Write-side backpressure when a slow subscriber falls behind
- Benchmarks: queue Mops/s, pool vs `new`, end-to-end msgs/s and p50/p95/p99

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/nmr-tests
./build/nmr-bench
```

Sanitizers:

```bash
cmake -S . -B build-asan -DNMR_ENABLE_SANITIZERS=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-asan -j
./build-asan/nmr-tests
```

## Run

```bash
./build/nmr-broker 9000 2          # port, worker threads
./build/nmr-loadgen 9000 4 2 3 64  # port pubs subs seconds payload_bytes
```

## Layout

| Path | Role |
|---|---|
| `include/nmr/poller.hpp` | `epoll` / `kqueue` |
| `include/nmr/spsc_queue.hpp` | lock-free SPSC + mutex queue for comparison |
| `include/nmr/memory_pool.hpp` | fixed-size slab pool |
| `include/nmr/protocol.hpp` | 12-byte binary header |
| `include/nmr/server.hpp` | broker, connections, workers |
| `bench/` | micro + loopback e2e |

Linux is the intended production I/O path (`epoll`). macOS is supported via `kqueue` so the same tree builds on a laptop.

## Measured (this machine, Release, loopback)

- Lock-free SPSC: **163 Mops/s** vs mutex queue **40 Mops/s** (~4×)
- Slab pool alloc/free: **94 Mops/s** vs `new`/`delete` **69 Mops/s**
- End-to-end throughput: **~740k msgs/s** (4 publishers, 2 subscribers, 64B payload)
- End-to-end latency: **p50 43 μs / p99 60 μs** (1–1, 64B, 20 μs pacing)

Re-run `./build/nmr-bench` before quoting numbers on another host.
