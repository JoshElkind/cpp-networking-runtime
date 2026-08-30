# cpp-networking-runtime

A pub/sub message broker in C++20, built around a shared-nothing threading
model: one IO thread owns every socket, topic workers own disjoint shards of
the subscription table, and the only thing they share is a pair of lock-free
SPSC rings per worker. There are no mutexes anywhere on the data path.

```
 publishers ──┐                                              ┌──▶ subscribers
              ▼                                              │
     ┌──────────────────────────── IO thread ─────────────────────────────┐
     │ epoll/kqueue (edge-triggered) → recv → parse → copy frame once     │
     │ into a pooled, refcounted Frame → route by hash(topic)             │
     │                                                                    │
     │ drain egress → append Frame* to each subscriber's write queue      │
     │ → one sendmsg() (scatter/gather) per dirty socket per loop         │
     └────────┬─────────────────────▲──────────────────┬──────────▲───────┘
      ingress │ SPSC          egress│ SPSC     ingress │    egress│
              ▼                     │                  ▼          │
     ┌──────────────────┐                     ┌──────────────────┐
     │ worker 0         │                     │ worker N-1       │
     │ TopicShard       │         ...         │ TopicShard       │
     │ topic → [conn]   │                     │ topic → [conn]   │
     └──────────────────┘                     └──────────────────┘
```

## Design

**Threads own data, not locks.** The IO thread is the only thread that
touches a socket, a `Connection`, or the frame allocator. Each worker is the
only thread that touches its `TopicShard`. Topics are assigned to workers by
FNV-1a hash, so a SUBSCRIBE and later PUBLISHes on the same topic go through
the same FIFO and can't be reordered.

**Zero-copy fan-out.** An incoming PUBLISH is copied once, from the socket
buffer into a pooled `Frame`, and that same `Frame*` is queued on every
subscriber. Its wire bytes are forwarded as-is, never re-encoded. Writes go
out with `sendmsg()` over an iovec of queued frames, once per socket per loop
iteration, so a burst to one subscriber costs one syscall instead of one per
message.

**Refcounting without atomics.** Workers never free anything. For each
publish, a worker emits one `Egress{frame, conn}` per subscriber followed by
`Egress{frame, kNoConn}`, which hands back the worker's reference. Because the
egress ring is FIFO, the IO thread sees every delivery before the release, so
it can do all retain/release itself with plain integers. That's also why
`FramePool` can be single-threaded: size-classed slabs with intrusive free
lists, no locks.

**Connection lifetime.** Workers refer to subscribers by a 64-bit connection
id that's never reused, not by pointer. A delivery to a connection that has
since closed is a failed hash lookup, not a use-after-free.

**Sleeping without missing wakeups.** When idle, the IO thread blocks in
`epoll_wait`/`kevent`, and workers wake it via an eventfd (a pipe on macOS).
The handshake is a Dekker-style pair of seq_cst fences: the IO thread stores
`io_sleeping_`, fences, and re-checks the egress rings; a worker publishes to
its ring, fences, and checks `io_sleeping_`. At least one side always sees the
other's store. `--busy-poll` skips the handshake entirely and spins.

**Backpressure.**
- *Slow subscribers.* Each connection has a byte high-watermark on its write
  queue. Past it, the broker either drops messages for that subscriber
  (market-data style) or disconnects it (`--slow-consumer`). Other subscribers
  are unaffected.
- *Overloaded workers.* If a worker's ingress ring is full, publishes are shed
  at the IO thread and counted. Control messages (subscribe, unsubscribe,
  disconnect) are never dropped; they wait in a small per-worker backlog, and
  publishes are shed while that backlog is non-empty so nothing overtakes it.
- *Fairness.* Edge-triggered IO has to read to `EAGAIN`, so one firehose
  publisher could otherwise starve the whole loop. Each connection gets a
  256 KiB read budget per iteration, and connections that run out are
  re-serviced on the next pass.

**Ordering guarantee.** Messages from one publisher on one topic arrive at
each subscriber in order. Heartbeats are echoed by the IO thread directly, so
a client that sends SUBSCRIBE then a heartbeat, and gets the echo back, knows
its subscription is queued ahead of anything published after that. The tests
and the load generator use this instead of sleeping.

## Wire format

Little-endian, 12-byte header, validated before any allocation:

| offset | size | field |
|---|---|---|
| 0 | 4 | magic `0x4E4D5254` |
| 4 | 1 | type: 1 PUBLISH, 2 SUBSCRIBE, 3 UNSUBSCRIBE, 4 HEARTBEAT |
| 5 | 1 | flags (reserved) |
| 6 | 2 | topic length (≤ 255) |
| 8 | 4 | payload length (≤ 1 MiB) |
| 12 | … | topic, then payload |

A malformed frame closes that connection and nothing else.

## Build and run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
./build/nmr-tests

./build/nmr-broker --port 9000 --workers 2 --stats 1
./build/nmr-loadgen --port 9000 --pubs 4 --subs 2 --topics 4 --rate 100000 --seconds 5
```

Sanitizers: `-DNMR_SANITIZER=address` (ASan + UBSan) or `-DNMR_SANITIZER=thread`.
CI runs GCC and Clang on Linux plus Clang on macOS, in Release, ASan/UBSan and
TSan, all with `-Werror`.

## Benchmarks

`./build/nmr-bench [queue|pool|e2e]`. Numbers below are from an Apple M3 laptop
(macOS 14) and from a Linux VM (Docker on the same machine). Treat them as
relative comparisons, not as a statement about production hardware.

**SPSC ring vs. mutex + deque** (Mops/s, 1 producer → 1 consumer):

| capacity | SPSC, M3 | SPSC, Linux VM | SPSC padded, M3 | mutex, M3 | mutex, Linux VM |
|---|---|---|---|---|---|
| 256 | 366 | 414 | 83 | 26 | 9 |
| 1024 | 301 | 471 | 87 | 44 | 16 |
| 4096 | 51 | 472 | 86 | 51 | 17 |
| 65536 | 18 | 466 | 90 | 54 | 19 |

Cross-core round trip through two rings: p50 83 ns, p99 125 ns (M3).

The M3 column is the interesting one. With 8-byte elements, 16 slots share a
128-byte line, and in this benchmark the consumer is faster, so it runs right
behind the producer. Small rings absorb that well, but past about 4K slots
macOS throughput collapses. Padding each slot to a full line (the "padded"
column) makes the cost flat at about one line transfer per element (~11 ns),
which points at line sharing between the two cores. The same binary, built in
a Linux VM on the same chip, doesn't degrade at all, so the trigger looks
OS-side (macOS is free to put the two threads on different core clusters,
where line transfers are much more expensive). I haven't pinned it down
further. It's a good reminder that ring capacity is a tuning parameter, not a
free "bigger is safer" knob.

**Frame allocation**, 1024 frames live, freed FIFO (Mops/s):

| | FramePool, M3 | new/delete, M3 | FramePool, Linux | new/delete, Linux |
|---|---|---|---|---|
| 64 B | 332 | 64 | 374 | 96 |
| 1 KiB | 293 | 66 | 181 | 43 |

**End to end** over loopback TCP, 64-byte payloads. Latency is measured from
each message's *scheduled* send time, so publisher stalls count against the
broker instead of being hidden (coordinated omission):

| scenario | result |
|---|---|
| 1 → 1 at 20k msg/s, M3 | p50 17 µs, p90 25 µs, p99 94 µs |
| 1 → 1 at 20k msg/s, M3, `--busy-poll` | p50 14 µs, p90 18 µs, p99 115 µs |
| 4 pubs × 100k msg/s → 2 subs, Linux VM | 800k deliveries/s, 0 loss, p50 209 µs |
| unpaced overload, Linux VM | 6.0M deliveries/s delivered; excess shed at ingress |

Everything here, clients included, shares one 8-core laptop, so the tail
percentiles mostly measure the OS scheduler. Pinning (`--pin-io`,
`--pin-workers`) and isolated cores on a Linux box are the next step for
meaningful tails.

## Layout

| Path | |
|---|---|
| `include/nmr/spsc_queue.hpp` | bounded SPSC ring with cached indices |
| `include/nmr/frame_pool.hpp` | size-class slab allocator, refcounted `Frame` |
| `include/nmr/broker.hpp` | `TopicShard`, ingress/egress messages, topic hashing |
| `include/nmr/server.hpp` | IO loop, workers, backpressure, shutdown |
| `include/nmr/connection.hpp` | per-socket buffers, scatter/gather writes |
| `include/nmr/poller.hpp` | epoll / kqueue, eventfd / pipe waker |
| `include/nmr/protocol.hpp` | wire format encode/decode |
| `include/nmr/histogram.hpp` | log-linear latency histogram (≤3.1% error) |
| `tools/` | `nmr-broker`, `nmr-loadgen`, shared workload driver |
| `tests/` | unit tests plus loopback integration tests |
| `bench/` | queue, allocator, end-to-end |

## Limitations

- IPv4 only, no TLS, no authentication.
- One IO thread. That's the throughput ceiling, by design, to keep socket
  ownership single-threaded. Scaling out would mean N IO threads with
  `SO_REUSEPORT` and an (N × workers) mesh of rings.
- No persistence or replay: a subscriber sees messages published after its
  subscription is acknowledged, and nothing before.
