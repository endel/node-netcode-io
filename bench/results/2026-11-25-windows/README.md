# 2026-11-25, Windows

A laptop: Intel Core i7-11800H (8 cores, 16 threads, 2.3 GHz base, turbo on),
Windows 11 Pro (build 26200), Balanced power plan on AC. Node.js 22.23.2,
Zig 0.17.1, Go 1.27.4, uWebSockets.js 20.71.0. Server and load on the same
machine, unpinned, the load at `GOMAXPROCS=4`.

The addon was built as the win32-x64 package is (`-Dtarget=x86_64-windows-gnu
-Dcpu=x86_64_v3+aes+pclmul -Dstrip`), from netcode-io `2d28071` with this
round's changes, against quic-zig `9cda382` (the `windows-perf`
branch: `87f8e92` and `9cda382` on `6a4fc6a`). `127c8ca` simplified them
afterwards: a socket's read cancel borrows its write completion, 272 bytes
less per connection than below, not re-measured.

CPU is the server's cycle time over the window (`wsload -pid`), since Windows
charges `process.cpuUsage` a clock tick at a time; the per-thread `main` and
`loop` splits in the logs are still tick-charged, so `other` there is noise.
Latency is timed with `QueryPerformanceCounter`. See "On Windows" in
`bench/README.md`.

## The tables

- `matrix.json`, `matrix.log`: `run.mjs --rounds 3 --warmup 30 --measure 10
  --transports uws,netcode,netcode@quic`, the README's 12 cells, the 2/s
  broadcast, and 10,000 held connections. `netcode` is `transientMessages`
  on Node's thread, `netcode@quic` the same with `websocket.thread: 'quic'`.
  Medians of 3.

CPU per message received, all threads:

| | netcode-io | uWebSockets.js | Less CPU | netcode-io, `thread: 'quic'` |
|---|---:|---:|---:|---:|
| Echo, 500 clients × 20/s | 24.2 µs | 29.4 µs | 1.21× | 28.6 µs |
| Echo, 2,000 clients × 20/s | 20.5 µs | 22.6 µs | 1.10× | 21.4 µs |
| Echo, 2,000 clients × 20/s, TLS | 20.8 µs | 24.4 µs | 1.17× | 21.3 µs |
| Echo, 500 clients, one in flight each | 15.4 µs | 17.0 µs | 1.10× | 14.0 µs |
| Broadcast, 32 rooms of 32, 2/s each | 437 µs | 461 µs | 1.06× | 435 µs |
| Broadcast, 32 rooms of 32, 4/s each | 245 µs | – | uWebSockets.js falls behind | 248 µs |
| Broadcast, 32 rooms of 32, 2/s, 3 messages each | 440 µs | – | uWebSockets.js falls behind | 438 µs |
| 1 KiB, 500 clients × 20/s | 26.3 µs | 36.1 µs | 1.37× | 34.7 µs |
| 16 KiB, 200 clients × 20/s | 37.8 µs | 53.7 µs | 1.42× | 59.1 µs |
| 16 KiB, 200 clients × 20/s, TLS | 61.1 µs | 72.8 µs | 1.19× | 87.6 µs |
| 64 KiB, 50 clients × 20/s | 110 µs | 149 µs | 1.35× | 215 µs |
| 64 KiB, 50 clients × 20/s, TLS | 196 µs | 324 µs | 1.65× | 309 µs |
| 16 KiB, 50 clients, one in flight each | 19.5 µs | 25.9 µs | 1.33× | 23.8 µs |
| 10,000 held connections × 1/s | 29.6 µs | 36.5 µs | 1.23× | 36.5 µs |

With one message in flight per client, netcode-io answers 64.9k messages a
second to uWebSockets.js's 58.9k (1.10×), 74.7k on the QUIC thread, and at
16 KiB 53.1k to 39.6k (1.34×).

Round trip at the client, p50 and p99:

| | netcode-io p50 | uWebSockets.js p50 | netcode-io p99 | uWebSockets.js p99 |
|---|---:|---:|---:|---:|
| Echo, 500 clients × 20/s | 0.147 ms | 0.155 ms | 0.607 ms | 0.747 ms |
| Echo, 2,000 clients × 20/s | 0.594 ms | 0.840 ms | 9.23 ms | 9.84 ms |
| Echo, 2,000 clients × 20/s, TLS | 0.429 ms | 1.14 ms | 6.08 ms | 8.94 ms |
| Echo, 500 clients, one in flight each | 7.56 ms | 8.37 ms | 10.6 ms | 12.1 ms |
| Broadcast, 32 rooms of 32, 2/s each | 1.62 ms | 2.77 ms | 7.73 ms | 12.6 ms |
| Broadcast, 32 rooms of 32, 4/s each | 222 ms | 10.5 s | 436 ms | 22.6 s |
| Broadcast, 32 rooms of 32, 2/s, 3 messages each | 1.75 ms | 14.5 s | 10.1 ms | 31.4 s |
| 1 KiB, 500 clients × 20/s | 0.144 ms | 0.176 ms | 0.808 ms | 1.16 ms |
| 16 KiB, 200 clients × 20/s | 0.114 ms | 0.140 ms | 0.447 ms | 0.699 ms |
| 16 KiB, 200 clients × 20/s, TLS | 0.182 ms | 0.195 ms | 0.705 ms | 0.914 ms |
| 64 KiB, 50 clients × 20/s | 0.231 ms | 0.406 ms | 0.997 ms | 1.09 ms |
| 64 KiB, 50 clients × 20/s, TLS | 0.339 ms | 0.705 ms | 1.34 ms | 1.76 ms |
| 16 KiB, 50 clients, one in flight each | 0.909 ms | 1.24 ms | 1.89 ms | 2.75 ms |
| 10,000 held connections × 1/s | 0.156 ms | 0.170 ms | 0.905 ms | 1.15 ms |

Held, 10,000 connections took 2.6 KB each in netcode-io, 2.5 KB on the QUIC
thread, and 1.4 KB in uWebSockets.js. Windows hands one machine about 16,000
client ports, so no higher.

Read with care:

- **Broadcasts at 4/s** run a full core on both: Windows' `send` costs about
  7 µs, and 131,072 of them a second is more than one core sends. netcode-io
  still handles 4,087 of the 4,096 messages a second, 222 ms behind at p50;
  uWebSockets.js handles none inside the window. With three messages each at
  2/s, netcode-io's one write per member keeps it under a core (0.90), where
  uWebSockets.js's three don't.
- **The QUIC thread** costs about as much as Node's for small messages, and
  up to twice as much for large ones: at 1,000 messages a second nearly every
  message wakes the other thread both ways (0.58 wakes and 0.70 notifies per
  message at 64 KiB), and a 64 KiB message is copied four or five times
  crossing over and back. That mode is for a process whose event loop is the
  limit, with cores to spare.
- **A laptop.** Turbo and the Balanced plan move the clock; rounds are
  paired and alternate, but uWebSockets.js's 64 KiB cell ranged 97–170 µs
  across this day's runs. Compare servers within a run, not runs or machines.

## WebTransport

No comparison here (uWebSockets.js has no WebTransport): `wtload` against
`wt-server.mjs`, one run per cell, 5 s warmup and 8 s measured, to check
the QUIC side holds up on Windows. No loss and no errors in any cell.

| Cell | Messages a second | p50 | p99 |
|---|---:|---:|---:|
| datagrams, 50 sessions × 20/s | 1,000 | 0.146 ms | 0.269 ms |
| stream, 50 × 20/s | 1,000 | 0.153 ms | 0.274 ms |
| datagrams, 250 × 20/s | 5,000 | 0.141 ms | 0.525 ms |
| stream, 250 × 20/s | 5,000 | 0.154 ms | 0.622 ms |
| datagrams, 50 sessions, one in flight each | 72,299 | 0.672 ms | 1.72 ms |
| stream, 50, one in flight each | 71,167 | 0.681 ms | 1.58 ms |
| datagrams of 1,000 bytes, 50 × 20/s | 1,000 | 0.136 ms | 0.552 ms |

## Findings

Each change measured paired on this machine (`experiments/`).

- **`PortWatch` reordered completions** (quic-zig `87f8e92`). On Windows a
  thread waits on libxev's completion port and wakes Node; it took the first
  packet off the port and posted it back, behind whatever had arrived
  meanwhile, so a tick's earliest event ran last. `http.test.js`'s "requests
  that arrive together" saw `/c` before `/b` in 3 of 6 runs here (CI's
  2-core runners passed by luck). A completion port is signaled while it
  holds packets and a wait on it takes none, so the thread now just waits:
  20 of 20.
- **Reads waited on a buffer per socket** (quic-zig `9cda382`). IOCP
  completes a read into memory given when it started, so each connection
  kept 16 KiB posted while idle, and a message over 16 KiB came 16 KiB per
  trip through the loop. Two fixes were measured, 2 rounds each:
  - `read-drain.*`: a read that filled the buffer took the rest at once with
    a nonblocking recv into a larger per-thread buffer (`netcode` there,
    against the original, `netcode-base`). 64 KiB went 178 → 139 µs, the
    16 KiB closed loop 44.6k → 50.3k msg/s, memory unchanged.
  - `read-poll.*`, kept: wait for readiness through AFD and recv into one
    256 KiB buffer per thread, as epoll does and as uWebSockets.js does on
    Windows (`netcode` there, against the drain, `netcode-drain`): 10,000
    held connections 18.6 →
    2.6 KB each, echo at 2,000 clients p99 26 → 14 ms, closed loop 61.7k →
    65.7k msg/s, the rest equal or better. Messages of 32 KiB or more are
    lent to JS from that buffer, as on Linux.
- **Before either** (`survey-before.*`, `ccu-before.*`, one round), netcode-io
  was even with uWebSockets.js on echo, behind at 64 KiB (150 against
  97 µs), and held 18.7 KB per connection to its 1.4.

Tried and dropped:

- **Waking Node without the thread**: a wait completion packet
  (`NtAssociateWaitCompletionPacket`) on libuv's port, shaped as the
  `uv_async_send` of a uv_async_t, would have let Node wake straight from
  its own wait. Windows refuses a completion port as the object to wait on
  (`STATUS_INVALID_PARAMETER_3`). Everything off Node's main thread, that
  thread included, came to 0.05 of a core or less in every cell.

Learned on the way:

- A cancelled libxev completion isn't always called back: one added in the
  same tick is dropped before it reaches the kernel, on every backend, and
  epoll never calls back a cancelled read or write. IOCP calls back one
  that was active, aborted. A readiness poll's cancel in `abort` hit the
  first case; the socket looks at the poll's state when the cancel
  completes (`xev_backend.cancelCompletionThen` states the rule).
