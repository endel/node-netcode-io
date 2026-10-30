# Benchmarks

How the numbers in the README's comparison with uWebSockets.js were taken,
how to take them again, and what skews them. Each round's raw data and
findings are in `results/<date>/`.

## What's here

- `run.mjs`: the cells (echo, broadcast, sizes, TLS, closed loops), each run
  paired across servers, the order alternating per round, a fresh server and
  load per run. `--only <regex>`, `--rounds`, `--warmup`, `--measure`,
  `--transports`, `--out <json>`; its header lists the rest.
- `server.mjs`: the server under test, an echo or room broadcast with nothing
  on top. `uws` is uWebSockets.js (`UWS=<path>`), `netcode` netcode-io with
  `transientMessages`, `netcode-kept` its default, and `netcode-<dir>` a build
  copied to `./netcode-<dir>` for A/B runs.
- `wsload/`: the WebSocket load client (Go). Paced (`-rate` per connection,
  `-spread`) or closed loop (`-rate 0 -inflight n`); prints one JSON line.
- `wtload/`: the same for WebTransport, datagrams or one bidi stream per
  session, and `wtload/echo`, quic-go's echo server. `wt-server.mjs` is
  netcode-io's.
- `baremetal/`: preparing a machine and running a round on it.

## A round

On bare-metal x86 Linux. A VM has no hardware counters and its timing
drifts by ±15% between identical runs. An Apple Silicon Mac can't measure
x86 at all: Docker Desktop runs arm64 Linux, and emulated x86 timings say
nothing about the real thing. The 2026-10-30 round used a Vultr `vbm-6c-32gb`
(Xeon E-2286G, 6 cores, 32 GB) with Ubuntu 24.04.

```sh
# a machine (or bring your own Ubuntu 24.04 x86_64 box)
VULTR_API_KEY=… bench/baremetal/vultr.sh create vbm-6c-32gb <region> <os id>
bench/baremetal/vultr.sh status            # ~20 min until it answers SSH

# from this checkout, with quic-zig beside it
bench/baremetal/sync.sh <ip>
ssh root@<ip> 'sh /root/netcode/netcode-io/bench/baremetal/setup.sh'   # once per boot
ssh root@<ip> 'sh /root/netcode/netcode-io/bench/baremetal/build.sh'   # after each sync

# on the machine, in /root/netcode/netcode-io/bench/baremetal
./matrix.sh && node tables.mjs ../../../results/matrix.json   # the README's CPU and latency tables
./latency.sh        # one connection, 200/s
./ccu.sh 2          # 10k to 100k held connections
./counters.sh       # instructions, cycles, cache and branch misses per message
./profile.sh 50 20 65536   # cycles by symbol; ./profile.sh syscalls
./epoll.sh          # what the extra epoll_wait per wake costs
./webtransport.sh   # netcode-io, quic-go and quic-zig's demo over WebTransport

bench/baremetal/vultr.sh delete            # it bills until deleted
```

Results land in `$OUT` (default `/root/netcode/results`). `SERVER_CPUS` and
`CLIENT_CPUS` pin the server and the load to separate cores; the defaults
fit 6 cores with SMT.

## Method

- **Paired rounds.** Every server runs in every round, the order alternating,
  each on a fresh process. Compare servers within one run, never across two:
  the same build differs more between two runs than two builds within one.
- **Warm up for 30 s, measure 10 s.** See V8 below.
- **Medians**: of 3 rounds for the matrix, 5 for latency, 2 for CCU.
- **CPU** is the server process's, every thread, per message received.
  **Latency** is the round trip at the client.
- **Like for like**: uWebSockets.js lends each message for the length of the
  call; netcode-io does too with `transientMessages`, so that is the mode
  compared. Its default copies each message into a Buffer that can be kept.

## What skews it

Each of these once looked like a difference between the servers.

- **CPU frequency.** Ubuntu's `powersave` governor idles cores at 800 MHz:
  paced cells came out 4× slower per message, depending on which server
  happened to keep its core busy. `setup.sh` fixes the clock at base, turbo
  off.
- **Deep C-states.** Waking a core from C6 took ~75 µs of a 31 µs round
  trip, so one-connection latency read 107–112 µs and swung between rounds.
  `setup.sh` disables every idle state but polling.
- **V8.** netcode-io's JS reaches TurboFan after ~3,000 calls per function,
  ~15 s at 200 messages a second; uWebSockets.js's handler is one line. And
  V8's memory reducer runs a mark-compact 20–30 s in, at a different time in
  each server: one inside a 10 s window adds ~2 µs per message at 200 a
  second. A window from 12 s read 98k instructions per message against 51k;
  from 34 s, 48.1k against 48.6k. Hence the 30 s warmup.
- **Noise floor.** p50 medians of identical builds differ by up to 5–6 µs.
  A smaller p50 gap is not a result.
- **The load client.** `wsload`'s sends arrive in small bursts, about 4 per
  wake at 500 clients × 20/s (Go's timers), which is when one-JS-call-per-wake
  batching shows in p50. `GOMAXPROCS=4` keeps it from starving the server.
- **Held connections.** One address pair has ~45,000 client ports, so
  `ccu.sh` spreads clients over 127.0.0.k. Server ports sit below the
  clients' range: Go's client sockets don't set `SO_REUSEADDR`, and their
  TIME_WAITs blocked the next server's bind.
- **A host firewall.** Connection tracking covers loopback too. Two
  100,000-connection runs back to back filled the default table (262,144)
  and the kernel dropped SYNs for ~2 minutes (`nf_conntrack: table full`).
  `setup.sh` raises `nf_conntrack_max`.
- **Broadcasts.** Both servers send to each member in a loop. A
  uWebSockets.js application could use `publish` or `cork`, which this
  doesn't measure.
- **quic-zig's demo `wt-echo-server`** declares `onPollComplete`, so every
  pass visits every connection, and it prefixes echoes with `Echo: ` and
  drops reads over 1 KiB. It is not a stand-in for the library: compare
  netcode-io or quic-go.
- **Caps.** netcode-io refuses QUIC connections past 256 and TCP ones past
  4,096 unless `maxConnections` and `websocket.maxConnections` say otherwise;
  the bench servers raise both.
