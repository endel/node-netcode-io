# 2026-10-30

Vultr `vbm-6c-32gb`: Xeon E-2286G, 6 cores at 4.0 GHz (turbo and deep
C-states off), 32 GB, Ubuntu 24.04 (kernel 6.8), Node.js 22.19.0, Zig
0.17.0, uWebSockets.js 20.52.0. Prepared with `bench/baremetal/setup.sh`.

## The README's tables

quic-zig `51c3285`, netcode-io `aec2cd1`; the packaging and ES-module
commits after it don't touch what's measured.

- `matrix.json`, `matrix.log`: `matrix.sh`, 12 cells × uWebSockets.js,
  netcode-io (`netcode-kept`) and `transientMessages` (`netcode`), 3 rounds.
  `node ../../baremetal/tables.mjs matrix.json` prints the README's tables.
  In the three-messages broadcast, uWebSockets.js falls behind; the bench
  server then crashed on teardown, sending to a closed socket (fixed since in
  `server.mjs`), after its numbers were taken.
- `latency.log`: one connection, 200/s, 5 rounds: 31 µs at p50 on both.
- `ccu.log`: 10,000 to 100,000 held connections, 2 rounds. netcode-io's
  first 100,000 round lost one connection to the firewall's full
  connection-tracking table, not to netcode-io (no listen-queue drops in
  `nstat`); the README uses the second.
- `epoll.log`: the extra `epoll_pwait` per wake in netcode-io's Node-thread
  mode, measured in place with bpftrace: 563 ns, against a 609 ns `getppid`
  on this CPU (it pays for Meltdown page-table isolation).

## WebTransport

quic-zig `4ae310a` with netcode-io `088c4de` (`webtransport/matrix.*`), then
the same with quic-zig's sparse `flush()` (`51c3285`) A/B'd against it
(`webtransport/sparse-flush-ab.*`: `old` is without, `new` with). Medians
of 3, as CPU per message and p50, both in µs:

| Cell | netcode-io, new | netcode-io, old | quic-go | quic-zig demo |
|---|---:|---:|---:|---:|
| datagrams, 50 sessions × 20/s | 39.0, 77 | 46.3, 89 | 99.3, 73 | 71.9, 85 |
| datagrams, 250 × 20/s | 22.5, 95 | 48.0, 198 | 61.1, 88 | 80.4, 319 |
| stream, 250 × 20/s | 23.8, 110 | 48.2, 207 | 50.8, 92 | 92.4, 354 |
| datagrams, 1 session × 200/s | 51.1, 83 | 50.5, 82 | 76.5, 85 | 26.9, 65 |

`flush()` had visited every connection after each batch of commands from
JS. The quic-zig demo visits every connection on every pass (`onPollComplete`),
which is its 250-session latency; at one session it shows what quic-zig
costs with no Node in the way.

## Findings

What moved the numbers this round, each measured paired on this machine:

- **AES-GCM in one pass, and vector GHASH on x86** (quic-zig `a31e8c0`):
  seal and open of a 1,200-byte QUIC packet 895 → 472 ns, a 16 KiB TLS
  record at 5.9 GB/s against OpenSSL's 6.0. The 16 KiB WebSocket-over-TLS
  cell went from 1.08× uWebSockets.js's CPU to 0.93×.
- **AVX2 unmask** (quic-zig `4ae310a`): 16 KiB 310 → 141 ns.
- **Sparse `flush()`** (quic-zig `51c3285`): the table above.

Tried and dropped:

- **Lending 4–32 KiB messages from quic-zig's read buffer** without copying.
  Each needs a JS call before the next socket read reuses the buffer: the
  16 KiB closed loop lost 10%.
- **A libxev read buffer chosen when the read is performed**
  (`ReadBuffer.provided: *[]u8`, which would let every message be lent with
  no early call). Not built: adding one more 16 KiB copy per message, its
  ceiling, cost 0.4 µs (3% of throughput) in the 16 KiB closed loop and
  nothing measurable when paced (`experiments/cost-injection.json`,
  `netcode-copy2`).
- **Sockets polled by libuv directly**, to drop the nested `epoll_pwait`.
  It costs 563 ns a wake; one more per wake changed nothing measurable
  (`netcode-epoll2` in the same file), and an earlier attempt lost 11% in a
  closed loop (in a VM).
- **Delivery policy** (`experiments/delivery-threshold.json`,
  `delivery-bytes.json`). A JS call per message, as uWebSockets.js makes,
  matched its 16 KiB p50 but fell below its closed-loop throughput (97.7k
  against 100.8k a second). 5 µs instead of 20 µs was no better. Delivering
  once 16 KiB were pending put the 16 KiB closed loop below uWebSockets.js.
  The 20 µs default stays.

These experiments ran with temporary switches in netcode-io, not in any
commit: `NETCODE_IO_TEMP` (one more copy, one more `epoll_wait`),
`NETCODE_IO_DELIVER_AFTER_NS`, `NETCODE_IO_DELIVER_BYTES`.

Open:

- **Memory per connection**, 1.3 KB against uWebSockets.js's 0.6: quic-zig's
  `http1.Conn` is 1,016 bytes, three 176-byte libxev completions and a
  128-byte `sockaddr_storage` among them.
- **WebTransport latency at low rates**: every echo crosses from the QUIC
  thread to Node and back, 17–23 µs of p50 and ~25 µs of CPU per message
  over quic-zig alone. QUIC on Node's thread (quic-zig's server can join a loop
  it doesn't own) would remove it; libuv's timers are 1 ms.
