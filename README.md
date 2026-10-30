# netcode-io

WebSocket and WebTransport on one port for Node.js, served by
[quic-zig](https://github.com/endel/quic-zig) through Node-API.

```sh
npm install netcode-io
```

Prebuilt for Linux on x64 and arm64 (glibc 2.28 or later) and macOS on
Apple silicon. The x64 build needs x86-64-v3 with AES-NI, which servers have
had since 2013. Other platforms build from source (below).

It needs Node.js 22 or later, and is an ES module only: CommonJS can still
`require()` it from Node.js 22.12, which loads ES modules that way.

```js
import { Server } from 'netcode-io';

const server = new Server({ port: 4433, cert: 'server.crt', key: 'server.key' }, {
  onWsMessage(ws, data, isText) { server.send(ws, data, isText); },

  onSessionRequest(conn, session, req) { server.acceptSession(conn, session); },
  onStreamData(conn, stream, data, fin) { server.sendStream(conn, stream, data, fin); },
  onDatagram(conn, session, data) { server.sendDatagram(conn, session, data); },
});
```

The API is ids and callbacks, nothing per connection: a framework builds
its own objects on top. `lib/index.d.ts` documents every call.

Plain HTTP on the same port goes to Node's own HTTP implementation, so
Express and friends work unchanged:

```js
const app = express();
server.serveHttp(http.createServer(app));
```

quic-zig answers WebSocket upgrades itself, and hands every other HTTP/1.1
connection over as bytes, TLS already removed, from its first request on.
`serveHttp` wraps each in a `net.Socket`-shaped `RawSocket` for the
`http.Server`'s 'connection' event; the server is never `listen()`ed.

Requests a hot path should answer without Node's HTTP stack, matchmaking
say, can be taken first: quic-zig offers each one, head and body parsed,
and what `onHttpRequest` passes goes on to the `http.Server`, bytes
untouched.

```js
const server = new Server(options, {
  onHttpRequest(id, req) {
    if (req.path !== '/match') { return server.passRequest(id); }
    server.respond(id, 200, { 'content-type': 'application/json' }, JSON.stringify(match(req.body)));
  },
}).serveHttp(http.createServer(app));
```

## How it runs

QUIC runs on a thread of its own, not Node's. Its acknowledgements and loss
timers live in userspace, so a room tick or a GC pause on the JS thread
would hold them up; TCP's run in the kernel regardless.

WebSockets and plain HTTP run on Node's thread by default, as with `ws` or
uWebSockets.js. quic-zig's listener runs on a libxev loop of its own whose
kqueue or epoll fd libuv polls, so Node wakes for it as for one of its own
sockets. Each wake reads the sockets that are ready and hands JS what came
of it in one call, where uWebSockets.js calls JS once per message; a wake
with more to read hands over what is 20 µs old and reads on. The commands
JS answers with are written to the sockets before the call returns. On
Linux, Colyseus rooms of echo, broadcast, state sync and events cost
0.6–1.0× the CPU they take on uWebSockets.js, and joins as much.

`websocket: { thread: 'quic' }` serves them beside QUIC instead. The socket
work then leaves Node's event loop, which spent a fifth to two-thirds of the
time it does with uWebSockets.js in the same rooms, at 0.9–1.4× the CPU of
Node's thread for the hop between threads: for a process whose event loop is
the limit, with cores to spare.

Between threads, JS and the loop share two byte queues under one lock,
each record a 24-byte header and a payload (`src/wire.zig`, mirrored in
`lib/index.js`). Node's thread uses the same records, without the queues:

- **Commands.** Every call appends to a JS-side batch, and a microtask
  submits it: one Node-API call, and at most one wake of the loop thread,
  per turn, however many sends a broadcast makes. On Node's thread a long
  delivery also submits between events, once its batch is 50 µs old, so the
  first replies don't wait for the last events to be handled. A buffer sent
  again in the same batch goes as a reference to the first copy, so a broadcast
  crosses once; it follows that a buffer refilled with other bytes within a
  turn must be sent as a new object. Messages a batch has for one socket
  leave in one send, so a client that gets a patch and two broadcasts in
  one tick costs one syscall and one TCP segment rather than three.
- **Events.** The loop thread appends, and wakes JS once until JS takes
  them, at the end of its tick. Each batch is copied into a Buffer JS
  owns, and messages are views into it, valid for as long as they're kept.
  With `websocket.transientMessages`, events are written straight into one
  Buffer reused for every batch, as uWebSockets.js lends its messages: a
  message is valid only during its `onWsMessage` call, copied once on its
  way in, and nothing is allocated per delivery for the garbage collector
  to reclaim. On Linux a message of 32 KiB or more isn't copied at all: JS
  gets a view of quic-zig's read buffer, in a call of its own before the
  next read. Each `onHttpRequest` request ends a call into JS, so the
  microtasks it starts run before the next one, as between an
  `http.Server`'s 'request' events.

Past 8 MiB of events waiting for JS, stream and raw connection reads
pause, so flow control holds peers back, and datagrams are dropped. A
`RawSocket`'s writes wait once 256 KiB are on their way to the kernel.

`server.stats()` counts what crossed and how often each thread woke the
other; `bench/` compares both modes with uWebSockets.js.

## Compared with uWebSockets.js

The same echo or broadcast server on each, with nothing on top
(`bench/server.mjs`), driven over loopback by a Go client (`bench/wsload`), on
a bare-metal Intel Xeon E-2286G with its 6 cores held at 4.0 GHz, turbo and
deep C-states off: Ubuntu 24.04, Node.js 22.19, uWebSockets.js 20.52. Each round starts a fresh
server of each, in turn, warms it up for 30 s and measures 10 s. A figure is
the median of 3 rounds.

Server CPU per message received, all threads, and against uWebSockets.js
(below 1.00× is less):

| | uWebSockets.js | netcode-io | | `transientMessages` | |
|---|---:|---:|---:|---:|---:|
| Echo, 500 clients × 20/s | 12.2 µs | 10.8 µs | 0.89× | 10.7 µs | 0.88× |
| Echo, 2,000 clients × 20/s | 11.3 µs | 8.9 µs | 0.79× | 9.0 µs | 0.80× |
| Echo, 500 clients, one in flight each | 10.0 µs | 7.5 µs | 0.75× | 7.3 µs | 0.74× |
| Echo, 2,000 clients × 20/s, TLS | 16.7 µs | 10.3 µs | 0.62× | 9.6 µs | 0.58× |
| Broadcast, 32 rooms of 32, 4/s each | 187 µs | 165 µs | 0.89× | 169 µs | 0.90× |
| Broadcast, 32 rooms of 32, 2/s, 3 messages each | 527 µs | 195 µs | 0.37× | 199 µs | 0.38× |
| 1 KiB, 500 clients × 20/s | 12.3 µs | 12.6 µs | 1.03× | 10.8 µs | 0.88× |
| 16 KiB, 200 clients × 20/s | 19.1 µs | 21.9 µs | 1.15× | 18.0 µs | 0.94× |
| 16 KiB, 200 clients × 20/s, TLS | 45.4 µs | 43.7 µs | 0.96× | 38.2 µs | 0.84× |
| 64 KiB, 50 clients × 20/s | 43.4 µs | 51.9 µs | 1.20× | 44.4 µs | 1.02× |
| 64 KiB, 50 clients × 20/s, TLS | 133 µs | 108 µs | 0.82× | 94.6 µs | 0.71× |
| 16 KiB, 50 clients, one in flight each | 14.4 µs | 14.4 µs | 1.00× | 12.0 µs | 0.83× |

`transientMessages` lends messages for the length of the call, as
uWebSockets.js does. By default netcode-io keeps them, at the cost of a copy
per message, which takes it past uWebSockets.js from 1 KiB up. With one
message in flight per client, netcode-io answers 133–136k messages a second
to uWebSockets.js's 101k, and at 16 KiB 83k with `transientMessages`, 70k
without, to its 70k.

Round trip at the client, p50 / p99 in ms:

| | uWebSockets.js | netcode-io | `transientMessages` |
|---|---:|---:|---:|
| Echo, 500 clients × 20/s | 0.078 / 0.154 | 0.086 / 0.127 | 0.085 / 0.124 |
| Echo, 2,000 clients × 20/s | 0.314 / 0.821 | 0.251 / 0.521 | 0.253 / 0.520 |
| Echo, 500 clients, one in flight each | 4.95 / 5.21 | 3.73 / 4.04 | 3.64 / 3.92 |
| Echo, 2,000 clients × 20/s, TLS | 0.473 / 1.13 | 0.275 / 0.529 | 0.262 / 0.506 |
| Broadcast, 32 rooms of 32, 4/s each | 0.911 / 2.82 | 0.743 / 2.17 | 0.744 / 2.05 |
| Broadcast, 32 rooms of 32, 2/s, 3 messages each | 2.7 s / 7.0 s | 0.405 / 1.54 | 0.416 / 1.60 |
| 1 KiB, 500 clients × 20/s | 0.075 / 0.200 | 0.094 / 0.193 | 0.077 / 0.167 |
| 16 KiB, 200 clients × 20/s | 0.083 / 0.133 | 0.101 / 0.169 | 0.088 / 0.129 |
| 16 KiB, 200 clients × 20/s, TLS | 0.178 / 0.454 | 0.198 / 0.398 | 0.178 / 0.374 |
| 64 KiB, 50 clients × 20/s | 0.158 / 0.228 | 0.174 / 0.255 | 0.165 / 0.241 |
| 64 KiB, 50 clients × 20/s, TLS | 0.303 / 0.603 | 0.291 / 0.455 | 0.260 / 0.448 |
| 16 KiB, 50 clients, one in flight each | 0.670 / 1.37 | 0.634 / 1.49 | 0.541 / 1.13 |

A broadcast of three messages to each member leaves each socket in one write
here, where uWebSockets.js makes three, and at this rate falls behind. At
low rates p50 is 0–7 µs above uWebSockets.js's with `transientMessages`,
about what one server varies by from run to run, and p99 is lower in every
row but 64 KiB. Keeping messages costs 9–31 µs of p50 at 1–64 KiB. One
connection at 200 messages a second takes 31 µs there and back on both.

Held connections, each sending a 50-byte message a second, uWebSockets.js /
netcode-io (`transientMessages`, mean of 2 rounds):

| Connections | CPU per message | p99 | Memory per connection |
|---|---:|---:|---:|
| 10,000 | 16.3 / 14.2 µs | 0.28 / 0.23 ms | 1.3 / 2.0 KB |
| 25,000 | 14.9 / 13.1 µs | 0.65 / 0.55 ms | 0.9 / 1.5 KB |
| 50,000 | 15.6 / 12.8 µs | 1.12 / 0.77 ms | 0.7 / 1.3 KB |
| 100,000 | 9.9 / 9.8 µs | 1.74 / 1.37 s | 0.6 / 1.3 KB |

Both hold 100,000. Near 100,000 messages a second both run a full core, and
latency there is queueing. netcode-io takes about twice the memory per
connection. Its first 100,000 round lost one connection to the box's
firewall, whose connection-tracking table was full of the run before
(`nf_conntrack: table full, dropping packet`); the row is the second.

`bench/run.mjs` reproduces the first two tables (`--transports
uws,netcode-kept,netcode`, where `netcode` is `transientMessages`).

## Building

Zig 0.17, with quic-zig checked out beside this repo (`../quic-zig`):

```sh
git clone https://github.com/endel/quic-zig
git clone https://github.com/endel/node-netcode-io
cd node-netcode-io
zig build -Doptimize=ReleaseFast   # zig-out/lib/netcode_io.node, zig-out/bin/wt-test-client
npm test                           # Zig wire tests, then node --test with each websocket.thread
```

`lib/binding.js` loads `zig-out/lib/netcode_io.node` when it exists, and
otherwise `netcode-io-<platform>-<arch>`, the package npm installed for this
machine. A `v*` tag builds those on each platform, tests them and publishes
them (`.github/workflows/release.yml`).

## Not yet

- Accepting a WebSocket later: upgrades are accepted on arrival, because
  quic-zig's `WsRequest` lives only for its callback.
- Keep-alive across kinds: once a connection carries a request for the
  `http.Server`, it's that server's to its end, so a WebSocket upgrade or a
  request `onHttpRequest` would take on it goes there too. Browsers open
  WebSockets on connections of their own.
- WebSocket backpressure: past `maxSendBuffer` the socket is closed with
  1008, and reads never pause.
- Listening on a Unix socket, as uWebSockets.js's `listen_unix` does: behind
  a proxy that forwards over one (Colyseus Cloud's nginx), WebSockets can't
  be served. WebTransport needs its UDP port reachable directly anyway.
- Windows. Prebuilt packages for Intel Macs and musl Linux (Alpine): those
  build from source.

## License

MIT
