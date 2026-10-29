# netcode-io

WebSocket and WebTransport on one port for Node.js, served by
[quic-zig](https://github.com/endel/quic-zig) through Node-API.

```js
const { Server } = require('netcode-io');

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
  On Node's thread a message of 32 KiB or more is copied once, straight
  into a Buffer of its own, instead. With `websocket.transientMessages`,
  events are written straight into one Buffer reused for every batch, as
  uWebSockets.js lends its messages: a message is valid only during its
  `onWsMessage` call, copied once on its way in, and nothing is allocated
  per delivery for the garbage collector to reclaim. On Linux a message of
  32 KiB or more isn't copied at all: JS gets a view of quic-zig's read
  buffer, in a call of its own before the next read. Each `onHttpRequest` request ends a call
  into JS, so the microtasks it starts run before the next one, as
  between an `http.Server`'s 'request' events.

Past 8 MiB of events waiting for JS, stream and raw connection reads
pause, so flow control holds peers back, and datagrams are dropped. A
`RawSocket`'s writes wait once 256 KiB are on their way to the kernel.

`server.stats()` counts what crossed and how often each thread woke the
other; `bench/` compares both modes with uWebSockets.js.

## Building

Zig 0.17, with quic-zig checked out beside this repo (`../quic-zig`):

```sh
zig build            # zig-out/lib/netcode_io.node, zig-out/bin/wt-test-client
npm test             # Zig wire tests, then node --test with each websocket.thread
```

`lib/binding.js` loads `zig-out/lib/netcode_io.node` when it exists, and
otherwise `netcode-io-<platform>-<arch>`.

## Not yet

- Accepting a WebSocket later: upgrades are accepted on arrival, because
  quic-zig's `WsRequest` lives only for its callback.
- Keep-alive across kinds: once a connection carries a request for the
  `http.Server`, it's that server's to its end, so a WebSocket upgrade or a
  request `onHttpRequest` would take on it goes there too. Browsers open
  WebSockets on connections of their own.
- WebSocket backpressure: past `maxSendBuffer` the socket is closed with
  1008, and reads never pause.
- Windows; prebuilt packages.
