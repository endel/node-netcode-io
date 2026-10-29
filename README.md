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

## How it runs

quic-zig runs on a thread of its own, not Node's. QUIC's acknowledgements
and loss timers live in userspace, so a room tick or a GC pause on the JS
thread would hold them up; TCP's run in the kernel regardless.

The two threads share two byte queues under one lock, each record a
24-byte header and a payload (`src/wire.zig`, mirrored in `lib/index.js`):

- **Commands.** Every call appends to a JS-side batch, and a microtask
  submits it: one Node-API call and one wake of the loop thread per turn,
  however many sends a broadcast makes. A buffer sent again in the same
  batch goes as a reference to the first copy, so a broadcast crosses once;
  it follows that a buffer refilled with other bytes within a turn must be
  sent as a new object.
- **Events.** The loop thread appends, and wakes JS once until JS takes
  them. Each batch is copied into an ArrayBuffer JS owns, and messages are
  views into it, valid for as long as they're kept.

Past 8 MiB of events waiting for JS, stream and raw connection reads
pause, so flow control holds peers back, and datagrams are dropped. A
`RawSocket`'s writes wait once 256 KiB are on their way to the loop
thread, until it has handed them to the kernel.

## Building

Zig 0.17, with quic-zig checked out beside this repo (`../quic-zig`):

```sh
zig build            # zig-out/lib/netcode_io.node, zig-out/bin/wt-test-client
npm test             # Zig wire tests, then node --test
```

`lib/binding.js` loads `zig-out/lib/netcode_io.node` when it exists, and
otherwise `netcode-io-<platform>-<arch>`.

## Not yet

- Accepting a WebSocket later: upgrades are accepted on arrival, because
  quic-zig's `WsRequest` lives only for its callback.
- Keep-alive across kinds: once a connection carries a plain request, it's
  JS's to its end, so a WebSocket upgrade on it reaches the `http.Server`.
  Browsers open WebSockets on connections of their own.
- WebSocket backpressure: past `maxSendBuffer` the socket is closed with
  1008, and reads never pause.
- Windows; prebuilt packages.
