// A WebTransport echo served by netcode-io, for bench/go/wtload in quic-zig:
//   node wt-server.mjs <port> [host]
// Datagrams and bidi stream bytes go straight back; any path is accepted.
import { Server } from 'netcode-io';

const certs = new URL('../test/certs/', import.meta.url).pathname;
const server = new Server({
  host: process.argv[3] ?? '127.0.0.1',
  port: Number(process.argv[2] ?? 4433),
  cert: `${certs}server.crt`,
  key: `${certs}server.key`,
  // WT_MAX_CONNECTIONS=default keeps quic-zig's cap of 256
  maxConnections: process.env.WT_MAX_CONNECTIONS === 'default' ? undefined : 1 << 20,
}, {
  onStreamData(conn, stream, data, fin) { server.sendStream(conn, stream, data, fin); },
  onDatagram(conn, session, data) { server.sendDatagram(conn, session, data); },
});
console.log(`ready ${server.port}`);
