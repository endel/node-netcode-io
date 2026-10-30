// A WebSocket echo or room broadcast with nothing on top, served by netcode-io
// or uWebSockets.js; run.mjs forks it:
//   node server.mjs <uws|netcode|netcode-DIR> <port> <plain|tls> [workUs] [echo|broadcast:N[:K]]
// IPC: "start" opens a window, "stop" answers with its metrics.
import { createRequire } from 'module';
import { pathToFileURL } from 'url';
import { performance } from 'perf_hooks';

const require = createRequire(import.meta.url);
const [name, portArg, security, workArg, modeArg] = process.argv.slice(2);
const port = Number(portArg);
const tls = security === 'tls';
// 0.0.0.0 lets a CCU run spread its connections over 127.0.0.x, past one address's ~64k ports
const HOST = process.env.BENCH_HOST ?? '127.0.0.1';
const workUs = Number(workArg ?? 0);
// "broadcast:N[:K]": connections join rooms of N in arrival order, and each
// message goes to the whole room, K times over as separate messages
const [, roomArg, copiesArg] = modeArg?.startsWith('broadcast:') ? modeArg.split(':') : [];
const roomSize = Number(roomArg ?? 0);
const copies = Number(copiesArg ?? 1);
const rooms = [];
let joined = 0;
function join(member) {
  const room = rooms[Math.floor(joined++ / roomSize)] ??= [];
  room.push(member);
  return room;
}
function leave(room, member) {
  const i = room ? room.indexOf(member) : -1;
  if (i >= 0) { room.splice(i, 1); }
}
// netcode-DIR: another build's lib/ and zig-out/ copied to ./netcode-DIR, for A/B runs
// netcode-kept: this tree, messages kept past the call; netcode-DIR: a snapshot
const KEPT = name === 'netcode-kept';
const NETCODE = name.startsWith('netcode-') && !KEPT ? new URL(`./${name}`, import.meta.url).pathname : new URL('..', import.meta.url).pathname;
// uWebSockets.js isn't a dependency: point UWS at an installed copy
const UWS = process.env.UWS ?? 'uWebSockets.js';
const CERT = `${NETCODE}/test/certs/server.crt`;
const KEY = `${NETCODE}/test/certs/server.key`;

let messages = 0;
// stands in for a room's handler: CPU on the JS thread per message
function work() {
  if (workUs === 0) { return; }
  const until = performance.now() + workUs / 1000;
  while (performance.now() < until) { /* spin */ }
}

let stats = () => undefined;
if (name === 'uws') {
  const uWS = require(UWS);
  const app = tls ? uWS.SSLApp({ cert_file_name: CERT, key_file_name: KEY }) : uWS.App();
  app.ws('/*', {
    maxPayloadLength: 1 << 20,
    // its default 64 KiB would drop large echoes under load
    maxBackpressure: 16 << 20,
    idleTimeout: 0,
    compression: uWS.DISABLED,
    open: (ws) => { if (roomSize) { ws.getUserData().room = join(ws); } },
    // uWS throws on a send to a closed socket: a run ending mid-broadcast crashed it
    close: (ws) => { leave(ws.getUserData().room, ws); },
    message: (ws, msg, isBinary) => {
      messages++; work();
      if (roomSize === 0) { ws.send(msg, isBinary); return; }
      const room = ws.getUserData().room;
      for (let k = 0; k < copies; k++) {
        for (let i = 0; i < room.length; i++) { room[i].send(msg, isBinary); }
      }
    },
  });
  await new Promise((resolve, reject) => app.listen(HOST, port, (s) => s ? resolve() : reject(new Error('listen failed'))));
} else {
  const { Server } = await import(pathToFileURL(`${NETCODE}/lib/index.js`).href);
  let server;
  const roomOf = new Map();
  server = new Server({ host: HOST, port, cert: CERT, key: KEY, websocket: { tls, maxConnections: 1 << 20, pingInterval: 0, maxMessageSize: 1 << 20, maxSendBuffer: 16 << 20, transientMessages: !KEPT, thread: process.env.NETCODE_THREAD } }, {
    onWsOpen: (ws) => { if (roomSize) { roomOf.set(ws, join(ws)); } },
    onWsClose: (ws) => { leave(roomOf.get(ws), ws); roomOf.delete(ws); },
    onWsMessage: (ws, data, isText) => {
      messages++; work();
      if (roomSize === 0) { server.send(ws, data, isText); return; }
      const room = roomOf.get(ws);
      for (let k = 0; k < copies; k++) {
        for (let i = 0; i < room.length; i++) { server.send(room[i], data, isText); }
      }
    },
  });
  stats = () => server.stats();
}

let w;
process.on('message', (m) => {
  if (m.type === 'start') {
    w = { at: performance.now(), cpu: process.cpuUsage(), main: process.threadCpuUsage(), elu: performance.eventLoopUtilization(), messages, stats: stats() };
  } else if (m.type === 'stop') {
    const seconds = (performance.now() - w.at) / 1000;
    const cpu = process.cpuUsage(w.cpu);
    const main = process.threadCpuUsage(w.main);
    const s = stats();
    const delta = {};
    if (s !== undefined) { for (const k of Object.keys(s)) { delta[k] = s[k] - w.stats[k]; } }
    process.send({
      type: 'metrics',
      seconds,
      cpu: (cpu.user + cpu.system) / 1e6 / seconds,
      mainCpu: (main.user + main.system) / 1e6 / seconds,
      loopCpu: delta.loopCpu === undefined ? 0 : delta.loopCpu / seconds,
      elu: performance.eventLoopUtilization(w.elu).utilization,
      messagesPerSecond: (messages - w.messages) / seconds,
      stats: delta,
    });
  }
});
process.send({ type: 'ready' });
