// A WebSocket echo or room broadcast with nothing on top, served by netcode-io
// or uWebSockets.js; run.mjs forks it:
//   node server.mjs <uws|netcode|netcode-DIR> <port> <plain|tls> [workUs] [echo|broadcast:N]
// IPC: "start" opens a window, "stop" answers with its metrics.
import { createRequire } from 'module';
import { performance } from 'perf_hooks';

const require = createRequire(import.meta.url);
const [name, portArg, security, workArg, modeArg] = process.argv.slice(2);
const port = Number(portArg);
const tls = security === 'tls';
const workUs = Number(workArg ?? 0);
// "broadcast:N": connections join rooms of N in arrival order, and each message goes to the whole room
const roomSize = modeArg?.startsWith('broadcast:') ? Number(modeArg.split(':')[1]) : 0;
const rooms = [];
let joined = 0;
function join(member) {
  const room = rooms[Math.floor(joined++ / roomSize)] ??= [];
  room.push(member);
  return room;
}
// netcode-DIR: another build's lib/ and zig-out/ copied to ./netcode-DIR, for A/B runs
const NETCODE = name.startsWith('netcode-') ? new URL(`./${name}`, import.meta.url).pathname : new URL('..', import.meta.url).pathname;
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
    maxPayloadLength: 4096,
    idleTimeout: 0,
    compression: uWS.DISABLED,
    open: (ws) => { if (roomSize) { ws.getUserData().room = join(ws); } },
    message: (ws, msg, isBinary) => {
      messages++; work();
      if (roomSize === 0) { ws.send(msg, isBinary); return; }
      const room = ws.getUserData().room;
      for (let i = 0; i < room.length; i++) { room[i].send(msg, isBinary); }
    },
  });
  await new Promise((resolve, reject) => app.listen('127.0.0.1', port, (s) => s ? resolve() : reject(new Error('listen failed'))));
} else {
  const { Server } = require(`${NETCODE}/lib/index.js`);
  let server;
  const roomOf = new Map();
  server = new Server({ host: '127.0.0.1', port, cert: CERT, key: KEY, websocket: { tls, pingInterval: 0, maxMessageSize: 4096 } }, {
    onWsOpen: (ws) => { if (roomSize) { roomOf.set(ws, join(ws)); } },
    onWsMessage: (ws, data, isText) => {
      messages++; work();
      if (roomSize === 0) { server.send(ws, data, isText); return; }
      const room = roomOf.get(ws);
      for (let i = 0; i < room.length; i++) { server.send(room[i], data, isText); }
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
