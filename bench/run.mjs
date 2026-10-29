// netcode-io against uWebSockets.js with nothing on top: paired runs, the
// order alternating each round, a fresh server and load per run.
//
//   (cd wsload && go build)
//   UWS=/path/to/uWebSockets.js GOMAXPROCS=4 node run.mjs [--rounds 3] [--only name]
//       [--measure 8] [--warmup 2] [--transports uws,netcode] [--out file.json]
//
// Cap the load generator (GOMAXPROCS): uncapped, its spinning threads starve
// the server's two threads at every handoff on a shared machine, and the
// single-threaded uWS barely notices.
import { fork, spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import readline from 'readline';

const here = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const flag = (n, d) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };
const rounds = Number(flag('rounds', 3));
const only = flag('only');
const MEASURE = Number(flag('measure', 8));
const WARMUP = Number(flag('warmup', 2));
const transports = flag('transports', 'uws,netcode').split(',');
const out = flag('out');

const CELLS = [
  { name: 'echo 500 @20/s', c: 500, rate: 20 },
  { name: 'echo 2000 @20/s', c: 2000, rate: 20 },
  { name: 'closed 500x1', c: 500, rate: 0, inflight: 1 },
  { name: 'closed 50x1', c: 50, rate: 0, inflight: 1 },
  { name: 'echo 2000 @20/s tls', c: 2000, rate: 20, tls: true },
  { name: 'echo 2000 @20/s work5', c: 2000, rate: 20, work: 5 },
  { name: 'broadcast 32x32 @4/s', c: 1024, rate: 4, spread: true, mode: 'broadcast:32' },
  { name: 'broadcast 32x32 @2/s', c: 1024, rate: 2, spread: true, mode: 'broadcast:32' },
  { name: 'spread 500 @20/s', c: 500, rate: 20, spread: true },
  { name: 'spread 2000 @20/s', c: 2000, rate: 20, spread: true },
  { name: 'spread 2000 @20/s tls', c: 2000, rate: 20, spread: true, tls: true },
  { name: 'spread 2000 @20/s work5', c: 2000, rate: 20, spread: true, work: 5 },
];

let port = 31000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const once = (child, type) => new Promise((resolve, reject) => {
  const on = (m) => { if (m?.type === type) { child.off('message', on); resolve(m); } };
  child.on('message', on);
  child.once('exit', (code) => reject(new Error(`server exited ${code}`)));
});

async function runOnce(cell, transport) {
  const p = port++;
  const server = fork(path.join(here, 'server.mjs'), [transport, String(p), cell.tls ? 'tls' : 'plain', String(cell.work ?? 0), cell.mode ?? 'echo'], {
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'], execArgv: ['--no-warnings'],
  });
  try {
    await once(server, 'ready');
    const args = ['-addr', `127.0.0.1:${p}`, '-c', String(cell.c), '-rate', String(cell.rate), '-inflight', String(cell.inflight ?? 1),
      '-size', '50', '-warmup', `${WARMUP}s`, '-duration', `${MEASURE}s`];
    if (cell.tls) { args.push('-tls'); }
    if (cell.spread) { args.push('-spread'); }
    const client = spawn(path.join(here, 'wsload', 'wsload'), args, { stdio: ['ignore', 'pipe', 'inherit'] });
    const lines = readline.createInterface({ input: client.stdout });
    let metrics;
    let result;
    for await (const line of lines) {
      if (line === 'measuring') {
        server.send({ type: 'start' });
        setTimeout(() => { metrics = once(server, 'metrics'); server.send({ type: 'stop' }); }, MEASURE * 1000);
      } else if (line.startsWith('{')) {
        result = JSON.parse(line);
      }
    }
    const m = await metrics;
    return { ...m, ...result };
  } finally {
    server.kill('SIGKILL');
    await sleep(200);
  }
}

const fmt = (x, d = 2) => (x === undefined || Number.isNaN(x)) ? '–' : x.toFixed(d);
const median = (xs) => { const s = xs.filter(Number.isFinite).sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : NaN; };

const all = [];
for (const cell of CELLS.filter((c) => only === undefined || c.name.includes(only))) {
  const runs = [];
  for (let round = 0; round < rounds; round++) {
    const order = round % 2 === 0 ? transports : [...transports].reverse();
    for (const t of order) {
      const r = await runOnce(cell, t);
      runs.push({ round, transport: t, r });
      const st = r.stats ?? {};
      process.stderr.write(`${cell.name} r${round} ${t.padEnd(14)} cpu ${fmt(r.cpu)} main ${fmt(r.mainCpu)} loop ${fmt(r.loopCpu)} other ${fmt(r.cpu - r.mainCpu - r.loopCpu)}` +
        ` | ${Math.round(r.messagesPerSecond)} msg/s, µs/msg ${fmt(r.cpu / r.messagesPerSecond * 1e6, 1)}` +
        ` | p50 ${fmt(r.p50)} p99 ${fmt(r.p99)} ms | client ${fmt(r.clientCpu)}` +
        (st.delivers ? ` | ev/deliver ${fmt(st.events / st.delivers, 1)} cmd/submit ${fmt(st.commands / st.submits, 1)} notify/msg ${fmt(st.notifies / (r.messagesPerSecond * r.seconds), 2)} wake/msg ${fmt(st.wakes / (r.messagesPerSecond * r.seconds), 2)}` : '') +
        '\n');
    }
  }
  all.push({ cell, runs });
  // summary
  const row = (t) => {
    const rs = runs.filter((x) => x.transport === t).map((x) => x.r);
    const m = (k) => median(rs.map((r) => typeof k === 'function' ? k(r) : r[k]));
    return { cpu: m('cpu'), main: m('mainCpu'), loop: m('loopCpu'), usPerMsg: m((r) => r.cpu / r.messagesPerSecond * 1e6), mps: m('messagesPerSecond'), p50: m('p50'), p99: m('p99') };
  };
  for (const t of transports) {
    const x = row(t);
    process.stderr.write(`  ${t.padEnd(14)} median: cpu ${fmt(x.cpu)} (main ${fmt(x.main)} loop ${fmt(x.loop)}) ${fmt(x.usPerMsg, 1)} µs/msg ${Math.round(x.mps)} msg/s p50 ${fmt(x.p50)} p99 ${fmt(x.p99)}\n`);
  }
}
if (out) { fs.writeFileSync(out, JSON.stringify(all, null, 2)); }
