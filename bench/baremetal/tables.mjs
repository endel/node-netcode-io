// The README's CPU and latency tables, from matrix.sh's JSON:
//   node tables.mjs matrix.json
import fs from 'node:fs';

const cells = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const med = (xs) => { const s = xs.filter(Number.isFinite).sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
const label = {
  'echo 500 @20/s': 'Echo, 500 clients × 20/s',
  'echo 2000 @20/s': 'Echo, 2,000 clients × 20/s',
  'echo 2000 @20/s tls': 'Echo, 2,000 clients × 20/s, TLS',
  'closed 500x1': 'Echo, 500 clients, one in flight each',
  'broadcast 32x32 @4/s': 'Broadcast, 32 rooms of 32, 4/s each',
  'broadcast 32x32 @2/s x3': 'Broadcast, 32 rooms of 32, 2/s, 3 messages each',
  'size 1KiB 500 @20/s': '1 KiB, 500 clients × 20/s',
  'size 16KiB 200 @20/s': '16 KiB, 200 clients × 20/s',
  'size 16KiB 200 @20/s tls': '16 KiB, 200 clients × 20/s, TLS',
  'size 64KiB 50 @20/s': '64 KiB, 50 clients × 20/s',
  'size 64KiB 50 @20/s tls': '64 KiB, 50 clients × 20/s, TLS',
  'size 16KiB closed 50x1': '16 KiB, 50 clients, one in flight each',
};
const stat = (c, t) => {
  const rs = c.runs.filter((x) => x.transport === t).map((x) => x.r);
  return {
    us: med(rs.map((r) => r.cpu / r.messagesPerSecond * 1e6)),
    mps: med(rs.map((r) => r.messagesPerSecond)),
    p50: med(rs.map((r) => r.p50)),
    p99: med(rs.map((r) => r.p99)),
  };
};
const f1 = (x) => !Number.isFinite(x) ? '–' : x >= 100 ? x.toFixed(0) : x.toFixed(1);
const ms = (x) => !Number.isFinite(x) ? '–' : x >= 10 ? x.toFixed(1) : x >= 1 ? x.toFixed(2) : x.toFixed(3);
const ratio = (a, b) => `${(a / b).toFixed(2)}×`;

console.log('| | netcode-io | uWebSockets.js | Less CPU |');
console.log('|---|---:|---:|---:|');
for (const c of cells) {
  const u = stat(c, 'uws'), k = stat(c, 'netcode-kept'), t = stat(c, 'netcode');
  const cpu = (s) => `${f1(s.us)} µs`;
  // how many times less CPU than uWebSockets.js
  const rat = (s) => ratio(u.us, s.us);
  console.log(`| ${label[c.cell.name] ?? c.cell.name} | ${cpu(t)} | ${cpu(u)} | ${rat(t)} |`);
}
for (const c of cells.filter((c) => c.cell.rate === 0)) {
  const u = stat(c, 'uws'), k = stat(c, 'netcode-kept'), t = stat(c, 'netcode');
  console.log(`<!-- ${c.cell.name}: msg/s uws ${Math.round(u.mps)} kept ${Math.round(k.mps)} transient ${Math.round(t.mps)} -->`);
}
const t1 = (x) => x >= 1000 ? `${(x / 1000).toFixed(1)} s` : `${ms(x)} ms`;
// above 1.00x netcode-io is faster; past 100x the other side has fallen behind
const faster = (us, them) => them / us > 100 ? 'uWebSockets.js falls behind' : ratio(them, us);
for (const [title, key] of [['Median round trip (p50):', 'p50'], ['99th percentile (p99):', 'p99']]) {
  console.log(`\n${title}\n`);
  console.log('| | netcode-io | uWebSockets.js | Faster |');
  console.log('|---|---:|---:|---:|');
  for (const c of cells) {
    const u = stat(c, 'uws'), t = stat(c, 'netcode');
    console.log(`| ${label[c.cell.name] ?? c.cell.name} | ${t1(t[key])} | ${t1(u[key])} | ${faster(t[key], u[key])} |`);
  }
}
