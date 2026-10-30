import { test } from 'node:test';
import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { once } from 'node:events';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server } from 'netcode-io';
import { thread } from './thread.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const certs = path.join(here, 'certs');
const options = {
  host: '127.0.0.1',
  port: 0,
  cert: path.join(certs, 'server.crt'),
  key: path.join(certs, 'server.key'),
  websocket: { tls: false, thread },
};

function open(url) {
  const ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';
  return new Promise((resolve, reject) => {
    ws.onopen = () => resolve(ws);
    ws.onerror = (e) => reject(e.error ?? new Error('websocket error'));
  });
}

function next(ws) {
  return new Promise((resolve) => { ws.onmessage = (e) => resolve(e.data); });
}

test('echoes binary and text, and reports the request', async (t) => {
  let request;
  const server = new Server(options, {
    onWsOpen(id, req) { request = req; },
    onWsMessage(id, data, isText) { server.send(id, data, isText); },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/room/abc?sessionId=xyz`);

  ws.send(new Uint8Array([1, 2, 3]));
  assert.deepStrictEqual(new Uint8Array(await next(ws)), new Uint8Array([1, 2, 3]));
  ws.send('héllo');
  assert.strictEqual(await next(ws), 'héllo');

  assert.strictEqual(request.path, '/room/abc?sessionId=xyz');
  assert.strictEqual(request.header('HOST'), `127.0.0.1:${server.port}`);
  assert.strictEqual(request.remoteAddress, '127.0.0.1');
  assert.strictEqual(request.remoteFamily, 'IPv4');

  ws.close();
  await server.stop();
});

test('websocket.thread picks the thread WebSockets are served on', async (t) => {
  const server = new Server(options, {
    onWsMessage(id, data) { server.send(id, data); },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  ws.send(new Uint8Array([7]));
  await next(ws);
  // only the QUIC thread ever wakes JS: on Node's, the echo never crossed
  const { notifies } = server.stats();
  if (thread === 'node') {
    assert.strictEqual(notifies, 0);
  } else {
    assert.ok(notifies > 0);
  }

  ws.close();
  await server.stop();
});

test('messages to one socket in one turn arrive whole and in order, a large one among them', async (t) => {
  // They leave corked in one send; the large one makes the kernel take only part.
  const sizes = [10, 200, 300 * 1024, 5, 70 * 1024, 1];
  const server = new Server({ ...options, websocket: { ...options.websocket, maxSendBuffer: 8 << 20 } }, {
    onWsMessage(id) {
      sizes.forEach((n, i) => server.send(id, new Uint8Array(n).fill(i + 1)));
    },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  const got = [];
  const all = new Promise((resolve) => {
    ws.onmessage = (e) => { got.push(new Uint8Array(e.data)); if (got.length === sizes.length) { resolve(); } };
  });
  ws.send('go');
  await all;
  got.forEach((m, i) => {
    assert.strictEqual(m.length, sizes[i]);
    assert.ok(m.every((b) => b === i + 1), `message ${i}`);
  });

  ws.close();
  await server.stop();
});

test('a large message stays whole while kept, through the batches after it', async (t) => {
  // big enough that its batch reaches JS as that batch's own memory
  const kept = [];
  let resolveAll;
  const all = new Promise((resolve) => { resolveAll = resolve; });
  const server = new Server({ ...options, websocket: { ...options.websocket, maxMessageSize: 1 << 20 } }, {
    onWsMessage(id, data) {
      kept.push(data);
      if (kept.length === 12) { resolveAll(); }
    },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  for (let i = 0; i < 12; i++) { ws.send(new Uint8Array(100 * 1024).fill(i + 1)); }
  await all;
  kept.forEach((m, i) => {
    assert.strictEqual(m.length, 100 * 1024);
    assert.ok(m.every((b) => b === i + 1), `message ${i}`);
  });

  ws.close();
  await server.stop();
});

test('an unknown websocket.thread throws', () => {
  assert.throws(() => new Server({ ...options, websocket: { thread: 'worker' } }), /websocket\.thread/);
});

test('websocket.maxConnections closes connections past it on accept', async (t) => {
  const server = new Server({ ...options, websocket: { ...options.websocket, maxConnections: 2 } }, {
    onWsMessage(id, data, isText) { server.send(id, data, isText); },
  });
  t.after(() => server.stop());
  const url = `ws://127.0.0.1:${server.port}/`;

  const a = await open(url);
  const b = await open(url);
  await assert.rejects(open(url));
  b.send('still served');
  assert.strictEqual(await next(b), 'still served');

  a.close();
  b.close();
  await server.stop();
});

test('a server close reaches the client with its code and reason', async (t) => {
  const server = new Server(options, {
    onWsOpen(id) { server.closeWebSocket(id, 4000, 'bye'); },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  const closed = await new Promise((resolve) => { ws.onclose = resolve; });
  assert.strictEqual(closed.code, 4000);
  assert.strictEqual(closed.reason, 'bye');

  await server.stop();
});

test('a client close is reported once, with its code', async (t) => {
  let resolveClose;
  const reported = new Promise((resolve) => { resolveClose = resolve; });
  const closes = [];
  const server = new Server(options, {
    onWsClose(id, code, reason) { closes.push([code, reason]); resolveClose(); },
  });
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  ws.close(4001, 'done');
  await reported;
  assert.deepStrictEqual(closes, [[4001, 'done']]);

  await server.stop();
});

test('one buffer sent to many sockets arrives whole at each', async (t) => {
  const ids = [];
  const server = new Server(options, {
    onWsOpen(id) { ids.push(id); },
  });
  t.after(() => server.stop());

  const sockets = await Promise.all([1, 2, 3].map(() => open(`ws://127.0.0.1:${server.port}/`)));
  while (ids.length < 3) { await new Promise((r) => setTimeout(r, 5)); }

  const shared = new Uint8Array(1000).map((_, i) => i & 0xff);
  const received = sockets.map(next);
  for (const id of ids) { server.send(id, shared); }
  for (const data of await Promise.all(received)) {
    assert.deepStrictEqual(new Uint8Array(data), shared);
  }

  sockets.forEach((ws) => ws.close());
  await server.stop();
});

test('stop() closes open sockets with 1001 and resolves', async (t) => {
  const server = new Server(options, {});
  t.after(() => server.stop());
  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  const closed = new Promise((resolve) => { ws.onclose = resolve; });

  await server.stop();
  assert.strictEqual((await closed).code, 1001);
  assert.strictEqual(server.port, undefined);
});

function socketPath(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'netcode-io-'));
  t.after(() => fs.rmSync(dir, { recursive: true }));
  return path.join(dir, 'ws.sock');
}

/** `WebSocket` can't dial a Unix socket: the upgrade by hand. */
function openUnix(file, urlPath) {
  return new Promise((resolve, reject) => {
    http.request({
      socketPath: file,
      path: urlPath,
      headers: {
        Connection: 'Upgrade',
        Upgrade: 'websocket',
        'Sec-WebSocket-Version': '13',
        'Sec-WebSocket-Key': crypto.randomBytes(16).toString('base64'),
      },
    }).on('upgrade', (res, socket) => resolve(socket)).on('error', reject).end();
  });
}

function maskedText(text) {
  const payload = Buffer.from(text);
  // An all-zero mask leaves the payload as it is.
  return Buffer.concat([Buffer.from([0x81, 0x80 | payload.length, 0, 0, 0, 0]), payload]);
}

test('websocket.socketPath serves WebSockets on a Unix socket, and stop() removes it', async (t) => {
  const file = socketPath(t);
  let request;
  const server = new Server({ ...options, websocket: { ...options.websocket, socketPath: file } }, {
    onWsOpen(id, req) { request = req; },
    onWsMessage(id, data, isText) { server.send(id, data, isText); },
  });
  t.after(() => server.stop());

  assert.ok(fs.statSync(file).isSocket());
  const socket = await openUnix(file, '/room/abc');
  const echoed = new Promise((resolve) => { socket.once('data', resolve); });
  socket.write(maskedText('héllo'));
  assert.strictEqual((await echoed).subarray(2).toString(), 'héllo');

  assert.strictEqual(request.path, '/room/abc');
  assert.strictEqual(request.remoteAddress, undefined);
  assert.strictEqual(request.remoteFamily, undefined);
  assert.strictEqual(request.remotePort, 0);

  socket.destroy();
  await server.stop();
  assert.strictEqual(fs.existsSync(file), false);
});

test('a path in use throws from the constructor; one left by a dead server is replaced', async (t) => {
  const file = socketPath(t);
  const opts = { ...options, websocket: { ...options.websocket, socketPath: file } };

  const live = net.createServer().listen(file);
  await once(live, 'listening');
  assert.throws(() => new Server(opts, {}), /AddressInUse/);
  await new Promise((resolve) => { live.close(resolve); });

  const script = `require('net').createServer().listen(${JSON.stringify(file)}, () => process.kill(process.pid, 'SIGKILL'))`;
  spawnSync(process.execPath, ['-e', script]);
  assert.ok(fs.statSync(file).isSocket());

  const server = new Server(opts, {});
  t.after(() => server.stop());
  await openUnix(file, '/').then((socket) => socket.destroy());
  await server.stop();

  fs.writeFileSync(file, 'not a socket');
  assert.throws(() => new Server(opts, {}), /AddressInUse/);
  assert.strictEqual(fs.readFileSync(file, 'utf8'), 'not a socket');
});

test('websocket.socketPath with websocket.port throws', (t) => {
  const websocket = { ...options.websocket, port: 0, socketPath: socketPath(t) };
  assert.throws(() => new Server({ ...options, websocket }, {}), /port or socketPath/);
});

test('a port in use throws from the constructor', (t) => {
  const a = new Server(options, {});
  t.after(() => a.stop());
  assert.throws(() => new Server({ ...options, port: a.port }), /listen failed/);
  return a.stop();
});

test('a buffer sent twice still arrives right when its turn spans several batches', async (t) => {
  // Past 1 MiB a turn's commands go out in more than one batch, and the loop
  // thread may take two at once: the second's references must still resolve
  // within that batch.
  const ids = [];
  const server = new Server(options, { onWsOpen(id) { ids.push(id); } });
  t.after(() => server.stop());

  for (let attempt = 0; attempt < 5; attempt++) {
    ids.length = 0;
    const [big, a, b] = await Promise.all([1, 2, 3].map(() => open(`ws://127.0.0.1:${server.port}/`)));
    while (ids.length < 3) { await new Promise((r) => setTimeout(r, 5)); }

    const shared = new Uint8Array(100).map((_, i) => (i * 7 + attempt) & 0xff);
    const got = [next(a), next(b)];
    for (let i = 0; i < 40; i++) { server.send(ids[0], new Uint8Array(64 * 1024).fill(i)); }
    server.send(ids[1], shared);
    server.send(ids[2], shared);
    for (const data of await Promise.all(got)) {
      assert.deepStrictEqual(new Uint8Array(data), shared, `attempt ${attempt}`);
    }
    [big, a, b].forEach((ws) => ws.close());
  }

  await server.stop();
});

test('transientMessages: each message is whole during its call; what JS reads later is a copy', async (t) => {
  const sizes = [10, 200, 70 * 1024, 5, 300 * 1024, 1];
  const copies = [];
  let request;
  const httpServer = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => res.end(Buffer.concat(chunks)));
  });
  const server = new Server({ ...options, websocket: { ...options.websocket, transientMessages: true } }, {
    onWsOpen(id, req) { request = req; },
    onWsMessage(id, data) {
      copies.push(Buffer.from(data));
      server.send(id, data);
    },
  }).serveHttp(httpServer);
  t.after(() => server.stop());

  const ws = await open(`ws://127.0.0.1:${server.port}/`);
  ws.binaryType = 'arraybuffer';
  const echoed = [];
  const done = new Promise((resolve) => {
    ws.onmessage = (e) => { echoed.push(Buffer.from(e.data)); if (echoed.length === sizes.length) { resolve(); } };
  });
  sizes.forEach((n, i) => ws.send(new Uint8Array(n).fill(i + 1)));
  await done;
  sizes.forEach((n, i) => {
    assert.ok(copies[i].length === n && copies[i].every((x) => x === i + 1), `message ${i}`);
    assert.ok(echoed[i].length === n && echoed[i].every((x) => x === i + 1), `echo ${i}`);
  });
  assert.strictEqual(request.remoteAddress, '127.0.0.1');

  // an http.Server reads raw bytes after their batch
  const body = Buffer.alloc(100 * 1024, 7);
  const res = await fetch(`http://127.0.0.1:${server.port}/up`, { method: 'POST', body });
  assert.ok(Buffer.from(await res.arrayBuffer()).equals(body));
  ws.close();
  await server.stop();
});

test('sockets closed and echoed to mid-burst, while other sockets wait to be read', async (t) => {
  const CLIENTS = 20;
  const BURST = 20;
  const seen = new Map();
  const server = new Server(options, {
    onWsMessage(ws, data) {
      const n = (seen.get(ws) ?? 0) + 1;
      seen.set(ws, n);
      // every fourth socket closes halfway through its own burst
      if (data[0] % 4 === 0 && n === BURST / 2) { return server.closeWebSocket(ws, 4000, 'half'); }
      server.send(ws, data);
    },
  });
  t.after(() => server.stop());
  const sockets = await Promise.all(Array.from({ length: CLIENTS }, () => open(`ws://127.0.0.1:${server.port}/`)));
  const results = sockets.map((ws, i) => new Promise((resolve) => {
    let echoes = 0;
    ws.onmessage = (e) => {
      const b = new Uint8Array(e.data);
      assert.ok(b.length === 8 * 1024 && b.every((x) => x === i));
      if (++echoes === BURST) { resolve(['done', echoes]); }
    };
    ws.onclose = (e) => resolve([e.code, echoes]);
  }));
  sockets.forEach((ws, i) => { for (let k = 0; k < BURST; k++) { ws.send(new Uint8Array(8 * 1024).fill(i)); } });
  const out = await Promise.all(results);
  out.forEach(([how, echoes], i) => {
    if (i % 4 === 0) {
      assert.deepStrictEqual([how, echoes], [4000, BURST / 2 - 1], `socket ${i}`);
    } else {
      assert.deepStrictEqual([how, echoes], ['done', BURST], `socket ${i}`);
    }
  });
  for (const ws of sockets) { ws.close(); }
  await server.stop();
});

test('a buffer sent to many sockets in one turn crosses once, and each gets its bytes', async (t) => {
  const N = 6;
  const SIZE = 4 * 1024;
  const ids = [];
  let before;
  const server = new Server(options, {
    onWsOpen(id) { ids.push(id); },
    onWsMessage() {
      before = server.stats().commandBytes;
      const [a, b, c] = [1, 2, 3].map((x) => Buffer.alloc(SIZE, x));
      for (const id of ids) { server.send(id, a); }
      for (const id of ids) { server.send(id, b); }
      for (const id of ids) { server.send(id, a); server.send(id, c); }
    },
  });
  t.after(() => server.stop());
  const sockets = await Promise.all(Array.from({ length: N }, () => open(`ws://127.0.0.1:${server.port}/`)));
  const got = sockets.map((ws) => new Promise((resolve) => {
    const seen = [];
    ws.onmessage = (e) => {
      const m = new Uint8Array(e.data);
      seen.push(m.length === SIZE && m.every((x) => x === m[0]) ? m[0] : -1);
      if (seen.length === 4) { resolve(seen); }
    };
  }));
  sockets[0].send('go');
  for (const seen of await Promise.all(got)) { assert.deepStrictEqual(seen, [1, 2, 1, 3]); }
  // three buffers' bytes, not N × 4 sends' worth
  assert.ok(server.stats().commandBytes - before < 4 * SIZE, `${server.stats().commandBytes - before} command bytes`);
  for (const ws of sockets) { ws.close(); }
  await server.stop();
});
