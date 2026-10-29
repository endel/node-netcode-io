'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { Server } = require('..');

const certs = path.join(__dirname, 'certs');
const options = {
  host: '127.0.0.1',
  port: 0,
  cert: path.join(certs, 'server.crt'),
  key: path.join(certs, 'server.key'),
  websocket: { tls: false },
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
