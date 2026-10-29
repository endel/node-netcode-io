'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
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

function request(port, method, urlPath, body, agent) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: urlPath, agent }, (res) => {
      readBody(res).then((b) => resolve({ res, body: b.toString() }), reject);
    });
    req.on('error', reject);
    req.end(body);
  });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

test('requests reach an http.Server, keep-alive and all, beside WebSockets on the port', async (t) => {
  const sockets = new Set();
  const httpServer = http.createServer(async (req, res) => {
    sockets.add(req.socket);
    const body = await readBody(req);
    res.setHeader('x-remote', req.socket.remoteAddress);
    res.end(`${req.method} ${req.url} ${body}`);
  });
  let opened = 0;
  const server = new Server(options, { onWsOpen() { opened++; } }).serveHttp(httpServer);
  t.after(() => server.stop());
  const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
  t.after(() => agent.destroy());

  const a = await request(server.port, 'POST', '/matchmake/joinOrCreate/room', '{"x":1}', agent);
  assert.strictEqual(a.body, 'POST /matchmake/joinOrCreate/room {"x":1}');
  assert.strictEqual(a.res.headers['x-remote'], '127.0.0.1');
  const b = await request(server.port, 'GET', '/second', undefined, agent);
  assert.strictEqual(b.body, 'GET /second ');
  assert.strictEqual(sockets.size, 1, 'both requests on one connection');

  const ws = new WebSocket(`ws://127.0.0.1:${server.port}/`);
  await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject; });
  // the client hears its 101 before our event reaches JS
  while (opened === 0) { await new Promise((r) => setTimeout(r, 5)); }
  ws.close();

  await server.stop();
});

test('large bodies go both ways whole', async (t) => {
  const size = 8 << 20;
  const big = Buffer.alloc(size, 0x61);
  let uploaded = 0;
  const httpServer = http.createServer(async (req, res) => {
    if (req.method === 'POST') {
      uploaded = (await readBody(req)).length;
      res.end('ok');
    } else {
      res.end(big);
    }
  });
  const server = new Server(options).serveHttp(httpServer);
  t.after(() => server.stop());
  const base = `http://127.0.0.1:${server.port}`;

  const down = Buffer.from(await (await fetch(`${base}/download`)).arrayBuffer());
  assert.strictEqual(down.length, size);
  assert.ok(down.equals(big));

  assert.strictEqual(await (await fetch(`${base}/upload`, { method: 'POST', body: big })).text(), 'ok');
  assert.strictEqual(uploaded, size);

  await server.stop();
});

test('without an http.Server, requests get 404', async (t) => {
  const server = new Server(options);
  t.after(() => server.stop());
  const res = await fetch(`http://127.0.0.1:${server.port}/`);
  assert.strictEqual(res.status, 404);
  await server.stop();
});

test('over TLS, the socket says it is encrypted', async (t) => {
  const httpServer = http.createServer((req, res) => res.end(String(req.socket.encrypted)));
  const server = new Server({ ...options, websocket: undefined }).serveHttp(httpServer);
  t.after(() => server.stop());

  const body = await new Promise((resolve, reject) => {
    https.get({
      host: '127.0.0.1',
      port: server.port,
      servername: 'localhost',
      ca: fs.readFileSync(path.join(certs, 'ca.crt')),
      path: '/',
    }, (res) => readBody(res).then(resolve, reject)).on('error', reject);
  });
  assert.strictEqual(body.toString(), 'true');

  await server.stop();
});

test('stop() ends open connections', async (t) => {
  let socket;
  const httpServer = http.createServer((req, res) => {
    socket = req.socket;
    res.writeHead(200);
    res.write('partial');
  });
  const server = new Server(options).serveHttp(httpServer);
  t.after(() => server.stop());

  const res = await fetch(`http://127.0.0.1:${server.port}/`);
  const reader = res.body.getReader();
  await reader.read();
  const closed = new Promise((resolve) => socket.on('close', resolve));
  await server.stop();
  await closed;
  await assert.rejects(reader.read());
});

test('certificates may be given as PEM instead of paths', async (t) => {
  const httpServer = http.createServer((req, res) => res.end('pem'));
  const server = new Server({
    ...options,
    websocket: undefined,
    cert: fs.readFileSync(path.join(certs, 'server.crt')),
    key: fs.readFileSync(path.join(certs, 'server.key'), 'utf8'),
  }).serveHttp(httpServer);
  t.after(() => server.stop());

  const body = await new Promise((resolve, reject) => {
    https.get({ host: '127.0.0.1', port: server.port, servername: 'localhost', ca: fs.readFileSync(path.join(certs, 'ca.crt')) },
      (res) => readBody(res).then(resolve, reject)).on('error', reject);
  });
  assert.strictEqual(body.toString(), 'pem');
  assert.throws(() => new Server({ ...options, cert: 'not a certificate -----BEGIN', key: options.key }), /no PEM certificate/);

  await server.stop();
});

test('a client that half-closes after its request still gets the answer', async (t) => {
  const net = require('node:net');
  const httpServer = http.createServer((req, res) => {
    req.resume();
    req.on('end', () => setTimeout(() => res.end('late'), 50));
  });
  httpServer.httpAllowHalfOpen = true; // Node's default ends the connection on the peer's end
  const server = new Server(options).serveHttp(httpServer);
  t.after(() => server.stop());

  const answer = await new Promise((resolve, reject) => {
    const sock = net.connect(server.port, '127.0.0.1', () => {
      sock.end('POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 2\r\n\r\nhi');
    });
    const chunks = [];
    sock.on('data', (c) => chunks.push(c));
    sock.on('end', () => resolve(Buffer.concat(chunks).toString()));
    sock.on('error', reject);
  });
  assert.match(answer, /^HTTP\/1\.1 200 OK\r\n/);
  assert.ok(answer.endsWith('\r\n\r\nlate'), answer);

  await server.stop();
});

test('destroy() drops what was queued instead of sending it', async (t) => {
  const size = 32 << 20;
  const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'content-length': size });
    res.write(Buffer.alloc(size, 0x62));
    // once the write has crossed to the loop thread: Node holds it until the next tick
    setImmediate(() => req.socket.destroy());
  });
  const server = new Server(options).serveHttp(httpServer);
  t.after(() => server.stop());

  const received = await new Promise((resolve) => {
    let n = 0;
    http.get({ host: '127.0.0.1', port: server.port, path: '/' }, (res) => {
      res.on('data', (c) => { n += c.length; });
      res.on('close', () => resolve(n));
    }).on('error', () => resolve(n));
  });
  assert.ok(received < size, `received all ${received} bytes`);

  await server.stop();
});

test('requests that arrive together are handled as separate sockets would be: microtasks run between them', async (t) => {
  // Node gives each socket's data its own callback. Code written for that,
  // like a matchmaker that reserves a seat before the next request looks for
  // one, mustn't see every connection's request in one synchronous run.
  const net = require('node:net');
  const order = [];
  let others;
  const httpServer = http.createServer((req, res) => {
    if (req.url === '/first') {
      // the others arrive while this blocks, so one event batch carries both
      for (const s of others) { s.write(`GET ${s.path} HTTP/1.1\r\nHost: a\r\n\r\n`); }
      const until = Date.now() + 150;
      while (Date.now() < until) { /* hold the JS thread */ }
    } else {
      order.push(req.url);
      queueMicrotask(() => order.push(`after ${req.url}`));
    }
    res.end('ok');
  });
  const server = new Server(options).serveHttp(httpServer);
  t.after(() => server.stop());

  const connect = (path) => new Promise((resolve) => {
    const s = net.connect(server.port, '127.0.0.1', () => resolve(s));
    s.path = path;
    s.resume();
  });
  others = await Promise.all([connect('/b'), connect('/c')]);
  const first = await connect('/first');
  first.write('GET /first HTTP/1.1\r\nHost: a\r\n\r\n');

  while (order.length < 4) { await new Promise((r) => setTimeout(r, 5)); }
  assert.deepStrictEqual(order, ['/b', 'after /b', '/c', 'after /c']);
  [first, ...others].forEach((s) => s.destroy());

  await server.stop();
});
