'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { Server } = require('..');
const { thread } = require('./thread.js');

const root = path.join(__dirname, '..');
const certs = path.join(__dirname, 'certs');
const client = path.join(root, 'zig-out', 'bin', 'wt-test-client');
const options = {
  host: '127.0.0.1',
  port: 0,
  cert: path.join(certs, 'server.crt'),
  key: path.join(certs, 'server.key'),
  websocket: { thread },
};

function runClient(port, urlPath) {
  return new Promise((resolve) => {
    execFile(client, ['--port', String(port), '--path', urlPath, '--ca', path.join(certs, 'ca.crt')], (err, stdout, stderr) => {
      resolve({ code: err ? err.code : 0, output: stdout + stderr });
    });
  });
}

test('a session: server-opened stream, client stream echo, datagram echo, server close', async (t) => {
  const seen = { request: undefined, ready: false, reply: '', closed: false, disconnected: false };
  let serverStream;

  const server = new Server(options, {
    onSessionRequest(conn, session, req) {
      seen.request = req;
      server.acceptSession(conn, session);
    },
    async onSessionReady(conn, session) {
      seen.ready = true;
      seen.session = session;
      serverStream = await server.openStream(conn, session);
      server.sendStream(conn, serverStream, 'hello');
    },
    onStreamData(conn, stream, data, fin) {
      if (stream === serverStream) {
        seen.reply += data.toString();
        if (fin) { server.closeSession(conn, seen.session, 4000, 'done'); }
      } else {
        server.sendStream(conn, stream, data, fin);
      }
    },
    onDatagram(conn, session, data) { server.sendDatagram(conn, session, data); },
    onSessionClosed() { seen.closed = true; },
    onConnectionClosed() { seen.disconnected = true; },
  });
  t.after(() => server.stop());

  const { code, output } = await runClient(server.port, '/room/1?sessionId=abc');
  assert.strictEqual(code, 0, output);

  // the client's close reaches us after it exits
  while (!seen.disconnected) { await new Promise((r) => setTimeout(r, 5)); }

  assert.strictEqual(seen.request.path, '/room/1?sessionId=abc');
  assert.strictEqual(seen.request.header(':authority'), 'localhost');
  assert.strictEqual(seen.request.remoteAddress, '127.0.0.1');
  assert.ok(seen.ready);
  assert.strictEqual(seen.reply, 'reply');
  assert.ok(seen.closed, 'session_closed before connection_closed');

  await server.stop();
});

test('a rejected session never becomes ready', async (t) => {
  let ready = false;
  const server = new Server(options, {
    onSessionRequest(conn, session) { server.rejectSession(conn, session, 403); },
    onSessionReady() { ready = true; },
  });
  t.after(() => server.stop());

  const { code, output } = await runClient(server.port, '/');
  assert.notStrictEqual(code, 0);
  assert.match(output, /rejected: 403/);
  assert.strictEqual(ready, false);

  await server.stop();
});
