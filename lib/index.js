'use strict';

const fs = require('node:fs');
const native = require('./binding.js');
const { RawSocket } = require('./socket.js');

// Mirrors src/wire.zig.
const HEADER = 24;
const TWO_32 = 4294967296;

const Event = {
  WS_OPEN: 1, WS_MESSAGE: 2, WS_CLOSE: 3,
  SESSION_REQUEST: 4, SESSION_READY: 5, SESSION_CLOSED: 6,
  STREAM_OPEN: 7, STREAM_DATA: 8, DATAGRAM: 9,
  CONNECTION_CLOSED: 10, STREAM_OPENED: 11, SESSION_DRAINING: 12,
  RAW_OPEN: 13, RAW_DATA: 14, RAW_CLOSE: 15, RAW_DRAIN: 16, RAW_END: 17,
  STOPPED: 20,
};

const Command = {
  WS_SEND: 1, WS_CLOSE: 2,
  SESSION_ACCEPT: 4, SESSION_REJECT: 5, SESSION_CLOSE: 6,
  STREAM_OPEN: 7, STREAM_SEND: 8, STREAM_RESET: 9,
  DATAGRAM_SEND: 10, CONNECTION_CLOSE: 11,
  RAW_SEND: 12, RAW_CLOSE: 13, RAW_PAUSE: 14, RAW_RESUME: 15,
  STOP: 20,
};

const FLAG_TEXT = 1;
const FLAG_FIN = 1;
const FLAG_ABRUPT = 1;
const FLAG_BIDI = 1;
const FLAG_DRAIN = 1;
const FLAG_ABORT = 1;
const FLAG_REF = 2;
const FLAG_FAILED = 4;

// A batch this big goes out at once instead of at the end of the turn.
const MAX_BATCH = 1 << 20;
// Below this a send is cheaper to copy again than to look up.
const MIN_REF = 64;

const EMPTY = new Uint8Array(0);

const NOT_FOUND = 'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n';

/**
 * A WebSocket upgrade or a WebTransport CONNECT, as the peer sent it.
 */
class Request {
  constructor(path, headers, family, port, address) {
    this.path = path;
    /** In arrival order; a name may repeat. */
    this.headers = headers;
    this.remotePort = port;
    this.remoteFamily = family === 4 ? 'IPv4' : family === 6 ? 'IPv6' : undefined;
    this._address = address;
    this._family = family;
    this._formatted = undefined;
  }

  /** The first value of `name`, matched case-insensitively. */
  header(name) {
    const lower = name.toLowerCase();
    for (let i = 0; i < this.headers.length; i++) {
      if (this.headers[i][0].toLowerCase() === lower) { return this.headers[i][1]; }
    }
    return undefined;
  }

  get remoteAddress() {
    // built on first read: most requests never ask
    return this._formatted ??= formatAddress(this._family, this._address);
  }
}

class Server {
  /**
   * Binds now, so a port in use throws here. Events arrive from the next
   * turn on; handlers may be set until then.
   */
  constructor(options, handler = {}) {
    this.handler = handler;

    this._batch = Buffer.allocUnsafe(64 * 1024);
    this._len = 0;
    /** Sends already in this batch, by buffer: a broadcast crosses once. */
    this._refs = new Map();
    this._scheduled = false;
    this._flush = this._flush.bind(this);

    this._tokens = new Map();
    this._nextToken = 1;
    this._stopped = undefined;
    this._resolveStopped = undefined;

    this._http = undefined;
    /** RawSockets by id, when the handler leaves raw connections to us. */
    this._sockets = new Map();
    this._encrypted = options.websocket?.tls !== false;

    this._handle = native.listen(toNativeOptions(options), (ab) => this._onEvents(ab));
  }

  /** The bound UDP port; WebSockets share it unless `websocket.port` says otherwise. */
  get port() {
    return this._handle === null ? undefined : native.port(this._handle);
  }

  // ── WebSocket ──────────────────────────────────────────────────────

  send(ws, data, isText = false) {
    this._send(Command.WS_SEND, isText ? FLAG_TEXT : 0, ws, 0, data);
  }

  closeWebSocket(ws, code = 1000, reason = '') {
    this._withReason(Command.WS_CLOSE, code, ws, 0, -1, reason);
  }

  // ── Raw HTTP/1.1 ───────────────────────────────────────────────────

  /**
   * HTTP/1.1 connections quic-zig doesn't serve itself go to `httpServer`
   * as `RawSocket`s, unless the handler has `onRawOpen`.
   */
  serveHttp(httpServer) {
    this._http = httpServer;
    return this;
  }

  /** `drain`: answer with `onRawDrain` once the output is low again. */
  sendRaw(raw, data, drain = false) {
    this._send(Command.RAW_SEND, drain ? FLAG_DRAIN : 0, raw, 0, data, false);
  }

  /** What was sent goes out first, unless `abort`: then it's dropped, as `destroy()` does. */
  closeRaw(raw, abort = false) {
    this._command(Command.RAW_CLOSE, abort ? FLAG_ABORT : 0, 0, raw, 0);
  }

  pauseRaw(raw) {
    this._command(Command.RAW_PAUSE, 0, 0, raw, 0);
  }

  resumeRaw(raw) {
    this._command(Command.RAW_RESUME, 0, 0, raw, 0);
  }

  // ── WebTransport ───────────────────────────────────────────────────

  acceptSession(conn, session) {
    this._command(Command.SESSION_ACCEPT, 0, 0, conn, session);
  }

  rejectSession(conn, session, status = 404) {
    this._command(Command.SESSION_REJECT, 0, status, conn, session);
  }

  closeSession(conn, session, code = 0, reason = '') {
    if (code === 0 && reason === '') {
      this._command(Command.SESSION_CLOSE, 0, 0, conn, session);
    } else {
      this._withReason(Command.SESSION_CLOSE, 0, conn, session, code, reason);
    }
  }

  /** Resolves with the new stream's id. */
  openStream(conn, session, bidi = true) {
    if (this._handle === null) { return Promise.reject(new Error('netcode-io: server stopped')); }
    const token = this._nextToken;
    this._nextToken = (token + 1) >>> 0 || 1;
    return new Promise((resolve, reject) => {
      this._tokens.set(token, { resolve, reject });
      this._reserve(HEADER + 4);
      this._header(Command.STREAM_OPEN, bidi ? FLAG_BIDI : 0, 0, 4, conn, session);
      this._batch.writeUInt32LE(token, this._len);
      this._len += 4;
      this._schedule();
    });
  }

  sendStream(conn, stream, data, fin = false) {
    this._send(Command.STREAM_SEND, fin ? FLAG_FIN : 0, conn, stream, data);
  }

  closeStream(conn, stream) {
    this._send(Command.STREAM_SEND, FLAG_FIN, conn, stream, EMPTY);
  }

  resetStream(conn, stream, code = 0) {
    if (this._handle === null) { return; }
    this._reserve(HEADER + 4);
    this._header(Command.STREAM_RESET, 0, 0, 4, conn, stream);
    this._batch.writeUInt32LE(code >>> 0, this._len);
    this._len += 4;
    this._schedule();
  }

  sendDatagram(conn, session, data) {
    this._send(Command.DATAGRAM_SEND, 0, conn, session, data);
  }

  /** Ends the QUIC connection at once, every session on it. */
  closeConnection(conn) {
    this._command(Command.CONNECTION_CLOSE, 0, 0, conn, 0);
  }

  // ── Lifecycle ──────────────────────────────────────────────────────

  /**
   * Closes every connection and resolves once the loop has exited. What
   * was sent before reaches the loop first.
   */
  stop() {
    if (this._stopped === undefined) {
      this._stopped = new Promise((resolve) => { this._resolveStopped = resolve; });
      this._command(Command.STOP, 0, 0, 0, 0);
    }
    return this._stopped;
  }

  /**
   * Counters for tuning, since the server started: events and commands
   * crossed, how often each thread woke the other, and the loop thread's
   * CPU in seconds.
   */
  stats() {
    return this._handle === null ? undefined : native.stats(this._handle);
  }

  /** Keep Node running while the server is up: the default, as with net.Server. */
  ref() {
    if (this._handle !== null) { native.ref(this._handle, true); }
    return this;
  }

  unref() {
    if (this._handle !== null) { native.ref(this._handle, false); }
    return this;
  }

  // ── Commands ───────────────────────────────────────────────────────

  _reserve(n) {
    if (this._len + n <= this._batch.length) { return; }
    const next = Buffer.allocUnsafe(Math.max(this._batch.length * 2, this._len + n));
    this._batch.copy(next, 0, 0, this._len);
    this._batch = next;
  }

  _header(kind, flags, code, len, conn, id) {
    const b = this._batch;
    const o = this._len;
    b[o] = kind;
    b[o + 1] = flags;
    b.writeUInt16LE(code, o + 2);
    b.writeUInt32LE(len, o + 4);
    b.writeUInt32LE(conn >>> 0, o + 8);
    b.writeUInt32LE((conn / TWO_32) >>> 0, o + 12);
    b.writeUInt32LE(id >>> 0, o + 16);
    b.writeUInt32LE((id / TWO_32) >>> 0, o + 20);
    this._len = o + HEADER;
  }

  _command(kind, flags, code, conn, id) {
    if (this._handle === null) { return; }
    this._reserve(HEADER);
    this._header(kind, flags, code, 0, conn, id);
    this._schedule();
  }

  /** `errorCode` -1 leaves it out of the payload (a WebSocket close carries it in `code`). */
  _withReason(kind, code, conn, id, errorCode, reason) {
    if (this._handle === null) { return; }
    const prefix = errorCode < 0 ? 0 : 4;
    const len = prefix + Buffer.byteLength(reason);
    this._reserve(HEADER + len);
    this._header(kind, 0, code, len, conn, id);
    if (prefix > 0) { this._batch.writeUInt32LE(errorCode >>> 0, this._len); }
    this._batch.write(reason, this._len + prefix, 'utf8');
    this._len += len;
    this._schedule();
  }

  /** `share`: a later send of the same buffer in this batch refers back to it. */
  _send(kind, flags, conn, id, data, share = true) {
    if (this._handle === null) { return; }
    if (!(data instanceof Uint8Array)) {
      data = (typeof data === 'string') ? Buffer.from(data) : new Uint8Array(data);
    }
    const n = data.byteLength;
    const at = (share && n >= MIN_REF) ? this._refs.get(data) : undefined;

    if (at !== undefined) {
      this._reserve(HEADER + 8);
      this._header(kind, flags | FLAG_REF, 0, 8, conn, id);
      this._batch.writeUInt32LE(at, this._len);
      this._batch.writeUInt32LE(n, this._len + 4);
      this._len += 8;
    } else {
      this._reserve(HEADER + n);
      this._header(kind, flags, 0, n, conn, id);
      if (share && n >= MIN_REF) { this._refs.set(data, this._len); }
      this._batch.set(data, this._len);
      this._len += n;
    }
    this._schedule();
  }

  _schedule() {
    if (this._len >= MAX_BATCH) {
      this._flush();
    } else if (!this._scheduled) {
      this._scheduled = true;
      queueMicrotask(this._flush);
    }
  }

  // One submit per turn: the loop thread wakes once for all of it.
  _flush() {
    this._scheduled = false;
    if (this._len === 0 || this._handle === null) { return; }
    native.submit(this._handle, this._batch, this._len);
    this._len = 0;
    // clear() reallocates the table, even an empty one
    if (this._refs.size > 0) { this._refs.clear(); }
  }

  // ── Events ─────────────────────────────────────────────────────────

  _onEvents(ab) {
    const b = Buffer.from(ab);
    let o = 0;
    while (o + HEADER <= b.length) {
      const kind = b[o];
      const flags = b[o + 1];
      const code = b.readUInt16LE(o + 2);
      const len = b.readUInt32LE(o + 4);
      const conn = b.readUInt32LE(o + 8) + b.readUInt32LE(o + 12) * TWO_32;
      const id = b.readUInt32LE(o + 16) + b.readUInt32LE(o + 20) * TWO_32;
      const start = o + HEADER;
      o = start + len;
      try {
        this._dispatch(kind, flags, code, conn, id, b, start, o);
      } catch (e) {
        // the rest of the batch still goes out; the error surfaces as uncaught
        process.nextTick(() => { throw e; });
      }
    }
  }

  _dispatch(kind, flags, code, conn, id, b, start, end) {
    const h = this.handler;
    switch (kind) {
      case Event.WS_MESSAGE:
        h.onWsMessage?.(conn, b.subarray(start, end), (flags & FLAG_TEXT) !== 0);
        break;
      case Event.RAW_DATA:
        if (h.onRawOpen) {
          h.onRawData?.(conn, b.subarray(start, end));
        } else {
          setImmediate(socketData, this, conn, b.subarray(start, end));
        }
        break;
      case Event.STREAM_DATA:
        h.onStreamData?.(conn, id, b.subarray(start, end), (flags & FLAG_FIN) !== 0);
        break;
      case Event.DATAGRAM:
        h.onDatagram?.(conn, id, b.subarray(start, end));
        break;
      case Event.WS_OPEN:
        h.onWsOpen?.(conn, parseRequest(b, start, end));
        break;
      case Event.WS_CLOSE:
        h.onWsClose?.(conn, code, b.toString('utf8', start, end));
        break;
      case Event.SESSION_REQUEST:
        if (h.onSessionRequest) {
          h.onSessionRequest(conn, id, parseRequest(b, start, end));
        } else {
          this.acceptSession(conn, id);
        }
        break;
      case Event.SESSION_READY:
        h.onSessionReady?.(conn, id);
        break;
      case Event.SESSION_CLOSED:
        h.onSessionClosed?.(conn, id, b.readUInt32LE(start), b.toString('utf8', start + 4, end), (flags & FLAG_ABRUPT) !== 0);
        break;
      case Event.SESSION_DRAINING:
        h.onSessionDraining?.(conn, id);
        break;
      case Event.STREAM_OPEN:
        h.onStream?.(conn, b.readUInt32LE(start) + b.readUInt32LE(start + 4) * TWO_32, id, (flags & FLAG_BIDI) !== 0);
        break;
      case Event.CONNECTION_CLOSED:
        h.onConnectionClosed?.(conn);
        break;
      case Event.RAW_OPEN:
        this._onRawOpen(conn, b, start);
        break;
      case Event.RAW_CLOSE:
        if (h.onRawOpen) {
          h.onRawClose?.(conn);
        } else {
          setImmediate(socketClose, this, conn);
        }
        break;
      case Event.RAW_DRAIN:
        if (h.onRawOpen) {
          h.onRawDrain?.(conn);
        } else {
          setImmediate(socketDrain, this, conn);
        }
        break;
      case Event.RAW_END:
        if (h.onRawOpen) {
          h.onRawEnd?.(conn);
        } else {
          setImmediate(socketEnd, this, conn);
        }
        break;
      case Event.STREAM_OPENED: {
        const token = b.readUInt32LE(start);
        const pending = this._tokens.get(token);
        if (pending === undefined) { break; }
        this._tokens.delete(token);
        if (flags & FLAG_FAILED) {
          pending.reject(new Error('netcode-io: could not open a stream'));
        } else {
          pending.resolve(id);
        }
        break;
      }
      case Event.STOPPED:
        this._onStopped();
        break;
    }
  }

  _onRawOpen(raw, b, start) {
    const family = b[start];
    const address = formatAddress(family, b.subarray(start + 3, start + 19));
    const port = b.readUInt16LE(start + 1);
    const familyName = family === 4 ? 'IPv4' : family === 6 ? 'IPv6' : undefined;
    if (this.handler.onRawOpen) {
      this.handler.onRawOpen(raw, { remoteAddress: address, remotePort: port, remoteFamily: familyName });
      return;
    }
    const socket = new RawSocket(this, raw, address, port, familyName, this._encrypted);
    this._sockets.set(raw, socket);
    setImmediate(socketOpen, this, socket);
  }

  _onStopped() {
    native.destroy(this._handle);
    this._handle = null;
    for (const pending of this._tokens.values()) {
      pending.reject(new Error('netcode-io: server stopped'));
    }
    this._tokens.clear();
    if (this._resolveStopped === undefined) {
      this._stopped = Promise.resolve();
    } else {
      this._resolveStopped();
    }
  }
}

// A RawSocket's events each get a macrotask of their own, in order: Node
// gives every socket's data its own callback, so code written for net.Socket
// sees microtasks run between one connection's request and the next.
function socketOpen(server, socket) {
  if (server._http === undefined) {
    socket.end(NOT_FOUND);
  } else {
    server._http.emit('connection', socket);
  }
}

function socketData(server, raw, data) {
  server._sockets.get(raw)?._onData(data);
}

function socketEnd(server, raw) {
  server._sockets.get(raw)?._onEnd();
}

function socketDrain(server, raw) {
  server._sockets.get(raw)?._onDrain();
}

function socketClose(server, raw) {
  const socket = server._sockets.get(raw);
  server._sockets.delete(raw);
  socket?._onClose();
}

function toNativeOptions(options) {
  return {
    host: options.host,
    port: options.port,
    cert: pem(options.cert),
    key: pem(options.key),
    websocket: options.websocket,
  };
}

/** PEM as given, or read from the path given. */
function pem(value) {
  if (value === undefined || value === null) { return undefined; }
  if (typeof value !== 'string') { return Buffer.from(value).toString('latin1'); }
  return value.includes('-----BEGIN') ? value : fs.readFileSync(value, 'latin1');
}

function parseRequest(b, start, end) {
  const family = b[start];
  const port = b.readUInt16LE(start + 1);
  const address = b.subarray(start + 3, start + 19);
  let o = start + 19;
  const pathLen = b.readUInt16LE(o);
  const path = b.toString('utf8', o + 2, o + 2 + pathLen);
  o += 2 + pathLen;
  const count = b.readUInt16LE(o);
  o += 2;
  const headers = new Array(count);
  for (let i = 0; i < count && o < end; i++) {
    const nameLen = b.readUInt16LE(o);
    const name = b.toString('latin1', o + 2, o + 2 + nameLen);
    o += 2 + nameLen;
    const valueLen = b.readUInt16LE(o);
    headers[i] = [name, b.toString('utf8', o + 2, o + 2 + valueLen)];
    o += 2 + valueLen;
  }
  return new Request(path, headers, family, port, address);
}

function formatAddress(family, a) {
  if (family === 4) { return `${a[0]}.${a[1]}.${a[2]}.${a[3]}`; }
  if (family !== 6) { return undefined; }

  // an IPv4 client of a dual-stack socket, the way Node prints it
  if (a.subarray(0, 10).every((x) => x === 0) && a[10] === 0xff && a[11] === 0xff) {
    return `::ffff:${a[12]}.${a[13]}.${a[14]}.${a[15]}`;
  }

  const groups = [];
  for (let i = 0; i < 16; i += 2) { groups.push(((a[i] << 8) | a[i + 1]).toString(16)); }

  // RFC 5952: the longest run of two or more zero groups becomes "::"
  let best = -1;
  let bestLen = 1;
  for (let i = 0; i < 8;) {
    if (groups[i] !== '0') { i++; continue; }
    let j = i;
    while (j < 8 && groups[j] === '0') { j++; }
    if (j - i > bestLen) { best = i; bestLen = j - i; }
    i = j;
  }
  if (best < 0) { return groups.join(':'); }
  return `${groups.slice(0, best).join(':')}::${groups.slice(best + bestLen).join(':')}`;
}

module.exports = { Server, Request, RawSocket };
