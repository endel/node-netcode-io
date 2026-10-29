'use strict';

const fs = require('node:fs');
const { performance } = require('node:perf_hooks');
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
  HTTP_REQUEST: 18,
  STOPPED: 20,
};

const Command = {
  WS_SEND: 1, WS_CLOSE: 2,
  SESSION_ACCEPT: 4, SESSION_REJECT: 5, SESSION_CLOSE: 6,
  STREAM_OPEN: 7, STREAM_SEND: 8, STREAM_RESET: 9,
  DATAGRAM_SEND: 10, CONNECTION_CLOSE: 11,
  RAW_SEND: 12, RAW_CLOSE: 13, RAW_PAUSE: 14, RAW_RESUME: 15,
  HTTP_RESPOND: 16, HTTP_PASS: 17,
  STOP: 20,
};

// the hot path's kinds as locals: a property load each, in the interpreter
const WS_MESSAGE = Event.WS_MESSAGE;
const WS_SEND = Command.WS_SEND;
const RAW_SEND = Command.RAW_SEND;

const FLAG_TEXT = 1;
const FLAG_FIN = 1;
const FLAG_ABRUPT = 1;
const FLAG_BIDI = 1;
const FLAG_DRAIN = 1;
const FLAG_ABORT = 1;
const FLAG_REF = 2;
const FLAG_FAILED = 4;
const FLAG_READ = 16;
const FLAG_LENT = 32;
const FLAG_LENT_READ = 64;

// A batch this big goes out at once instead of at the end of the turn.
const MAX_BATCH = 1 << 20;
// Below this a send is cheaper to copy again than to look up.
const MIN_REF = 64;
// buffers a batch remembers for references: a broadcast sends one buffer
// back to back, so a few identity checks find it without hashing; a power
// of two
const RECENT = 4;
// On Node's thread, a batch older than this goes out between events: the
// first replies of a long delivery don't wait on the last events.
const FLUSH_AFTER_MS = 0.05;
// On Node's thread, a WebSocket or raw send this big goes to the socket at
// once, from the caller's buffer: copying it into the batch costs more than
// the extra call.
const DIRECT_MIN = 16 * 1024;

const EMPTY = new Uint8Array(0);

const NOT_FOUND = 'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n';

/**
 * A WebSocket upgrade, a WebTransport CONNECT, or a request `onHttpRequest`
 * answers, as the peer sent it.
 */
class Request {
  constructor(path, headers, family, port, address) {
    /** For `onHttpRequest`'s: the method, and the whole body. */
    this.method = undefined;
    this.body = undefined;
    this.path = path;
    /** In arrival order; a name may repeat. */
    this.headers = headers;
    this.remotePort = port;
    this.remoteFamily = family === 4 ? 'IPv4' : family === 6 ? 'IPv6' : undefined;
    this._address = address;
    this._family = family;
    this._formatted = undefined;
    this._end = 0;
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
    this._view = viewOf(this._batch);
    // the last delivery's Buffer, and a view of it: lent, it's the same one each time
    this._eventsBuf = undefined;
    this._eventsView = undefined;
    this._len = 0;
    /** Sends already in this batch, by buffer: a broadcast crosses once. */
    this._recent = new Array(RECENT).fill(undefined);
    this._recentAt = new Array(RECENT).fill(0);
    this._recentNext = 0;
    this._scheduled = false;
    // inside _onEvents, which flushes on its way out
    this._delivering = false;
    this._events = 0;
    this._batchedAt = 0;
    this._flush = this._flush.bind(this);
    // WebSockets on Node's thread: sends run as they're submitted. (The QUIC
    // thread is woken per submit: there, one per turn.)
    this._local = options.websocket?.thread !== 'quic';
    this._transient = this._local && options.websocket?.transientMessages === true;
    this._readBuf = undefined;
    // During a lent delivery: the Buffer its bytes are in, which a send of
    // them points into instead of copying
    this._lentBuf = undefined;

    this._tokens = new Map();
    this._nextToken = 1;
    this._stopped = undefined;
    this._resolveStopped = undefined;

    this._http = undefined;
    /** RawSockets by id, when the handler leaves raw connections to us. */
    this._sockets = new Map();
    this._encrypted = options.websocket?.tls !== false;

    this._handle = native.listen(toNativeOptions(options, handler), (b, from, to) => this._onEvents(b, from, to));
  }

  /** The bound UDP port; WebSockets share it unless `websocket.port` says otherwise. */
  get port() {
    return this._handle === null ? undefined : native.port(this._handle);
  }

  // ── WebSocket ──────────────────────────────────────────────────────

  send(ws, data, isText = false) {
    this._send(WS_SEND, isText ? FLAG_TEXT : 0, ws, 0, data, true);
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

  // ── HTTP requests the handler answers ──────────────────────────────

  /**
   * Answers an `onHttpRequest` request. `headers`: an object, `[name,
   * value]` pairs, or a `Headers`; Content-Length is added.
   */
  respond(request, status, headers, body = EMPTY) {
    if (this._handle === null) { return; }
    let lines = '';
    const add = (name, value) => {
      name = String(name);
      value = String(value);
      if (/[\r\n:]/.test(name) || /[\r\n]/.test(value)) { throw new TypeError(`netcode-io: invalid header ${JSON.stringify(name)}`); }
      lines += `${name}: ${value}\r\n`;
    };
    if (headers !== undefined && headers !== null) {
      const entries = (typeof headers[Symbol.iterator] === 'function') ? headers : Object.entries(headers);
      for (const [name, value] of entries) { add(name, value); }
    }
    const head = Buffer.from(lines, 'latin1');
    const data = (typeof body === 'string') ? Buffer.from(body) : body;
    const len = 4 + head.length + data.byteLength;
    this._reserve(HEADER + len);
    this._header(Command.HTTP_RESPOND, 0, status, len, request, 0);
    this._batch.writeUInt32LE(head.length, this._len);
    this._batch.set(head, this._len + 4);
    this._batch.set(data, this._len + 4 + head.length);
    this._len += len;
    this._schedule();
  }

  /** Serves an `onHttpRequest` request as if it had never been taken: through `serveHttp`'s server. */
  passRequest(request) {
    this._command(Command.HTTP_PASS, 0, 0, request, 0);
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
    this._view = viewOf(next);
  }

  // A DataView call per field: until V8 optimizes this, which at low rates
  // takes seconds, both Buffer's write methods and byte stores cost more.
  _header(kind, flags, code, len, conn, id) {
    const v = this._view;
    const o = this._len;
    v.setUint32(o, (kind | (flags << 8) | (code << 16)) >>> 0, true);
    v.setUint32(o + 4, len, true);
    v.setUint32(o + 8, conn >>> 0, true);
    v.setUint32(o + 12, conn < TWO_32 ? 0 : (conn / TWO_32) >>> 0, true);
    if (id === 0) {
      // +0's eight bytes are all zero: one call for both halves
      v.setFloat64(o + 16, 0);
    } else {
      v.setUint32(o + 16, id >>> 0, true);
      v.setUint32(o + 20, id < TWO_32 ? 0 : (id / TWO_32) >>> 0, true);
    }
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
    if (n >= DIRECT_MIN && this._local && (kind === WS_SEND || kind === RAW_SEND)) {
      // what's queued goes first
      this._flush();
      native.send(this._handle, kind, flags, conn, data);
      return;
    }
    // bytes of this delivery: point at them, native still has them at submit
    const lent = this._lentBuf;
    if (lent !== undefined && (kind === WS_SEND || kind === RAW_SEND)) {
      const ab = data.buffer;
      let base = -1;
      let lflag = 0;
      if (ab === lent.buffer) {
        base = lent.byteOffset;
        lflag = FLAG_LENT;
      } else if (this._readBuf !== undefined && ab === this._readBuf.buffer) {
        base = this._readBuf.byteOffset;
        lflag = FLAG_LENT_READ;
      }
      if (lflag !== 0) {
        if (this._len + HEADER + 8 > this._batch.length) { this._reserve(HEADER + 8); }
        this._header(kind, flags | lflag, 0, 8, conn, id);
        this._view.setUint32(this._len, data.byteOffset - base, true);
        this._view.setUint32(this._len + 4, n, true);
        this._len += 8;
        if (!this._scheduled || this._len >= MAX_BATCH) { this._schedule(); }
        return;
      }
    }
    // inline, as below: until V8 optimizes this, each call costs more than its work
    let at = -1;
    if (share && n >= MIN_REF) {
      const recent = this._recent;
      for (let i = 0; i < RECENT; i++) {
        if (recent[i] === data) { at = this._recentAt[i]; break; }
      }
    }

    if (at !== -1) {
      this._reserve(HEADER + 8);
      this._header(kind, flags | FLAG_REF, 0, 8, conn, id);
      this._batch.writeUInt32LE(at, this._len);
      this._batch.writeUInt32LE(n, this._len + 4);
      this._len += 8;
    } else {
      if (this._len + HEADER + n > this._batch.length) { this._reserve(HEADER + n); }
      this._header(kind, flags, 0, n, conn, id);
      if (share && n >= MIN_REF) {
        this._recent[this._recentNext] = data;
        this._recentAt[this._recentNext] = this._len;
        this._recentNext = (this._recentNext + 1) & (RECENT - 1);
      }
      this._batch.set(data, this._len);
      this._len += n;
    }
    if (!this._scheduled || this._len >= MAX_BATCH) { this._schedule(); }
  }

  _schedule() {
    if (this._len >= MAX_BATCH) {
      this._flush();
    } else if (!this._scheduled) {
      this._scheduled = true;
      // stamped by the first early-flush check, if a delivery gets that far
      this._batchedAt = 0;
      if (!this._delivering) { queueMicrotask(this._flush); }
    }
  }

  // One submit per turn: the loop thread wakes once for all of it.
  _flush() {
    this._scheduled = false;
    if (this._len === 0 || this._handle === null) { return; }
    native.submit(this._handle, this._batch, this._len);
    this._len = 0;
    const r = this._recent;
    if (r[0] !== undefined) { for (let i = 0; i < RECENT; i++) { r[i] = undefined; } }
    this._recentNext = 0;
  }

  // ── Events ─────────────────────────────────────────────────────────

  /**
   * `from`, `to`: the batch's bytes in `b`, a Buffer reused for every batch
   * with `transientMessages`.
   */
  _onEvents(b, from, to) {
    // a range only for the lent Buffer
    if (to === undefined) {
      from = 0;
      to = b.length;
      this._lentBuf = undefined;
    } else {
      this._lentBuf = b;
    }
    this._delivering = true;
    let v = this._eventsView;
    if (b !== this._eventsBuf) {
      this._eventsBuf = b;
      v = this._eventsView = viewOf(b);
    }
    try {
      let o = from;
      while (o + HEADER <= to) {
        // a DataView call per field, as in _header
        const at = o;
        const kf = v.getUint16(o, true);
        const kind = kf & 0xff;
        const flags = kf >>> 8;
        const len = v.getUint32(o + 4, true);
        // ids are small: kept a small integer, not made a double by * TWO_32
        const connHi = v.getUint32(o + 12, true);
        const conn = connHi === 0 ? v.getUint32(o + 8, true) : v.getUint32(o + 8, true) + connHi * TWO_32;
        o += HEADER + len;
        try {
          if (kind === WS_MESSAGE) {
            const data = (flags & FLAG_READ) === 0 ? b.subarray(at + HEADER, o) : this._lentRead(v, at);
            this.handler.onWsMessage?.(conn, data, (flags & FLAG_TEXT) !== 0);
          } else {
            this._dispatch(kind, flags, conn, b, at, o);
          }
        } catch (e) {
          // the rest of the batch still goes out; the error surfaces as uncaught
          process.nextTick(() => { throw e; });
        }
        // the clock every few events: reading it is a tenth of an echo's JS
        if (this._local && this._scheduled && (++this._events & 7) === 0) {
          const now = performance.now();
          if (this._batchedAt === 0) { this._batchedAt = now; } else if (now - this._batchedAt >= FLUSH_AFTER_MS) { this._flush(); }
        }
      }
    } finally {
      this._delivering = false;
      if (this._scheduled) { this._flush(); }
      this._lentBuf = undefined;
    }
  }

  /** A message not in the batch: a view of quic-zig's read buffer. */
  _lentRead(v, at) {
    this._readBuf ??= native.readBuffer(this._handle);
    const off = v.getUint32(at + HEADER, true);
    return this._readBuf.subarray(off, off + v.getUint32(at + HEADER + 4, true));
  }

  /** The record at `at`, its payload up to `end`. */
  _dispatch(kind, flags, conn, b, at, end) {
    const h = this.handler;
    const start = at + HEADER;
    const v = this._eventsView;
    const code = v.getUint16(at + 2, true);
    const idHi = v.getUint32(at + 20, true);
    const id = idHi === 0 ? v.getUint32(at + 16, true) : v.getUint32(at + 16, true) + idHi * TWO_32;
    switch (kind) {
      case Event.RAW_DATA: {
        // an http.Server reads it later: never a view of a reused Buffer
        const data = this._transient ? Buffer.from(b.subarray(start, end)) : b.subarray(start, end);
        if (h.onRawOpen) {
          h.onRawData?.(conn, data);
        } else {
          setImmediate(socketData, this, conn, data);
        }
        break;
      }
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
      case Event.HTTP_REQUEST:
        h.onHttpRequest?.(conn, parseHttpRequest(b, start, end));
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
    // with the listener on this thread: its last closes
    const rest = native.destroy(this._handle);
    this._handle = null;
    if (rest !== undefined) { this._onEvents(rest); }
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

function toNativeOptions(options, handler) {
  return {
    host: options.host,
    port: options.port,
    cert: pem(options.cert),
    key: pem(options.key),
    websocket: options.websocket,
    requests: typeof handler.onHttpRequest === 'function',
  };
}

/** PEM as given, or read from the path given. */
function pem(value) {
  if (value === undefined || value === null) { return undefined; }
  if (typeof value !== 'string') { return Buffer.from(value).toString('latin1'); }
  return value.includes('-----BEGIN') ? value : fs.readFileSync(value, 'latin1');
}

function viewOf(b) {
  return new DataView(b.buffer, b.byteOffset, b.byteLength);
}

function parseHttpRequest(b, start, end) {
  const methodLen = b.readUInt16LE(start);
  const method = b.toString('latin1', start + 2, start + 2 + methodLen);
  const req = parseRequest(b, start + 2 + methodLen, end);
  req.method = method;
  req.body = Buffer.from(b.subarray(req._end, end));
  return req;
}

function parseRequest(b, start, end) {
  const family = b[start];
  const port = b.readUInt16LE(start + 1);
  // copied: formatted on demand, perhaps after the batch's Buffer is reused
  const address = Buffer.from(b.subarray(start + 3, start + 19));
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
  const req = new Request(path, headers, family, port, address);
  req._end = o;
  return req;
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
