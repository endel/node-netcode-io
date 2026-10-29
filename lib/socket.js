'use strict';

const { Duplex } = require('node:stream');

// Bytes written and not yet confirmed drained past which a write waits.
const HIGH_WATER = 256 * 1024;

function onTimeout(socket) {
  socket.emit('timeout');
}

/**
 * An HTTP/1.1 connection quic-zig handed over, shaped like `net.Socket` for
 * `http.Server.emit('connection', socket)`: TLS is already off, so it reads
 * and writes plaintext.
 */
class RawSocket extends Duplex {
  constructor(server, id, remoteAddress, remotePort, remoteFamily, encrypted) {
    super();
    this.remoteAddress = remoteAddress;
    this.remotePort = remotePort;
    this.remoteFamily = remoteFamily;
    /** Set when quic-zig terminated TLS: what `tls.TLSSocket` reports, and Express reads. */
    this.encrypted = encrypted;
    this.connecting = false;
    this.bytesRead = 0;
    this.bytesWritten = 0;

    this._server = server;
    this._id = id;
    this._inflight = 0;
    /** A write's callback, held until the loop says the output drained. */
    this._waiting = null;
    this._readPaused = false;
    this._ended = false;
    this._timer = null;
  }

  get id() { return this._id; }

  // ── net.Socket's surface, as http.Server uses it ───────────────────

  setTimeout(ms, callback) {
    if (this._timer !== null) {
      clearTimeout(this._timer);
      this._timer = null;
    }
    if (ms > 0 && !this.destroyed) {
      this._timer = setTimeout(onTimeout, ms, this);
      this._timer.unref();
    }
    if (callback !== undefined) {
      if (ms > 0) { this.once('timeout', callback); } else { this.removeListener('timeout', callback); }
    }
    return this;
  }

  setNoDelay() { return this; }
  setKeepAlive() { return this; }
  ref() { return this; }
  unref() { return this; }
  address() { return {}; }

  destroySoon() {
    if (this.writable) { this.end(); }
    if (this.writableFinished) {
      this.destroy();
    } else {
      this.once('finish', this.destroy);
    }
  }

  // ── Duplex ─────────────────────────────────────────────────────────

  _read() {
    if (!this._readPaused) { return; }
    this._readPaused = false;
    this._server.resumeRaw(this._id);
  }

  _write(chunk, encoding, callback) {
    this._out(chunk, callback, true);
  }

  _writev(chunks, callback) {
    for (let i = 0; i < chunks.length; i++) {
      this._out(chunks[i].chunk, callback, i === chunks.length - 1);
    }
  }

  _out(chunk, callback, last) {
    this._timer?.refresh();
    this.bytesWritten += chunk.length;
    this._inflight += chunk.length;
    const wait = last && this._inflight >= HIGH_WATER;
    this._server.sendRaw(this._id, chunk, wait);
    if (!last) { return; }
    if (wait) {
      this._waiting = callback;
    } else {
      callback();
    }
  }

  _final(callback) {
    this._server.closeRaw(this._id);
    callback();
  }

  _destroy(err, callback) {
    // before the writes finished, what's queued is dropped, as net.Socket's destroy() does
    if (!this.writableFinished) { this._server.closeRaw(this._id, true); }
    if (this._timer !== null) { clearTimeout(this._timer); }
    const waiting = this._waiting;
    this._waiting = null;
    waiting?.(err ?? new Error('netcode-io: connection closed'));
    callback(err);
  }

  // ── From the server ────────────────────────────────────────────────

  _onData(data) {
    this._timer?.refresh();
    this.bytesRead += data.length;
    if (!this.push(data) && !this._readPaused) {
      this._readPaused = true;
      this._server.pauseRaw(this._id);
    }
  }

  _onDrain() {
    this._inflight = 0;
    const waiting = this._waiting;
    this._waiting = null;
    waiting?.();
  }

  /** The peer sent its last byte; writes go on until we end. */
  _onEnd() {
    this._ended = true;
    this.push(null);
  }

  _onClose() {
    if (!this._ended) {
      this._ended = true;
      this.push(null);
    }
    this.destroy();
  }
}

module.exports = { RawSocket };
