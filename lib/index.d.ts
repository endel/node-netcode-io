import type { Server as HttpServer } from 'node:http';
import type { Duplex } from 'node:stream';

export interface ServerOptions {
  /** Default "0.0.0.0". */
  host?: string;
  /** UDP for QUIC, and TCP for WebSockets unless `websocket.port` says otherwise. 0 lets the kernel pick. */
  port: number;
  /** PEM certificate chain, or a path to one: TLS for both WebTransport and wss://. */
  cert: string | Uint8Array;
  /** PEM private key, or a path to one: ECDSA P-256, Ed25519 or RSA. */
  key: string | Uint8Array;
  websocket?: {
    /** false serves ws:// and http://, for a proxy that terminates TLS. Default true. */
    tls?: boolean;
    /** TCP port, when not `port`. */
    port?: number;
    /** Largest message accepted, fragments included; bigger closes with 1009. Default 1 MiB. */
    maxMessageSize?: number;
    /** Bytes queued for a peer that isn't reading past which it is closed with 1008. Default 4 MiB. */
    maxSendBuffer?: number;
    /** Ping a quiet connection this often (ms), and drop it after two intervals. 0 disables. Default 30000. */
    pingInterval?: number;
  };
}

/** A WebSocket upgrade or a WebTransport CONNECT, as the peer sent it. */
export declare class Request {
  /** The request target: path and query. */
  readonly path: string;
  /** In arrival order; a name may repeat. WebTransport's include pseudo-headers (":authority"). */
  readonly headers: ReadonlyArray<readonly [name: string, value: string]>;
  readonly remoteAddress: string | undefined;
  readonly remotePort: number;
  readonly remoteFamily: 'IPv4' | 'IPv6' | undefined;
  /** The first value of `name`, matched case-insensitively. */
  header(name: string): string | undefined;
}

/**
 * Every callback is optional. Ids are Numbers: `ws` and `conn` come from one
 * counter, so they never collide.
 */
export interface Handler {
  /** The upgrade is already accepted. */
  onWsOpen?(ws: number, request: Request): void;
  /** `data` stays valid for as long as it's kept. */
  onWsMessage?(ws: number, data: Buffer, isText: boolean): void;
  /** The socket's last event. */
  onWsClose?(ws: number, code: number, reason: string): void;

  /** Answer with `acceptSession` or `rejectSession`, now or later. Without this callback, every session is accepted. */
  onSessionRequest?(conn: number, session: number, request: Request): void;
  /** Accepted: streams and datagrams can flow. */
  onSessionReady?(conn: number, session: number): void;
  /** `abrupt`: the CONNECT stream was reset or the connection ended, so `code` is 0. */
  onSessionClosed?(conn: number, session: number, code: number, reason: string, abrupt: boolean): void;
  onSessionDraining?(conn: number, session: number): void;
  /** A stream the peer opened. */
  onStream?(conn: number, session: number, stream: number, bidi: boolean): void;
  onStreamData?(conn: number, stream: number, data: Buffer, fin: boolean): void;
  onDatagram?(conn: number, session: number, data: Buffer): void;
  /** The QUIC connection's last event, after every session on it closed. */
  onConnectionClosed?(conn: number): void;

  /**
   * An HTTP/1.1 connection that isn't a WebSocket upgrade, as bytes from its
   * first request line on, TLS already removed. Declaring this takes them
   * from `serveHttp`.
   */
  onRawOpen?(raw: number, address: RawAddress): void;
  onRawData?(raw: number, data: Buffer): void;
  /** The connection's last event. */
  onRawClose?(raw: number): void;
  /** The answer to `sendRaw(raw, data, true)`: the output is low again. */
  onRawDrain?(raw: number): void;
  /**
   * The peer sent its last byte: a FIN, or TLS's close_notify. The
   * connection stays open for writes until `closeRaw`, since an HTTP/1.1
   * client may half-close after its request.
   */
  onRawEnd?(raw: number): void;
}

export interface RawAddress {
  readonly remoteAddress: string | undefined;
  readonly remotePort: number;
  readonly remoteFamily: 'IPv4' | 'IPv6' | undefined;
}

/**
 * A raw connection shaped like `net.Socket`, which `serveHttp` hands to an
 * `http.Server` through its 'connection' event. Writes wait once 256 KiB
 * are on their way, until the loop has sent them.
 */
export declare class RawSocket extends Duplex {
  readonly id: number;
  readonly remoteAddress: string | undefined;
  readonly remotePort: number;
  readonly remoteFamily: 'IPv4' | 'IPv6' | undefined;
  /** quic-zig terminated TLS for it. */
  readonly encrypted: boolean;
  readonly connecting: false;
  bytesRead: number;
  bytesWritten: number;
  setTimeout(ms: number, callback?: () => void): this;
  setNoDelay(noDelay?: boolean): this;
  setKeepAlive(enable?: boolean, initialDelay?: number): this;
  ref(): this;
  unref(): this;
  address(): {};
  destroySoon(): void;
}

type Bytes = Uint8Array | ArrayBuffer | string;

/**
 * A WebSocket and WebTransport server on one port, run by quic-zig on a
 * thread of its own. Calls queue commands; one batch per turn reaches that
 * thread. A command for a socket, session or stream that's gone does nothing.
 *
 * Sends copy `data` before they return. A buffer of 64 bytes or more sent
 * again in the same turn goes as a reference to its first copy, so a
 * broadcast crosses once: a buffer reused for other bytes within a turn must
 * be a new object each time (`sendRaw` excepted, which never refers back).
 */
export declare class Server {
  /** Binds now: a port in use throws here. */
  constructor(options: ServerOptions, handler?: Handler);
  handler: Handler;
  /** The bound UDP port; undefined once stopped. */
  readonly port: number | undefined;

  send(ws: number, data: Bytes, isText?: boolean): void;
  closeWebSocket(ws: number, code?: number, reason?: string): void;

  acceptSession(conn: number, session: number): void;
  /** `status`: 300 to 599. Default 404. */
  rejectSession(conn: number, session: number, status?: number): void;
  closeSession(conn: number, session: number, code?: number, reason?: string): void;
  /** Resolves with the stream's id. */
  openStream(conn: number, session: number, bidi?: boolean): Promise<number>;
  /** A write the stream can't take resets it rather than lose bytes. */
  sendStream(conn: number, stream: number, data: Bytes, fin?: boolean): void;
  closeStream(conn: number, stream: number): void;
  resetStream(conn: number, stream: number, code?: number): void;
  sendDatagram(conn: number, session: number, data: Bytes): void;
  /** Ends the QUIC connection at once, every session on it. */
  closeConnection(conn: number): void;

  /**
   * Raw connections go to `httpServer` as `RawSocket`s, unless the handler
   * has `onRawOpen`. Without either, each request gets a 404.
   */
  serveHttp(httpServer: HttpServer): this;
  /** `drain`: answer with `onRawDrain` once the output is low again. */
  sendRaw(raw: number, data: Bytes, drain?: boolean): void;
  /** What was sent goes out first, unless `abort`: then it's dropped, as `destroy()` does. */
  closeRaw(raw: number, abort?: boolean): void;
  pauseRaw(raw: number): void;
  resumeRaw(raw: number): void;

  /** Closes every connection; resolves once the loop has exited. */
  stop(): Promise<void>;
  ref(): this;
  unref(): this;
}
