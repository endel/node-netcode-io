export interface ServerOptions {
  /** Default "0.0.0.0". */
  host?: string;
  /** UDP for QUIC, and TCP for WebSockets unless `websocket.port` says otherwise. 0 lets the kernel pick. */
  port: number;
  /** PEM certificate chain file: TLS for both WebTransport and wss://. */
  cert: string;
  /** PEM private key file: ECDSA P-256, Ed25519 or RSA. */
  key: string;
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
}

type Bytes = Uint8Array | ArrayBuffer | string;

/**
 * A WebSocket and WebTransport server on one port, run by quic-zig on a
 * thread of its own. Calls queue commands; one batch per turn reaches that
 * thread. A command for a socket, session or stream that's gone does nothing.
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

  /** Closes every connection; resolves once the loop has exited. */
  stop(): Promise<void>;
  ref(): this;
  unref(): this;
}
