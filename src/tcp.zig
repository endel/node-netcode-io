//! WebSockets and the HTTP/1.1 connections JS serves itself: quic-zig's
//! listener callbacks and the commands for them, on whichever thread runs
//! that listener. `Sink` takes the events:
//!
//!   emit(kind, header, parts) usize           bytes now waiting for JS
//!   emitRequest(kind, header, addr, path, headers) void
//!   emitHttpRequest(header, method, addr, target, headers, body) void
//!   markPaused() void                         a read was paused for JS

const std = @import("std");
const quic = @import("quic");
const event_loop = quic.event_loop;
const wire = @import("wire.zig");

const gpa = std.heap.c_allocator;

/// Event bytes waiting for JS past which a raw connection's reads pause,
/// until JS has taken them.
pub const max_pending_events = 8 << 20;

pub fn Tcp(comptime Sink: type) type {
    return struct {
        const Self = @This();

        sink: *Sink,
        sockets: std.AutoHashMapUnmanaged(u64, Socket) = .empty,
        raws: std.AutoHashMapUnmanaged(u64, Raw) = .empty,
        /// Raw connections whose reads the cap paused.
        paused: std.ArrayList(u64) = .empty,
        /// Connections the batch being run corked, by id; `raw_bit` marks a
        /// raw one.
        corked: std.ArrayList(u64) = .empty,
        /// Which batch `Socket.sends` and `Raw.sends` count for.
        batch: u32 = 0,
        /// Requests JS answers itself, while they wait on it.
        exchanges: std.AutoHashMapUnmanaged(u64, *event_loop.HttpExchange) = .empty,
        /// JS has `onHttpRequest`; without, each request passes at once.
        takes_requests: bool = false,

        const Socket = struct {
            ws: *event_loop.WsConn,
            /// Sends in batch `batch_seen`.
            sends: u32 = 0,
            batch_seen: u32 = 0,
        };

        const Raw = struct {
            conn: *event_loop.RawConn,
            sends: u32 = 0,
            batch_seen: u32 = 0,
            /// JS asked for `raw_drain`, which `onRawDrain` owes.
            drain_wanted: bool = false,
            /// By `raw_pause`, which the cap's resume leaves alone.
            js_paused: bool = false,
            /// By the cap.
            cap_paused: bool = false,
        };

        pub fn deinit(self: *Self) void {
            self.sockets.deinit(gpa);
            self.raws.deinit(gpa);
            self.paused.deinit(gpa);
            self.corked.deinit(gpa);
            self.exchanges.deinit(gpa);
        }

        /// Before running `batch`: corks each connection it sends to more
        /// than once, so those messages leave in one send instead of one
        /// each. A connection sent to once keeps its direct send.
        pub fn corkFor(self: *Self, batch: []const u8) void {
            // One command corks nothing.
            if (batch.len < wire.header_len or wire.header_len + std.mem.readInt(u32, batch[4..8], .little) >= batch.len) return;
            self.batch +%= 1;
            var r: wire.Reader = .{ .batch = batch };
            while (r.next()) |rec| {
                const h = rec.header;
                switch (@as(wire.Command, @fromBackingInt(h.kind))) {
                    .ws_send => if (self.sockets.getPtr(h.conn)) |sock| {
                        if (self.counted(&sock.sends, &sock.batch_seen) == 2) {
                            self.corked.append(gpa, h.conn) catch continue;
                            sock.ws.cork();
                        }
                    },
                    .raw_send => if (self.raws.getPtr(h.conn)) |raw| {
                        if (self.counted(&raw.sends, &raw.batch_seen) == 2) {
                            self.corked.append(gpa, h.conn | raw_bit) catch continue;
                            raw.conn.cork();
                        }
                    },
                    else => {},
                }
            }
        }

        /// One counter draws both kinds' ids, all below 2^53: the top bit
        /// is free to say which map an id is in.
        const raw_bit: u64 = 1 << 63;

        fn counted(self: *const Self, sends: *u32, seen: *u32) u32 {
            if (seen.* != self.batch) {
                seen.* = self.batch;
                sends.* = 0;
            }
            sends.* += 1;
            return sends.*;
        }

        /// After the batch: sends what `corkFor` held back.
        pub fn uncork(self: *Self) void {
            for (self.corked.items) |id| {
                if (id & raw_bit != 0) {
                    if (self.raws.getPtr(id & ~raw_bit)) |raw| raw.conn.uncork();
                } else if (self.sockets.getPtr(id)) |sock| {
                    sock.ws.uncork();
                }
            }
            self.corked.clearRetainingCapacity();
        }

        fn pauseRaw(self: *Self, raw: *Raw) void {
            if (raw.cap_paused) return;
            self.paused.append(gpa, raw.conn.id()) catch return;
            raw.cap_paused = true;
            raw.conn.pauseReading();
            self.sink.markPaused();
        }

        /// JS has taken what was waiting.
        pub fn resumeReads(self: *Self) void {
            for (self.paused.items) |id| {
                const raw = self.raws.getPtr(id) orelse continue;
                raw.cap_paused = false;
                if (!raw.js_paused) raw.conn.resumeReading();
            }
            self.paused.clearRetainingCapacity();
        }

        // ── WebSocket ────────────────────────────────────────────────

        pub fn onWsUpgrade(self: *Self, req: *event_loop.WsRequest, path: []const u8) void {
            const ws = req.accept(.{}) catch return;
            self.sockets.put(gpa, ws.id(), .{ .ws = ws }) catch return ws.close(1011, "");
            const addr = wire.Address.fromSockaddr(ws.peerAddress());
            self.sink.emitRequest(.ws_open, .{ .conn = ws.id() }, addr, path, req.headers());
        }

        pub fn onWsMessage(self: *Self, ws: *event_loop.WsConn, data: []const u8, kind: event_loop.WsMessageKind) void {
            const flags: u8 = if (kind == .text) wire.flag.text else 0;
            _ = self.sink.emit(.ws_message, .{ .conn = ws.id(), .flags = flags }, &.{data});
        }

        pub fn onWsClose(self: *Self, ws: *event_loop.WsConn, code: u16, reason: []const u8) void {
            if (!self.sockets.remove(ws.id())) return;
            _ = self.sink.emit(.ws_close, .{ .conn = ws.id(), .code = code }, &.{reason});
        }

        // ── Raw HTTP/1.1 ─────────────────────────────────────────────

        pub fn onRawOpen(self: *Self, raw: *event_loop.RawConn) void {
            self.raws.put(gpa, raw.id(), .{ .conn = raw }) catch return raw.close();
            const addr = wire.Address.fromSockaddr(raw.peerAddress()).encode();
            _ = self.sink.emit(.raw_open, .{ .conn = raw.id() }, &.{&addr});
        }

        pub fn onRawData(self: *Self, raw: *event_loop.RawConn, data: []const u8) void {
            const pending = self.sink.emit(.raw_data, .{ .conn = raw.id() }, &.{data});
            if (pending > max_pending_events) if (self.raws.getPtr(raw.id())) |r| self.pauseRaw(r);
        }

        pub fn onRawClose(self: *Self, raw: *event_loop.RawConn) void {
            if (!self.raws.remove(raw.id())) return;
            _ = self.sink.emit(.raw_close, .{ .conn = raw.id() }, &.{});
        }

        pub fn onRawEnd(self: *Self, raw: *event_loop.RawConn) void {
            if (!self.raws.contains(raw.id())) return;
            _ = self.sink.emit(.raw_end, .{ .conn = raw.id() }, &.{});
        }

        pub fn onRawDrain(self: *Self, raw: *event_loop.RawConn) void {
            const r = self.raws.getPtr(raw.id()) orelse return;
            if (!r.drain_wanted) return;
            r.drain_wanted = false;
            _ = self.sink.emit(.raw_drain, .{ .conn = raw.id() }, &.{});
        }

        // ── Requests JS answers ──────────────────────────────────────

        pub fn onHttpExchange(self: *Self, ex: *event_loop.HttpExchange) void {
            if (!self.takes_requests) return ex.pass();
            self.exchanges.put(gpa, ex.id(), ex) catch return ex.pass();
            self.sink.emitHttpRequest(.{ .conn = ex.id() }, ex.method(), wire.Address.fromSockaddr(ex.peerAddress()), ex.target(), ex.headers(), ex.body());
        }

        pub fn onHttpExchangeGone(self: *Self, ex: *event_loop.HttpExchange) void {
            _ = self.exchanges.remove(ex.id());
        }

        // ── Commands ─────────────────────────────────────────────────

        /// Whether `kind` is one of ours.
        pub fn owns(kind: u8) bool {
            return switch (@as(wire.Command, @fromBackingInt(kind))) {
                .ws_send, .ws_close, .raw_send, .raw_close, .raw_pause, .raw_resume, .http_respond, .http_pass => true,
                else => false,
            };
        }

        /// Runs a command `owns` says is ours.
        pub fn runCommand(self: *Self, r: *const wire.Reader, rec: wire.Record) void {
            const h = rec.header;
            switch (@as(wire.Command, @fromBackingInt(h.kind))) {
                .ws_send => {
                    const ws = (self.sockets.get(h.conn) orelse return).ws;
                    const data = r.bytes(rec) orelse return;
                    const sent = if (h.flags & wire.flag.text != 0) ws.sendText(data) else ws.send(data);
                    // A dropped message would leave the peer's state wrong for good.
                    sent catch ws.close(1008, "send buffer full");
                },
                .ws_close => {
                    const ws = (self.sockets.get(h.conn) orelse return).ws;
                    ws.close(h.code, rec.payload);
                },
                .raw_send => {
                    const raw = self.raws.getPtr(h.conn) orelse return;
                    const data = r.bytes(rec) orelse return;
                    // Closed: raw_close follows.
                    raw.conn.write(data) catch return;
                    if (h.flags & wire.flag.drain == 0) return;
                    if (raw.conn.owesDrain()) {
                        raw.drain_wanted = true;
                    } else {
                        _ = self.sink.emit(.raw_drain, .{ .conn = h.conn }, &.{});
                    }
                },
                .raw_close => {
                    const raw = self.raws.get(h.conn) orelse return;
                    if (h.flags & wire.flag.abort != 0) raw.conn.abort() else raw.conn.close();
                },
                .raw_pause => {
                    const raw = self.raws.getPtr(h.conn) orelse return;
                    raw.js_paused = true;
                    raw.conn.pauseReading();
                },
                .raw_resume => {
                    const raw = self.raws.getPtr(h.conn) orelse return;
                    raw.js_paused = false;
                    if (!raw.cap_paused) raw.conn.resumeReading();
                },
                .http_respond => {
                    const ex = (self.exchanges.fetchRemove(h.conn) orelse return).value;
                    if (rec.payload.len < 4) return ex.respond(500, "", "");
                    const hlen = std.mem.readInt(u32, rec.payload[0..4], .little);
                    if (hlen > rec.payload.len - 4) return ex.respond(500, "", "");
                    ex.respond(h.code, rec.payload[4..][0..hlen], rec.payload[4 + hlen ..]);
                },
                .http_pass => {
                    const ex = (self.exchanges.fetchRemove(h.conn) orelse return).value;
                    ex.pass();
                },
                else => unreachable,
            }
        }
    };
}
