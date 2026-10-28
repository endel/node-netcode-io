//! A WebTransport peer for `webtransport.test.js`, shaped like a Colyseus
//! client: it opens a stream and sends a datagram for the server to echo,
//! and once both are back, answers the stream the server opened. The server
//! closes the session on the answer. Exits 0 on that close, 1 if the
//! connection ended first, 2 on a timeout.
//!
//! The server ends it, not us: an immediate close drops what we queued
//! but haven't sent, the answer included.
//!
//!   wt-test-client --port N --path /room?x=1 --ca test/certs/ca.crt

const std = @import("std");
const quic = @import("quic");
const event_loop = quic.event_loop;

const Peer = struct {
    pub const protocol: event_loop.Protocol = .webtransport;

    server_stream: ?u64 = null,
    server_bytes: std.ArrayList(u8) = .empty,
    echo_bytes: std.ArrayList(u8) = .empty,
    answered: bool = false,
    echoed: bool = false,
    datagram: bool = false,
    done: bool = false,

    pub fn onSessionReady(_: *Peer, session: *event_loop.ClientSession, session_id: u64) void {
        const id = session.openBidiStream(session_id, null) catch return fail("openBidiStream");
        session.sendStreamData(id, "from-client") catch return fail("sendStreamData");
        session.closeStream(id);
        session.sendDatagram(session_id, "ping") catch return fail("sendDatagram");
    }

    pub fn onBidiStream(self: *Peer, _: *event_loop.ClientSession, _: u64, stream_id: u64) void {
        self.server_stream = stream_id;
    }

    pub fn onStreamData(self: *Peer, session: *event_loop.ClientSession, stream_id: u64, data: []const u8, fin: bool) void {
        if (self.server_stream == stream_id) {
            self.server_bytes.appendSlice(std.heap.smp_allocator, data) catch return fail("oom");
        } else {
            self.echo_bytes.appendSlice(std.heap.smp_allocator, data) catch return fail("oom");
            if (fin) self.echoed = std.mem.eql(u8, self.echo_bytes.items, "from-client");
        }
        self.answerOnceEchoed(session);
    }

    pub fn onDatagram(self: *Peer, session: *event_loop.ClientSession, _: u64, data: []const u8) void {
        if (std.mem.eql(u8, data, "ping")) self.datagram = true;
        self.answerOnceEchoed(session);
    }

    /// Last, so the server's close can't overtake an echo still on its way.
    fn answerOnceEchoed(self: *Peer, session: *event_loop.ClientSession) void {
        if (self.answered or !self.echoed or !self.datagram) return;
        const stream_id = self.server_stream orelse return;
        if (!std.mem.eql(u8, self.server_bytes.items, "hello")) return;
        self.answered = true;
        session.sendStreamData(stream_id, "reply") catch return fail("reply");
        session.closeStream(stream_id);
    }

    pub fn onSessionClosed(self: *Peer, session: *event_loop.ClientSession, _: u64, _: u32, _: []const u8) void {
        self.done = self.answered;
        session.closeConnection();
    }

    pub fn onSessionRejected(_: *Peer, session: *event_loop.ClientSession, _: u64, status: []const u8) void {
        std.debug.print("session rejected: {s}\n", .{status});
        session.closeConnection();
    }
};

fn fail(what: []const u8) void {
    std.debug.print("{s} failed\n", .{what});
    std.process.exit(1);
}

fn watchdog() void {
    quic.sys.sleepNs(5 * std.time.ns_per_s);
    std.debug.print("timed out\n", .{});
    std.process.exit(2);
}

pub fn main(init: std.process.Init.Minimal) !void {
    var port: u16 = 4433;
    var path: []const u8 = "/";
    var ca: []const u8 = "test/certs/ca.crt";

    var args = std.process.Args.Iterator.init(init.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            port = try std.fmt.parseInt(u16, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--path")) {
            path = args.next() orelse return error.MissingValue;
        } else if (std.mem.eql(u8, arg, "--ca")) {
            ca = args.next() orelse return error.MissingValue;
        }
    }

    _ = try std.Thread.spawn(.{}, watchdog, .{});

    var peer: Peer = .{};
    var client = try event_loop.Client(Peer).init(std.heap.smp_allocator, &peer, .{
        .port = port,
        .path = path,
        .ca = .{ .file = ca },
    });
    defer client.deinit();
    try client.run();

    if (!peer.done) {
        std.debug.print("connection ended early: echoed={} datagram={} hello={} answered={}\n", .{ peer.echoed, peer.datagram, std.mem.eql(u8, peer.server_bytes.items, "hello"), peer.answered });
        std.process.exit(1);
    }
}
