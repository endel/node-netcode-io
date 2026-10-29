//! The byte format both queues use: events from the loop to JS, commands
//! from JS to the loop. Each record is a 24-byte little-endian header, then
//! `len` bytes of payload:
//!
//!     u8 kind | u8 flags | u16 code | u32 len | u64 conn | u64 id
//!
//! `conn` is a WebSocket's, a raw connection's or a QUIC connection's id
//! (one counter draws them all); `id` is a session or stream id, by kind. Ids stay below 2^53, so JS
//! reads them as Numbers.
//!
//! `lib/index.js` mirrors this file; change both together.

const std = @import("std");

pub const header_len = 24;

pub const Event = enum(u8) {
    /// conn: WebSocket. Payload: a `Request`.
    ws_open = 1,
    /// conn: WebSocket. `flag.text`. Payload: the message.
    ws_message = 2,
    /// conn: WebSocket, its last event. code: close code. Payload: reason.
    ws_close = 3,
    /// id: session. Payload: a `Request`. Answer with `session_accept` or
    /// `session_reject`.
    session_request = 4,
    /// id: session.
    session_ready = 5,
    /// id: session. `flag.abrupt`. Payload: u32 error code, then the reason.
    session_closed = 6,
    /// id: stream the peer opened. `flag.bidi`. Payload: u64 session id.
    stream_open = 7,
    /// id: stream. `flag.fin`. Payload: the bytes.
    stream_data = 8,
    /// id: session. Payload: the datagram.
    datagram = 9,
    /// The QUIC connection is gone, its sessions reported first: its last event.
    connection_closed = 10,
    /// The answer to `stream_open`. id: the stream, unless `flag.failed`.
    /// Payload: u32 token from the command.
    stream_opened = 11,
    /// id: session.
    session_draining = 12,
    /// conn: an HTTP/1.1 connection JS serves itself, from its first
    /// request on. Payload: an `Address`.
    raw_open = 13,
    /// conn: raw. Payload: the bytes, TLS already removed.
    raw_data = 14,
    /// conn: raw, its last event.
    raw_close = 15,
    /// conn: raw. The answer to a `raw_send` with `flag.drain`: what JS
    /// wrote before is down to the kernel's buffers.
    raw_drain = 16,
    /// conn: raw. The peer sent its last byte; the connection stays open
    /// for writes until `raw_close`.
    raw_end = 17,
    /// conn: an HTTP/1.1 request JS answers itself, head and body whole.
    /// Payload: u16 method length | method | `Request` | the body. Answer
    /// with `http_respond` or `http_pass`.
    http_request = 18,
    /// The server stopped: the last event.
    stopped = 20,
    _,
};

pub const Command = enum(u8) {
    /// conn: WebSocket. `flag.text`, `flag.ref`. Payload: the message.
    ws_send = 1,
    /// conn: WebSocket. code: close code. Payload: reason.
    ws_close = 2,
    /// id: session.
    session_accept = 4,
    /// id: session. code: HTTP status, 300 to 599.
    session_reject = 5,
    /// id: session. Payload: u32 error code, then the reason; empty closes
    /// without an error.
    session_close = 6,
    /// id: session. `flag.bidi`. Payload: u32 token, echoed by `stream_opened`.
    stream_open = 7,
    /// id: stream. `flag.fin`, `flag.ref`. Payload: the bytes.
    stream_send = 8,
    /// id: stream. Payload: u32 error code.
    stream_reset = 9,
    /// id: session. `flag.ref`. Payload: the datagram.
    datagram_send = 10,
    /// Close the QUIC connection now, every session on it.
    connection_close = 11,
    /// conn: raw. `flag.drain`, `flag.ref`. Payload: the bytes.
    raw_send = 12,
    /// conn: raw. `flag.abort`. What was sent goes out first.
    raw_close = 13,
    /// conn: raw. No `raw_data` until `raw_resume`.
    raw_pause = 14,
    raw_resume = 15,
    /// conn: an `http_request`'s. code: status. Payload: u32 header bytes |
    /// header lines, each ending in CRLF | the body.
    http_respond = 16,
    /// conn: an `http_request`'s. Served as if JS took no requests: by the
    /// raw handler, so JS's `http.Server`.
    http_pass = 17,
    stop = 20,
    _,
};

pub const flag = struct {
    /// ws_message, ws_send: a text message.
    pub const text: u8 = 1;
    /// stream_data, stream_send: the stream's last bytes.
    pub const fin: u8 = 1;
    /// session_closed: the CONNECT stream was reset, or the connection ended.
    pub const abrupt: u8 = 1;
    /// stream_open: bidirectional.
    pub const bidi: u8 = 1;
    /// raw_send: answer with `raw_drain` once the output is low again.
    pub const drain: u8 = 1;
    /// raw_close: drop what is queued, as `destroy()` does.
    pub const abort: u8 = 1;
    /// Sends: the payload is u32 offset, u32 length of bytes earlier in the
    /// same batch, so a buffer sent to many peers crosses once.
    pub const ref: u8 = 2;
    /// stream_opened: no stream; the token's id is meaningless.
    pub const failed: u8 = 4;
    /// ws_message, raw_data: the payload is a Buffer of its own, the next
    /// in the delivery's second argument.
    pub const side: u8 = 8;
    /// ws_message: the payload is u32 offset, u32 length of bytes in
    /// quic-zig's read buffer (`readBuffer`), valid during this call only.
    pub const read: u8 = 16;
};

pub const Header = struct {
    kind: u8 = 0,
    flags: u8 = 0,
    code: u16 = 0,
    len: u32 = 0,
    conn: u64 = 0,
    id: u64 = 0,

    pub fn write(self: Header, out: *[header_len]u8) void {
        out[0] = self.kind;
        out[1] = self.flags;
        std.mem.writeInt(u16, out[2..4], self.code, .little);
        std.mem.writeInt(u32, out[4..8], self.len, .little);
        std.mem.writeInt(u64, out[8..16], self.conn, .little);
        std.mem.writeInt(u64, out[16..24], self.id, .little);
    }

    pub fn read(in: *const [header_len]u8) Header {
        return .{
            .kind = in[0],
            .flags = in[1],
            .code = std.mem.readInt(u16, in[2..4], .little),
            .len = std.mem.readInt(u32, in[4..8], .little),
            .conn = std.mem.readInt(u64, in[8..16], .little),
            .id = std.mem.readInt(u64, in[16..24], .little),
        };
    }
};

/// Appends a record whose payload is `parts`, concatenated.
pub fn append(gpa: std.mem.Allocator, out: *std.ArrayList(u8), header: Header, parts: []const []const u8) !void {
    var len: usize = 0;
    for (parts) |p| len += p.len;
    var h = header;
    h.len = std.math.cast(u32, len) orelse return error.TooLarge;
    try out.ensureUnusedCapacity(gpa, header_len + len);
    h.write(out.addManyAsArrayAssumeCapacity(header_len));
    for (parts) |p| out.appendSliceAssumeCapacity(p);
}

pub const Record = struct {
    header: Header,
    payload: []const u8,
};

/// Walks a batch. A record that runs past the end stops the walk: the
/// batch came from our own JS, so that's a bug there, not input to recover.
pub const Reader = struct {
    batch: []const u8,
    pos: usize = 0,

    pub fn next(self: *Reader) ?Record {
        const rest = self.batch[self.pos..];
        if (rest.len < header_len) return null;
        const h = Header.read(rest[0..header_len]);
        if (rest.len - header_len < h.len) return null;
        self.pos += header_len + h.len;
        return .{ .header = h, .payload = rest[header_len..][0..h.len] };
    }

    /// A send's bytes: its payload, or what `flag.ref` points at.
    pub fn bytes(self: *const Reader, r: Record) ?[]const u8 {
        if (r.header.flags & flag.ref == 0) return r.payload;
        if (r.payload.len != 8) return null;
        const off = std.mem.readInt(u32, r.payload[0..4], .little);
        const len = std.mem.readInt(u32, r.payload[4..8], .little);
        if (@as(u64, off) + len > self.batch.len) return null;
        return self.batch[off..][0..len];
    }
};

/// A peer's address, path and headers: the payload of `ws_open` and
/// `session_request`.
///
///     Address
///     u16 path length | path
///     u16 header count | (u16 name length | name | u16 value length | value)*
///
/// A header too long for its u16 is left out.
pub const Request = struct {
    pub fn append(
        gpa: std.mem.Allocator,
        out: *std.ArrayList(u8),
        header: Header,
        addr: Address,
        path: []const u8,
        names: anytype,
    ) !void {
        const start = out.items.len;
        errdefer out.shrinkRetainingCapacity(start);
        try out.appendNTimes(gpa, 0, header_len);
        try appendFields(gpa, out, addr, path, names);
        try finish(out, start, header);
    }

    /// An `http_request`: the method, a `Request`'s fields, then the body.
    pub fn appendHttp(
        gpa: std.mem.Allocator,
        out: *std.ArrayList(u8),
        header: Header,
        method: []const u8,
        addr: Address,
        target: []const u8,
        names: anytype,
        body: []const u8,
    ) !void {
        const start = out.items.len;
        errdefer out.shrinkRetainingCapacity(start);
        try out.appendNTimes(gpa, 0, header_len);
        try appendString(gpa, out, method[0..@min(method.len, std.math.maxInt(u16))]);
        try appendFields(gpa, out, addr, target, names);
        try out.appendSlice(gpa, body);
        var h = header;
        h.kind = @backingInt(Event.http_request);
        try finish(out, start, h);
    }

    fn appendFields(gpa: std.mem.Allocator, out: *std.ArrayList(u8), addr: Address, path: []const u8, names: anytype) !void {
        try out.appendSlice(gpa, &addr.encode());
        try appendString(gpa, out, path[0..@min(path.len, std.math.maxInt(u16))]);

        const count_at = out.items.len;
        try out.appendNTimes(gpa, 0, 2);
        var count: u16 = 0;
        for (names) |h| {
            if (h.name.len > std.math.maxInt(u16) or h.value.len > std.math.maxInt(u16)) continue;
            if (count == std.math.maxInt(u16)) break;
            try appendString(gpa, out, h.name);
            try appendString(gpa, out, h.value);
            count += 1;
        }
        std.mem.writeInt(u16, out.items[count_at..][0..2], count, .little);
    }

    /// Writes the record header at `start`, its length counted to the end.
    fn finish(out: *std.ArrayList(u8), start: usize, header: Header) !void {
        var h = header;
        h.len = std.math.cast(u32, out.items.len - start - header_len) orelse return error.TooLarge;
        h.write(out.items[start..][0..header_len]);
    }

    fn appendString(gpa: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
        var len: [2]u8 = undefined;
        std.mem.writeInt(u16, &len, @intCast(s.len), .little);
        try out.appendSlice(gpa, &len);
        try out.appendSlice(gpa, s);
    }
};

/// u8 family (4, 6, or 0 unknown) | u16 port | [16]u8 address
pub const Address = struct {
    family: u8 = 0,
    port: u16 = 0,
    bytes: [16]u8 = @splat(0),

    pub fn encode(self: Address) [19]u8 {
        var out: [19]u8 = undefined;
        out[0] = self.family;
        std.mem.writeInt(u16, out[1..3], self.port, .little);
        out[3..19].* = self.bytes;
        return out;
    }

    pub fn fromSockaddr(sa: *const std.posix.sockaddr.storage) Address {
        const posix = std.posix;
        if (sa.family == posix.AF.INET) {
            const in: *const posix.sockaddr.in = @ptrCast(@alignCast(sa));
            var a: Address = .{ .family = 4, .port = std.mem.bigToNative(u16, in.port) };
            a.bytes[0..4].* = @bitCast(in.addr);
            return a;
        }
        if (sa.family == posix.AF.INET6) {
            const in6: *const posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
            return .{ .family = 6, .port = std.mem.bigToNative(u16, in6.port), .bytes = in6.addr };
        }
        return .{};
    }
};

const testing = std.testing;

test "records round-trip through a batch" {
    const gpa = testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);

    try append(gpa, &out, .{ .kind = 2, .flags = flag.text, .conn = 7 }, &.{ "he", "llo" });
    try append(gpa, &out, .{ .kind = 3, .code = 1000, .conn = (1 << 53) - 1, .id = 9 }, &.{});

    var r: Reader = .{ .batch = out.items };
    const a = r.next().?;
    try testing.expectEqual(@as(u8, 2), a.header.kind);
    try testing.expectEqual(flag.text, a.header.flags);
    try testing.expectEqual(@as(u64, 7), a.header.conn);
    try testing.expectEqualStrings("hello", a.payload);
    const b = r.next().?;
    try testing.expectEqual(@as(u16, 1000), b.header.code);
    try testing.expectEqual(@as(u64, (1 << 53) - 1), b.header.conn);
    try testing.expectEqual(@as(u64, 9), b.header.id);
    try testing.expectEqual(@as(usize, 0), b.payload.len);
    try testing.expect(r.next() == null);
}

test "a truncated record ends the walk" {
    const gpa = testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    try append(gpa, &out, .{ .kind = 1 }, &.{"abc"});

    var r: Reader = .{ .batch = out.items[0 .. out.items.len - 1] };
    try testing.expect(r.next() == null);
}

test "a ref points at earlier bytes of the batch, and nowhere else" {
    const gpa = testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    try append(gpa, &out, .{ .kind = 1 }, &.{"shared"});

    var ref: [8]u8 = undefined;
    std.mem.writeInt(u32, ref[0..4], header_len, .little);
    std.mem.writeInt(u32, ref[4..8], 6, .little);
    try append(gpa, &out, .{ .kind = 1, .flags = flag.ref }, &.{&ref});
    std.mem.writeInt(u32, ref[0..4], 1 << 20, .little);
    try append(gpa, &out, .{ .kind = 1, .flags = flag.ref }, &.{&ref});

    var r: Reader = .{ .batch = out.items };
    _ = r.next().?;
    try testing.expectEqualStrings("shared", r.bytes(r.next().?).?);
    try testing.expect(r.bytes(r.next().?) == null);
}

test "a request carries address, path and headers" {
    const gpa = testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    const H = struct { name: []const u8, value: []const u8 };
    const headers = [_]H{ .{ .name = "host", .value = "localhost" }, .{ .name = "x", .value = "" } };
    var addr: Address = .{ .family = 4, .port = 443 };
    addr.bytes[0..4].* = .{ 127, 0, 0, 1 };
    try Request.append(gpa, &out, .{ .kind = 1, .conn = 3 }, addr, "/room?x=1", &headers);

    var r: Reader = .{ .batch = out.items };
    const rec = r.next().?;
    try testing.expectEqual(@as(u64, 3), rec.header.conn);
    const p = rec.payload;
    try testing.expectEqual(@as(u8, 4), p[0]);
    try testing.expectEqual(@as(u16, 443), std.mem.readInt(u16, p[1..3], .little));
    try testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, p[3..7]);
    try testing.expectEqual(@as(u16, 9), std.mem.readInt(u16, p[19..21], .little));
    try testing.expectEqualStrings("/room?x=1", p[21..30]);
    try testing.expectEqual(@as(u16, 2), std.mem.readInt(u16, p[30..32], .little));
    try testing.expectEqual(p.len, 32 + (2 + 4 + 2 + 9) + (2 + 1 + 2 + 0));
}
