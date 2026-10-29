//! The HTTP/1.1 listener on Node's own thread: WebSockets and the
//! connections JS serves itself, with no hop between threads. A libxev loop
//! of our own runs quic-zig's listener; libuv polls that loop's kqueue or
//! epoll fd, so Node wakes for it as for one of its own sockets, and a timer
//! stands in for the loop's own timers. Each wake runs the loop once without
//! blocking and hands JS what came of it in one call. The commands JS
//! answers with run as `submit` receives them, so a reply is written before
//! the call returns.
//!
//! QUIC stays on its own thread (`server.zig`): its acknowledgements and
//! timers can't wait for a room tick, where TCP's run in the kernel.

const std = @import("std");
const quic = @import("quic");
const http1 = quic.http1;
const xev = quic.event_loop.Xev;
const xev_backend = quic.xev_backend;
const napi = @import("napi.zig");
const uv = @import("uv.zig");
const wire = @import("wire.zig");
const server = @import("server.zig");
const Host = server.Host;
const Tcp = @import("tcp.zig").Tcp;

const gpa = std.heap.c_allocator;
const log = std.log.scoped(.netcode_io);

pub const Local = struct {
    env: napi.Env,
    /// The QUIC side, for the commands that belong to it.
    host: *Host,
    loop: xev.Loop,
    h1: http1.Server,
    tcp: Tcp(Local),
    /// The listener's ids, JS seeing them odd (`wire.ids`).
    next_id: u64 = 1,

    /// What the loop produced since JS last heard.
    events: std.ArrayList(u8) = .empty,
    /// `events` while `deliver` hands it out a request at a time.
    delivering: std.ArrayList(u8) = .empty,
    /// `websocket.transientMessages`: batches go through `lent`, valid for
    /// the call only.
    transient: bool = false,
    /// One Buffer, reused for every batch: no allocation per delivery, so
    /// no external memory for V8 to collect.
    lent: ?napi.Ref = null,
    lent_data: [*]u8 = undefined,
    lent_cap: usize = 0,
    /// During a `pump` with nothing waiting in `events`, `emit` writes into
    /// `lent` itself and `deliver` hands it over without a copy. Never
    /// past `lent_cap`: it must not reallocate memory JS owns.
    direct: std.ArrayList(u8) = .empty,
    direct_on: bool = false,
    /// The most a pump wanted `direct` to hold, for `lent`'s next size.
    direct_want: usize = 0,
    /// Inside `pump`'s handle scope, before `deliver`: payloads can go
    /// straight into Buffers of their own (`side`).
    pumping: bool = false,
    side: ?napi.Value = null,
    side_len: u32 = 0,
    side_bytes: usize = 0,
    /// A Buffer over quic-zig's read buffer, for `flag.read` messages.
    read_buf: ?napi.Ref = null,
    /// When `events`' second arrived, during `pump`.
    first_ns: i64 = 0,
    /// Events this pump queued since JS last took them.
    pump_events: u32 = 0,
    /// From the loop's run to the last delivery: `pump` arms the timer
    /// itself once that's done, so what JS submits meanwhile needn't.
    in_pump: bool = false,
    /// A batch's QUIC commands, bound for the loop thread.
    forward: std.ArrayList(u8) = .empty,
    /// Where bytes a reference points at went in `forward`, by their place
    /// in JS's batch: copied once, however many sends share them.
    moved: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    paused_any: bool = false,
    stopping: bool = false,

    poll: *uv.Handle,
    timer: *uv.Handle,
    /// uv handles not yet closed; the last close frees us.
    open_handles: u8 = 0,
    on_events: napi.Ref,
    async_ctx: napi.AsyncContext,

    /// Binds the TCP port now, so a port in use fails the caller.
    pub fn create(env: napi.Env, host: *Host, config: http1.Config, opts: http1.ListenOptions, on_events: napi.Value) !*Local {
        const self = try gpa.create(Local);
        errdefer gpa.destroy(self);
        self.* = .{
            .env = env,
            .host = host,
            .loop = try xev.Loop.init(.{}),
            .h1 = undefined,
            .tcp = .{ .sink = self },
            .poll = undefined,
            .timer = undefined,
            .on_events = undefined,
            .async_ctx = undefined,
        };
        errdefer self.loop.deinit();
        self.h1 = try http1.Server.init(gpa, config, opts);
        errdefer self.h1.deinit();
        try self.h1.start(&self.loop, http1.handlersFor(Tcp(Local), &self.tcp), &self.next_id);

        var uv_loop: *anyopaque = undefined;
        if (napi.napi_get_uv_event_loop(env, &uv_loop) != .ok) return error.Napi;
        if (napi.napi_create_reference(env, on_events, 1, &self.on_events) != .ok) return error.Napi;
        errdefer _ = napi.napi_delete_reference(env, self.on_events);
        var name: napi.Value = undefined;
        _ = napi.napi_create_string_utf8(env, "netcode-io", napi.auto_length, &name);
        if (napi.napi_async_init(env, null, name, &self.async_ctx) != .ok) return error.Napi;
        errdefer _ = napi.napi_async_destroy(env, self.async_ctx);

        // Last: once libuv has them, only uv_close gives them back.
        self.poll = try newHandle(uv.POLL);
        errdefer freeHandle(self.poll, uv.POLL);
        self.timer = try newHandle(uv.TIMER);
        errdefer freeHandle(self.timer, uv.TIMER);
        if (uv.uv_poll_init(@ptrCast(uv_loop), self.poll, xev_backend.pollFd(&self.loop)) != 0) return error.Uv;
        // Can't fail: it only fills the handle in.
        _ = uv.uv_timer_init(@ptrCast(uv_loop), self.timer);
        self.open_handles = 2;
        uv.uv_handle_set_data(self.poll, self);
        uv.uv_handle_set_data(self.timer, self);
        // The poll keeps Node alive, as a listening net.Server does; the
        // timer only stands in for the loop's.
        uv.uv_unref(self.timer);
        _ = uv.uv_poll_start(self.poll, uv.READABLE, onPoll);
        // The listener's watch goes to the kernel on the loop's first tick.
        self.loop.run(.no_wait) catch |err| log.err("event loop: {any}", .{err});
        self.armTimer();
        return self;
    }

    fn newHandle(kind: c_int) !*uv.Handle {
        const mem = try gpa.alignedAlloc(u8, .@"16", uv.uv_handle_size(kind));
        return @ptrCast(mem.ptr);
    }

    fn freeHandle(handle: *uv.Handle, kind: c_int) void {
        gpa.free(@as([*]align(16) u8, @ptrCast(@alignCast(handle)))[0..uv.uv_handle_size(kind)]);
    }

    pub fn ref(self: *Local, keep: bool) void {
        if (keep) uv.uv_ref(self.poll) else uv.uv_unref(self.poll);
    }

    // ── Events ───────────────────────────────────────────────────────

    fn onPoll(handle: *uv.Handle, _: c_int, _: c_int) callconv(.c) void {
        const self: *Local = @ptrCast(@alignCast(uv.uv_handle_get_data(handle).?));
        self.pump();
    }

    fn onTimer(handle: *uv.Handle) callconv(.c) void {
        const self: *Local = @ptrCast(@alignCast(uv.uv_handle_get_data(handle).?));
        self.pump();
    }

    fn pump(self: *Local) void {
        var scope: napi.HandleScope = undefined;
        const scoped = napi.napi_open_handle_scope(self.env, &scope) == .ok;
        defer if (scoped) {
            _ = napi.napi_close_handle_scope(self.env, scope);
        };
        self.pumping = scoped;
        self.first_ns = 0;
        self.pump_events = 0;
        self.in_pump = true;
        if (scoped) self.startDirect();
        self.loop.run(.no_wait) catch |err| log.err("event loop: {any}", .{err});
        self.pumping = false;
        // Without a scope no value can be made: the events wait for the next.
        if (scoped) self.deliver();
        self.direct_on = false;
        self.direct = .empty;
        self.in_pump = false;
        self.armTimer();
    }

    /// Hands JS what the loop produced, in one call, or one per HTTP
    /// request when JS takes them. Inside `pump`'s handle scope.
    fn deliver(self: *Local) void {
        if (self.direct.items.len > 0) return self.deliverDirect();
        if (self.events.items.len == 0) return;
        const env = self.env;

        self.host.stats.delivers += 1;
        self.host.stats.event_bytes += self.events.items.len;
        var func: ?napi.Value = null;
        _ = napi.napi_get_reference_value(env, self.on_events, &func);
        var global: napi.Value = undefined;
        _ = napi.napi_get_global(env, &global);

        var side: napi.Value = undefined;
        if (self.side) |arr| side = arr else _ = napi.napi_get_undefined(env, &side);
        const lent = if (self.transient) self.lend() else null;
        if (lent == null and !self.tcp.takes_requests) {
            self.call(func orelse return, global, batchBuffer(env, &self.events) orelse return, side, null);
        } else {
            // What JS's answers produce lands in `events`, for the next.
            std.mem.swap(std.ArrayList(u8), &self.events, &self.delivering);
            defer self.delivering.clearRetainingCapacity();
            var at: usize = 0;
            while (at < self.delivering.items.len) {
                const rest = self.delivering.items[at..];
                const n = if (self.tcp.takes_requests) cut(rest) else rest.len;
                if (lent) |b| {
                    self.call(func orelse return, global, b, side, .{ at, at + n });
                } else {
                    self.call(func orelse return, global, buffer(env, rest[0..n]) orelse return, side, null);
                }
                at += n;
            }
        }
        self.side = null;
        self.side_len = 0;
        self.side_bytes = 0;
        if (self.paused_any) {
            self.paused_any = false;
            self.tcp.resumeReads();
        }
    }

    /// `direct`'s events, read by JS where `emit` wrote them. `events` is
    /// empty: it takes only what is emitted while JS reads, and pumping
    /// resumes in it if that is anything (`out`).
    fn deliverDirect(self: *Local) void {
        const env = self.env;
        const n = self.direct.items.len;
        self.direct.items.len = 0;
        self.host.stats.delivers += 1;
        self.host.stats.event_bytes += n;
        var func: ?napi.Value = null;
        _ = napi.napi_get_reference_value(env, self.on_events, &func);
        var global: napi.Value = undefined;
        _ = napi.napi_get_global(env, &global);
        var side: napi.Value = undefined;
        _ = napi.napi_get_undefined(env, &side);
        var buf: ?napi.Value = null;
        if (napi.napi_get_reference_value(env, self.lent.?, &buf) != .ok) {
            // Delivered through `events` instead.
            self.events.appendSlice(gpa, self.lent_data[0..n]) catch |err| log.err("dropped events: {any}", .{err});
            return;
        }
        var at: usize = 0;
        while (at < n) {
            const len = if (self.tcp.takes_requests) cut(self.lent_data[at..n]) else n - at;
            self.call(func orelse return, global, buf.?, side, .{ at, at + len });
            at += len;
        }
        if (self.paused_any) {
            self.paused_any = false;
            self.tcp.resumeReads();
        }
    }

    /// Mid-`pump`, from inside a socket's callback: what JS answers goes out
    /// while the loop still has sockets to read, as uWebSockets.js answers
    /// each in turn. Not pumping meanwhile: events JS's commands produce
    /// wait in `events`, and none goes `side`, whose Buffers would die with
    /// the commands' handle scope.
    fn deliverEarly(self: *Local) void {
        self.pumping = false;
        defer self.pumping = true;
        self.deliver();
        self.pump_events = 0;
    }

    /// Microtasks run on the way out: the batch JS answers with arrives in
    /// `submit` before this returns. `range`: the part of `batch` this call
    /// is for, when it's `lent`'s.
    fn call(self: *Local, func: napi.Value, global: napi.Value, batch: napi.Value, side: napi.Value, range: ?[2]usize) void {
        var argv: [4]napi.Value = .{ batch, side, undefined, undefined };
        var argc: usize = 2;
        if (range) |r| {
            // below max_lent
            _ = napi.napi_create_uint32(self.env, @intCast(r[0]), &argv[2]);
            _ = napi.napi_create_uint32(self.env, @intCast(r[1]), &argv[3]);
            argc = 4;
        }
        if (napi.napi_make_callback(self.env, self.async_ctx, global, func, argc, &argv, null) == .pending_exception) {
            var err: napi.Value = undefined;
            if (napi.napi_get_and_clear_last_exception(self.env, &err) == .ok) _ = napi.napi_fatal_exception(self.env, err);
        }
    }

    /// Runs the loop again when something is due: events JS hasn't had,
    /// work queued on the loop, or its next timer.
    fn armTimer(self: *Local) void {
        const due_now = self.events.items.len > 0 or xev_backend.hasQueued(&self.loop);
        const ms = if (due_now) 0 else xev_backend.nextTimerMs(&self.loop) orelse {
            _ = uv.uv_timer_stop(self.timer);
            return;
        };
        _ = uv.uv_timer_start(self.timer, onTimer, ms, 0);
    }

    // `Tcp`'s sink. Delivered within the same wake, so nothing piles up
    // past what one tick reads.

    pub fn fromJs(id: u64) ?u64 {
        return wire.ids.fromLocal(id);
    }

    pub fn emit(self: *Local, kind: wire.Event, header: wire.Header, parts: []const []const u8) usize {
        var h = header;
        h.kind = @backingInt(kind);
        h.conn = wire.ids.local(h.conn);
        self.host.stats.events.add(1);
        defer if (self.pumping) self.deliverIfOld();
        if (self.pumping and self.transient and kind == .ws_message and parts.len == 1 and self.lendsRead(h, parts[0])) {
            // Taken before the loop's next read reuses the buffer.
            self.deliverEarly();
        } else if (self.pumping and !self.transient and (kind == .ws_message or kind == .raw_data) and self.toSide(parts)) {
            h.flags |= wire.flag.side;
            wire.append(gpa, &self.events, h, &.{}) catch |err| log.err("dropped an event: {any}", .{err});
        } else {
            var n: usize = wire.header_len;
            for (parts) |p| n += p.len;
            wire.append(gpa, self.out(n), h, parts) catch |err| log.err("dropped an event: {any}", .{err});
        }
        return self.events.items.len + self.direct.items.len + self.side_bytes;
    }

    /// A large message quic-zig parsed in place goes to JS as a view of its
    /// read buffer: copying it costs more than the JS call delivering it at
    /// once adds.
    fn lendsRead(self: *Local, header: wire.Header, data: []const u8) bool {
        if (data.len < read_lend_min) return false;
        const rb = http1.sharedReadBuffer() orelse return false;
        const at = @intFromPtr(data.ptr);
        const base = @intFromPtr(rb.ptr);
        if (at < base or at + data.len > base + rb.len) return false;
        var h = header;
        h.flags |= wire.flag.read;
        var payload: [8]u8 = undefined;
        std.mem.writeInt(u32, payload[0..4], @intCast(at - base), .little);
        std.mem.writeInt(u32, payload[4..8], @intCast(data.len), .little);
        wire.append(gpa, self.out(wire.header_len + payload.len), h, &.{&payload}) catch |err| {
            log.err("dropped an event: {any}", .{err});
            return true;
        };
        return true;
    }

    pub fn readBuffer(self: *Local) ?napi.Value {
        const rb = http1.sharedReadBuffer() orelse return null;
        var buf: ?napi.Value = null;
        if (self.read_buf) |r| {
            if (napi.napi_get_reference_value(self.env, r, &buf) == .ok and buf != null) return buf;
        }
        var made: napi.Value = undefined;
        if (napi.napi_create_external_buffer(self.env, rb.len, rb.ptr, null, null, &made) != .ok) return null;
        var held: napi.Ref = undefined;
        if (napi.napi_create_reference(self.env, made, 1, &held) == .ok) self.read_buf = held;
        return made;
    }

    /// Where an event of `n` bytes goes: `direct` while pumping, with room,
    /// and nothing in `events` ahead of it. Else what `direct` holds moves
    /// to `events`, which takes the rest of the pump.
    fn out(self: *Local, n: usize) *std.ArrayList(u8) {
        if (!self.direct_on or !self.pumping) return &self.events;
        if (self.events.items.len == 0) {
            if (self.direct.items.len + n <= self.direct.capacity) return &self.direct;
            self.direct_want = @max(self.direct_want, self.direct.items.len + n);
        }
        self.spill();
        return &self.events;
    }

    fn spill(self: *Local) void {
        self.direct_on = false;
        if (self.direct.items.len == 0) return;
        self.events.appendSlice(gpa, self.direct.items) catch |err| log.err("dropped events: {any}", .{err});
        self.direct.items.len = 0;
    }

    /// `pump` writes into `lent`, unless events are waiting to go first.
    fn startDirect(self: *Local) void {
        if (!self.transient or self.events.items.len > 0) return;
        if (!self.growLent(@max(min_lent, self.direct_want))) return;
        self.direct = .initBuffer(self.lent_data[0..self.lent_cap]);
        self.direct_on = true;
    }

    /// A pump's second event starts the clock; one queued
    /// `deliver_after_ns` later has JS take them all.
    fn deliverIfOld(self: *Local) void {
        // A lone event reads no clock: the pump hands it over anyway.
        self.pump_events += 1;
        if (self.pump_events < 2) return;
        const now = quic.sys.nanoTimestamp();
        if (self.pump_events == 2) {
            self.first_ns = now;
        } else if (now - self.first_ns >= deliver_after_ns) {
            self.deliverEarly();
        }
    }

    /// `events` copied into `lent`, grown to fit; null past `max_lent`,
    /// where a batch goes in a Buffer of its own rather than keep that much.
    fn lend(self: *Local) ?napi.Value {
        const len = self.events.items.len;
        if (!self.growLent(len)) return null;
        var buf: ?napi.Value = null;
        if (napi.napi_get_reference_value(self.env, self.lent.?, &buf) != .ok) return null;
        @memcpy(self.lent_data[0..len], self.events.items);
        return buf;
    }

    /// `lent` holds `want` bytes at least, or false: past `max_lent`, or
    /// out of memory. JS may still hold a smaller one it replaced.
    fn growLent(self: *Local, want: usize) bool {
        if (want <= self.lent_cap) return true;
        if (want > max_lent) return false;
        const cap = @max(min_lent, std.math.ceilPowerOfTwoAssert(usize, want));
        var data: ?*anyopaque = null;
        var buf: napi.Value = undefined;
        if (napi.napi_create_buffer(self.env, cap, &data, &buf) != .ok) return false;
        var held: napi.Ref = undefined;
        if (napi.napi_create_reference(self.env, buf, 1, &held) != .ok) return false;
        if (self.lent) |old| _ = napi.napi_delete_reference(self.env, old);
        self.lent = held;
        self.lent_data = @ptrCast(data.?);
        self.lent_cap = cap;
        return true;
    }

    /// A large payload copied once, into a Buffer of its own, instead of
    /// twice through the batch: a batch of large messages outgrew malloc's
    /// heap, so each came as fresh pages.
    fn toSide(self: *Local, parts: []const []const u8) bool {
        var total: usize = 0;
        for (parts) |p| total += p.len;
        if (total < side_min) return false;
        if (self.side == null) {
            var arr: napi.Value = undefined;
            if (napi.napi_create_array(self.env, &arr) != .ok) return false;
            self.side = arr;
        }
        var data: ?*anyopaque = null;
        var buf: napi.Value = undefined;
        if (napi.napi_create_buffer(self.env, total, &data, &buf) != .ok) return false;
        var at: usize = 0;
        for (parts) |p| {
            @memcpy(@as([*]u8, @ptrCast(data.?))[at..][0..p.len], p);
            at += p.len;
        }
        if (napi.napi_set_element(self.env, self.side.?, self.side_len, buf) != .ok) return false;
        self.side_len += 1;
        self.side_bytes += total;
        return true;
    }

    pub fn emitRequest(self: *Local, kind: wire.Event, header: wire.Header, addr: wire.Address, path: []const u8, headers: anytype) void {
        var h = header;
        h.kind = @backingInt(kind);
        h.conn = wire.ids.local(h.conn);
        self.host.stats.events.add(1);
        // after what `direct` holds
        if (self.direct_on) self.spill();
        wire.Request.append(gpa, &self.events, h, addr, path, headers) catch |err| log.err("dropped an event: {any}", .{err});
    }

    pub fn emitHttpRequest(self: *Local, header: wire.Header, method: []const u8, addr: wire.Address, target: []const u8, headers: anytype, body: []const u8) void {
        self.host.stats.events.add(1);
        if (self.direct_on) self.spill();
        var h = header;
        h.conn = wire.ids.local(h.conn);
        wire.Request.appendHttp(gpa, &self.events, h, method, addr, target, headers, body) catch |err| log.err("dropped an event: {any}", .{err});
    }

    pub fn markPaused(self: *Local) void {
        self.paused_any = true;
    }

    // ── Commands ─────────────────────────────────────────────────────

    /// Runs WebSocket and raw commands now; the rest go to the loop thread.
    pub fn submit(self: *Local, batch: []const u8) !void {
        self.host.stats.submits += 1;
        self.host.stats.command_bytes += batch.len;
        // Last, after the uncork: a write the kernel didn't take, or a
        // close, left work on the loop.
        defer if (!self.in_pump) self.armTimer();
        self.tcp.corkFor(batch);
        defer self.tcp.uncork();
        var r: wire.Reader = .{
            .batch = batch,
            .lent = if (self.lent_cap > 0) self.lent_data[0..self.lent_cap] else &.{},
            .read = http1.sharedReadBuffer() orelse &.{},
        };
        var n: u64 = 0;
        while (r.next()) |rec| {
            if (Tcp(Local).owns(rec.header.kind)) {
                self.tcp.runCommand(&r, rec);
                n += 1;
                continue;
            }
            if (rec.header.kind == @backingInt(wire.Command.stop)) self.stop();
            try self.forwardCommand(&r, rec);
        }
        self.host.stats.commands.add(n);
        if (self.forward.items.len > 0) {
            defer self.forward.clearRetainingCapacity();
            defer self.moved.clearRetainingCapacity();
            try self.host.enqueue(self.forward.items);
        }
    }

    /// One WebSocket or raw send, run now from the caller's `data`, not
    /// copied into a batch first. JS submits what it queued before.
    pub fn sendNow(self: *Local, kind: u8, flags: u8, conn: u64, data: []const u8) error{NotASend}!void {
        switch (@as(wire.Command, @fromBackingInt(kind))) {
            .ws_send, .raw_send => {},
            else => return error.NotASend,
        }
        if (flags & wire.flag.ref != 0) return error.NotASend;
        self.host.stats.commands.add(1);
        const rec: wire.Record = .{ .header = .{ .kind = kind, .flags = flags, .len = @intCast(data.len), .conn = conn }, .payload = data };
        const r: wire.Reader = .{ .batch = data };
        self.tcp.runCommand(&r, rec);
        // A write the kernel didn't take left work on the loop.
        if (!self.in_pump) self.armTimer();
    }

    fn forwardCommand(self: *Local, r: *const wire.Reader, rec: wire.Record) !void {
        // Lent bytes are reused before the QUIC thread would read them.
        if (rec.header.flags & (wire.flag.lent | wire.flag.lent_read) != 0) return;
        const bytes = r.bytes(rec) orelse return;
        const key = (@as(u64, @intCast(@intFromPtr(bytes.ptr) - @intFromPtr(r.batch.ptr))) << 32) | bytes.len;
        if (rec.header.flags & wire.flag.ref != 0) {
            if (self.moved.get(key)) |at| {
                var at_len: [8]u8 = undefined;
                std.mem.writeInt(u32, at_len[0..4], at, .little);
                std.mem.writeInt(u32, at_len[4..8], @intCast(bytes.len), .little);
                return wire.append(gpa, &self.forward, rec.header, &.{&at_len});
            }
        }
        // The first send of some bytes carries them, and later ones refer to it.
        var h = rec.header;
        h.flags &= ~wire.flag.ref;
        const at: u32 = @intCast(self.forward.items.len + wire.header_len);
        try wire.append(gpa, &self.forward, h, &.{bytes});
        if (bytes.len > 0) try self.moved.put(gpa, key, at);
    }

    fn stop(self: *Local) void {
        if (self.stopping) return;
        self.stopping = true;
        self.h1.stop();
    }

    /// JS thread, once the loop thread has stopped. Returns the events left
    /// for JS, closes mostly; they stay valid until the uv handles have
    /// closed, after the caller returns to Node's loop.
    pub fn destroy(self: *Local) []const u8 {
        self.stop();
        self.loop.run(.no_wait) catch {};
        // Closes whatever is still open, with onWsClose for each WebSocket.
        self.h1.deinit();
        self.tcp.deinit();
        self.loop.deinit();
        self.forward.deinit(gpa);
        self.moved.deinit(gpa);
        if (self.lent) |r| _ = napi.napi_delete_reference(self.env, r);
        if (self.read_buf) |r| _ = napi.napi_delete_reference(self.env, r);
        _ = napi.napi_delete_reference(self.env, self.on_events);
        _ = napi.napi_async_destroy(self.env, self.async_ctx);
        uv.uv_close(self.poll, onClosed);
        uv.uv_close(self.timer, onClosed);
        return self.events.items;
    }

    fn onClosed(handle: *uv.Handle) callconv(.c) void {
        const self: *Local = @ptrCast(@alignCast(uv.uv_handle_get_data(handle).?));
        freeHandle(handle, if (handle == self.poll) uv.POLL else uv.TIMER);
        self.open_handles -= 1;
        if (self.open_handles > 0) return;
        self.events.deinit(gpa);
        self.delivering.deinit(gpa);
        gpa.destroy(self);
    }
};

/// `list`'s bytes copied into a Buffer JS owns, leaving `list` empty with
/// its capacity. (Handing JS the list's own memory instead, as an external
/// ArrayBuffer, cost more than the copy: fresh pages, a finalizer each, and
/// V8 collecting sooner for the external memory.)
/// Where a delivery starting at `events` ends: past its first HTTP request.
/// Each request gets a call of its own, so the microtasks one starts run
/// before the next is handled, as between Node's 'request' events: two
/// matchmaking requests in one call would both see the same free seats.
/// Payloads from here on go into `side` Buffers: below, the copy into the
/// batch is cheaper than a Buffer of their own.
const side_min = 32 * 1024;

/// How long events wait during a pump while more sockets are read: the
/// first replies go out without waiting for the last reads.
const deliver_after_ns = 20 * std.time.ns_per_us;

const min_lent = 64 * 1024;

/// From here on a message quic-zig parsed in place is lent, not copied:
/// below, the copy costs less than the JS call delivering it at once.
const read_lend_min = 32 * 1024;

/// The most a `lent` Buffer grows to.
const max_lent = 4 * 1024 * 1024;

pub fn cut(events: []const u8) usize {
    var at: usize = 0;
    while (at + wire.header_len <= events.len) {
        const end = at + wire.header_len + std.mem.readInt(u32, events[at + 4 ..][0..4], .little);
        if (events[at] == @backingInt(wire.Event.http_request)) return @min(end, events.len);
        at = end;
    }
    return events.len;
}

pub fn batchBuffer(env: napi.Env, list: *std.ArrayList(u8)) ?napi.Value {
    defer list.clearRetainingCapacity();
    return buffer(env, list.items);
}

/// A Buffer holding a copy of `bytes`. Not an ArrayBuffer: that comes
/// zero-filled, which for a large batch cost as much as the copy.
pub fn buffer(env: napi.Env, bytes: []const u8) ?napi.Value {
    var data: ?*anyopaque = null;
    var buf: napi.Value = undefined;
    if (napi.napi_create_buffer(env, bytes.len, &data, &buf) != .ok) return null;
    if (bytes.len > 0) @memcpy(@as([*]u8, @ptrCast(data.?))[0..bytes.len], bytes);
    return buf;
}
