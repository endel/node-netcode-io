//! A quic-zig server on a thread of its own, and the two queues between it
//! and JS: commands in, events out, each a batch in `wire`'s format. The
//! lock guarding them is held for an append or a swap, never across a
//! callback.
//!
//! The loop thread owns every connection. JS names them by id, and a
//! command for one that's gone does nothing.
//!
//! The thread, not Node's: QUIC's acknowledgements and loss timers live in
//! userspace, so a room tick or a GC pause on the JS thread would stall
//! them, where TCP's run in the kernel regardless.

const std = @import("std");
const builtin = @import("builtin");
const quic = @import("quic");
const event_loop = quic.event_loop;
const xev = event_loop.Xev;
const ConnEntry = quic.connection_manager.ConnEntry;
const wire = @import("wire.zig");

const gpa = std.heap.c_allocator;
const log = std.log.scoped(.netcode_io);

/// Event bytes waiting for JS past which reads pause, so flow control holds
/// QUIC peers back, and datagrams are dropped. Reads resume once JS has
/// taken the batch.
const max_pending_events = 8 << 20;

/// Events in one tick past which JS is woken before the tick ends, so a
/// long one doesn't leave it idle.
const early_notify = 32;

pub const Server = event_loop.Server(Handler);

/// What crossed between the threads, and how often each woke the other:
/// for tuning. Never reset.
pub const Stats = struct {
    // Loop thread's.
    events: Counter = .{},
    /// JS woken.
    notifies: Counter = .{},
    /// Woken by JS.
    wakes: Counter = .{},
    commands: Counter = .{},
    // JS thread's.
    delivers: u64 = 0,
    event_bytes: u64 = 0,
    submits: u64 = 0,
    command_bytes: u64 = 0,

    const Counter = struct {
        v: std.atomic.Value(u64) = .init(0),
        fn add(self: *Counter, n: u64) void {
            _ = self.v.fetchAdd(n, .monotonic);
        }
        pub fn get(self: *const Counter) u64 {
            return self.v.load(.monotonic);
        }
    };
};

pub const Host = struct {
    /// Strings the config borrows.
    arena: std.heap.ArenaAllocator,
    handler: Handler,
    server: Server,
    thread: std.Thread,
    started: bool = false,
    wake: xev.Async,
    wake_c: xev.Completion = .{},

    /// Tells JS that events are waiting. Called from the loop thread, at
    /// most once until JS takes them.
    notify: *const fn (ctx: ?*anyopaque) void,
    notify_ctx: ?*anyopaque,

    lock: SpinLock = .{},
    // Guarded by `lock`.
    events: std.ArrayList(u8) = .empty,
    events_pending: bool = false,
    commands: std.ArrayList(u8) = .empty,
    /// Where each submitted batch in `commands` ends: a reference's offset
    /// counts from its own batch's start, and several may queue up.
    command_ends: std.ArrayList(usize) = .empty,
    commands_pending: bool = false,
    /// The loop may block, so a submit must wake it. False from a tick's
    /// first callback to its end, where the loop looks for commands itself:
    /// once a completion has run, libxev's tick doesn't block again.
    asleep: bool = true,
    /// Reads were paused for JS; its next take resumes them.
    paused_any: bool = false,
    resume_wanted: bool = false,
    quit: bool = false,

    /// The loop thread's: the command batches being run, and their ends.
    running: std.ArrayList(u8) = .empty,
    running_ends: std.ArrayList(usize) = .empty,
    /// The JS thread's: the event batch being handed over.
    taken: std.ArrayList(u8) = .empty,
    /// The loop thread's: events JS is yet to be woken for. The end of the
    /// tick wakes it once, however many sockets were ready in it.
    owed: u32 = 0,

    stats: Stats = .{},

    pub fn create() !*Host {
        const self = try gpa.create(Host);
        self.* = .{
            .arena = .init(gpa),
            .handler = .{ .host = self },
            .server = undefined,
            .thread = undefined,
            .wake = undefined,
            .notify = undefined,
            .notify_ctx = null,
        };
        return self;
    }

    /// Strings in `config` must come from `self.arena`.
    pub fn allocator(self: *Host) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Binds the sockets here, so a port in use fails the caller, then hands
    /// the server to its thread.
    pub fn listen(
        self: *Host,
        config: event_loop.Config,
        notify: *const fn (ctx: ?*anyopaque) void,
        notify_ctx: ?*anyopaque,
    ) !void {
        self.notify = notify;
        self.notify_ctx = notify_ctx;
        self.wake = try xev.Async.init();
        errdefer self.wake.deinit();
        self.server = try Server.init(gpa, &self.handler, config);
        errdefer self.server.deinit();
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        self.started = true;
    }

    /// The UDP port, which the TCP listener shares unless told otherwise.
    pub fn port(self: *const Host) u16 {
        return self.server.port();
    }

    /// Any thread. CPU the loop thread has used, in nanoseconds; null when
    /// the platform won't say.
    pub fn loopCpuNs(self: *const Host) ?u64 {
        if (!self.started) return null;
        return threadCpuNs(self.thread.getHandle());
    }

    fn run(self: *Host) void {
        nameThread("netcode-io");
        self.server.start();
        const loop = self.server.eventLoop();
        self.wake.wait(loop, &self.wake_c, Host, self, onWake);
        // `wake` always rearms, so only the server's stop ends the loop.
        while (!loop.stopped()) {
            loop.run(.once) catch |err| {
                log.err("event loop: {any}", .{err});
                break;
            };
            self.endTick();
        }
        self.handler.deinit();
        self.server.deinit();
        _ = self.emit(.stopped, .{}, &.{});
        self.wakeJs();
    }

    /// JS thread. Copies `batch` into the command queue.
    pub fn submit(self: *Host, batch: []const u8) !void {
        self.stats.submits += 1;
        self.stats.command_bytes += batch.len;
        self.lock.lock();
        const was = self.commands_pending;
        self.command_ends.ensureUnusedCapacity(gpa, 1) catch |err| {
            self.lock.unlock();
            return err;
        };
        self.commands.appendSlice(gpa, batch) catch |err| {
            self.lock.unlock();
            return err;
        };
        self.command_ends.appendAssumeCapacity(self.commands.items.len);
        self.commands_pending = true;
        const wake = !was and self.asleep;
        self.lock.unlock();
        if (wake) self.wake.notify() catch {};
    }

    /// JS thread. The events since the last take, valid until the next.
    pub fn take(self: *Host) []const u8 {
        self.taken.clearRetainingCapacity();
        self.lock.lock();
        std.mem.swap(std.ArrayList(u8), &self.taken, &self.events);
        self.events_pending = false;
        if (self.taken.items.len > 0) {
            self.stats.delivers += 1;
            self.stats.event_bytes += self.taken.items.len;
        }
        const wake = self.paused_any and !self.resume_wanted;
        if (wake) self.resume_wanted = true;
        self.lock.unlock();
        if (wake) self.wake.notify() catch {};
        return self.taken.items;
    }

    /// Any thread. Stops the server without a command: for a process
    /// exiting under it.
    pub fn requestStop(self: *Host) void {
        self.lock.lock();
        self.quit = true;
        self.lock.unlock();
        self.wake.notify() catch {};
    }

    /// JS thread: after the `stopped` event, after `requestStop`, or when
    /// `listen` failed or never ran.
    pub fn destroy(self: *Host) void {
        if (self.started) {
            self.thread.join();
            self.wake.deinit();
        }
        self.events.deinit(gpa);
        self.commands.deinit(gpa);
        self.command_ends.deinit(gpa);
        self.running.deinit(gpa);
        self.running_ends.deinit(gpa);
        self.taken.deinit(gpa);
        self.arena.deinit();
        gpa.destroy(self);
    }

    fn onWake(self_opt: ?*Host, _: *xev.Loop, _: *xev.Completion, r: xev.Async.WaitError!void) xev.CallbackAction {
        const self = self_opt.?;
        _ = r catch return .rearm;
        self.stats.wakes.add(1);
        self.runCommands();
        // Even stopping: the server ends the loop itself once it's done.
        return .rearm;
    }

    /// Loop thread, after each tick: wakes JS for the tick's events, and
    /// runs what JS submitted meanwhile without waking us.
    fn endTick(self: *Host) void {
        self.wakeJs();
        while (true) {
            self.lock.lock();
            if (!self.commands_pending) {
                self.asleep = true;
                self.lock.unlock();
                return;
            }
            self.lock.unlock();
            self.runCommands();
            self.wakeJs();
        }
    }

    fn runCommands(self: *Host) void {
        self.lock.lock();
        self.asleep = false;
        std.mem.swap(std.ArrayList(u8), &self.running, &self.commands);
        std.mem.swap(std.ArrayList(usize), &self.running_ends, &self.command_ends);
        self.commands_pending = false;
        const resume_reads = self.resume_wanted;
        if (resume_reads) {
            self.resume_wanted = false;
            self.paused_any = false;
        }
        const quit = self.quit;
        self.lock.unlock();

        if (resume_reads) self.handler.resumeReads();
        var start: usize = 0;
        for (self.running_ends.items) |end| {
            self.handler.runBatch(self.running.items[start..end]);
            start = end;
        }
        self.running.clearRetainingCapacity();
        self.running_ends.clearRetainingCapacity();
        if (quit) self.handler.stop();
        self.server.flush();
    }

    /// Loop thread. Returns the bytes now waiting for JS.
    fn emit(self: *Host, kind: wire.Event, header: wire.Header, parts: []const []const u8) usize {
        var h = header;
        h.kind = @backingInt(kind);
        self.stats.events.add(1);
        self.lock.lock();
        self.asleep = false;
        wire.append(gpa, &self.events, h, parts) catch |err| log.err("dropped an event: {any}", .{err});
        return self.signal();
    }

    fn emitRequest(self: *Host, kind: wire.Event, header: wire.Header, addr: wire.Address, path: []const u8, headers: anytype) void {
        var h = header;
        h.kind = @backingInt(kind);
        self.stats.events.add(1);
        self.lock.lock();
        self.asleep = false;
        wire.Request.append(gpa, &self.events, h, addr, path, headers) catch |err| log.err("dropped an event: {any}", .{err});
        _ = self.signal();
    }

    /// Ends `emit`'s critical section, waking JS unless it's already due.
    fn signal(self: *Host) usize {
        const was = self.events_pending;
        self.events_pending = true;
        const waiting = self.events.items.len;
        self.lock.unlock();
        // `was` with nothing owed: JS is woken already, and takes these too.
        if (!was or self.owed > 0) self.owed += 1;
        if (self.owed >= early_notify) self.wakeJs();
        return waiting;
    }

    /// Loop thread.
    fn wakeJs(self: *Host) void {
        if (self.owed == 0) return;
        self.owed = 0;
        self.stats.notifies.add(1);
        self.notify(self.notify_ctx);
    }

    fn pendingBytes(self: *Host) usize {
        self.lock.lock();
        defer self.lock.unlock();
        return self.events.items.len;
    }

    fn markPaused(self: *Host) void {
        self.lock.lock();
        self.paused_any = true;
        self.lock.unlock();
    }
};

const Handler = struct {
    pub const protocol: event_loop.Protocol = .webtransport;

    host: *Host,
    /// QUIC connections JS has heard of, by `Session.id()`.
    conns: std.AutoHashMapUnmanaged(u64, *ConnEntry) = .empty,
    sockets: std.AutoHashMapUnmanaged(u64, *event_loop.WsConn) = .empty,
    raws: std.AutoHashMapUnmanaged(u64, Raw) = .empty,
    paused: std.ArrayList(Paused) = .empty,
    stopping: bool = false,

    /// `stream` 0 is the connection itself, for a raw one.
    const Paused = struct { conn: u64, stream: u64 };

    const Raw = struct {
        conn: *event_loop.RawConn,
        /// JS asked for `raw_drain`, which `onRawDrain` owes.
        drain_wanted: bool = false,
        /// By `raw_pause`, which the queue's own resume leaves alone.
        js_paused: bool = false,
        /// By the queue's cap.
        cap_paused: bool = false,
    };

    fn deinit(self: *Handler) void {
        self.conns.deinit(gpa);
        self.sockets.deinit(gpa);
        self.raws.deinit(gpa);
        self.paused.deinit(gpa);
    }

    fn stop(self: *Handler) void {
        if (self.stopping) return;
        self.stopping = true;
        self.host.server.stop();
    }

    fn track(self: *Handler, session: *event_loop.Session) u64 {
        const id = session.id();
        self.conns.put(gpa, id, session.entry) catch {};
        return id;
    }

    fn sessionOf(self: *Handler, conn: u64) ?event_loop.Session {
        return .{ .entry = self.conns.get(conn) orelse return null };
    }

    fn pause(self: *Handler, s: *event_loop.Session, stream: u64) void {
        const conn = s.id();
        for (self.paused.items) |p| if (p.conn == conn and p.stream == stream) return;
        s.pauseStream(stream) catch return;
        self.paused.append(gpa, .{ .conn = conn, .stream = stream }) catch {
            s.resumeStream(stream);
            return;
        };
        self.host.markPaused();
    }

    fn pauseRaw(self: *Handler, raw: *Raw) void {
        if (raw.cap_paused) return;
        self.paused.append(gpa, .{ .conn = raw.conn.id(), .stream = 0 }) catch return;
        raw.cap_paused = true;
        raw.conn.pauseReading();
        self.host.markPaused();
    }

    fn resumeReads(self: *Handler) void {
        for (self.paused.items) |p| {
            if (p.stream == 0) {
                const raw = self.raws.getPtr(p.conn) orelse continue;
                raw.cap_paused = false;
                if (!raw.js_paused) raw.conn.resumeReading();
                continue;
            }
            var s = self.sessionOf(p.conn) orelse continue;
            s.resumeStream(p.stream);
        }
        self.paused.clearRetainingCapacity();
    }

    // ── WebSocket ────────────────────────────────────────────────────

    pub fn onWsUpgrade(self: *Handler, req: *event_loop.WsRequest, path: []const u8) void {
        const ws = req.accept(.{}) catch return;
        self.sockets.put(gpa, ws.id(), ws) catch return ws.close(1011, "");
        const addr = wire.Address.fromSockaddr(ws.peerAddress());
        self.host.emitRequest(.ws_open, .{ .conn = ws.id() }, addr, path, req.headers());
    }

    pub fn onWsMessage(self: *Handler, ws: *event_loop.WsConn, data: []const u8, kind: event_loop.WsMessageKind) void {
        const flags: u8 = if (kind == .text) wire.flag.text else 0;
        _ = self.host.emit(.ws_message, .{ .conn = ws.id(), .flags = flags }, &.{data});
    }

    pub fn onWsClose(self: *Handler, ws: *event_loop.WsConn, code: u16, reason: []const u8) void {
        if (!self.sockets.remove(ws.id())) return;
        _ = self.host.emit(.ws_close, .{ .conn = ws.id(), .code = code }, &.{reason});
    }

    // ── Raw HTTP/1.1 ─────────────────────────────────────────────────

    pub fn onRawOpen(self: *Handler, raw: *event_loop.RawConn) void {
        self.raws.put(gpa, raw.id(), .{ .conn = raw }) catch return raw.close();
        const addr = wire.Address.fromSockaddr(raw.peerAddress()).encode();
        _ = self.host.emit(.raw_open, .{ .conn = raw.id() }, &.{&addr});
    }

    pub fn onRawData(self: *Handler, raw: *event_loop.RawConn, data: []const u8) void {
        const pending = self.host.emit(.raw_data, .{ .conn = raw.id() }, &.{data});
        if (pending > max_pending_events) if (self.raws.getPtr(raw.id())) |r| self.pauseRaw(r);
    }

    pub fn onRawClose(self: *Handler, raw: *event_loop.RawConn) void {
        if (!self.raws.remove(raw.id())) return;
        _ = self.host.emit(.raw_close, .{ .conn = raw.id() }, &.{});
    }

    pub fn onRawEnd(self: *Handler, raw: *event_loop.RawConn) void {
        if (!self.raws.contains(raw.id())) return;
        _ = self.host.emit(.raw_end, .{ .conn = raw.id() }, &.{});
    }

    pub fn onRawDrain(self: *Handler, raw: *event_loop.RawConn) void {
        const r = self.raws.getPtr(raw.id()) orelse return;
        if (!r.drain_wanted) return;
        r.drain_wanted = false;
        _ = self.host.emit(.raw_drain, .{ .conn = raw.id() }, &.{});
    }

    // ── WebTransport ─────────────────────────────────────────────────

    pub fn onConnectRequest(self: *Handler, s: *event_loop.Session, session_id: u64, path: []const u8, headers: []const quic.qpack.Header) void {
        const conn = self.track(s);
        const addr = wire.Address.fromSockaddr(s.entry.conn.peerAddress());
        self.host.emitRequest(.session_request, .{ .conn = conn, .id = session_id }, addr, path, headers);
    }

    pub fn onSessionDraining(self: *Handler, s: *event_loop.Session, session_id: u64) void {
        _ = self.host.emit(.session_draining, .{ .conn = self.track(s), .id = session_id }, &.{});
    }

    pub fn onSessionClosed(self: *Handler, s: *event_loop.Session, session_id: u64, code: u32, reason: []const u8, abrupt: bool) void {
        var code_le: [4]u8 = undefined;
        std.mem.writeInt(u32, &code_le, code, .little);
        const flags: u8 = if (abrupt) wire.flag.abrupt else 0;
        _ = self.host.emit(.session_closed, .{ .conn = self.track(s), .id = session_id, .flags = flags }, &.{ &code_le, reason });
    }

    pub fn onBidiStream(self: *Handler, s: *event_loop.Session, session_id: u64, stream_id: u64) void {
        self.streamOpened(s, session_id, stream_id, wire.flag.bidi);
    }

    pub fn onUniStream(self: *Handler, s: *event_loop.Session, session_id: u64, stream_id: u64) void {
        self.streamOpened(s, session_id, stream_id, 0);
    }

    fn streamOpened(self: *Handler, s: *event_loop.Session, session_id: u64, stream_id: u64, flags: u8) void {
        var sid: [8]u8 = undefined;
        std.mem.writeInt(u64, &sid, session_id, .little);
        const pending = self.host.emit(.stream_open, .{ .conn = self.track(s), .id = stream_id, .flags = flags }, &.{&sid});
        // Its data and FIN could come in one event, too late to pause.
        if (pending > max_pending_events) self.pause(s, stream_id);
    }

    pub fn onStreamData(self: *Handler, s: *event_loop.Session, stream_id: u64, data: []const u8, fin: bool) void {
        const flags: u8 = if (fin) wire.flag.fin else 0;
        const pending = self.host.emit(.stream_data, .{ .conn = self.track(s), .id = stream_id, .flags = flags }, &.{data});
        // Copied, the data's credit goes back to the peer: past the cap, only
        // pausing holds it back.
        if (!fin and pending > max_pending_events) self.pause(s, stream_id);
    }

    pub fn onDatagram(self: *Handler, s: *event_loop.Session, session_id: u64, data: []const u8) void {
        if (self.host.pendingBytes() > max_pending_events) return;
        _ = self.host.emit(.datagram, .{ .conn = self.track(s), .id = session_id }, &.{data});
    }

    pub fn onConnectionClosed(self: *Handler, s: *event_loop.Session) void {
        if (!self.conns.remove(s.id())) return;
        _ = self.host.emit(.connection_closed, .{ .conn = s.id() }, &.{});
    }

    // ── Commands ─────────────────────────────────────────────────────

    fn runBatch(self: *Handler, batch: []const u8) void {
        var r: wire.Reader = .{ .batch = batch };
        var n: u64 = 0;
        while (r.next()) |rec| : (n += 1) self.runCommand(&r, rec);
        self.host.stats.commands.add(n);
    }

    fn runCommand(self: *Handler, r: *const wire.Reader, rec: wire.Record) void {
        const h = rec.header;
        switch (@as(wire.Command, @fromBackingInt(h.kind))) {
            .ws_send => {
                const ws = self.sockets.get(h.conn) orelse return;
                const data = r.bytes(rec) orelse return;
                const sent = if (h.flags & wire.flag.text != 0) ws.sendText(data) else ws.send(data);
                // A dropped message would leave the peer's state wrong for good.
                sent catch ws.close(1008, "send buffer full");
            },
            .ws_close => {
                const ws = self.sockets.get(h.conn) orelse return;
                ws.close(h.code, rec.payload);
            },
            .session_accept => {
                var s = self.sessionOf(h.conn) orelse return;
                s.acceptSession(h.id) catch |err| return log.debug("accept: {any}", .{err});
                // A server's session is open once accepted: quic-zig reports
                // `session_ready` only to clients.
                _ = self.host.emit(.session_ready, .{ .conn = h.conn, .id = h.id }, &.{});
            },
            .session_reject => {
                var s = self.sessionOf(h.conn) orelse return;
                s.rejectSession(h.id, h.code) catch |err| log.debug("reject: {any}", .{err});
            },
            .session_close => {
                var s = self.sessionOf(h.conn) orelse return;
                if (rec.payload.len < 4) return s.closeSession(h.id);
                const code = std.mem.readInt(u32, rec.payload[0..4], .little);
                s.closeSessionWithError(h.id, code, rec.payload[4..]) catch s.closeSession(h.id);
            },
            .stream_open => {
                if (rec.payload.len != 4) return;
                var s = self.sessionOf(h.conn) orelse return;
                const opened = if (h.flags & wire.flag.bidi != 0) s.openBidiStream(h.id, null) else s.openUniStream(h.id, null);
                const stream_id, const flags: u8 = if (opened) |id| .{ id, 0 } else |_| .{ 0, wire.flag.failed };
                _ = self.host.emit(.stream_opened, .{ .conn = h.conn, .id = stream_id, .flags = flags }, &.{rec.payload});
            },
            .stream_send => {
                var s = self.sessionOf(h.conn) orelse return;
                const data = r.bytes(rec) orelse return;
                if (data.len > 0) s.sendStreamData(h.id, data) catch {
                    // The stream lost bytes its reader counts on: end it loudly.
                    s.resetStream(h.id, 0);
                    return;
                };
                if (h.flags & wire.flag.fin != 0) s.closeStream(h.id);
            },
            .stream_reset => {
                if (rec.payload.len != 4) return;
                var s = self.sessionOf(h.conn) orelse return;
                s.resetStream(h.id, std.mem.readInt(u32, rec.payload[0..4], .little));
            },
            .datagram_send => {
                var s = self.sessionOf(h.conn) orelse return;
                const data = r.bytes(rec) orelse return;
                s.sendDatagram(h.id, data) catch {};
            },
            .connection_close => {
                var s = self.sessionOf(h.conn) orelse return;
                s.closeConnection();
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
                    _ = self.host.emit(.raw_drain, .{ .conn = h.conn }, &.{});
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
            .stop => self.stop(),
            _ => {},
        }
    }
};

fn threadCpuNs(t: std.Thread.Handle) ?u64 {
    switch (builtin.os.tag) {
        .macos => {
            const darwin = std.c;
            var info: darwin.thread_basic_info = undefined;
            var count: darwin.mach_msg_type_number_t = darwin.THREAD.BASIC.INFO_COUNT;
            if (darwin.thread_info(pthread_mach_thread_np(t), darwin.THREAD.BASIC.INFO, @ptrCast(&info), &count) != 0) return null;
            const us = (@as(u64, @intCast(info.user_time.seconds)) + @as(u64, @intCast(info.system_time.seconds))) * std.time.us_per_s +
                @as(u64, @intCast(info.user_time.microseconds)) + @as(u64, @intCast(info.system_time.microseconds));
            return us * std.time.ns_per_us;
        },
        .linux => {
            var clock: c_int = undefined;
            if (pthread_getcpuclockid(t, &clock) != 0) return null;
            var ts: std.os.linux.timespec = undefined;
            // the raw syscall: std's clockid_t can't hold a thread's clock
            if (std.os.linux.errno(std.os.linux.syscall2(.clock_gettime, @as(usize, @bitCast(@as(isize, clock))), @intFromPtr(&ts))) != .SUCCESS) return null;
            return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
        },
        else => return null,
    }
}

extern "c" fn pthread_mach_thread_np(std.Thread.Handle) std.c.mach_port_t;
extern "c" fn pthread_getcpuclockid(std.Thread.Handle, *c_int) c_int;
extern "c" fn pthread_setname_np(name: [*:0]const u8) c_int; // Darwin's: names the calling thread

/// So profilers and `top -H` tell it from Node's own threads.
fn nameThread(comptime name: [:0]const u8) void {
    switch (builtin.os.tag) {
        .macos => _ = pthread_setname_np(name.ptr),
        .linux => _ = std.os.linux.prctl(@intFromEnum(std.os.linux.PR.SET_NAME), @intFromPtr(name.ptr), 0, 0, 0),
        else => {},
    }
}

/// A lock held for an append or a swap at most: std keeps its mutex behind
/// `Io`.
const SpinLock = struct {
    held: std.atomic.Value(bool) = .init(false),

    fn lock(self: *SpinLock) void {
        var spins: u32 = 0;
        while (self.held.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            spins += 1;
            // Its holder may have been preempted.
            if (spins % 64 == 0) std.Thread.yield() catch {} else std.atomic.spinLoopHint();
        }
    }

    fn unlock(self: *SpinLock) void {
        self.held.store(false, .release);
    }
};
