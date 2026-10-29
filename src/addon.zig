//! The Node-API module: what `lib/index.js` calls, all on the JS thread:
//!
//!   listen(options, onEvents) -> handle   binds, starts the loop thread
//!   submit(handle, Uint8Array, len?)      runs or queues a command batch: its first `len` bytes
//!   send(handle, kind, flags, conn, data) runs one WebSocket or raw send now, from `data`
//!                                         (`Mode.node`)
//!   port(handle) -> number                the bound UDP port
//!   ref(handle, bool)                     whether the server keeps Node alive
//!   stats(handle) -> object               counters, for tuning
//!   destroy(handle) -> Buffer?            frees it, after the `stopped` event;
//!                                         returns the events left, if any
//!
//! `onEvents(Buffer)` gets every event batch: the loop thread's
//! through a threadsafe function it calls once per batch, and in
//! `Mode.node` the HTTP/1.1 listener's straight from Node's loop
//! (`local.zig`).

const std = @import("std");
const quic = @import("quic");
const napi = @import("napi.zig");
const server = @import("server.zig");
const Host = server.Host;
const Local = @import("local.zig").Local;
const buffer = @import("local.zig").buffer;
const batchBuffer = @import("local.zig").batchBuffer;
const cut = @import("local.zig").cut;

pub const std_options: std.Options = .{ .log_level = .warn };

const gpa = std.heap.c_allocator;

/// What a JS handle points at. Outlives the host: the threadsafe function's
/// finalizer frees it, as a call may still be queued when the host goes.
const Binding = struct {
    host: ?*Host,
    /// The HTTP/1.1 listener, when it runs on this thread (`Mode.node`).
    local: ?*Local = null,
    tsfn: napi.ThreadsafeFunction = undefined,
    /// JS has `onHttpRequest`.
    takes_requests: bool = false,
    /// A batch handed out a request per call (`local.cut`), from `rest_at`.
    rest: std.ArrayList(u8) = .empty,
    rest_at: usize = 0,
    /// The environment is going away: no more calls into it.
    exiting: std.atomic.Value(bool) = .init(false),
};

export fn napi_register_module_v1(env: napi.Env, exports: napi.Value) callconv(.c) ?napi.Value {
    define(env, exports, "listen", jsListen) catch return null;
    define(env, exports, "submit", jsSubmit) catch return null;
    define(env, exports, "send", jsSend) catch return null;
    define(env, exports, "port", jsPort) catch return null;
    define(env, exports, "ref", jsRef) catch return null;
    define(env, exports, "stats", jsStats) catch return null;
    define(env, exports, "destroy", jsDestroy) catch return null;
    return exports;
}

export fn node_api_module_get_api_version_v1() callconv(.c) i32 {
    return 8;
}

fn define(env: napi.Env, exports: napi.Value, comptime name: [:0]const u8, cb: napi.Callback) !void {
    var f: napi.Value = undefined;
    try check(napi.napi_create_function(env, name.ptr, name.len, cb, null, &f));
    try check(napi.napi_set_named_property(env, exports, name.ptr, f));
}

fn check(status: napi.Status) !void {
    if (status != .ok) return error.Napi;
}

fn throw(env: napi.Env, msg: [:0]const u8) ?napi.Value {
    _ = napi.napi_throw_error(env, null, msg.ptr);
    return null;
}

fn args(env: napi.Env, info: napi.CallbackInfo, comptime n: usize) [n]?napi.Value {
    var argv: [n]napi.Value = undefined;
    var argc: usize = n;
    if (napi.napi_get_cb_info(env, info, &argc, &argv, null, null) != .ok) return @splat(null);
    var out: [n]?napi.Value = @splat(null);
    for (0..@min(argc, n)) |i| out[i] = argv[i];
    return out;
}

fn bindingOf(env: napi.Env, handle: ?napi.Value) ?*Binding {
    var ptr: ?*anyopaque = null;
    if (napi.napi_get_value_external(env, handle orelse return null, &ptr) != .ok) return null;
    return @ptrCast(@alignCast(ptr orelse return null));
}

// ── listen ───────────────────────────────────────────────────────────

fn jsListen(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 2);
    const options = argv[0] orelse return throw(env, "listen(options, onEvents): options missing");
    const on_events = argv[1] orelse return throw(env, "listen(options, onEvents): onEvents missing");

    const mode = readMode(env, options) catch return throw(env, "options.websocket.thread must be 'node' or 'quic'");
    const host = Host.create(mode) catch return throw(env, "out of memory");
    var h1: quic.event_loop.Http1Config = undefined;
    const config = readConfig(env, options, host.allocator(), mode, &h1) catch |err| {
        host.destroy();
        return throw(env, switch (err) {
            error.MissingCertificate => "options.cert and options.key are required",
            error.BadCertificate => "options.cert holds no PEM certificate",
            error.BadKey => "options.key holds no PEM private key: ECDSA P-256, Ed25519 or RSA",
            else => "invalid options",
        });
    };

    const takes_requests = (boolean(env, options, "requests") catch null) orelse false;
    if (takes_requests) host.takeRequests();

    const b = gpa.create(Binding) catch {
        host.destroy();
        return throw(env, "out of memory");
    };
    b.* = .{ .host = host, .takes_requests = takes_requests };

    var name: napi.Value = undefined;
    _ = napi.napi_create_string_utf8(env, "netcode-io", napi.auto_length, &name);
    if (napi.napi_create_threadsafe_function(env, on_events, null, name, 0, 1, b, finalizeBinding, b, deliver, &b.tsfn) != .ok) {
        host.destroy();
        gpa.destroy(b);
        return throw(env, "could not create the event callback");
    }

    host.listen(config, notify, b) catch |err| {
        b.host = null;
        host.destroy();
        _ = napi.napi_release_threadsafe_function(b.tsfn, .release);
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrintSentinel(&buf, "listen failed: {s}", .{@errorName(err)}, 0) catch "listen failed";
        return throw(env, msg);
    };
    if (mode == .node) {
        const tls = config.tls_config.?;
        b.local = Local.create(env, host, h1, .{
            .address = config.address,
            .ipv6 = config.ipv6,
            .quic_port = host.port(),
            .reuse_port = config.reuse_port,
            .certs = .{
                .entries = tls.certs,
                .single = .{
                    .cert_chain_der = tls.cert_chain_der,
                    .private_key_bytes = tls.private_key_bytes,
                    .private_key_algorithm = tls.private_key_algorithm,
                },
                .client_auth = tls.client_auth,
            },
        }, on_events) catch |err| {
            host.requestStop();
            b.host = null;
            host.destroy();
            _ = napi.napi_release_threadsafe_function(b.tsfn, .release);
            var buf: [128]u8 = undefined;
            const msg = std.fmt.bufPrintSentinel(&buf, "listen failed: {s}", .{@errorName(err)}, 0) catch "listen failed";
            return throw(env, msg);
        };
        b.local.?.tcp.takes_requests = takes_requests;
        if (object(env, options, "websocket") catch null) |ws| {
            b.local.?.transient = (boolean(env, ws, "transientMessages") catch null) orelse false;
        }
    }
    _ = napi.napi_add_env_cleanup_hook(env, onEnvExit, b);

    var handle: napi.Value = undefined;
    if (napi.napi_create_external(env, b, null, null, &handle) != .ok) return null;
    return handle;
}

fn readMode(env: napi.Env, options: napi.Value) !server.Mode {
    const ws = try object(env, options, "websocket") orelse return .node;
    var buf: [8]u8 = undefined;
    var fba: std.heap.FixedBufferAllocator = .init(&buf);
    const v = string(env, ws, "thread", fba.allocator()) catch return error.BadMode;
    return std.meta.stringToEnum(server.Mode, v orelse return .node) orelse error.BadMode;
}

/// `h1` is the HTTP/1.1 listener's, which the QUIC server runs itself
/// in `Mode.quic`.
fn readConfig(env: napi.Env, options: napi.Value, a: std.mem.Allocator, mode: server.Mode, h1_out: *quic.event_loop.Http1Config) !quic.event_loop.Config {
    const cert = try string(env, options, "cert", a) orelse return error.MissingCertificate;
    const key = try string(env, options, "key", a) orelse return error.MissingCertificate;
    var config: quic.event_loop.Config = .{
        .address = try string(env, options, "host", a) orelse "0.0.0.0",
        .port = std.math.cast(u16, try uint(env, options, "port") orelse 0) orelse return error.InvalidPort,
        .tls_config = try tlsConfig(a, cert, key),
    };

    // Always on: quic-zig wants an HTTP/1.1 listener for a handler that
    // takes WebSockets, and ours always does.
    var h1: quic.event_loop.Http1Config = .{
        // Browsers would take it as leave to move fetch() to HTTP/3, where
        // nothing answers plain requests.
        .alt_svc = false,
    };
    if (try object(env, options, "websocket")) |ws| {
        if (try boolean(env, ws, "tls")) |v| h1.tls = v;
        if (try uint(env, ws, "port")) |p| h1.port = std.math.cast(u16, p) orelse return error.InvalidPort;
        if (try uint(env, ws, "maxMessageSize")) |v| h1.websocket.max_message_size = v;
        if (try uint(env, ws, "maxSendBuffer")) |v| h1.websocket.max_send_buffer = v;
        if (try uint(env, ws, "pingInterval")) |v| h1.websocket.ping_interval_ms = v;
    }
    h1_out.* = h1;
    if (mode == .quic) config.http1 = h1;
    return config;
}

/// From PEM in memory: `lib/index.js` reads any file itself.
fn tlsConfig(a: std.mem.Allocator, cert_pem: []const u8, key_pem: []const u8) !quic.tls13.TlsConfig {
    const tls13 = quic.tls13;
    const chain = tls13.parsePemCertChain(a, cert_pem) catch return error.BadCertificate;
    if (chain.len == 0) return error.BadCertificate;
    var der_buf: [4096]u8 = undefined;
    const der = tls13.parsePemPrivateKey(key_pem, &der_buf) catch return error.BadKey;
    const key = tls13.extractPrivateKey(der) catch return error.BadKey;
    var ticket_key: [16]u8 = undefined;
    quic.sys.randomBytes(&ticket_key);
    return .{
        .cert_chain_der = chain,
        .private_key_bytes = try a.dupe(u8, key.bytes),
        .private_key_algorithm = key.algorithm,
        .alpn = &.{"h3"},
        .ticket_key = ticket_key,
    };
}

fn property(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8) !?napi.Value {
    var has = false;
    try check(napi.napi_has_named_property(env, obj, name.ptr, &has));
    if (!has) return null;
    var v: napi.Value = undefined;
    try check(napi.napi_get_named_property(env, obj, name.ptr, &v));
    var t: napi.ValueType = undefined;
    try check(napi.napi_typeof(env, v, &t));
    return if (t == .undefined or t == .null) null else v;
}

fn string(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8, a: std.mem.Allocator) !?[]const u8 {
    const v = try property(env, obj, name) orelse return null;
    var len: usize = 0;
    if (napi.napi_get_value_string_utf8(env, v, null, 0, &len) != .ok) return error.NotAString;
    const buf = try a.alloc(u8, len + 1);
    try check(napi.napi_get_value_string_utf8(env, v, buf.ptr, buf.len, &len));
    return buf[0..len];
}

fn uint(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8) !?u32 {
    const v = try property(env, obj, name) orelse return null;
    var n: u32 = 0;
    if (napi.napi_get_value_uint32(env, v, &n) != .ok) return error.NotANumber;
    return n;
}

fn boolean(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8) !?bool {
    const v = try property(env, obj, name) orelse return null;
    var b = false;
    if (napi.napi_get_value_bool(env, v, &b) != .ok) return error.NotABoolean;
    return b;
}

fn object(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8) !?napi.Value {
    const v = try property(env, obj, name) orelse return null;
    var t: napi.ValueType = undefined;
    try check(napi.napi_typeof(env, v, &t));
    return if (t == .object) v else error.NotAnObject;
}

// ── Events ───────────────────────────────────────────────────────────

/// Loop thread: events are waiting.
fn notify(ctx: ?*anyopaque) void {
    const b: *Binding = @ptrCast(@alignCast(ctx.?));
    if (b.exiting.load(.acquire)) return;
    _ = napi.napi_call_threadsafe_function(b.tsfn, null, .nonblocking);
}

/// JS thread: hands the waiting events to `onEvents`, in a Buffer JS owns
/// (`batchBuffer`), so a message stays valid for as long as it's kept.
fn deliver(env_opt: ?napi.Env, cb_opt: ?napi.Value, ctx: ?*anyopaque, _: ?*anyopaque) callconv(.c) void {
    const env = env_opt orelse return;
    const cb = cb_opt orelse return;
    const b: *Binding = @ptrCast(@alignCast(ctx.?));
    const host = b.host orelse return;
    var recv: napi.Value = undefined;
    _ = napi.napi_get_undefined(env, &recv);
    if (b.rest_at == b.rest.items.len) {
        if (host.take().len == 0) return;
        if (!b.takes_requests) {
            const ab = batchBuffer(env, &host.taken) orelse return;
            // A throw stays pending, and Node reports it as uncaught.
            _ = napi.napi_call_function(env, recv, cb, 1, &.{ab}, null);
            return;
        }
        std.mem.swap(std.ArrayList(u8), &b.rest, &host.taken);
        b.rest_at = 0;
    }
    // A request per call; microtasks run between calls, so the rest
    // comes from a call of its own.
    const rest = b.rest.items[b.rest_at..];
    const n = cut(rest);
    const ab = buffer(env, rest[0..n]) orelse return;
    b.rest_at += n;
    if (b.rest_at < b.rest.items.len) {
        _ = napi.napi_call_threadsafe_function(b.tsfn, null, .nonblocking);
    } else {
        b.rest.clearRetainingCapacity();
        b.rest_at = 0;
    }
    _ = napi.napi_call_function(env, recv, cb, 1, &.{ab}, null);
}

fn finalizeBinding(_: ?napi.Env, data: ?*anyopaque, _: ?*anyopaque) callconv(.c) void {
    const b: *Binding = @ptrCast(@alignCast(data.?));
    b.rest.deinit(gpa);
    gpa.destroy(b);
}

/// Node is tearing the environment down with the server still up: a
/// process exiting, or a worker ending. Stop it and wait, so no thread
/// outlives the environment holding its sockets.
fn onEnvExit(ctx: ?*anyopaque) callconv(.c) void {
    const b: *Binding = @ptrCast(@alignCast(ctx.?));
    b.exiting.store(true, .release);
    if (b.local) |local| {
        b.local = null;
        _ = local.destroy();
    }
    const host = b.host orelse return;
    b.host = null;
    host.requestStop();
    host.destroy();
}

// ── Commands ─────────────────────────────────────────────────────────

/// A large send on Node's thread skips the batch, and its copy.
fn jsSend(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 5);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "send: not a server handle");
    const local = b.local orelse return throw(env, "send: only with websocket.thread 'node'");
    var kind: u32 = 0;
    var flags: u32 = 0;
    var conn: i64 = 0;
    if (napi.napi_get_value_uint32(env, argv[1] orelse return null, &kind) != .ok) return null;
    if (napi.napi_get_value_uint32(env, argv[2] orelse return null, &flags) != .ok) return null;
    if (napi.napi_get_value_int64(env, argv[3] orelse return null, &conn) != .ok) return null;
    const data = argv[4] orelse return throw(env, "send: data missing");
    var is_typed = false;
    if (napi.napi_is_typedarray(env, data, &is_typed) != .ok or !is_typed) return throw(env, "send: data must be a Uint8Array");
    var len: usize = 0;
    var ptr: ?*anyopaque = null;
    if (napi.napi_get_typedarray_info(env, data, null, &len, &ptr, null, null) != .ok) return null;
    const bytes: []const u8 = if (len == 0) &.{} else @as([*]const u8, @ptrCast(ptr.?))[0..len];
    local.sendNow(std.math.cast(u8, kind) orelse return null, std.math.cast(u8, flags) orelse return null, @intCast(conn), bytes) catch
        return throw(env, "send: not a WebSocket or raw send");
    return null;
}

fn jsSubmit(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 3);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "submit: not a server handle");
    const host = b.host orelse return null;
    const batch = argv[1] orelse return throw(env, "submit: batch missing");

    var is_typed = false;
    if (napi.napi_is_typedarray(env, batch, &is_typed) != .ok or !is_typed) return throw(env, "submit: batch must be a Uint8Array");
    var kind: c_int = 0;
    var len: usize = 0;
    var data: ?*anyopaque = null;
    if (napi.napi_get_typedarray_info(env, batch, &kind, &len, &data, null, null) != .ok) return null;
    if (kind != 1) return throw(env, "submit: batch must be a Uint8Array");
    // a length, so JS needn't make a subarray of its batch buffer each turn
    if (argv[2]) |v| {
        var n: u32 = 0;
        if (napi.napi_get_value_uint32(env, v, &n) != .ok or n > len) return throw(env, "submit: bad length");
        len = n;
    }
    if (len == 0) return null;
    const bytes = @as([*]const u8, @ptrCast(data.?))[0..len];
    (if (b.local) |local| local.submit(bytes) else host.submit(bytes)) catch return throw(env, "out of memory");
    return null;
}

fn jsPort(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 1);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "port: not a server handle");
    const host = b.host orelse return null;
    var v: napi.Value = undefined;
    _ = napi.napi_create_uint32(env, host.port(), &v);
    return v;
}

fn jsRef(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 2);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "ref: not a server handle");
    if (b.host == null) return null;
    var keep = true;
    if (argv[1]) |v| _ = napi.napi_get_value_bool(env, v, &keep);
    _ = if (keep) napi.napi_ref_threadsafe_function(env, b.tsfn) else napi.napi_unref_threadsafe_function(env, b.tsfn);
    if (b.local) |local| local.ref(keep);
    return null;
}

fn jsStats(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 1);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "stats: not a server handle");
    const host = b.host orelse return null;
    const st = &host.stats;
    var obj: napi.Value = undefined;
    if (napi.napi_create_object(env, &obj) != .ok) return null;
    const fields = .{
        .{ "events", st.events.get() },
        .{ "notifies", st.notifies.get() },
        .{ "wakes", st.wakes.get() },
        .{ "commands", st.commands.get() },
        .{ "delivers", st.delivers },
        .{ "eventBytes", st.event_bytes },
        .{ "submits", st.submits },
        .{ "commandBytes", st.command_bytes },
    };
    inline for (fields) |f| setNumber(env, obj, f[0], @floatFromInt(f[1]));
    if (host.loopCpuNs()) |ns| setNumber(env, obj, "loopCpu", @as(f64, @floatFromInt(ns)) / std.time.ns_per_s);
    return obj;
}

fn setNumber(env: napi.Env, obj: napi.Value, comptime name: [:0]const u8, x: f64) void {
    var v: napi.Value = undefined;
    if (napi.napi_create_double(env, x, &v) != .ok) return;
    _ = napi.napi_set_named_property(env, obj, name.ptr, v);
}

fn jsDestroy(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 1);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "destroy: not a server handle");
    const host = b.host orelse return null;
    b.host = null;
    _ = napi.napi_remove_env_cleanup_hook(env, onEnvExit, b);
    // Before the host: the listener's certificates live in its arena.
    const rest: ?napi.Value = if (b.local) |local| blk: {
        b.local = null;
        const events = local.destroy();
        break :blk if (events.len > 0) buffer(env, events) else null;
    } else null;
    host.destroy();
    _ = napi.napi_release_threadsafe_function(b.tsfn, .release);
    return rest;
}
