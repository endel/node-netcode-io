//! The Node-API module: what `lib/index.js` calls. Five functions, all on
//! the JS thread:
//!
//!   listen(options, onEvents) -> handle   binds, starts the loop thread
//!   submit(handle, Uint8Array)            queues a command batch
//!   port(handle) -> number                the bound UDP port
//!   ref(handle, bool)                     whether the server keeps Node alive
//!   destroy(handle)                       frees it, after the `stopped` event
//!
//! `onEvents(ArrayBuffer)` gets every event batch, through a threadsafe
//! function the loop thread calls once per batch.

const std = @import("std");
const quic = @import("quic");
const napi = @import("napi.zig");
const server = @import("server.zig");
const Host = server.Host;

pub const std_options: std.Options = .{ .log_level = .warn };

const gpa = std.heap.c_allocator;

/// What a JS handle points at. Outlives the host: the threadsafe function's
/// finalizer frees it, as a call may still be queued when the host goes.
const Binding = struct {
    host: ?*Host,
    tsfn: napi.ThreadsafeFunction = undefined,
    /// The environment is going away: no more calls into it.
    exiting: std.atomic.Value(bool) = .init(false),
};

export fn napi_register_module_v1(env: napi.Env, exports: napi.Value) callconv(.c) ?napi.Value {
    define(env, exports, "listen", jsListen) catch return null;
    define(env, exports, "submit", jsSubmit) catch return null;
    define(env, exports, "port", jsPort) catch return null;
    define(env, exports, "ref", jsRef) catch return null;
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

    const host = Host.create() catch return throw(env, "out of memory");
    const config = readConfig(env, options, host.allocator()) catch |err| {
        host.destroy();
        return throw(env, switch (err) {
            error.MissingCertificate => "options.cert and options.key are required",
            else => "invalid options",
        });
    };

    const b = gpa.create(Binding) catch {
        host.destroy();
        return throw(env, "out of memory");
    };
    b.* = .{ .host = host };

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
    _ = napi.napi_add_env_cleanup_hook(env, onEnvExit, b);

    var handle: napi.Value = undefined;
    if (napi.napi_create_external(env, b, null, null, &handle) != .ok) return null;
    return handle;
}

fn readConfig(env: napi.Env, options: napi.Value, a: std.mem.Allocator) !quic.event_loop.Config {
    var config: quic.event_loop.Config = .{
        .address = try string(env, options, "host", a) orelse "0.0.0.0",
        .port = std.math.cast(u16, try uint(env, options, "port") orelse 0) orelse return error.InvalidPort,
        .cert_path = try string(env, options, "cert", a) orelse return error.MissingCertificate,
        .key_path = try string(env, options, "key", a) orelse return error.MissingCertificate,
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
    config.http1 = h1;
    return config;
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

/// JS thread: hands the waiting events to `onEvents`, copied into an
/// ArrayBuffer JS owns, so a message stays valid for as long as it's kept.
fn deliver(env_opt: ?napi.Env, cb_opt: ?napi.Value, ctx: ?*anyopaque, _: ?*anyopaque) callconv(.c) void {
    const env = env_opt orelse return;
    const cb = cb_opt orelse return;
    const b: *Binding = @ptrCast(@alignCast(ctx.?));
    const host = b.host orelse return;
    const bytes = host.take();
    if (bytes.len == 0) return;

    var data: ?*anyopaque = null;
    var ab: napi.Value = undefined;
    if (napi.napi_create_arraybuffer(env, bytes.len, &data, &ab) != .ok) return;
    @memcpy(@as([*]u8, @ptrCast(data.?))[0..bytes.len], bytes);
    var recv: napi.Value = undefined;
    _ = napi.napi_get_undefined(env, &recv);
    // A throw stays pending, and Node reports it as uncaught.
    _ = napi.napi_call_function(env, recv, cb, 1, &.{ab}, null);
}

fn finalizeBinding(_: ?napi.Env, data: ?*anyopaque, _: ?*anyopaque) callconv(.c) void {
    gpa.destroy(@as(*Binding, @ptrCast(@alignCast(data.?))));
}

/// Node is tearing the environment down with the server still up: a
/// process exiting, or a worker ending. Stop it and wait, so no thread
/// outlives the environment holding its sockets.
fn onEnvExit(ctx: ?*anyopaque) callconv(.c) void {
    const b: *Binding = @ptrCast(@alignCast(ctx.?));
    b.exiting.store(true, .release);
    const host = b.host orelse return;
    b.host = null;
    host.requestStop();
    host.destroy();
}

// ── Commands ─────────────────────────────────────────────────────────

fn jsSubmit(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 2);
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
    if (len == 0) return null;
    host.submit(@as([*]const u8, @ptrCast(data.?))[0..len]) catch return throw(env, "out of memory");
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
    return null;
}

fn jsDestroy(env: napi.Env, info: napi.CallbackInfo) callconv(.c) ?napi.Value {
    const argv = args(env, info, 1);
    const b = bindingOf(env, argv[0]) orelse return throw(env, "destroy: not a server handle");
    const host = b.host orelse return null;
    b.host = null;
    _ = napi.napi_remove_env_cleanup_hook(env, onEnvExit, b);
    host.destroy();
    _ = napi.napi_release_threadsafe_function(b.tsfn, .release);
    return null;
}
