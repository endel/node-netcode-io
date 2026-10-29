//! The Node-API functions the addon calls, declared by hand from Node's
//! `js_native_api.h` and `node_api.h`. Node-API is a stable C ABI, so one
//! build loads into every Node release that has these (all of them since
//! Node-API 8); the symbols resolve against the host process at load time.

pub const Env = *opaque {};
pub const Value = *opaque {};
pub const CallbackInfo = *opaque {};
pub const ThreadsafeFunction = *opaque {};
pub const Ref = *opaque {};
pub const HandleScope = *opaque {};
pub const AsyncContext = *opaque {};

pub const Status = enum(c_int) {
    ok,
    invalid_arg,
    object_expected,
    string_expected,
    name_expected,
    function_expected,
    number_expected,
    boolean_expected,
    array_expected,
    generic_failure,
    pending_exception,
    cancelled,
    escape_called_twice,
    handle_scope_mismatch,
    callback_scope_mismatch,
    queue_full,
    closing,
    bigint_expected,
    date_expected,
    arraybuffer_expected,
    detachable_arraybuffer_expected,
    would_deadlock,
    no_external_buffers_allowed,
    cannot_run_js,
    _,
};

pub const ValueType = enum(c_int) {
    undefined,
    null,
    boolean,
    number,
    string,
    symbol,
    object,
    function,
    external,
    bigint,
    _,
};

pub const TsfnReleaseMode = enum(c_int) { release, abort };
pub const TsfnCallMode = enum(c_int) { nonblocking, blocking };

pub const Callback = *const fn (Env, CallbackInfo) callconv(.c) ?Value;
pub const Finalize = *const fn (?Env, ?*anyopaque, ?*anyopaque) callconv(.c) void;
pub const CallJs = *const fn (?Env, ?Value, ?*anyopaque, ?*anyopaque) callconv(.c) void;
pub const CleanupHook = *const fn (?*anyopaque) callconv(.c) void;

/// `NAPI_AUTO_LENGTH`: the string is NUL-terminated.
pub const auto_length: usize = std.math.maxInt(usize);

pub extern fn napi_create_function(env: Env, name: ?[*]const u8, len: usize, cb: Callback, data: ?*anyopaque, result: *Value) Status;
pub extern fn napi_set_named_property(env: Env, object: Value, name: [*:0]const u8, value: Value) Status;
pub extern fn napi_get_named_property(env: Env, object: Value, name: [*:0]const u8, result: *Value) Status;
pub extern fn napi_has_named_property(env: Env, object: Value, name: [*:0]const u8, result: *bool) Status;
pub extern fn napi_get_cb_info(env: Env, info: CallbackInfo, argc: *usize, argv: [*]Value, this: ?*Value, data: ?*?*anyopaque) Status;
pub extern fn napi_typeof(env: Env, value: Value, result: *ValueType) Status;
pub extern fn napi_get_value_uint32(env: Env, value: Value, result: *u32) Status;
pub extern fn napi_get_value_int64(env: Env, value: Value, result: *i64) Status;
pub extern fn napi_get_value_bool(env: Env, value: Value, result: *bool) Status;
pub extern fn napi_get_value_string_utf8(env: Env, value: Value, buf: ?[*]u8, bufsize: usize, result: ?*usize) Status;
pub extern fn napi_get_typedarray_info(env: Env, value: Value, kind: ?*c_int, length: ?*usize, data: ?*?*anyopaque, arraybuffer: ?*Value, byte_offset: ?*usize) Status;
pub extern fn napi_is_typedarray(env: Env, value: Value, result: *bool) Status;
pub extern fn napi_create_array(env: Env, result: *Value) Status;
pub extern fn napi_set_element(env: Env, object: Value, index: u32, value: Value) Status;
pub extern fn napi_create_arraybuffer(env: Env, len: usize, data: *?*anyopaque, result: *Value) Status;
pub extern fn napi_create_buffer(env: Env, len: usize, data: *?*anyopaque, result: *Value) Status;
pub extern fn napi_create_uint32(env: Env, value: u32, result: *Value) Status;
pub extern fn napi_create_double(env: Env, value: f64, result: *Value) Status;
pub extern fn napi_create_object(env: Env, result: *Value) Status;
pub extern fn napi_get_undefined(env: Env, result: *Value) Status;
pub extern fn napi_throw_error(env: Env, code: ?[*:0]const u8, msg: [*:0]const u8) Status;
pub extern fn napi_create_external(env: Env, data: ?*anyopaque, finalize: ?Finalize, hint: ?*anyopaque, result: *Value) Status;
pub extern fn napi_get_value_external(env: Env, value: Value, result: *?*anyopaque) Status;
pub extern fn napi_call_function(env: Env, recv: Value, func: Value, argc: usize, argv: [*]const Value, result: ?*Value) Status;
pub extern fn napi_create_string_utf8(env: Env, str: [*]const u8, len: usize, result: *Value) Status;
pub extern fn napi_add_env_cleanup_hook(env: Env, hook: CleanupHook, arg: ?*anyopaque) Status;
pub extern fn napi_remove_env_cleanup_hook(env: Env, hook: CleanupHook, arg: ?*anyopaque) Status;
pub extern fn napi_create_threadsafe_function(
    env: Env,
    func: ?Value,
    async_resource: ?Value,
    async_resource_name: Value,
    max_queue_size: usize,
    initial_thread_count: usize,
    thread_finalize_data: ?*anyopaque,
    thread_finalize_cb: ?Finalize,
    context: ?*anyopaque,
    call_js_cb: ?CallJs,
    result: *ThreadsafeFunction,
) Status;
pub extern fn napi_call_threadsafe_function(func: ThreadsafeFunction, data: ?*anyopaque, mode: TsfnCallMode) Status;
pub extern fn napi_release_threadsafe_function(func: ThreadsafeFunction, mode: TsfnReleaseMode) Status;
pub extern fn napi_ref_threadsafe_function(env: Env, func: ThreadsafeFunction) Status;
pub extern fn napi_unref_threadsafe_function(env: Env, func: ThreadsafeFunction) Status;
pub extern fn napi_get_global(env: Env, result: *Value) Status;
pub extern fn napi_create_reference(env: Env, value: Value, initial_refcount: u32, result: *Ref) Status;
pub extern fn napi_delete_reference(env: Env, ref: Ref) Status;
pub extern fn napi_get_reference_value(env: Env, ref: Ref, result: *?Value) Status;
pub extern fn napi_open_handle_scope(env: Env, result: *HandleScope) Status;
pub extern fn napi_close_handle_scope(env: Env, scope: HandleScope) Status;
pub extern fn napi_async_init(env: Env, async_resource: ?Value, async_resource_name: Value, result: *AsyncContext) Status;
pub extern fn napi_async_destroy(env: Env, async_context: AsyncContext) Status;
pub extern fn napi_make_callback(env: Env, async_context: ?AsyncContext, recv: Value, func: Value, argc: usize, argv: [*]const Value, result: ?*Value) Status;
pub extern fn napi_get_and_clear_last_exception(env: Env, result: *Value) Status;
pub extern fn napi_fatal_exception(env: Env, err: Value) Status;
pub extern fn napi_get_uv_event_loop(env: Env, loop: **anyopaque) Status;

const std = @import("std");
