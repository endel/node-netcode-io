//! The libuv calls `local.zig` drives Node's loop with, declared by hand:
//! Node exports libuv to addons, and handles are sized at run time
//! (`uv_handle_size`), so no header is needed.

pub const Loop = opaque {};
pub const Handle = opaque {};

// uv_handle_type and uv_poll_event values, from uv.h.
pub const POLL: c_int = 8;
pub const TIMER: c_int = 13;
pub const READABLE: c_int = 1;

pub const PollCb = *const fn (*Handle, c_int, c_int) callconv(.c) void;
pub const TimerCb = *const fn (*Handle) callconv(.c) void;
pub const CloseCb = *const fn (*Handle) callconv(.c) void;

pub extern fn uv_handle_size(kind: c_int) usize;
pub extern fn uv_poll_init(loop: *Loop, handle: *Handle, fd: c_int) c_int;
pub extern fn uv_poll_start(handle: *Handle, events: c_int, cb: PollCb) c_int;
pub extern fn uv_timer_init(loop: *Loop, handle: *Handle) c_int;
pub extern fn uv_timer_start(handle: *Handle, cb: TimerCb, timeout: u64, repeat: u64) c_int;
pub extern fn uv_timer_stop(handle: *Handle) c_int;
pub extern fn uv_close(handle: *Handle, cb: ?CloseCb) void;
pub extern fn uv_handle_set_data(handle: *Handle, data: ?*anyopaque) void;
pub extern fn uv_handle_get_data(handle: *const Handle) ?*anyopaque;
pub extern fn uv_ref(handle: *Handle) void;
pub extern fn uv_unref(handle: *Handle) void;
