#!/bin/bash
# What each epoll_pwait on a server's main thread costs, by epoll fd, at one
# connection and 200 messages a second (cold caches), after JS warmup; then
# the same call warm, in a loop (epollcost.c). netcode-io's Node-thread mode
# makes one more per wake than uWebSockets.js: libuv reports libxev's epoll
# fd, and libxev's own epoll_wait finds the socket.
set -e
. "$(dirname "$0")/common.sh"
cd "$BENCH"
for t in uws netcode; do
  start $t 60000
  wsload -addr 127.0.0.1:$PORT -c 1 -rate 200 -spread -size 1024 -warmup 34s -duration 14s > /dev/null &
  L=$!
  sleep 36
  echo "== $t"
  bpftrace -q -e "
    tracepoint:syscalls:sys_enter_epoll_pwait /tid == $PID/ { @fd[tid] = args->epfd; @t[tid] = nsecs; @to[tid] = args->timeout; }
    tracepoint:syscalls:sys_exit_epoll_pwait /@t[tid]/ {
      \$d = nsecs - @t[tid];
      if (@to[tid] == 0) { @zero_n[@fd[tid]] = count(); @zero_ns[@fd[tid]] = avg(\$d); }
      else { @blocking_n[@fd[tid]] = count(); }
      delete(@t[tid]); delete(@fd[tid]); delete(@to[tid]);
    }
    interval:s:10 { exit(); }"
  wait $L || true
  stop
done | tee "$OUT/epoll.log"
zig cc -O2 "$(dirname "$0")/epollcost.c" -o /tmp/epollcost && /tmp/epollcost | tee -a "$OUT/epoll.log"
