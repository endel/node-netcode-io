#!/bin/sh
# On the benchmark machine, after sync.sh: the addon for this CPU, the load
# clients, and quic-zig's own WebTransport echo for webtransport.sh.
set -e
. "$(dirname "$0")/common.sh"
(cd "$NETCODE" && zig build -Doptimize=ReleaseFast -Dcpu=native)
(cd "$BENCH/wsload" && go build -o wsload .)
(cd "$BENCH/wtload" && go build -o wtload . && go build -o wtecho ./echo)
# quic-zig has no step for that one binary, and an app that reads interop/
# (not synced) fails the rest: take what builds
(cd "$QUIC" && zig build -Doptimize=ReleaseFast -Dcpu=native >/dev/null 2>&1) || true
[ -x "$QUIC/zig-out/bin/wt-echo-server" ] || echo "no quic-zig wt-echo-server: webtransport.sh skips it"
ls -la "$NETCODE/zig-out/lib/netcode_io.node" "$BENCH/wsload/wsload" "$BENCH/wtload/wtload" "$BENCH/wtload/wtecho" | awk '{print $5, $NF}'
