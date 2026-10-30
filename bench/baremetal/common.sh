# Sourced by the scripts beside it, on the benchmark machine. ROOT holds
# quic-zig and netcode-io side by side, as build.zig.zon expects, and uws.
ROOT=${ROOT:-/root/netcode}
NETCODE=$ROOT/netcode-io
QUIC=$ROOT/quic-zig
BENCH=$NETCODE/bench
export UWS=${UWS:-$ROOT/uws/node_modules/uWebSockets.js}
OUT=${OUT:-$ROOT/results}
# Servers and load on separate cores: the defaults fit 6 cores with SMT,
# where CPU n and n+6 are one core's two threads.
SERVER_CPUS=${SERVER_CPUS:-1-3}
CLIENT_CPUS=${CLIENT_CPUS:-4,5,10,11}
mkdir -p "$OUT"
ulimit -n 1048576 2>/dev/null || ulimit -n 65536

# start <transport> <ms> [server.mjs mode]: an echo server from bench/server.mjs,
# killed after <ms>. Sets PID and PORT. Ports sit below the clients'
# (setup.sh: 20000-65535), so a run's TIME_WAITs never block the next bind.
start() {
  PORT=$((10000 + RANDOM % 9000)); rm -f /tmp/netcode-bench.pid
  taskset -c "$SERVER_CPUS" node -e "
    const { fork } = require('child_process');
    const s = fork('$BENCH/server.mjs', ['$1', '$PORT', 'plain', '0', '${3:-echo}'], { stdio: ['ignore','ignore','inherit','ipc'] });
    s.on('message', (m) => { if (m.type === 'ready') require('fs').writeFileSync('/tmp/netcode-bench.pid', String(s.pid)); });
    setTimeout(() => s.kill('SIGKILL'), $2);" &
  HOLDER=$!
  for _ in $(seq 1 150); do [ -f /tmp/netcode-bench.pid ] && break; sleep 0.1; done
  [ -f /tmp/netcode-bench.pid ] || { echo "server $1 did not start" >&2; return 1; }
  PID=$(cat /tmp/netcode-bench.pid)
}
stop() { kill "$HOLDER" "$PID" 2>/dev/null || true; wait "$HOLDER" 2>/dev/null || true; }

wsload() { GOMAXPROCS=4 taskset -c "$CLIENT_CPUS" "$BENCH/wsload/wsload" "$@"; }
