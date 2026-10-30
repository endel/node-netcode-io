#!/bin/bash
# WebTransport echo under bench/wtload: netcode-io (bench/wt-server.mjs),
# quic-go (bench/wtload/echo) and quic-zig's demo echo, paired per round,
# the order rotating. Per run: server CPU per message from perf stat over
# 10 s after warmup, latency and loss from wtload.
#   webtransport.sh [rounds] [warmup s]   ONLY=regex SERVERS="netcode go zig"
# A/B of two netcode-io builds: SERVERS="a b" A_DIR=<netcode-io tree>
# B_DIR=<another>, each with its own ../quic-zig.
set -e
. "$(dirname "$0")/common.sh"
ROUNDS=${1:-3}
W=${2:-30}
TSV=$OUT/webtransport.tsv
: > "$TSV"
SERVERS=${SERVERS:-"netcode go zig"}
CRT=$NETCODE/test/certs/server.crt
KEY=$NETCODE/test/certs/server.key
# name | wtload args. quic-zig's demo drops a read over 1018 bytes and
# prefixes echoes with "Echo: " (wtload skips it): stay under
CELLS=(
  "dgram 50x20 64B|-mode datagram -c 50 -rate 20 -size 64 -spread"
  "stream 50x20 64B|-mode stream -c 50 -rate 20 -size 64 -spread"
  "dgram 250x20 64B|-mode datagram -c 250 -rate 20 -size 64 -spread"
  "stream 250x20 64B|-mode stream -c 250 -rate 20 -size 64 -spread"
  "dgram 1x200 64B|-mode datagram -c 1 -rate 200 -size 64 -spread"
  "stream 1x200 64B|-mode stream -c 1 -rate 200 -size 64 -spread"
  "dgram 50 closed 64B|-mode datagram -c 50 -rate 0 -inflight 1 -size 64"
  "stream 50 closed 64B|-mode stream -c 50 -rate 0 -inflight 1 -size 64"
  "dgram 50x20 1000B|-mode datagram -c 50 -rate 20 -size 1000 -spread"
)

wt_start() { # server port -> WPID
  case $1 in
    netcode) taskset -c "$SERVER_CPUS" node "$BENCH/wt-server.mjs" "$2" 127.0.0.1 > /dev/null & ;;
    a|b) d=$([ "$1" = a ] && echo "$A_DIR" || echo "$B_DIR")
         taskset -c "$SERVER_CPUS" node "$d/bench/wt-server.mjs" "$2" 127.0.0.1 > /dev/null & ;;
    go) GOMAXPROCS=3 taskset -c "$SERVER_CPUS" "$BENCH/wtload/wtecho" -addr 127.0.0.1:"$2" -cert "$CRT" -key "$KEY" -q > /dev/null 2>&1 & ;;
    zig) taskset -c "$SERVER_CPUS" "$QUIC/zig-out/bin/wt-echo-server" --cert "$QUIC/interop/certs/server.crt" --key "$QUIC/interop/certs/server.key" --port "$2" > /dev/null 2>&1 & ;;
  esac
  WPID=$!
  for _ in $(seq 1 100); do ss -Hlun "sport = :$2" | grep -q . && break; sleep 0.1; done
  sleep 0.5
}

for cell in "${CELLS[@]}"; do
  name=${cell%%|*}; args=${cell#*|}
  [ -n "$ONLY" ] && ! [[ "$name" =~ $ONLY ]] && continue
  for r in $(seq 1 "$ROUNDS"); do
    read -ra order <<< "$SERVERS"
    # each server goes first once every len rounds
    k=$(( (r - 1) % ${#order[@]} ))
    order=("${order[@]:$k}" "${order[@]:0:$k}")
    for s in "${order[@]}"; do
      [ "$s" = zig ] && [ ! -x "$QUIC/zig-out/bin/wt-echo-server" ] && continue
      PORT=$((10000 + RANDOM % 9000))
      wt_start "$s" "$PORT"
      GOMAXPROCS=4 taskset -c "$CLIENT_CPUS" "$BENCH/wtload/wtload" -addr 127.0.0.1:$PORT -path / $args -warmup ${W}s -duration 14s > /tmp/wt.json 2> /tmp/wt.err &
      L=$!
      sleep $((W + 2))
      perf stat -x, -e task-clock,instructions,cycles -p $WPID -- sleep 10 2> /tmp/wt.stat || true
      wait $L || true
      kill $WPID 2>/dev/null || true; wait $WPID 2>/dev/null || true
      line=$(node -e "
        const fs = require('fs');
        let j = {}; try { j = JSON.parse(fs.readFileSync('/tmp/wt.json','utf8').trim().split('\n').pop()); } catch {}
        const v = {}; for (const l of fs.readFileSync('/tmp/wt.stat','utf8').split('\n')) { const f = l.split(','); if (f.length > 3 && f[0] && !isNaN(+f[0])) v[f[2]] = +f[0]; }
        const m = (j.receivedPerSecond || NaN) * 10;
        console.log([ '$name', '$s', $r, (v['task-clock'] * 1000 / m).toFixed(2), (v.instructions / m).toFixed(0), (v.cycles / m).toFixed(0),
          (j.receivedPerSecond ?? NaN).toFixed(0), j.p50, j.p90, j.p99, j.loss ?? 0, j.errors ?? 0, j.silent ?? 0 ].join('\t'));")
      echo "$line" >> "$TSV"
      echo "$line" | awk -F'\t' '{printf "%-22s %-8s r%s  %7s us/msg  %7s instr  %7s msg/s  p50 %s p99 %s  loss %s\n", $1,$2,$3,$4,$5,$7,$8,$10,$11}'
      grep -m2 -i "fail\|refused\|error" /tmp/wt.err || true
      sleep 3 # quic-zig counts draining connections against its cap
    done
  done
done | tee "$OUT/webtransport.log"
echo "== medians" | tee -a "$OUT/webtransport.log"
node -e "
  const rows = require('fs').readFileSync('$TSV','utf8').trim().split('\n').map((l) => l.split('\t'));
  const med = (xs) => { const s = xs.map(Number).filter(Number.isFinite).sort((a, b) => a - b); return s.length ? s[Math.floor(s.length / 2)] : NaN; };
  for (const c of [...new Set(rows.map((r) => r[0]))]) {
    for (const s of [...new Set(rows.map((r) => r[1]))]) {
      const rs = rows.filter((r) => r[0] === c && r[1] === s);
      if (!rs.length) continue;
      const col = (i) => med(rs.map((r) => r[i]));
      console.log(c.padEnd(22), s.padEnd(8), String(col(3)).padStart(7), 'us/msg', String(col(6)).padStart(7), 'msg/s', 'p50', col(7), 'p99', col(9), 'loss', col(10));
    }
  }" | tee -a "$OUT/webtransport.log"
