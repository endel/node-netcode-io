#!/bin/bash
# Where each server's cycles go, by symbol, under the same load:
#   profile.sh <conns> <rate> <size> [rows]
# and syscalls per message at one connection, 200 a second:
#   profile.sh syscalls
set -e
. "$(dirname "$0")/common.sh"
cd "$BENCH"
if [ "$1" = syscalls ]; then
  for t in uws netcode; do
    start $t 30000
    wsload -addr 127.0.0.1:$PORT -c 1 -rate 200 -spread -size 1024 -warmup 4s -duration 18s > /tmp/load.json &
    L=$!
    sleep 6
    timeout -s INT 10 strace -c -f -p $PID -o /tmp/strace-$t.txt > /dev/null 2>&1 || true
    wait $L; stop
    echo "== $t (2000 messages)"
    awk 'NR>2 && $4 ~ /^[0-9]+$/ {printf "  %-16s %6.2f per msg\n", $NF, $4/2000}' /tmp/strace-$t.txt | sort -k2 -rn | head -12
  done
  exit
fi
for t in uws netcode; do
  start $t 70000
  wsload -addr 127.0.0.1:$PORT -c $1 -rate $2 -inflight 1 -spread -size $3 -warmup 6s -duration 56s > /tmp/load.json &
  L=$!
  sleep 36 # past V8's tiering and memory reducer
  perf record -q -e cycles -F 9999 -p $PID -o /tmp/perf-$t.data -- sleep 10 2>/dev/null
  wait $L; stop
  echo "== $t: $(tail -1 /tmp/load.json | node -pe 'JSON.parse(require("fs").readFileSync(0)).receivedPerSecond') msg/s"
  perf report -i /tmp/perf-$t.data --stdio --no-children --sort dso,sym 2>/dev/null | grep -v '^#' | grep . | head -"${4:-30}"
done
