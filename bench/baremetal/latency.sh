#!/bin/bash
# One connection, 1 KiB at 200 messages a second, after a warmup past V8's
# tiering and memory reducer: the round trip with nothing queued.
set -e
. "$(dirname "$0")/common.sh"
W=${WARMUP:-34}
for r in $(seq 1 "${ROUNDS:-5}"); do
  for t in uws netcode; do
    start $t $(( (W + 14) * 1000 ))
    wsload -addr 127.0.0.1:$PORT -c 1 -rate 200 -spread -size 1024 -warmup ${W}s -duration 10s | tail -1 \
      | node -pe "const j=JSON.parse(require('fs').readFileSync(0)); 'r$r $t'.padEnd(12)+' p50 '+j.p50+' p90 '+j.p90+' p99 '+j.p99+' ms'"
    stop
  done
done | tee "$OUT/latency.log"
