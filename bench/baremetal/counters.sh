#!/bin/bash
# Hardware counters per message (perf stat on the server, all threads) for
# both servers under the same load. DELAY: seconds before the 10 s window;
# keep it past ~30 so V8's memory reducer has run. SPECS: "conns rate size
# label" rows, ';'-separated.
set -e
. "$(dirname "$0")/common.sh"
EV=task-clock,instructions,cycles,cache-misses,L1-icache-load-misses,branch-misses,page-faults
D=${DELAY:-34}
if [ -n "$SPECS" ]; then IFS=';' read -ra LIST <<< "$SPECS"; else LIST=("50 0 1024 closed" "50 4 1024 paced" "200 20 16384 16k" "50 20 65536 64k"); fi
cd "$BENCH"
for spec in "${LIST[@]}"; do
  set -- $spec
  for t in uws netcode; do
    start $t $(( (D + 28) * 1000 ))
    wsload -addr 127.0.0.1:$PORT -c $1 -rate $2 -inflight 1 -spread -size $3 -warmup 6s -duration $((D + 14))s > /tmp/load.json &
    L=$!
    sleep "$D"
    perf stat -x, -e $EV -p $PID -- sleep 10 2> /tmp/stat.csv
    wait $L; stop
    node -e "
      const rps = JSON.parse(require('fs').readFileSync('/tmp/load.json','utf8').trim().split('\n').pop()).receivedPerSecond;
      const m = rps * 10, v = {};
      for (const l of require('fs').readFileSync('/tmp/stat.csv','utf8').split('\n')) { const f = l.split(','); if (f.length > 3 && f[0] && !isNaN(+f[0])) v[f[2]] = +f[0]; }
      const per = (k) => v[k] === undefined ? 'n/a' : (v[k] / m).toFixed(0);
      console.log('$4'.padEnd(7), '$t'.padEnd(8), 'us', (v['task-clock'] * 1000 / m).toFixed(2), '| instr', per('instructions'), '| cycles', per('cycles'), '| IPC', (v.instructions / v.cycles).toFixed(2), '| cache-miss', per('cache-misses'), '| L1i-miss', per('L1-icache-load-misses'), '| br-miss', per('branch-misses'), '| faults', (v['page-faults'] / m).toFixed(2));"
  done
done | tee "$OUT/counters.log"
