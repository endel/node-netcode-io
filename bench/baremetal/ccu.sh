#!/bin/bash
# Held connections: N clients each sending a 50-byte echo a second, against
# uWebSockets.js and netcode-io (transientMessages). One wsload per 25,000,
# each to its own 127.0.0.k: one address pair has ~45,000 ports. Per run:
# time to connect all, failures, server RSS per connection, CPU per message,
# latency.   ccu.sh [rounds] [levels...]
set -e
. "$(dirname "$0")/common.sh"
ROUNDS=${1:-2}; shift || true
LEVELS=${*:-"10000 25000 50000 100000"}
PER=25000
# every core but the server's
CCU_CLIENT_CPUS=${CCU_CLIENT_CPUS:-0,4-11}
export BENCH_HOST=0.0.0.0
cd "$BENCH"
for n in $LEVELS; do
  for r in $(seq 1 "$ROUNDS"); do
    order="uws netcode"; [ $((r % 2)) -eq 0 ] && order="netcode uws"
    for t in $order; do
      start $t 400000 || continue
      sleep 1
      rss0=$(awk '/VmRSS/{print $2}' /proc/$PID/status)
      procs=$(( (n + PER - 1) / PER ))
      t0=$(date +%s.%N)
      pids=""
      for k in $(seq 1 $procs); do
        GOMAXPROCS=3 taskset -c "$CCU_CLIENT_CPUS" ./wsload/wsload -addr 127.0.0.$k:$PORT -c $(( n / procs )) -dial 100 -rate 1 -spread -size 50 -warmup 30s -duration 20s > /tmp/ccu-$k.out 2> /tmp/ccu-$k.err &
        pids="$pids $!"
      done
      # every client connected, or one gave up
      until [ "$(cat /tmp/ccu-*.out 2>/dev/null | grep -c connected)" -ge $procs ] || ! kill -0 $pids 2>/dev/null; do sleep 0.2; done
      t1=$(date +%s.%N)
      sleep 32
      perf stat -x, -e task-clock,instructions,cycles -p $PID -- sleep 10 2> /tmp/ccu.stat || true
      rss=$(awk '/VmRSS/{print $2}' /proc/$PID/status 2>/dev/null || echo 0)
      for p in $pids; do wait $p || true; done
      stop
      node -e "
        const fs = require('fs');
        const outs = [], fails = [];
        for (let k = 1; k <= $procs; k++) {
          const lines = fs.readFileSync('/tmp/ccu-' + k + '.out', 'utf8').trim().split('\n');
          try { outs.push(JSON.parse(lines.pop())); } catch {}
          const err = fs.readFileSync('/tmp/ccu-' + k + '.err', 'utf8').match(/(\d+) connections failed/); if (err) fails.push(+err[1]);
        }
        const v = {}; for (const l of fs.readFileSync('/tmp/ccu.stat','utf8').split('\n')) { const f = l.split(','); if (f.length > 3 && f[0] && !isNaN(+f[0])) v[f[2]] = +f[0]; }
        const rps = outs.reduce((a, o) => a + (o.receivedPerSecond || 0), 0);
        const worst = (k) => Math.max(...outs.map((o) => o[k] ?? NaN));
        console.log([ 'ccu ' + $n, '$t'.padEnd(8), 'r$r',
          'connect ' + ($t1 - $t0).toFixed(1) + ' s', 'failed ' + fails.reduce((a, b) => a + b, 0),
          'rss ' + ($rss / 1024).toFixed(0) + ' MB (' + (($rss - $rss0) / $n).toFixed(1) + ' KB/conn)',
          'cpu ' + (v['task-clock'] / 10000).toFixed(2) + ' cores', (v['task-clock'] * 1000 / (rps * 10)).toFixed(2) + ' us/msg',
          Math.round(rps) + ' msg/s', 'p50 ' + worst('p50'), 'p99 ' + worst('p99') ].join(' | '));"
      rm -f /tmp/ccu-*.out /tmp/ccu-*.err
      sleep 5 # the last run's TIME_WAITs
    done
  done
done | tee "$OUT/ccu.log"
