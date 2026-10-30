#!/bin/bash
# The README's CPU and latency tables: uWebSockets.js against netcode-io by
# default (messages kept) and with transientMessages, 3 paired rounds.
# Then: node tables.mjs $OUT/matrix.json
set -e
. "$(dirname "$0")/common.sh"
cd "$BENCH"
GOMAXPROCS=4 node run.mjs --rounds "${ROUNDS:-3}" --warmup 30 --measure 10 \
  --transports uws,netcode-kept,netcode \
  --only '^(echo 500 @20/s|echo 2000 @20/s|echo 2000 @20/s tls|closed 500x1|broadcast 32x32 @4/s|broadcast 32x32 @2/s x3|size 1KiB 500 @20/s|size 16KiB 200 @20/s|size 16KiB 200 @20/s tls|size 64KiB 50 @20/s|size 64KiB 50 @20/s tls|size 16KiB closed 50x1)$' \
  --out "$OUT/matrix.json" 2>&1 | tee "$OUT/matrix.log"
