#!/bin/sh
# From a development machine: copies this checkout and quic-zig's, working
# trees as they are, to the benchmark machine.
#   SSH="ssh -i ~/.ssh/bench" bench/baremetal/sync.sh <host> [path to quic-zig]
set -e
HOST=root@$1
HERE=$(cd "$(dirname "$0")/../.." && pwd)
QUIC=${2:-$HERE/../quic-zig}
ROOT=${ROOT:-/root/netcode}
SSH=${SSH:-ssh}
$SSH "$HOST" mkdir -p "$ROOT/quic-zig/interop/certs" "$ROOT/netcode-io"
X="--exclude .git --exclude .zig-cache --exclude zig-out --exclude node_modules"
rsync -az -e "$SSH" $X --exclude /interop --exclude /bench "$QUIC/" "$HOST:$ROOT/quic-zig/"
# the default cert_path of quic-zig's own servers
rsync -az -e "$SSH" "$QUIC/interop/certs/" "$HOST:$ROOT/quic-zig/interop/certs/"
rsync -az -e "$SSH" $X --exclude 'bench/netcode-*' --exclude bench/wsload/wsload \
  --exclude bench/wtload/wtload --exclude bench/wtload/wtecho "$HERE/" "$HOST:$ROOT/netcode-io/"
