#!/bin/sh
# Prepares a benchmark machine: Ubuntu 24.04, x86_64, as root. Toolchains,
# uWebSockets.js, and a machine that measures the same twice: see
# bench/README.md for why each setting is here. Run it once per boot (the
# CPU and sysctl settings don't survive a reboot), then sync.sh, then
# build.sh.
set -e
ROOT=${ROOT:-/root/netcode}
ZIG=${ZIG:-0.17.0}
NODE=${NODE:-22.19.0}
GO=${GO:-1.26.0}
UWS_TAG=${UWS_TAG:-v20.52.0}

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq rsync xz-utils curl git strace bpftrace linux-tools-common linux-tools-generic >/dev/null
apt-get install -y -qq "linux-tools-$(uname -r)" >/dev/null 2>&1 || true

cd /opt
[ -d "zig-x86_64-linux-$ZIG" ] || curl -fsSL "https://ziglang.org/download/$ZIG/zig-x86_64-linux-$ZIG.tar.xz" | tar xJ
ln -sf "/opt/zig-x86_64-linux-$ZIG/zig" /usr/local/bin/zig
[ -d "node-v$NODE-linux-x64" ] || curl -fsSL "https://nodejs.org/dist/v$NODE/node-v$NODE-linux-x64.tar.xz" | tar xJ
for b in node npm npx; do ln -sf "/opt/node-v$NODE-linux-x64/bin/$b" /usr/local/bin/$b; done
[ -d "go-$GO" ] || { mkdir "go-$GO" && curl -fsSL "https://go.dev/dl/go$GO.linux-amd64.tar.gz" | tar xz -C "go-$GO" --strip-components=1; }
ln -sf "/opt/go-$GO/bin/go" /usr/local/bin/go

# uWebSockets.js is published on GitHub, not npm
mkdir -p "$ROOT/uws"
[ -d "$ROOT/uws/node_modules/uWebSockets.js" ] || npm install --prefix "$ROOT/uws" --no-audit --no-fund "uNetworking/uWebSockets.js#$UWS_TAG" >/dev/null

# perf may read hardware counters and kernel symbols
sysctl -qw kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0
# A fixed clock with no deep sleep: powersave idles at 800 MHz, and waking
# from C6 put ~75 us into a 31 us round trip.
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance > "$g"; done
[ -f /sys/devices/system/cpu/intel_pstate/no_turbo ] && echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
for f in /sys/devices/system/cpu/cpu*/cpuidle/state[1-9]*/disable; do echo 1 > "$f"; done
# Socket buffers (quic-go asks for 7 MB of UDP receive buffer), a listen
# queue that holds a connection ramp, and client ports apart from servers'
sysctl -qw net.core.rmem_max=16777216 net.core.wmem_max=16777216
sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535
sysctl -qw net.ipv4.ip_local_port_range="20000 65535"
# A host firewall tracks every loopback connection too: two 100k-connection
# runs back to back filled the default table and dropped SYNs for minutes.
sysctl -qw net.netfilter.nf_conntrack_max=2097152 2>/dev/null || true

echo "zig $(zig version), node $(node -v), $(go version | cut -d' ' -f3), $(nproc) cpus:$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
perf stat -e instructions,cycles -- true 2>&1 | grep -E "instructions|cycles|supported" | head -2
