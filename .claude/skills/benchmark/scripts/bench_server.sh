#!/usr/bin/env bash
# Start / stop / check a server (Dragonfly binary or Valkey/Redis Docker) on a
# remote host. Two modes:
#
#   binary mode  — launches a local binary, tracks PID in /tmp/bench-server.pid
#   docker mode  — runs a named Docker container (bench-server); Docker manages
#                  lifecycle, no PID file needed
#
# Reliable shutdown matters: tmux kill-session does NOT reap the server process,
# and a stale server silently steals the port.
#
# Usage:
#   bench_server.sh start        <ssh> <binary_path> [maxmem] [port] [extra_env]
#   bench_server.sh start-docker <ssh> <image>       [maxmem] [port]
#   bench_server.sh stop         <ssh> [port]
#   bench_server.sh stop-docker  <ssh> [port]
#   bench_server.sh status       <ssh> [port]
#
# Examples:
#   bench_server.sh start        dev@HOST ~/projects/dragonfly/build-opt/dragonfly 27gb 6380
#   bench_server.sh start        dev@HOST ~/projects/dragonfly/build-opt/dragonfly 27gb 6380 'MIMALLOC_ALLOW_LARGE_OS_PAGES=0'
#   bench_server.sh start-docker dev@HOST valkey/valkey:9 27gb 6380
#   bench_server.sh stop         dev@HOST 6380
#   bench_server.sh stop-docker  dev@HOST 6380
set -euo pipefail

cmd=${1:?start|start-docker|stop|stop-docker|status}; tgt=${2:?ssh target}
SSH="ssh -o BatchMode=yes -o ConnectTimeout=15"

# Shared: build --maxmemory flag (called per-subcommand after args are parsed)
_mm() { [ -n "${1:-}" ] && echo "--maxmemory $1" || echo ""; }

case "$cmd" in
  start)
    bin=${3:?binary path}; maxmem=${4:-}; port=${5:-6380}; env_prefix=${6:-}
    $SSH "$tgt" "$env_prefix nohup $bin --conn_use_incoming_cpu --dbfilename= $(_mm "$maxmem") \
        --logtostderr --port $port >/tmp/bench-server.log 2>&1 & echo \$! > /tmp/bench-server.pid; sleep 3; \
        echo PID=\$(cat /tmp/bench-server.pid); redis-cli -p $port ping; \
        redis-cli -p $port info server | grep -iE 'dragonfly_version|redis_version' | head -2"
    ;;
  start-docker)
    image=${3:?docker image}; maxmem=${4:-}; port=${5:-6380}
    # Detect server binary name from image name (valkey → valkey-server, else redis-server)
    case "$image" in
      *[Vv]alkey*) srv_bin="valkey-server" ;;
      *)            srv_bin="redis-server" ;;
    esac
    # io-threads: computed on the remote side, capped at 8 (diminishing returns beyond that)
    $SSH "$tgt" "io_t=\$(n=\$(nproc); echo \$((n < 8 ? n : 8))); \
        docker rm -f bench-server 2>/dev/null || true; \
        docker run -d --name bench-server --network host $image \
          $srv_bin --port $port --save \"\" --loglevel notice $(_mm "$maxmem") --io-threads \$io_t; \
        sleep 3; docker ps --filter name=bench-server --format 'Status: {{.Status}}'; \
        redis-cli -p $port ping; \
        redis-cli -p $port info server | grep -iE 'redis_version|valkey_version' | head -2; \
        redis-cli -p $port config get io-threads"
    ;;
  stop)
    port=${3:-6380}
    $SSH "$tgt" "kill -9 \$(cat /tmp/bench-server.pid 2>/dev/null) 2>/dev/null || true; sleep 3; \
        pgrep -af 'dragonfly|valkey-server|redis-server' | grep -v 'pgrep\|bash -c' || echo 'stopped'; \
        redis-cli -p $port ping 2>&1 | head -1; free -h | head -2"
    ;;
  stop-docker)
    port=${3:-6380}
    $SSH "$tgt" "docker stop bench-server 2>/dev/null; docker rm bench-server 2>/dev/null || true; \
        redis-cli -p $port ping 2>&1 | head -1; free -h | head -2"
    ;;
  status)
    port=${3:-6380}
    $SSH "$tgt" "pgrep -af 'dragonfly|valkey-server|redis-server' | grep -v 'pgrep\|bash -c' || true; \
        docker ps --filter name=bench-server --format 'docker: {{.Status}}' 2>/dev/null || true; \
        redis-cli -p $port ping 2>&1 | head -1; \
        redis-cli -p $port info memory 2>/dev/null | grep -E '^used_memory:|^used_memory_rss:|^maxmemory:' || true"
    ;;
  *) echo "usage: $0 start|start-docker|stop|stop-docker|status <ssh_target> ..." >&2; exit 2;;
esac
