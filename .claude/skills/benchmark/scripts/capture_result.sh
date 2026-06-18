#!/usr/bin/env bash
# Capture one write run's result and ENFORCE the zero-error rule. Pulls the
# dfly_bench summary (QPS/P99/errors) from the client log and the server's
# INFO memory + DBSIZE, saves them under <outdir>, and exits non-zero if the run
# had ANY eviction/OOM errors — because those contaminate QPS, latency, and
# bytes-per-entry, so the run must be discarded and re-run at a lower key count.
#
# Usage:
#   capture_result.sh <server_ssh> <client_ssh> <client_log_path> <outdir> [label] [port]
#
# Example:
#   capture_result.sh dev@SERVER dev@CLIENT /tmp/prebuilt_write.log /tmp/bench-run/prebuilt prebuilt 6380
set -uo pipefail
srv=${1:?server ssh}; cli=${2:?client ssh}; log=${3:?client log path}; out=${4:?outdir}
label=${5:-run}; port=${6:-6380}
SSH="ssh -o BatchMode=yes -o ConnectTimeout=12"
mkdir -p "$out"

$SSH "$cli" "cat $log" > "$out/write.log"
$SSH "$srv" "redis-cli -p $port info memory" > "$out/info_memory.txt"
dbsize=$($SSH "$srv" "redis-cli -p $port dbsize")
# OS-level host memory — used_memory_rss alone misses allocator arena waste
# (visible as large discrepancy between INFO rss and OS `free`, e.g. with huge pages).
read -r host_free host_total < <($SSH "$srv" 'free -b | awk "/^Mem:/{print \$4, \$2}"')
$SSH "$srv" 'free -h' > "$out/host_memory.txt"
host_used_pct=$(awk "BEGIN{printf \"%.1f\", ($host_total-$host_free)/$host_total*100}")

summary=$(grep -E 'Total time' "$out/write.log" | tail -1)
errline=$(grep -iE 'error responses' "$out/write.log" | tail -1 || true)
errs=$(echo "$errline" | grep -oE '[0-9]+' | head -1)
errs=${errs:-0}

echo "=== $label ==="
echo "$summary"
echo "DBSIZE: $dbsize"
grep -E '^used_memory:|^used_memory_rss:|^maxmemory:' "$out/info_memory.txt"
echo "host_free: $(awk "BEGIN{printf \"%.2f\", $host_free/2^30}")GiB  host_used: ${host_used_pct}%  (see $out/host_memory.txt)"

if [ "$errs" -gt 0 ]; then
  echo "!! INVALID: $errs eviction/OOM error responses — DISCARD this run."
  echo "   Cut --key_maximum ~10-15% and re-run until errors are zero."
  exit 4
fi
echo "OK: zero errors — run is clean."
