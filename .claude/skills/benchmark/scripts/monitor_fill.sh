#!/usr/bin/env bash
# Watch a running write fill memory and ABORT before OOM. Polls the server's
# used_memory_rss (the accurate signal — not `free` available, which page cache
# inflates) and the live DBSIZE, prints a line per sample, and kills the client
# bench tmux session if RSS crosses the abort threshold. Exits when the write
# finishes (client tmux session gone) or on abort.
#
# Usage:
#   monitor_fill.sh <server_ssh> <client_ssh> [tmux_session] [abort_pct] [interval_s] [max_polls] [port]
#
# Example:
#   monitor_fill.sh dev@SERVER dev@CLIENT bench 92 10 60 6380
#
# Emits poll lines like: "poll 7: RUN dbsize=172018929 rss=18.37GiB (60%)"
# so the output doubles as the data for scripts/plot_fill.py.
#
# Done-detection: checks whether dfly_bench or memtier_benchmark is running on
# the client (pgrep), not just whether the tmux session exists. The tmux session
# outlives the bench process, so has-session alone gives false "RUN" forever.
set -uo pipefail

srv=${1:?server ssh target}; cli=${2:?client ssh target}
sess=${3:-bench}; abort=${4:-92}; iv=${5:-10}; maxp=${6:-60}; port=${7:-6380}
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10"

total=$($SSH "$srv" 'free -b | awk "/^Mem:/{print \$2}"')
echo "host total bytes=$total, abort at ${abort}% RSS"

for i in $(seq 1 "$maxp"); do
  sleep "$iv"
  state=$($SSH "$cli" "if pgrep -x dfly_bench >/dev/null 2>&1 || \
      pgrep -f '(^|/)[m]emtier_benchmark([[:space:]]|$)' >/dev/null 2>&1; then \
      echo RUN; else echo DONE; fi")
  read -r db rss host_used host_free < <($SSH "$srv" "D=\$(redis-cli -p $port dbsize); \
      R=\$(redis-cli -p $port info memory | grep '^used_memory_rss:' | cut -d: -f2 | tr -d '\r'); \
      HU=\$(free -b | awk '/^Mem:/{print \$3}'); HF=\$(free -b | awk '/^Mem:/{print \$4}'); echo \$D \$R \$HU \$HF")
  pct=$(awk "BEGIN{printf \"%.0f\", $rss/$total*100}")
  rssg=$(awk "BEGIN{printf \"%.2f\", $rss/2^30}")
  host_used_g=$(awk "BEGIN{printf \"%.2f\", $host_used/2^30}")
  host_free_g=$(awk "BEGIN{printf \"%.2f\", $host_free/2^30}")
  echo "poll $i: $state dbsize=$db rss=${rssg}GiB (${pct}%) host_used=${host_used_g}GiB host_free=${host_free_g}GiB"
  if [ "$pct" -ge "$abort" ]; then
    echo "!! ABORT: RSS ${pct}% >= ${abort}% — killing write"
    $SSH "$cli" "tmux kill-session -t $sess 2>/dev/null" || true
    exit 3
  fi
  [ "$state" = "DONE" ] && { echo "write finished"; exit 0; }
done
echo "monitor: max polls reached, write still running"; exit 2
