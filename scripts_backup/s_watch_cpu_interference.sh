#!/usr/bin/env bash
# Usage: watch_cpu_interference.sh [observed_cpu=32] [interval_seconds=1] [output_file]
#
# Start this before the benchmark and stop it afterwards with the PID it prints.

set -euo pipefail

observed_cpu=${1:-32}
interval_seconds=${2:-1}
output_file=${3:-"cpu${observed_cpu}-interference-$(date +%Y%m%d-%H%M%S).log"}
monitor_cpu=${MONITOR_CPU:-0}

if [[ ! $observed_cpu =~ ^[0-9]+$ ]] || [[ ! $monitor_cpu =~ ^[0-9]+$ ]]; then
  echo "CPU arguments must be non-negative integers" >&2
  exit 2
fi

if [[ ! -r /proc/interrupts ]] || [[ ! -r /proc/softirqs ]]; then
  echo "This script must run on the server Linux host." >&2
  exit 2
fi

if command -v taskset >/dev/null && [[ $monitor_cpu != "$observed_cpu" ]]; then
  taskset -pc "$monitor_cpu" "$$" >/dev/null || \
    echo "warning: could not pin sampler PID $$ to CPU $monitor_cpu" >&2
fi

cleanup() {
  echo "$(date -Is) watcher stopped pid=$$" >>"$output_file"
  rm -f "$previous_snapshot" "$current_snapshot"
}

snapshot_counters() {
  awk -v observed_cpu="$observed_cpu" '
    NR == 1 {
      for (column = 1; column <= NF; ++column)
        if ($column == "CPU" observed_cpu)
          cpu_column = column
      next
    }
    cpu_column && $cpu_column ~ /^[0-9]+$/ { print "irq", $1, $cpu_column }
    END {
      if (!cpu_column) {
        print "missing CPU" observed_cpu " column in /proc/interrupts" > "/dev/stderr"
        exit 1
      }
    }
  ' /proc/interrupts

  awk -v observed_cpu="$observed_cpu" '
    NR == 1 {
      cpu_column = observed_cpu + 2
      next
    }
    $cpu_column ~ /^[0-9]+$/ { print "softirq", $1, $cpu_column }
    END {
      if (!cpu_column) {
        print "missing CPU" observed_cpu " column in /proc/softirqs" > "/dev/stderr"
        exit 1
      }
    }
  ' /proc/softirqs

  awk -v observed_cpu="$observed_cpu" '
    $1 == "cpu" observed_cpu {
      found_cpu = 1
      printf "cpu_time state"
      for (field = 2; field <= NF; ++field)
        printf " %s", $field
      print ""
    }
    END {
      if (!found_cpu) {
        print "missing cpu" observed_cpu " row in /proc/stat" > "/dev/stderr"
        exit 1
      }
    }
  ' /proc/stat
  awk -v observed_cpu="$observed_cpu" '
    $1 == "cpu" observed_cpu {
      found_cpu = 1
      printf "schedstat state"
      for (field = 2; field <= NF; ++field)
        printf " %s", $field
      print ""
    }
    END {
      if (!found_cpu) {
        print "missing cpu" observed_cpu " row in /proc/schedstat" > "/dev/stderr"
        exit 1
      }
    }
  ' /proc/schedstat
}

emit_delta() {
  awk '
    NR == FNR { previous[$1 FS $2] = $0; next }
    {
      key = $1 FS $2
      if (key in previous) {
        split(previous[key], old, FS)
        if ($1 == "cpu_time" || $1 == "schedstat") {
          printf "%s", $1
          for (field = 3; field <= NF; ++field) {
            value = $field - old[field]
            printf " %d", value
          }
          print ""
        } else if ($3 - old[3] > 0) {
          print $1, $2, $3 - old[3]
        }
      }
    }
  ' "$previous_snapshot" "$current_snapshot"
}

emit_static_routing() {
  echo "=== static routing ==="
  echo "online_cpus=$(cat /sys/devices/system/cpu/online 2>/dev/null || true)"
  echo "observed_cpu=$observed_cpu monitor_cpu=$monitor_cpu"
  echo "-- RPS maps --"
  for rps_map in /sys/class/net/*/queues/rx-*/rps_cpus; do
    [[ -r $rps_map ]] || continue
    printf '%s=%s\n' "$rps_map" "$(cat "$rps_map")"
  done
  echo "-- IRQ affinity lists containing CPU $observed_cpu --"
  for affinity in /proc/irq/*/smp_affinity_list; do
    [[ -r $affinity ]] || continue
    if awk -v observed_cpu="$observed_cpu" '
      {
        count = split($0, ranges, ",")
        for (range_index = 1; range_index <= count; ++range_index) {
          split(ranges[range_index], limits, "-")
          if ((length(limits) == 1 && limits[1] == observed_cpu) ||
              (length(limits) == 2 && limits[1] <= observed_cpu && observed_cpu <= limits[2]))
            exit 0
        }
        exit 1
      }
    ' "$affinity"; then
      printf '%s=%s\n' "$affinity" "$(cat "$affinity")"
    fi
  done
  echo "=== samples ==="
}

emit_processes_on_cpu() {
  ps -eLo pid=,tid=,psr=,pcpu=,stat=,comm= --sort=-pcpu |
    awk -v observed_cpu="$observed_cpu" '$3 == observed_cpu { print "task", $0 }'
}

emit_host_state() {
  local frequency_path="/sys/devices/system/cpu/cpu${observed_cpu}/cpufreq/scaling_cur_freq"

  printf 'loadavg %s\n' "$(cat /proc/loadavg)"
  printf 'pressure_cpu %s\n' "$(tr '\n' ';' </proc/pressure/cpu)"
  if [[ -r $frequency_path ]]; then
    printf 'frequency_khz %s\n' "$(cat "$frequency_path")"
  fi
  emit_processes_on_cpu
}

mkdir -p "$(dirname "$output_file")"
output_file=$(readlink -f "$output_file")
previous_snapshot=$(mktemp)
current_snapshot=$(mktemp)
trap 'exit 0' INT TERM
trap cleanup EXIT

{
  echo "watcher_pid=$$"
  echo "output_file=$output_file"
  echo "started_at=$(date -Is)"
  echo "Stop after the benchmark with: kill $$"
  emit_static_routing
} | tee "$output_file"

snapshot_counters >"$previous_snapshot"
while true; do
  sleep "$interval_seconds"
  snapshot_counters >"$current_snapshot"
  {
    echo "timestamp=$(date -Is)"
    emit_delta
    emit_host_state
    echo
  } >>"$output_file"
  mv "$current_snapshot" "$previous_snapshot"
  current_snapshot=$(mktemp)
done
