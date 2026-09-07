#!/usr/bin/env bash

# Exports inclusive and self-time CSVs for every top-level Tracy trace in parallel.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  s_tracy_export_parallel.sh <trace-dir> <out-dir> [trace-glob]

Exports inclusive and self-time CSV statistics for every *.tracy file directly
inside <trace-dir>. Each export runs in parallel. Output files are written to
<out-dir> as <trace-name>.inclusive.csv and <trace-name>.self.csv.

When provided, [trace-glob] selects matching trace filenames. It defaults to
*.tracy.

Requirements:
  tracy-csvexport must be available on PATH.

Examples:
  s_tracy_export_parallel.sh ~/workspace/dragonfly/traces ~/workspace/dragonfly/traces/csv
  s_tracy_export_parallel.sh . ./csv 'get_p100_1k_cap35_*.tracy'
EOF
}

export_csv() {
  local exporter="$1"
  local trace="$2"
  local output="$3"
  local mode="$4"
  local temporary_output="${output}.tmp.$$"

  if [[ "$mode" == "self" ]]; then
    "$exporter" -e "$trace" > "$temporary_output"
  else
    "$exporter" "$trace" > "$temporary_output"
  fi
  mv "$temporary_output" "$output"
  echo "Completed: $(basename "$output")"
}

main() {
  case "${1:-}" in
    -h|--help|help)
      usage
      return
      ;;
  esac

  [[ $# -ge 2 && $# -le 3 ]] || {
    usage >&2
    exit 2
  }

  local trace_dir="$1"
  local out_dir="$2"
  local trace_glob="${3:-*.tracy}"
  local exporter
  exporter="$(command -v tracy-csvexport || true)"

  [[ -n "$exporter" ]] || {
    echo "Error: tracy-csvexport is not available on PATH." >&2
    exit 1
  }

  [[ -d "$trace_dir" ]] || {
    echo "Error: Trace directory '$trace_dir' not found." >&2
    exit 1
  }

  local running_exporters
  running_exporters="$(pgrep -af '[t]racy-csvexport' || true)"
  [[ -z "$running_exporters" ]] || {
    echo "Error: tracy-csvexport is already running. Stop or wait for these jobs first:" >&2
    echo "$running_exporters" >&2
    exit 1
  }

  mkdir -p "$out_dir"

  local traces=()
  local trace
  shopt -s nullglob
  for trace in "$trace_dir"/$trace_glob; do
    traces+=("$trace")
  done
  shopt -u nullglob

  [[ ${#traces[@]} -gt 0 ]] || {
    echo "Error: No traces matching '$trace_glob' found directly in '$trace_dir'." >&2
    exit 1
  }

  echo "Exporter: $exporter"
  echo "Traces: ${#traces[@]}"
  echo "Pattern: $trace_glob"
  echo "Output: $out_dir"

  local pids=()
  local name
  for trace in "${traces[@]}"; do
    name="$(basename "${trace%.tracy}")"
    echo "Exporting: $name"

    export_csv "$exporter" "$trace" "$out_dir/$name.inclusive.csv" inclusive &
    pids+=("$!")
    export_csv "$exporter" "$trace" "$out_dir/$name.self.csv" self &
    pids+=("$!")
  done

  local status=0
  local pid
  for pid in "${pids[@]}"; do
    wait "$pid" || status=1
  done

  if [[ $status -ne 0 ]]; then
    echo "Error: One or more Tracy exports failed." >&2
    exit "$status"
  fi

  echo
  echo "Completed ${#pids[@]} exports:"
  ls -lh "$out_dir"
}

main "$@"
