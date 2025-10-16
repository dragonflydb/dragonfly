#!/usr/bin/env bash
set -euo pipefail

# list_crashes.sh — list AFL++ crash files for a target
# Usage:
#   ./list_crashes.sh                 # lists for default target "resp"
#   ./list_crashes.sh <target>        # lists for specified target (e.g. resp)
#   ./list_crashes.sh <crashes_dir>   # lists for a given crashes directory
#
# Notes:
# - Prints absolute file paths sorted by id.
# - Skips README.txt.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

print_info() {
  printf "[INFO] %s\n" "$1"
}

print_warn() {
  printf "[WARN] %s\n" "$1"
}

arg="${1:-}"

if [[ -z "$arg" ]]; then
  TARGET="resp"
  CRASHES_DIR="$PROJECT_ROOT/fuzz/artifacts/$TARGET/default/crashes"
elif [[ -d "$arg" ]]; then
  CRASHES_DIR="$(realpath "$arg")"
else
  TARGET="$arg"
  CRASHES_DIR="$PROJECT_ROOT/fuzz/artifacts/$TARGET/default/crashes"
fi

if [[ ! -d "$CRASHES_DIR" ]]; then
  print_warn "Crashes directory not found: $CRASHES_DIR"
  exit 1
fi

mapfile -t CRASH_FILES < <(find "$CRASHES_DIR" -maxdepth 1 -type f -name 'id:*' | sort)

if (( ${#CRASH_FILES[@]} == 0 )); then
  print_warn "No crash files in: $CRASHES_DIR"
  exit 2
fi

print_info "Crashes in $CRASHES_DIR:"
idx=1
for f in "${CRASH_FILES[@]}"; do
  abs="$(realpath "$f")"
  base="$(basename "$f")"
  # Extract fields from filename: id:XXXX,sig:YY,...
  id_part="$(sed -n 's/^id:\([^,]*\).*/\1/p' <<<"$base")"
  sig_part="$(sed -n 's/.*sig:\([^,]*\).*/\1/p' <<<"$base")"
  size_b="$(stat -c %s "$f" 2>/dev/null || echo "-")"
  printf "%3d) id:%s sig:%s size:%sB  %s\n" "$idx" "${id_part:-?}" "${sig_part:-?}" "$size_b" "$abs"
  ((idx++))
done

print_info "Total: $((idx-1)) crash file(s)."
print_info "Use: $SCRIPT_DIR/replay_gdb.sh <path-to-crash-file>"
