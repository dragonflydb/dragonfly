#!/usr/bin/env bash
set -euo pipefail

# replay_gdb.sh — reproduce an AFL++ crash under gdb with original-like environment
# Usage:
#   ./replay_gdb.sh <path-to-crash-file>
#
# Behavior:
# - Derives target name from crash path (.../artifacts/<target>/default/crashes/id:...)
# - Sets memory limit similar to fuzzing (-m 2048 by default; override via MEM_LIMIT_MB)
# - Runs gdb and feeds the crash input via stdin: "run < file"
# - Sets ASAN/UBSAN options to abort on error if sanitizers are present

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path-to-crash-file>" >&2
  exit 1
fi

CRASH_FILE_INPUT="$1"
if [[ ! -f "$CRASH_FILE_INPUT" ]]; then
  echo "Crash file not found: $CRASH_FILE_INPUT" >&2
  exit 1
fi

CRASH_FILE="$(realpath "$CRASH_FILE_INPUT")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Infer directories from crash file path
CRASHES_DIR="$(dirname "$CRASH_FILE")"           # .../default/crashes
DEFAULT_DIR="$(dirname "$CRASHES_DIR")"          # .../default
OUTPUT_DIR="$(dirname "$DEFAULT_DIR")"           # .../artifacts/<target>
TARGET="$(basename "$OUTPUT_DIR")"

BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-fuzz}"
FUZZ_TARGET="$BUILD_DIR/fuzz/${TARGET}_fuzz"

if [[ ! -x "$FUZZ_TARGET" ]]; then
  echo "Target binary not found or not executable: $FUZZ_TARGET" >&2
  echo "Build with: fuzz/build_fuzzer.sh (TARGET defaults to 'resp')." >&2
  exit 2
fi

# Memory limit: default 2048MB, can be overridden
MEM_LIMIT_MB="${MEM_LIMIT_MB:-2048}"
if [[ -f "$CRASHES_DIR/README.txt" ]]; then
  # Try to parse memory hint (best-effort). If parsing fails, keep default.
  parsed_gb=$(awk '/memory limit/ {for(i=1;i<=NF;i++) if($i=="was") print $(i+1)}' "$CRASHES_DIR/README.txt" | sed -E 's/GB//I') || true
  if [[ -n "${parsed_gb:-}" ]]; then
    # Convert GB (integer or decimal) to MB; fallback silently on failure
    if command -v python3 >/dev/null 2>&1; then
      MEM_LIMIT_MB=$(python3 - <<PY
v="$parsed_gb".strip()
try:
    print(int(float(v)*1024))
except Exception:
    pass
PY
)
    fi
  fi
fi

# Apply soft virtual memory limit similar to AFL++ (-m)
KB_LIMIT=$(( MEM_LIMIT_MB * 1024 ))
ulimit -Sv "$KB_LIMIT" || true

# Sanitizer options (harmless if not built with sanitizers)
export ASAN_OPTIONS="${ASAN_OPTIONS:-abort_on_error=1:detect_leaks=0:symbolize=1}"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:abort_on_error=1:print_stacktrace=1}"

# Change to the same working directory used by the fuzzer run so that logs go to the same place
cd "$OUTPUT_DIR"
echo "[INFO] CWD: $PWD"
echo "[INFO] Target: $FUZZ_TARGET"
echo "[INFO] Crash:  $CRASH_FILE"
echo "[INFO] MemLimit: ${MEM_LIMIT_MB}MB (ulimit -Sv ${KB_LIMIT})"

# Launch gdb. Keep interactive session; it will stop on SIGSEGV/SIGABRT and allow inspection.
exec gdb -q \
  --ex "set pagination off" \
  --ex "run < $CRASH_FILE" \
  --args "$FUZZ_TARGET"
