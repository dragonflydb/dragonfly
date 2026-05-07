#!/usr/bin/env bash
# run_defrag_experiment.sh — single defrag experiment: start → fragment → defrag → report
#
# Starts a pinned single-shard Dragonfly, creates heap fragmentation with
# defrag_baseline.py, then repeatedly calls MEMORY DEFRAGMENT via defrag_drive.py
# until arena waste drops below a target percentage (or max cycles is reached).
# Results are written as JSONL for later analysis.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TOOLS_DIR="${REPO_ROOT}/tools"
RUNS_DIR="${REPO_ROOT}/runs"

# ── Defaults (all overridable via flags or environment variables) ─────────────
DRAGONFLY="${DRAGONFLY:-${REPO_ROOT}/build-opt/dragonfly}"
PORT="${PORT:-6379}"
WORKLOAD="${WORKLOAD:-wide}"        # uniform | wide
MUL="${MUL:-20}"                    # scale key count by this multiplier
CYCLES="${CYCLES:-100}"             # max MEMORY DEFRAGMENT calls
TARGET_WASTE="${TARGET_WASTE:-5}"   # stop when arena waste_pct <= this %
SLEEP_MS="${SLEEP_MS:-200}"         # ms between defrag calls
LOG_PATH="/tmp/dragonfly.INFO"
OUTPUT=""                           # set after mode is known
# Single core each — keeps logs single-shard and reproducible.
DF_CORES=1
CLIENT_CORES=1
# ─────────────────────────────────────────────────────────────────────────────

usage() {
    cat <<'HEADER'
Usage: defrag_run.sh <phased|legacy> [OPTIONS]

Run one defrag experiment end-to-end:
  1. Start a single-shard Dragonfly (CPU-pinned to core 0)
  2. Create fragmentation baseline (defrag_baseline.py)
  3. Drive MEMORY DEFRAGMENT and record results (defrag_drive.py)
HEADER
    cat <<EOF

Modes:
  phased   4-phase algorithm (CENSUS → SELECT_TARGETS → EVACUATE → VERIFY)
  legacy   Single-pass incremental reclaim

Options (also settable via environment variables):
  -m, --mul NUM         Key-count multiplier              [MUL=${MUL}]
  -c, --cycles NUM      Max defrag cycles                 [CYCLES=${CYCLES}]
  -w, --workload NAME   Workload profile (uniform|wide)   [WORKLOAD=${WORKLOAD}]
  -p, --port NUM        Dragonfly listen port             [PORT=${PORT}]
  -t, --target NUM      Stop when waste_pct <= NUM%       [TARGET_WASTE=${TARGET_WASTE}]
  -s, --sleep-ms NUM    Sleep between cycles (ms)         [SLEEP_MS=${SLEEP_MS}]
  -o, --output PATH     JSONL output file                 [auto: runs/<mode>_<workload>_mul<N>.jsonl]
  -b, --binary PATH     Dragonfly binary                  [DRAGONFLY=${DRAGONFLY}]
  -h, --help            Show this help

Examples:
  ./tools/defrag_run.sh phased
  ./tools/defrag_run.sh legacy -m 10 -c 200
  MUL=5 CYCLES=50 ./tools/defrag_run.sh phased
EOF
    exit 0
}

# ── Argument parsing ──────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    usage
fi

# First pass: find mode (first positional arg)
EXPERIMENTAL=""
MODE=""
POSITIONAL_SET=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)     usage ;;
        -m|--mul)          MUL="$2"; shift 2 ;;
        -c|--cycles)       CYCLES="$2"; shift 2 ;;
        -w|--workload)     WORKLOAD="$2"; shift 2 ;;
        -p|--port)         PORT="$2"; shift 2 ;;
        -t|--target)       TARGET_WASTE="$2"; shift 2 ;;
        -s|--sleep-ms)     SLEEP_MS="$2"; shift 2 ;;
        -o|--output)       OUTPUT="$2"; shift 2 ;;
        -b|--binary)       DRAGONFLY="$2"; shift 2 ;;
        phased)
            EXPERIMENTAL="true"; MODE="phased"; POSITIONAL_SET=true; shift ;;
        legacy)
            EXPERIMENTAL="false"; MODE="legacy"; POSITIONAL_SET=true; shift ;;
        true)
            EXPERIMENTAL="true"; MODE="phased"; POSITIONAL_SET=true; shift ;;
        false)
            EXPERIMENTAL="false"; MODE="legacy"; POSITIONAL_SET=true; shift ;;
        *)
            echo "error: unknown argument '$1' (expected phased|legacy or an option flag)"
            echo "run with --help for usage"
            exit 1 ;;
    esac
done

if [[ "${POSITIONAL_SET}" == "false" ]]; then
    echo "error: mode argument required (phased or legacy)"
    echo "run with --help for usage"
    exit 1
fi

OUTPUT="${OUTPUT:-${RUNS_DIR}/${MODE}_${WORKLOAD}_mul${MUL}.jsonl}"

# ── CPU pinning ───────────────────────────────────────────────────────────────
# Dragonfly on core 0, client on core 1. Single-shard makes logs trivial to read.
DF_CORE_LIST="0"
CLIENT_CORE_LIST="1"
echo "cpu_pinning: dragonfly=${DF_CORE_LIST} (${DF_CORES} cores)  client=${CLIENT_CORE_LIST} (${CLIENT_CORES} cores)"

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [[ ! -x "${DRAGONFLY}" ]]; then
    echo "error: dragonfly binary not found or not executable: ${DRAGONFLY}"
    echo "       build it with: ninja -C build-opt dragonfly"
    exit 1
fi

if ! command -v redis-cli &>/dev/null; then
    echo "error: redis-cli not found in PATH (needed for readiness check)"
    exit 1
fi

# Check nothing is already accepting connections on the port
if redis-cli -p "${PORT}" ping 2>/dev/null | grep -q PONG; then
    echo "error: port ${PORT} is already in use — is dragonfly or another server running?"
    echo "       stop it first, or change PORT in this script"
    exit 1
fi

mkdir -p "${RUNS_DIR}"

# aioredis 2.x has a duplicate-base-class bug on Python 3.12+ where
# asyncio.TimeoutError became an alias for builtins.TimeoutError.
# Patch the installed exceptions.py in-place without touching the script.
python3 - <<'PYEOF'
import sys, importlib.util, pathlib

spec = importlib.util.find_spec("aioredis")
if spec is None:
    print("error: aioredis not installed", file=sys.stderr)
    sys.exit(1)

exc_file = pathlib.Path(spec.origin).parent / "exceptions.py"
original = exc_file.read_text()
old = "class TimeoutError(asyncio.TimeoutError, builtins.TimeoutError, RedisError):"
new = "class TimeoutError(*dict.fromkeys([asyncio.TimeoutError, builtins.TimeoutError]), RedisError):"
if old in original:
    exc_file.write_text(original.replace(old, new))
    print("patched aioredis/exceptions.py for Python 3.12 compatibility")

# Verify it actually imports now.
try:
    import aioredis  # noqa: F401
except Exception as e:
    print(f"error: aioredis still broken after patch: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF

echo "=== defrag experiment: mode=${MODE}  workload=${WORKLOAD}  mul=${MUL}x ==="

# ── Step 1: Start Dragonfly ───────────────────────────────────────────────────
echo ""
echo "[1/3] starting dragonfly (mode=${MODE}) on port ${PORT} ..."

EXTRA_FLAGS=(
    # Disable the per-shard minimum-reclaimable guard so EVACUATE runs even
    # when per-shard fragmentation is small (e.g. 433MiB / 14 shards ≈ 30MiB,
    # which would be blocked by the default 64MiB threshold).
    --defrag_min_plan_reclaimable_bytes=0
    # Disable RDB snapshot on shutdown to avoid "direct I/O not supported"
    # warnings on encrypted filesystems.
    --dbfilename ""
)

taskset -c "${DF_CORE_LIST}" \
"${DRAGONFLY}" \
    --alsologtostderr \
    --experimental_defrag="${EXPERIMENTAL}" \
    --enable_bg_defrag=false \
    --proactor_threads="${DF_CORES}" \
    --port="${PORT}" \
    "${EXTRA_FLAGS[@]}" &
DF_PID=$!

cleanup() {
    echo ""
    echo "stopping dragonfly (pid=${DF_PID}) ..."
    kill "${DF_PID}" 2>/dev/null || true
    # Give it up to 5s to exit cleanly, then SIGKILL
    for _i in $(seq 1 10); do
        sleep 0.5
        if ! kill -0 "${DF_PID}" 2>/dev/null; then
            break
        fi
        if [[ ${_i} -eq 10 ]]; then
            echo "dragonfly did not exit cleanly, sending SIGKILL ..."
            kill -9 "${DF_PID}" 2>/dev/null || true
        fi
    done
    wait "${DF_PID}" 2>/dev/null || true
    # Verify the port is free before returning
    for _i in $(seq 1 10); do
        if ! redis-cli -p "${PORT}" ping 2>/dev/null | grep -q PONG; then
            echo "port ${PORT} is free"
            return
        fi
        sleep 0.5
    done
    echo "warning: port ${PORT} may still be in use after shutdown"
}
trap cleanup EXIT

echo "waiting for dragonfly to be ready ..."
for i in $(seq 1 30); do
    if redis-cli -p "${PORT}" ping 2>/dev/null | grep -q PONG; then
        echo "dragonfly ready (${i} probes)"
        break
    fi
    if [[ ${i} -eq 30 ]]; then
        echo "error: dragonfly did not respond within 15s"
        exit 1
    fi
    sleep 0.5
done

# ── Step 2: Create fragmentation baseline ────────────────────────────────────
echo ""
echo "[2/3] creating fragmentation baseline (workload=${WORKLOAD} mul=${MUL}x) ..."
taskset -c "${CLIENT_CORE_LIST}" \
python3 "${TOOLS_DIR}/defrag_baseline.py" \
    --workload "${WORKLOAD}" \
    --mul      "${MUL}"      \
    --port     "${PORT}"     \
    --arena

# ── Step 3: Drive defrag ─────────────────────────────────────────────────────
echo ""
echo "[3/3] driving defrag (cycles=${CYCLES} target-waste=${TARGET_WASTE}% output=${OUTPUT}) ..."
taskset -c "${CLIENT_CORE_LIST}" \
python3 "${TOOLS_DIR}/defrag_drive.py" \
    --cycles       "${CYCLES}"       \
    --target-waste "${TARGET_WASTE}" \
    --sleep-ms     "${SLEEP_MS}"     \
    --log-path     "${LOG_PATH}"     \
    --port         "${PORT}"         \
    --output       "${OUTPUT}"

echo ""
echo "=== done: results written to ${OUTPUT} ==="
