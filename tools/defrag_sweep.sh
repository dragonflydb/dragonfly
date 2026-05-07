#!/usr/bin/env bash
# run_defrag_sweep.sh — sweep multiplier × algorithm combinations
#
# Runs run_defrag_experiment.sh for every combination of MUL values and
# defrag modes (legacy, phased). Each run gets its own log and JSONL file
# under runs/sweep/. Runs are sequential (they share the same port).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Defaults (overridable via flags or environment) ──────────────────────────
SWEEP_DIR="${SWEEP_DIR:-${REPO_ROOT}/runs/sweep}"
CYCLES="${CYCLES:-100}"
MODES="${MODES:-legacy,phased}"     # comma-separated: legacy, phased, or both
# Default MUL series — override with -m "1 2 5" or MULS="1 2 5"
MULS_STR="${MULS:-1 2 5 10 15 20}"
# Extra flags forwarded to run_defrag_experiment.sh (e.g. --port 6380)
EXTRA_ARGS=()
# ─────────────────────────────────────────────────────────────────────────────

usage() {
    cat <<'HEADER'
Usage: defrag_sweep.sh [OPTIONS]

Run defrag_run.sh for every MUL × mode combination, capturing
stdout to per-run log files and JSONL results under runs/sweep/.
HEADER
    cat <<EOF

Options:
  -m, --muls "N ..."        Space-separated MUL values        [MULS="${MULS_STR}"]
  -c, --cycles NUM          Max defrag cycles per run          [CYCLES=${CYCLES}]
  -M, --modes LIST          Comma-separated modes              [MODES=${MODES}]
                             (legacy, phased, or legacy,phased)
  -d, --dir PATH            Output directory                   [SWEEP_DIR=${SWEEP_DIR}]
  -h, --help                Show this help

Any extra flags after '--' are forwarded to run_defrag_experiment.sh:
  defrag_sweep.sh -m "5 10" -- --port 6380 --workload uniform

Environment variables (MULS, CYCLES, MODES, SWEEP_DIR) work as alternatives
to flags. Flags take precedence.

Examples:
  ./tools/defrag_sweep.sh                           # default: 6 MULs × 2 modes
  ./tools/defrag_sweep.sh -m "5 10 20" -c 300      # custom MULs, 300 cycles
  ./tools/defrag_sweep.sh -M phased -m "15 20"     # phased only, two MULs
EOF
    exit 0
}

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)     usage ;;
        -m|--muls)     MULS_STR="$2"; shift 2 ;;
        -c|--cycles)   CYCLES="$2"; shift 2 ;;
        -M|--modes)    MODES="$2"; shift 2 ;;
        -d|--dir)      SWEEP_DIR="$2"; shift 2 ;;
        --)            shift; EXTRA_ARGS=("$@"); break ;;
        *)
            echo "error: unknown argument '$1' (use -- to pass flags to experiment script)"
            echo "run with --help for usage"
            exit 1 ;;
    esac
done

# Parse MULS_STR into array
read -ra MULS_ARR <<< "${MULS_STR}"

# Parse MODES into array of true/false values
MODES_ARR=()
IFS=',' read -ra MODE_NAMES <<< "${MODES}"
for m in "${MODE_NAMES[@]}"; do
    case "${m}" in
        phased) MODES_ARR+=("true") ;;
        legacy) MODES_ARR+=("false") ;;
        *)
            echo "error: unknown mode '${m}' (expected legacy or phased)"
            exit 1 ;;
    esac
done

mkdir -p "${SWEEP_DIR}"

total=$(( ${#MULS_ARR[@]} * ${#MODES_ARR[@]} ))
run=0

echo "=== DEFRAG SWEEP: ${#MULS_ARR[@]} multipliers × ${#MODES_ARR[@]} modes = ${total} runs ==="
echo "MULs:   ${MULS_ARR[*]}"
echo "modes:  ${MODES}"
echo "cycles: ${CYCLES}"
echo "dir:    ${SWEEP_DIR}"
echo ""

for mul in "${MULS_ARR[@]}"; do
    for mode in "${MODES_ARR[@]}"; do
        run=$((run + 1))
        label=$( [[ "${mode}" == "true" ]] && echo "phased" || echo "legacy" )
        logfile="${SWEEP_DIR}/mul${mul}_${label}.log"

        echo "────────────────────────────────────────────────────────────────"
        echo "[${run}/${total}] MUL=${mul} MODE=${label} → ${logfile}"
        echo "────────────────────────────────────────────────────────────────"

        export MUL="${mul}"
        export CYCLES
        export OUTPUT="${SWEEP_DIR}/mul${mul}_${label}.jsonl"

        if "${SCRIPT_DIR}/defrag_run.sh" "${mode}" "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" 2>&1 | tee "${logfile}"; then
            echo "[${run}/${total}] ✓ MUL=${mul} ${label} completed"
        else
            echo "[${run}/${total}] ✗ MUL=${mul} ${label} FAILED (see ${logfile})"
        fi

        echo ""
        sleep 2
    done
done

echo ""
echo "=== SWEEP COMPLETE: ${total} runs ==="
echo "logs:  ${SWEEP_DIR}/mul*_*.log"
echo "jsonl: ${SWEEP_DIR}/mul*_*.jsonl"
