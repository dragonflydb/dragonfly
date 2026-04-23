#!/bin/bash
set -eo pipefail

# Usage: ./bench_v2.sh [binary] [mode] [runs]
#   binary: path to dragonfly binary (default: ./build-opt/dragonfly)
#   mode:   throughput | fragmentation | all (default: all)
#   runs:   how many times to repeat the full benchmark (default: 3)
#           If runs > 1, a final averaged report is printed at the end.
#
# Modes:
#   throughput     - 50 clients, 2KB SET, heavy saturation.
#                    Tests peak RPS and confirms V2 doesn't regress under load.
#   fragmentation  - 1 client, 2KB SET, max_busy_read_usec=50000.
#                    Exposes V2's per-fragment flush vs V1's busy-read batching.
#                    Mirrors test_reply_count conditions.
#   all            - Run both modes sequentially.

DFLY_BIN=${1:-"./build-opt/dragonfly"}
MODE=${2:-"all"}
RUNS=${3:-3}
PORT=6379
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="bench_v2_${MODE}_${GIT_SHA}_${TIMESTAMP}.log"
ACCUM_FILE=$(mktemp)
trap 'rm -f "$ACCUM_FILE"' EXIT

# Number of CPU cores available; pin dragonfly to first 2, memtier to next 2 if possible.
NUM_CPUS=$(nproc)
DFLY_CPUS="0,1"
MEMTIER_CPUS="2,3"
if [[ $NUM_CPUS -le 2 ]]; then
    DFLY_TASKSET=""
    MEMTIER_TASKSET=""
else
    DFLY_TASKSET="taskset -c ${DFLY_CPUS}"
    MEMTIER_TASKSET="taskset -c ${MEMTIER_CPUS}"
fi

wait_for_server() {
    local timeout=${1:-10}
    local elapsed=0
    while ! redis-cli -p $PORT ping > /dev/null 2>&1; do
        sleep 0.2
        elapsed=$(( elapsed + 1 ))
        if [[ $elapsed -ge $(( timeout * 5 )) ]]; then
            echo "[!] Error: Dragonfly did not become ready within ${timeout}s."
            return 1
        fi
    done
}

get_send_count() {
    local raw
    raw=$(curl -s "http://127.0.0.1:${PORT}/metrics" 2>/dev/null) || true
    local val
    val=$(echo "$raw" | grep '^dragonfly_reply_total' | awk '{sum += $2} END {print (sum ? sum : 0)}') || true
    echo "${val:-0}"
}

# run_bench <v2_flag> <label> <threads> <clients> <data_size> <pipelines...> <extra_dfly_flags...>
run_bench() {
    local v2_flag=$1; shift
    local label=$1; shift
    local threads=$1; shift
    local clients=$1; shift
    local data_size=$1; shift
    local mode_name=$1; shift
    # Remaining positional args: pipeline sizes
    local pipelines=()
    while [[ $# -gt 0 && "$1" != "--" ]]; do
        pipelines+=("$1"); shift
    done
    [[ "${1:-}" == "--" ]] && shift
    # Remaining: extra dragonfly flags
    local extra_flags=("$@")

    echo ""
    echo ">>> Starting Dragonfly [${label}] mode=${mode_name} (io_loop_v2=${v2_flag})"
    $DFLY_TASKSET "$DFLY_BIN" \
        --proactor_threads=2 \
        --experimental_io_loop_v2="${v2_flag}" \
        --port=$PORT \
        "${extra_flags[@]}" \
        > /dev/null 2>&1 &
    DFLY_PID=$!

    if ! wait_for_server 10; then
        kill "$DFLY_PID" 2>/dev/null || true
        exit 1
    fi

    redis-cli -p $PORT PING > /dev/null

    if ! command -v memtier_benchmark > /dev/null 2>&1; then
        echo "[!] memtier_benchmark not found in PATH."
        kill "$DFLY_PID" 2>/dev/null || true
        exit 1
    fi

    RESULTS_TMP=$(mktemp)
    echo -e "PIPELINE\tRPS\tAVG_LAT(ms)\tSEND_SYSCALLS" > "$RESULTS_TMP"

    for PIPELINE in "${pipelines[@]}"; do
        echo "  [+] pipeline=$PIPELINE ..."

        SENDS_BEFORE=$(get_send_count)

        OUTPUT=$($MEMTIER_TASKSET memtier_benchmark \
            -s 127.0.0.1 \
            -p $PORT \
            -t "$threads" \
            -c "$clients" \
            --pipeline="$PIPELINE" \
            --ratio=1:0 \
            -d "$data_size" \
            --key-pattern=R:R \
            --key-prefix=bench \
            -n 30000 \
            --hide-histogram 2>&1) || true

        SENDS_AFTER=$(get_send_count)
        SEND_DELTA=$(( SENDS_AFTER - SENDS_BEFORE ))

        RPS=$(echo "$OUTPUT"     | grep "^Totals" | awk '{print $2}')
        LATENCY=$(echo "$OUTPUT" | grep "^Totals" | awk '{print $5}')
        RPS=${RPS:-"Error"}
        LATENCY=${LATENCY:-"Error"}

        echo -e "$PIPELINE\t$RPS\t$LATENCY\t$SEND_DELTA" >> "$RESULTS_TMP"
        echo "${label}|${mode_name}|${PIPELINE}|${RPS}|${LATENCY}|${SEND_DELTA}" >> "$ACCUM_FILE"
    done

    kill "$DFLY_PID" 2>/dev/null || true
    wait "$DFLY_PID" 2>/dev/null || true

    echo ""
    echo "====================================================="
    printf "  %-4s  %s  (commit: %s)\n" "$label" "$mode_name" "$GIT_SHA"
    echo "====================================================="
    column -t -s $'\t' "$RESULTS_TMP"
    echo "====================================================="
    rm "$RESULTS_TMP"
}

print_final_report() {
    [[ $RUNS -le 1 ]] && return
    echo ""
    echo "======================================================"
    echo "  MEDIAN RESULTS ($RUNS runs, commit: $GIT_SHA)"
    echo "======================================================"
    awk -F'|' '
    function get_median(bk, n,    i, j, tmp, arr) {
        if (n == 0) return 0
        for (i = 1; i <= n; i++) arr[i] = vals[bk, i]
        for (i = 1; i < n; i++)
            for (j = i + 1; j <= n; j++)
                if (arr[j] < arr[i]) { tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp }
        return arr[int((n + 1) / 2)]
    }
    {
        key = $1 SUBSEP $2 SUBSEP $3
        if (!(key in seen)) {
            seen[key] = 1
            order[++n] = key
            lbl[key] = $1; mod[key] = $2; pip[key] = $3
        }
        if ($4+0 > 0) { rps_n[key]++; vals["r" SUBSEP key, rps_n[key]] = $4 }
        if ($5+0 > 0) { lat_n[key]++; vals["l" SUBSEP key, lat_n[key]] = $5 }
        sys_sum[key] += $6; sys_cnt[key]++
    }
    END {
        prev_group = ""
        for (i = 1; i <= n; i++) {
            k = order[i]
            g = lbl[k] SUBSEP mod[k]
            if (g != prev_group) {
                if (prev_group != "") print "====================================================="
                print ""
                printf "  %-4s  %s\n", lbl[k], mod[k]
                print "====================================================="
                print "PIPELINE\tRPS\tAVG_LAT(ms)\tSEND_SYSCALLS"
                prev_group = g
            }
            rps_med = get_median("r" SUBSEP k, rps_n[k])
            lat_med = get_median("l" SUBSEP k, lat_n[k])
            printf "%s\t%.2f\t%.5f\t%.0f\n", pip[k], rps_med, lat_med, sys_sum[k]/sys_cnt[k]
        }
        print "====================================================="
    }
    ' "$ACCUM_FILE" | column -t -s $'\t'
}

# ---------- MODE: throughput ----------
# 50 concurrent clients saturate the server.  Both V1 and V2 batch well.
# Purpose: regression-guard for peak RPS; confirms V2 doesn't lose throughput.
run_throughput() {
    run_bench false "V1" 2 25 2048 "throughput" 1 10 100 500
    run_bench true  "V2" 2 25 2048 "throughput" 1 10 100 500
}

# ---------- MODE: fragmentation ----------
# 1 client, large payloads, max_busy_read_usec=50000.
# Mirrors test_reply_count: V1's read fiber spin-accumulates fragments for 50ms
# while V2 flushes after each partial read.
# Purpose: expose V2's per-fragment unconditional flush penalty.
run_fragmentation() {
    run_bench false "V1" 1 1 2048 "fragmentation" 1 10 100 500 -- --max_busy_read_usec=50000
    run_bench true  "V2" 1 1 2048 "fragmentation" 1 10 100 500 -- --max_busy_read_usec=50000
}

# ---------- Main ----------
run_selected_modes() {
    case "$MODE" in
        throughput)    run_throughput ;;
        fragmentation) run_fragmentation ;;
        all)           run_throughput; run_fragmentation ;;
    esac
}

case "$MODE" in
    throughput|fragmentation|all)
        {
            for ((r=1; r<=RUNS; r++)); do
                [[ $RUNS -gt 1 ]] && echo "" && echo "############## Run $r / $RUNS ##############"
                run_selected_modes
            done
            print_final_report
        } 2>&1 | tee "$LOG_FILE"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [binary] [throughput|fragmentation|all] [runs]"
        exit 1
        ;;
esac

echo ""
echo "Full log saved to: $LOG_FILE"
