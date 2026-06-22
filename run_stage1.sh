#!/bin/bash
# Stage 1 baseline matrix — run all 9 invocations in sequence.
# Run this ON THE SERVER (172.31.30.209) from the dragonfly repo root.
# Usage: ./run_stage1.sh
set -euo pipefail

export SERVER_HOST=172.31.30.209
export CLIENT_HOST=172.31.20.3
B="./bench_v2.sh"
NUM_RUNS=${NUM_RUNS:-3}
START_ROW=${START_ROW:-1}   # set to skip already-completed rows (e.g. START_ROW=3)
END_ROW=${END_ROW:-9}       # set to stop early (e.g. END_ROW=5 runs rows 1-5 only)

# ---------------------------------------------------------------------------
# Preflight: verify all binaries exist before starting any run.
# ---------------------------------------------------------------------------
echo "========================================"
echo " Stage 1 preflight checks"
echo "========================================"
_check() { [[ -f "$1" || -L "$1" ]] && echo "  [ok] $1" || { echo "  [!!] MISSING: $1"; exit 1; }; }
_check ../valkey/src/valkey-server
_check ./build-opt/ok_backend
_check ./build-opt/dragonfly
_check ./bench_v2.sh
echo "  All binaries present."
echo ""

echo "========================================"
echo " Stage 1 baseline matrix"
echo " Server: ${SERVER_HOST}  Client: ${CLIENT_HOST}"
echo " BENCH_DURATION=${BENCH_DURATION:-15}s per pipeline depth, NUM_RUNS=${NUM_RUNS} runs each"
echo "======================================="

_run() {
    local n="$1"; shift
    if (( n < START_ROW )); then
        echo "--- [${n}/9] SKIP (START_ROW=${START_ROW}) ---"
        return
    fi
    if (( n > END_ROW )); then
        echo "--- [${n}/9] SKIP (END_ROW=${END_ROW}) ---"
        return
    fi
    echo ""
    echo "--- [${n}/9] $* ---"
    # Run with env vars already exported; use 'env' to pass any overrides cleanly.
    eval "$@"
}

# Row 1: Valkey, 1 io-thread, single_conn
_run 1 "SERVER_TYPE=valkey $B ../valkey/src/valkey-server single_conn $NUM_RUNS valkey_1t_single"

# Row 2: Valkey, 1 io-thread, multi_conn
_run 2 "SERVER_TYPE=valkey $B ../valkey/src/valkey-server multi_conn $NUM_RUNS valkey_1t_multi"

# Row 3: ok_backend, 1 proactor thread, V1+V2, single_conn
# (No cross-shard hops with 1 thread — measures pure loop overhead)
_run 3 "$B ./build-opt/ok_backend single_conn $NUM_RUNS ok_v1v2_1t_single both 1"

# Row 4: ok_backend, 4 proactor threads, V1+V2, single_conn
# (1 connection, 4 threads — shows V1/V2 delta without connection fan-out)
_run 4 "$B ./build-opt/ok_backend single_conn $NUM_RUNS ok_v1v2_4t_single both 4"

# Row 5: ok_backend, 4 proactor threads, V1+V2, multi_conn
# (50 connections, 4 threads — cross-shard hops, this is where V2 25-41% regression lives)
_run 5 "$B ./build-opt/ok_backend multi_conn $NUM_RUNS ok_v1v2_4t_multi both 4"

# Row 6: ok_backend, 4 threads, V2 ONLY, multishot ON, multi_conn
# (Task 17 baseline: does multishot help ok_backend V2?)
# Only p=1,10,50: multishot is buggy at p>=100 (deadlock/hang)
_run 6 "PIPELINE=1,10,50 EXTRA_SERVER_FLAGS=--uring_recv_buffer_cnt=16000 $B ./build-opt/ok_backend multi_conn $NUM_RUNS ok_v2_ms_4t_multi v2 4"

# Row 7: Dragonfly, 4 threads, V1+V2, single_conn
# 8gb: enough headroom for single-conn load; prevents OOM killer from prior row data
_run 7 "EXTRA_SERVER_FLAGS='--maxmemory 8gb' $B ./build-opt/dragonfly single_conn $NUM_RUNS dfly_4t_single both 4"

# Row 8: Dragonfly, 4 threads, V1+V2, multi_conn
# (Engine cost = delta between row 8 and row 5)
# Flush accumulated data from prior rows; 8gb cap keeps RSS well under physical RAM
_run 8 "EXTRA_SERVER_FLAGS='--maxmemory 8gb' $B ./build-opt/dragonfly multi_conn $NUM_RUNS dfly_4t_multi both 4"

# Row 9: Dragonfly, 4 threads, V2 ONLY, multishot ON, multi_conn
# Only p=1,10,50: multishot is buggy at p>=100 (deadlock/hang)
# maxmemory 24gb: must be higher than row 8's 8gb to avoid inheriting throttle state
# from OS page cache not being reclaimed between rows.
_run 9 "PIPELINE=1,10,50 EXTRA_SERVER_FLAGS='--uring_recv_buffer_cnt=16000 --maxmemory 24gb' $B ./build-opt/dragonfly multi_conn $NUM_RUNS dfly_v2_ms_4t_multi v2 4"

echo ""
echo "========================================"
echo " Stage 1 complete. Log files produced:"
ls -1t *.log 2>/dev/null | grep -E "^(valkey_1t|ok_v1v2|ok_v2|dfly_4t|dfly_v2)" | head -20 || true
echo "========================================"
