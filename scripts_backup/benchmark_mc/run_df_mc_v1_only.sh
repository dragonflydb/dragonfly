#!/bin/bash

# Configuration
TEST_TIME=300
PIPELINES=(1 20 40)
RATIOS=("1:0" "0:1" "1:1" "1:10")
DFLY_BIN="./dragonfly" # Using your local binary

echo "====================================================="
echo " RUNNING DRAGONFLY V1 BASELINE (12 RUNS)            "
echo " Flag: --experimental_io_loop_v2=false              "
echo "====================================================="

force_cleanup() {
    sudo pkill -9 -x memtier_benchmark 2>/dev/null || true
    sudo pkill -9 -x dragonfly 2>/dev/null || true
    sleep 2
}

run_number=1
for RATIO in "${RATIOS[@]}"; do
    for PIPE in "${PIPELINES[@]}"; do

        echo "-----------------------------------------------------"
        echo "[Run $run_number/12] Ratio: $RATIO | Pipeline: $PIPE"
        force_cleanup

        # Start Dragonfly with V2 loop DISABLED
        $DFLY_BIN --experimental_io_loop_v2=false --memcached_port 11211 &
        sleep 3 # Wait for startup

        # Run memtier (calling your existing run_bench.sh)
        ./run_bench.sh "$DFLY_BIN" memcache_text 11211 "$RATIO" "$PIPE" "$TEST_TIME"

        run_number=$((run_number + 1))
        sleep 5
    done
done
