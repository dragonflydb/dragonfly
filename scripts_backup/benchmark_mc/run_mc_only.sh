#!/bin/bash

# Configuration
TEST_TIME=300
PIPELINES=(1 20 40)
RATIOS=("1:0" "0:1" "1:1" "1:10")

echo "====================================================="
echo " STARTING 12 MISSING MEMCACHED BASELINE RUNS         "
echo "====================================================="

force_cleanup() {
    echo "Sweeping for zombie processes..."
    sudo pkill -9 memtier_benchmark 2>/dev/null || true
    sudo pkill -9 memcached 2>/dev/null || true
    sleep 2
}

run_number=1

for RATIO in "${RATIOS[@]}"; do
    for PIPE in "${PIPELINES[@]}"; do

        echo "-----------------------------------------------------"
        echo "[Run $run_number/12] Official Memcached | Ratio $RATIO | Pipe $PIPE"

        # Ensure no previous instance is blocking the port
        force_cleanup

        # This calls your FIXED run_bench.sh which now uses realpath "./memcached"
        ./run_bench.sh memcached memcache_text 11211 $RATIO $PIPE $TEST_TIME

        run_number=$((run_number + 1))
        sleep 5
    done
done

echo "====================================================="
echo " ALL 12 MISSING RUNS COMPLETED!                      "
echo "====================================================="
