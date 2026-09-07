#!/bin/bash

# Configuration
TEST_TIME=300
PIPELINES=(1 20 40)
RATIOS=("1:0" "0:1" "1:1" "1:10")

echo "====================================================="
echo " STARTING ISOLATED OVERNIGHT BENCHMARK (60 RUNS)     "
echo "====================================================="

force_cleanup() {
    echo "Sweeping for zombie processes..."
    sudo pkill -9 memtier_benchmark 2>/dev/null || true
    sudo pkill -9 dragonfly 2>/dev/null || true
    sudo pkill -9 valkey-server 2>/dev/null || true
    sudo pkill -9 memcached 2>/dev/null || true
    sleep 2
}

run_number=1

for RATIO in "${RATIOS[@]}"; do
    for PIPE in "${PIPELINES[@]}"; do

        # --- 1. Dragonfly (Memcached V1 - Baseline) ---
        echo "-----------------------------------------------------"
        echo "[Run $run_number/60] Dragonfly Memcached V1 | Ratio $RATIO | Pipe $PIPE"
        force_cleanup
        ./run_bench.sh dragonfly-v1 memcache_text 11211 $RATIO $PIPE $TEST_TIME
        run_number=$((run_number + 1))
        sleep 5

        # --- 2. Dragonfly (Memcached V2 - PR Code) ---
        echo "-----------------------------------------------------"
        echo "[Run $run_number/60] Dragonfly Memcached V2 | Ratio $RATIO | Pipe $PIPE"
        force_cleanup
        ./run_bench.sh dragonfly-v2 memcache_text 11211 $RATIO $PIPE $TEST_TIME
        run_number=$((run_number + 1))
        sleep 5

        # --- 3. Dragonfly (Redis API Baseline) ---
        echo "-----------------------------------------------------"
        echo "[Run $run_number/60] Dragonfly Redis | Ratio $RATIO | Pipe $PIPE"
        force_cleanup
        ./run_bench.sh ./dragonfly redis 6379 $RATIO $PIPE $TEST_TIME
        run_number=$((run_number + 1))
        sleep 5

        # --- 4. Valkey (Redis API Baseline) ---
        echo "-----------------------------------------------------"
        echo "[Run $run_number/60] Valkey Redis | Ratio $RATIO | Pipe $PIPE"
        force_cleanup
        ./run_bench.sh ./valkey-server redis 6379 $RATIO $PIPE $TEST_TIME
        run_number=$((run_number + 1))
        sleep 5

        # --- 5. Official Memcached Baseline ---
        echo "-----------------------------------------------------"
        echo "[Run $run_number/60] Official Memcached | Ratio $RATIO | Pipe $PIPE"
        force_cleanup
        ./run_bench.sh memcached memcache_text 11211 $RATIO $PIPE $TEST_TIME
        run_number=$((run_number + 1))
        sleep 5

    done
done

echo "====================================================="
echo " ALL 60 BENCHMARKS COMPLETED SUCCESSFULLY!           "
echo "====================================================="
