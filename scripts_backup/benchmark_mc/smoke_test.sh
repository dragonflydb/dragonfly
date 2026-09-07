#!/bin/bash

echo "Starting 20-second smoke tests for all 4 engines..."

# 1. Dragonfly (Memcached API)
echo "---------------------------------"
echo "1. Testing: Dragonfly (Memcached)"
sudo pkill -9 memtier_benchmark dragonfly valkey-server memcached 2>/dev/null || true
./run_bench.sh ./dragonfly memcache_text 11211 0:1 1 20
sleep 2

# 2. Dragonfly (Redis API)
echo "---------------------------------"
echo "2. Testing: Dragonfly (Redis)"
sudo pkill -9 memtier_benchmark dragonfly valkey-server memcached 2>/dev/null || true
./run_bench.sh ./dragonfly redis 6379 0:1 20 20
sleep 2

# 3. Valkey (Redis API)
echo "---------------------------------"
echo "3. Testing: Valkey"
sudo pkill -9 memtier_benchmark dragonfly valkey-server memcached 2>/dev/null || true
./run_bench.sh ./valkey-server redis 6379 0:1 40 20
sleep 2

# 4. Official Memcached
echo "---------------------------------"
echo "4. Testing: Official Memcached"
sudo pkill -9 memtier_benchmark dragonfly valkey-server memcached 2>/dev/null || true
./run_bench.sh memcached memcache_text 11211 0:1 1 20

echo "Smoke tests complete! If all 4 folders exist, you are ready for bed."
