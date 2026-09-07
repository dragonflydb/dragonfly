#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
NORMAL_EXIT=false

cleanup() {
    if [ "$NORMAL_EXIT" = false ]; then
        echo -e "\n${RED}[!] INTERRUPTED: Cleaning up...${NC}"
        [[ -n "$TOP_PID" ]] && kill -9 $TOP_PID 2>/dev/null || true
        [[ -n "$SERVER_PID" ]] && kill -9 $SERVER_PID 2>/dev/null || true
    else
        echo -e "\n${GREEN}[V] CLEAN EXIT: Shutting down...${NC}"
        [[ -n "$TOP_PID" ]] && kill $TOP_PID 2>/dev/null || true
        [[ -n "$SERVER_PID" ]] && kill $SERVER_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [ "$#" -ne 6 ]; then
    echo "Usage: ./run_bench.sh <ENGINE_PATH> <PROTOCOL> <PORT> <RATIO> <PIPELINE> <TEST_TIME>"
    exit 1
fi

ENGINE=$1
PROTOCOL=$2
PORT=$3
RATIO=$4
PIPELINE=$5
TEST_TIME=$6

# Resolve absolute path
if [[ "$ENGINE" == "memcached" ]]; then
    ENGINE_CMD=$(realpath "./memcached")
elif [[ "$ENGINE" == "dragonfly-v1" || "$ENGINE" == "dragonfly-v2" ]]; then
    ENGINE_CMD=$(realpath "./dragonfly")
else
    ENGINE_CMD=$(realpath "$ENGINE")
fi

sudo fuser -k ${PORT}/tcp 2>/dev/null || true
sleep 1

DIR_NAME="run_${PROTOCOL}_ratio${RATIO}_pipe${PIPELINE}_time${TEST_TIME}_$(basename $ENGINE)"
DIR_NAME=$(echo "$DIR_NAME" | tr ':' '_')

# CLEANUP LOGIC: If the folder exists, delete it to remove old/corrupt files
if [ -d "$DIR_NAME" ]; then
    echo "Cleaning existing directory: $DIR_NAME"
    rm -rf "$DIR_NAME"
fi
mkdir -p "$DIR_NAME" && cd "$DIR_NAME"

if [[ "$ENGINE" == "dragonfly-v1" ]]; then
    SERVER_START_CMD="$ENGINE_CMD --memcached_port=$PORT --proactor_threads=2 --experimental_io_loop_v2=false --logtostderr"
elif [[ "$ENGINE" == "dragonfly-v2" ]]; then
    SERVER_START_CMD="$ENGINE_CMD --memcached_port=$PORT --proactor_threads=2 --experimental_io_loop_v2=true --logtostderr"
elif [[ "$ENGINE" == *"dragonfly"* ]]; then
    SERVER_START_CMD="$ENGINE_CMD --port=$PORT --proactor_threads=2 --logtostderr"
elif [[ "$ENGINE" == "memcached" ]]; then
    SERVER_START_CMD="$ENGINE_CMD -p $PORT -t 2"
else
    SERVER_START_CMD="$ENGINE_CMD --port $PORT"
fi

echo "[1/4] Starting $(basename $ENGINE) on Port $PORT..."
$SERVER_START_CMD > "server.log" 2>&1 &
SERVER_PID=$!

sleep 3
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}ERROR: Server failed to start. Check $DIR_NAME/server.log${NC}"
    exit 1
fi

echo "[2/4] Monitoring..."
top -b -d 5 -p $SERVER_PID > "resources.log" &
TOP_PID=$!

echo "[3/4] Running Client isolated on Core 3 for ${TEST_TIME}s..."
set +e
taskset -c 3 memtier_benchmark -s 127.0.0.1 -p "$PORT" -P "$PROTOCOL" --ratio="$RATIO" --pipeline="$PIPELINE" -c 1 -t 1 --test-time="$TEST_TIME" --key-maximum=100000 > "memtier.log"
set -e

echo "[4/4] Done."
NORMAL_EXIT=true
