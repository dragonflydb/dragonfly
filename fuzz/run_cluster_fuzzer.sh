#!/usr/bin/env bash
# Fuzz Dragonfly cluster mode with AFL++.
#
# Two-node setup:
#   node0 (port 6380) — regular build, stable cluster peer
#   node1 (port 6379) — AFL-instrumented build, fuzz target
#
# Usage:
#   ./run_cluster_fuzzer.sh
#
# Build requirements:
#   cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja && ninja -C build-dbg dragonfly
#   cmake -B build    -DCMAKE_BUILD_TYPE=Debug -GNinja              && ninja -C build dragonfly

set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-dbg}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/cluster}"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/cluster}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/cluster}"
DICT_FILE="${DICT_FILE:-$FUZZ_DIR/dict/cluster.dict}"
TIMEOUT="5000"
FUZZ_TARGET="$BUILD_DIR/dragonfly"

# Migration spawns per-shard background fibers — 2 threads needed to expose races.
AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-2}"
AFL_LOOP_LIMIT="${AFL_LOOP_LIMIT:-10000}"

PEER_BINARY="${PEER_BINARY:-}"
NODE0_PORT="${NODE0_PORT:-6380}"
NODE1_PORT="${NODE1_PORT:-6379}"
NODE0_ID="0000000000000000000000000000000000000000"
NODE1_ID="1111111111111111111111111111111111111111"

PEER_PID=""
MONITOR_PID=""

print_info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
print_note()    { echo -e "${BLUE}[NOTE]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

check_requirements() {
    if [[ ! -f "$FUZZ_TARGET" ]]; then
        print_warning "AFL-instrumented binary not found: $FUZZ_TARGET"
        print_warning "Build with: cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja && ninja -C build-dbg dragonfly"
        exit 1
    fi

    if [[ -z "$PEER_BINARY" ]]; then
        for candidate in "$PROJECT_ROOT/build/dragonfly" "$PROJECT_ROOT/build-opt/dragonfly"; do
            if [[ -f "$candidate" ]]; then
                PEER_BINARY="$candidate"
                break
            fi
        done
    fi
    if [[ -z "$PEER_BINARY" || ! -f "$PEER_BINARY" ]]; then
        print_warning "Peer binary not found. Build a regular Dragonfly:"
        print_warning "  cmake -B build -DCMAKE_BUILD_TYPE=Debug -GNinja && ninja -C build dragonfly"
        print_warning "Or set PEER_BINARY=/path/to/dragonfly"
        exit 1
    fi
    print_info "Peer binary: $PEER_BINARY"
}

setup_directories() {
    print_info "Setting up directories..."
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$CORPUS_DIR"

    if [[ -z "$(ls -A "$CORPUS_DIR" 2>/dev/null)" ]]; then
        if [[ -d "$SEEDS_DIR" ]] && [[ -n "$(ls -A "$SEEDS_DIR" 2>/dev/null)" ]]; then
            print_info "Copying seeds to corpus..."
            cp "$SEEDS_DIR"/* "$CORPUS_DIR/" 2>/dev/null || true
        else
            print_warning "No seeds found in $SEEDS_DIR"
            exit 1
        fi
    fi
}

show_config() {
    echo ""
    print_info "AFL++ Cluster Fuzzing Configuration:"
    echo "  Fuzz target:      $FUZZ_TARGET"
    echo "  Peer binary:      $PEER_BINARY"
    echo "  Corpus:           $CORPUS_DIR"
    echo "  Output:           $OUTPUT_DIR"
    echo "  Timeout:          ${TIMEOUT}ms"
    echo "  Proactor threads: $AFL_PROACTOR_THREADS"
    echo "  Loop limit:       $AFL_LOOP_LIMIT (= AFL_PERSISTENT_RECORD)"
    echo "  Node0 (peer):     port $NODE0_PORT  id=$NODE0_ID"
    echo "  Node1 (fuzz):     port $NODE1_PORT  id=$NODE1_ID"
    echo ""
    print_note "node0=peer (non-AFL, stable), node1=fuzz target (AFL-instrumented)"
    print_note "cluster_monitor re-pushes CONFIG to both nodes on every node1 restart"
    print_note "To change proactor threads: export AFL_PROACTOR_THREADS=N (default: 2)"
    print_note "To change loop limit: export AFL_LOOP_LIMIT=N (default: 10000)"
    echo ""
}

cleanup() {
    if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
        kill "$MONITOR_PID" 2>/dev/null || true
    fi
    if [[ -n "$PEER_PID" ]] && kill -0 "$PEER_PID" 2>/dev/null; then
        print_info "Stopping peer node (pid $PEER_PID)..."
        kill "$PEER_PID" 2>/dev/null || true
    fi
}

main() {
    check_requirements
    setup_directories
    show_config

    trap cleanup EXIT INT TERM

    # Start peer node (node0)
    print_info "Starting peer node (node0) on port $NODE0_PORT..."
    "$PEER_BINARY" \
        --port=$NODE0_PORT \
        --cluster_mode=yes \
        --cluster_node_id=$NODE0_ID \
        --proactor_threads=1 \
        --dbfilename="" \
        --logtostderr \
        --omit_basic_usage \
        &>/tmp/dragonfly-cluster-peer.log &
    PEER_PID=$!
    print_info "Peer node started (pid $PEER_PID), log: /tmp/dragonfly-cluster-peer.log"

    # Wait for peer to be ready
    print_info "Waiting for peer node to be ready..."
    PEER_READY=false
    for _ in $(seq 1 30); do
        if redis-cli -p $NODE0_PORT PING 2>/dev/null | grep -q PONG; then
            PEER_READY=true
            break
        fi
        if ! kill -0 "$PEER_PID" 2>/dev/null; then
            print_warning "Peer node exited prematurely. Check /tmp/dragonfly-cluster-peer.log"
            exit 1
        fi
        sleep 0.5
    done
    if ! $PEER_READY; then
        print_warning "Peer node not ready after 15s. Check /tmp/dragonfly-cluster-peer.log"
        exit 1
    fi
    print_info "Peer node is ready"

    # Push initial cluster config to node0
    print_info "Configuring node0 cluster slot map..."
    python3 - <<PYEOF
import asyncio, json
import redis.asyncio as aioredis

config = json.dumps([
    {"slot_ranges": [{"start": 0, "end": 8191}],
     "master": {"id": "${NODE0_ID}", "ip": "127.0.0.1", "port": ${NODE0_PORT}},
     "replicas": []},
    {"slot_ranges": [{"start": 8192, "end": 16383}],
     "master": {"id": "${NODE1_ID}", "ip": "127.0.0.1", "port": ${NODE1_PORT}},
     "replicas": []},
])

async def push():
    r = aioredis.Redis(host="127.0.0.1", port=${NODE0_PORT},
                       socket_connect_timeout=5, socket_timeout=5)
    await r.execute_command("DFLYCLUSTER", "CONFIG", config)
    await r.aclose()
    print("node0 configured")

asyncio.run(push())
PYEOF

    # Start cluster config monitor
    print_info "Starting cluster config monitor..."
    python3 "$FUZZ_DIR/cluster_monitor.py" \
        --node0-port=$NODE0_PORT \
        --node0-id=$NODE0_ID \
        --node1-port=$NODE1_PORT \
        --node1-id=$NODE1_ID \
        &>/tmp/dragonfly-cluster-monitor.log &
    MONITOR_PID=$!
    print_info "Cluster monitor started (pid $MONITOR_PID), log: /tmp/dragonfly-cluster-monitor.log"

    # Build AFL++ command
    AFL_CMD=(
        afl-fuzz
        -o "$OUTPUT_DIR"
        -t "$TIMEOUT"
        -m 4096
        -i "$CORPUS_DIR"
    )
    if [[ -f "$DICT_FILE" ]]; then
        AFL_CMD+=(-x "$DICT_FILE")
    fi
    AFL_CMD+=(
        --
        "$FUZZ_TARGET"
        --logtostderr
        --proactor_threads=$AFL_PROACTOR_THREADS
        --afl_loop_limit=$AFL_LOOP_LIMIT
        --bind=0.0.0.0
        --bind=::
        --dbfilename=""
        --omit_basic_usage
        --rename_command=SHUTDOWN=
        --rename_command=DEBUG=
        --rename_command=FLUSHALL=
        --rename_command=FLUSHDB=
        --max_bulk_len=1048576
        --port=$NODE1_PORT
        --cluster_mode=yes
        --cluster_node_id=$NODE1_ID
        --hz=0
    )

    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    cd "$OUTPUT_DIR"

    export AFL_HANG_TMOUT=60000
    export AFL_PERSISTENT_RECORD=$AFL_LOOP_LIMIT
    export AFL_IGNORE_PROBLEMS=1
    export AFL_SKIP_CPUFREQ=1
    export AFL_NO_AFFINITY=1
    export AFL_EXPAND_HAVOC_NOW=1
    export PYTHONPATH="$FUZZ_DIR"
    export AFL_PYTHON_MODULE=cluster_mutator

    # Run as child (not exec) so the EXIT trap fires on exit,
    # killing the peer node and cluster monitor.
    "${AFL_CMD[@]}"
}

main "$@"
