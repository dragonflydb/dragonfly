#!/usr/bin/env bash
# Run AFL++ replication fuzzer.
#
# Starts two plain (uninstrumented) Dragonfly instances – master and replica –
# then runs AFL++ in dumb mode (-n) using the replication orchestrator as the
# target.  AFL++ mutates the multiplexed input format handled by
# replication_orchestrator.py and detects crashes / consistency violations.
#
# Usage:
#   ./run_replication_fuzzer.sh
#
# Configuration (environment variables):
#   MASTER_PORT   (default 7379)
#   REPLICA_PORT  (default 7380)
#   BUILD_DIR     (default build-dbg)
#   OUTPUT_DIR    (default fuzz/artifacts/replication)
#   TIMEOUT       AFL++ per-iteration timeout in ms (default 30000)

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MASTER_PORT="${MASTER_PORT:-7379}"
REPLICA_PORT="${REPLICA_PORT:-7380}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-dbg}"
DRAGONFLY="${DRAGONFLY:-$BUILD_DIR/dragonfly}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/replication}"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/replication}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/replication}"
TIMEOUT="${TIMEOUT:-30000}"

MASTER_PID=""
REPLICA_PID=""

print_info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
print_note()    { echo -e "${BLUE}[NOTE]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

cleanup() {
    print_info "Stopping instances..."
    [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" 2>/dev/null || true
    [[ -n "$REPLICA_PID" ]] && kill "$REPLICA_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ── Checks ────────────────────────────────────────────────────────────────────

check_requirements() {
    if [[ ! -f "$DRAGONFLY" ]]; then
        print_warning "dragonfly binary not found at $DRAGONFLY"
        print_warning "Build with: cd $BUILD_DIR && ninja dragonfly"
        exit 1
    fi

    if ! command -v afl-fuzz &>/dev/null; then
        print_warning "afl-fuzz not found. Install AFL++ from https://github.com/AFLplusplus/AFLplusplus"
        exit 1
    fi

    if ! python3 -c "import afl" 2>/dev/null; then
        print_note "python-afl not installed (pip install python-afl)"
        print_note "Orchestrator will run in single-shot mode (slower)."
    fi
}

# ── Instance management ───────────────────────────────────────────────────────

wait_for_ping() {
    local host="$1" port="$2" label="$3"
    local deadline=$((SECONDS + 15))
    while [[ $SECONDS -lt $deadline ]]; do
        # NOTE: use `except Exception` not bare `except` — bare except catches
        # SystemExit too, which would swallow sys.exit(0) and always report failure.
        if python3 -c "
import socket, sys
ok = False
s = None
try:
    s = socket.create_connection(('$host', $port), timeout=1.0)
    s.sendall(b'*1\r\n\$4\r\nPING\r\n')
    data = s.recv(64)
    ok = b'PONG' in data
except Exception:
    pass
finally:
    if s:
        try: s.close()
        except Exception: pass
sys.exit(0 if ok else 1)
" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    print_warning "$label on port $port did not become ready in time"
    return 1
}

start_instances() {
    print_info "Starting master on port $MASTER_PORT..."
    "$DRAGONFLY" \
        --port="$MASTER_PORT" \
        --logtostderr \
        --proactor_threads=2 \
        --dbfilename="" \
        --rename_command=SHUTDOWN= \
        &>/dev/null &
    MASTER_PID=$!

    print_info "Starting replica on port $REPLICA_PORT..."
    "$DRAGONFLY" \
        --port="$REPLICA_PORT" \
        --logtostderr \
        --proactor_threads=2 \
        --dbfilename="" \
        --rename_command=SHUTDOWN= \
        &>/dev/null &
    REPLICA_PID=$!

    wait_for_ping "127.0.0.1" "$MASTER_PORT"  "master"
    wait_for_ping "127.0.0.1" "$REPLICA_PORT" "replica"
    print_info "Both instances ready."

    # Connect replica to master via REPLICAOF
    python3 -c "
import socket, sys
master_port = str($MASTER_PORT)
replica_port = $REPLICA_PORT
args = [b'REPLICAOF', b'127.0.0.1', master_port.encode()]
cmd  = b'*%d\r\n' % len(args)
for a in args:
    cmd += b'\$%d\r\n%s\r\n' % (len(a), a)
try:
    s = socket.create_connection(('127.0.0.1', replica_port), timeout=3)
    s.sendall(cmd)
    s.recv(64)
    s.close()
    print('Replication configured.')
except Exception as e:
    print('WARNING: REPLICAOF failed:', e, file=sys.stderr)
    sys.exit(1)
"
}

# ── Corpus setup ──────────────────────────────────────────────────────────────

setup_corpus() {
    mkdir -p "$OUTPUT_DIR" "$CORPUS_DIR"

    if [[ -z "$(ls -A "$CORPUS_DIR" 2>/dev/null)" ]]; then
        if [[ -d "$SEEDS_DIR" ]] && [[ -n "$(ls -A "$SEEDS_DIR" 2>/dev/null)" ]]; then
            print_info "Copying seeds to corpus..."
            cp "$SEEDS_DIR"/* "$CORPUS_DIR/"
        else
            print_info "Generating seeds..."
            python3 "$FUZZ_DIR/generate_replication_seeds.py" --output-dir "$SEEDS_DIR"
            cp "$SEEDS_DIR"/* "$CORPUS_DIR/"
        fi
    fi
}

# ── Fuzzer ────────────────────────────────────────────────────────────────────

show_config() {
    echo ""
    print_info "AFL++ Replication Fuzzer Configuration:"
    echo "  Master port  : $MASTER_PORT"
    echo "  Replica port : $REPLICA_PORT"
    echo "  Binary       : $DRAGONFLY"
    echo "  Corpus       : $CORPUS_DIR"
    echo "  Output       : $OUTPUT_DIR"
    echo "  Timeout      : ${TIMEOUT}ms per iteration"
    echo ""
    print_note "Mode: dumb (-n), no instrumentation on Dragonfly"
    print_note "Detects: crashes in master or replica, data inconsistency"
    print_note "Replay: python3 fuzz/replay_replication_crash.py <crashes_dir> <id>"
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++..."

    AFL_CMD=(
        afl-fuzz
        -n                     # dumb mode: Dragonfly is not instrumented
        -o "$OUTPUT_DIR"
        -t "$TIMEOUT"
        -i "$CORPUS_DIR"
    )

    # Note: don't use the RESP dict here — our input is a binary mux format,
    # not raw RESP.  Dictionary insertions would corrupt the mux framing.

    AFL_CMD+=(
        --
        python3
        "$FUZZ_DIR/replication_orchestrator.py"
    )

    export MASTER_PORT REPLICA_PORT
    export PYTHONPATH="$FUZZ_DIR"
    export AFL_PYTHON_MODULE=replication_mutator
    export AFL_CUSTOM_MUTATOR_ONLY=1
    export AFL_EXPAND_HAVOC_NOW=1

    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    exec "${AFL_CMD[@]}"
}

main() {
    check_requirements
    start_instances
    setup_corpus
    show_config
    run_fuzzer
}

main "$@"
