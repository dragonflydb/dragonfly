#!/usr/bin/env bash
# Fuzz Dragonfly replication by running AFL++ against an instrumented master
# (build-dbg/) with a plain replica (build/) connected in the background.
#
# The replica continuously replicates from the master, exercising full-sync,
# journal streaming, and PSYNC code paths under AFL++ mutation pressure.
# When the master crashes, AFL++ detects it immediately via instrumentation.
#
# Builds:
#   Instrumented master : BUILD_DIR     (default: build-dbg, must have USE_AFL=ON)
#   Plain replica       : REPLICA_BUILD (default: build, normal release build)
#
# Usage:
#   ./fuzz/run_replication_fuzzer.sh
#
# Configuration (environment variables):
#   BUILD_DIR       Instrumented master build  (default: build-dbg)
#   REPLICA_BUILD   Plain replica build        (default: build)
#   REPLICA_PORT    Replica listen port        (default: 7380)
#   MASTER_PORT     Master listen port         (default: 6379, matches run_fuzzer.sh)
#
# All other env vars accepted by run_fuzzer.sh apply here too
# (AFL_LOOP_LIMIT, OUTPUT_DIR, etc.).

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-dbg}"
REPLICA_BUILD="${REPLICA_BUILD:-$PROJECT_ROOT/build}"
REPLICA_PORT="${REPLICA_PORT:-7380}"
MASTER_PORT="${MASTER_PORT:-6379}"   # must match --port in run_fuzzer.sh

REPLICA_PID=""
WATCHDOG_PID=""

print_info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

cleanup() {
    [[ -n "$WATCHDOG_PID" ]] && kill "$WATCHDOG_PID" 2>/dev/null || true
    [[ -n "$REPLICA_PID" ]] && kill "$REPLICA_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ── Checks ────────────────────────────────────────────────────────────────────

check_requirements() {
    if [[ ! -f "$BUILD_DIR/dragonfly" ]]; then
        print_warning "Instrumented master not found at $BUILD_DIR/dragonfly"
        print_warning "Build with: cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja"
        print_warning "           ninja -C build-dbg dragonfly"
        exit 1
    fi

    if [[ ! -f "$REPLICA_BUILD/dragonfly" ]]; then
        print_warning "Plain replica binary not found at $REPLICA_BUILD/dragonfly"
        print_warning "Build with: ./helio/blaze.sh && ninja -C build dragonfly"
        exit 1
    fi

    if ! command -v afl-fuzz &>/dev/null; then
        print_warning "afl-fuzz not found. Install AFL++ from https://github.com/AFLplusplus/AFLplusplus"
        exit 1
    fi
}

# ── Replica ───────────────────────────────────────────────────────────────────

wait_for_port() {
    local port="$1" label="$2"
    local deadline=$((SECONDS + 15))
    while [[ $SECONDS -lt $deadline ]]; do
        if python3 -c "
import socket, sys
s = None
ok = False
try:
    s = socket.create_connection(('127.0.0.1', $port), timeout=1.0)
    s.sendall(b'*1\r\n\$4\r\nPING\r\n')
    ok = b'PONG' in s.recv(64)
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
    print_warning "$label on port $port did not become ready"
    return 1
}

start_replica() {
    print_info "Starting plain replica on port $REPLICA_PORT ($REPLICA_BUILD/dragonfly)..."
    "$REPLICA_BUILD/dragonfly" \
        --port="$REPLICA_PORT" \
        --logtostderr \
        --proactor_threads=2 \
        --dbfilename="" \
        --rename_command=SHUTDOWN= \
        &>/dev/null &
    REPLICA_PID=$!

    wait_for_port "$REPLICA_PORT" "replica"
    print_info "Replica ready (PID=$REPLICA_PID)."
}

configure_replication() {
    # Point replica at master. The master is not running yet; Dragonfly retries
    # the connection automatically once AFL++ starts the master binary.
    print_info "Configuring replica → master 127.0.0.1:$MASTER_PORT (retries until master starts)..."
    python3 -c "
import socket, sys
mport = '$MASTER_PORT'.encode()
cmd = (b'*3\r\n\$9\r\nREPLICAOF\r\n'
       b'\$9\r\n127.0.0.1\r\n'
       b'\$' + str(len(mport)).encode() + b'\r\n' + mport + b'\r\n')
try:
    s = socket.create_connection(('127.0.0.1', $REPLICA_PORT), timeout=3)
    s.sendall(cmd)
    s.recv(64)
    s.close()
    print('REPLICAOF configured.')
except Exception as e:
    print('WARNING: REPLICAOF failed:', e, file=sys.stderr)
    sys.exit(1)
"
}

# ── Watchdog ──────────────────────────────────────────────────────────────────

start_watchdog() {
    # Runs in background. Every 2 seconds checks that the replica is in slave
    # mode; if not (e.g. after a replica restart or an unexpected REPLICAOF NO ONE),
    # re-sends REPLICAOF so it reconnects to the master automatically.
    (
        local mport="$MASTER_PORT"
        local rport="$REPLICA_PORT"
        while true; do
            sleep 2
            python3 - <<EOF 2>/dev/null
import socket, sys
rport = $rport
mport = '$mport'.encode()
try:
    s = socket.create_connection(('127.0.0.1', rport), timeout=1)
    s.sendall(b'*2\r\n\$4\r\nINFO\r\n\$11\r\nreplication\r\n')
    data = b''
    for _ in range(10):
        chunk = s.recv(4096)
        if not chunk: break
        data += chunk
        if b'role:' in data: break
    s.close()
    if b'role:slave' in data:
        sys.exit(0)
    # Not a slave – reconfigure
    s = socket.create_connection(('127.0.0.1', rport), timeout=2)
    s.sendall(b'*3\r\n\$9\r\nREPLICAOF\r\n\$9\r\n127.0.0.1\r\n\$'
              + str(len(mport)).encode() + b'\r\n' + mport + b'\r\n')
    s.recv(64)
    s.close()
except Exception:
    pass
EOF
        done
    ) &
    WATCHDOG_PID=$!
    print_info "Watchdog started (PID=$WATCHDOG_PID) — auto-reconnects replica if needed."
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    check_requirements
    start_replica
    configure_replication
    start_watchdog

    echo ""
    print_info "Replica is up. It connects to master once AFL++ starts it on port $MASTER_PORT."
    print_info "Starting instrumented master via run_fuzzer.sh (AFL_PROACTOR_THREADS=2)..."
    echo ""

    # Run the standard instrumented fuzzer with 2 proactor threads.
    # The replica in the background connects to master as soon as AFL++ starts it,
    # and re-syncs (full sync) every time AFL++ restarts the master process.
    export AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-2}"
    export BUILD_DIR="$BUILD_DIR"

    # Keep the loop limit low so the master restarts frequently, triggering a full
    # sync on every restart. This maximises coverage of the full-sync, PSYNC, and
    # journal-streaming code paths. The default 10000 used for single-instance
    # fuzzing is too high here: replication bugs are most likely to surface during
    # the sync handshake, not after thousands of steady-state commands.
    export AFL_LOOP_LIMIT="${AFL_LOOP_LIMIT:-100}"

    # Do NOT use exec here — we need the trap to fire on exit so that
    # the replica and watchdog are cleaned up when AFL++ stops.
    "$SCRIPT_DIR/run_fuzzer.sh" "$@"
}

main "$@"
