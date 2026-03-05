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

WATCHDOG_PID=""

print_info()    { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }

cleanup() {
    [[ -n "$WATCHDOG_PID" ]] && kill "$WATCHDOG_PID" 2>/dev/null || true
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

# ── Lifecycle watchdog ────────────────────────────────────────────────────────
#
# Polls the master port every 200 ms.
# Master UP  (was down) → kill any stale replica, start a fresh one + REPLICAOF.
# Master DOWN (was up)  → kill the replica.
#
# Result: each AFL++ master instance gets exactly one fresh replica that lives
# and dies together with it.

run_lifecycle_watchdog() {
    (
        _rep_pid=""

        # Kill the replica (if running) when this subshell exits for any reason.
        trap 'kill "$_rep_pid" 2>/dev/null; wait "$_rep_pid" 2>/dev/null' EXIT
        trap 'exit 0' TERM INT

        kill_replica() {
            if [[ -n "$_rep_pid" ]] && kill -0 "$_rep_pid" 2>/dev/null; then
                kill "$_rep_pid" 2>/dev/null
                wait "$_rep_pid" 2>/dev/null
            fi
            _rep_pid=""
        }

        start_fresh_replica() {
            kill_replica

            "$REPLICA_BUILD/dragonfly" \
                --port="$REPLICA_PORT" \
                --logtostderr \
                --proactor_threads=2 \
                --dbfilename="" \
                --rename_command=SHUTDOWN= \
                &>/dev/null &
            _rep_pid=$!

            # Wait for replica to accept connections (max 5 s).
            local t=0
            while [[ $t -lt 50 ]]; do
                nc -z -w1 127.0.0.1 "$REPLICA_PORT" 2>/dev/null && break
                sleep 0.1
                t=$((t + 1))
            done

            # Point replica at master.
            python3 -c "
import socket
mport = '$MASTER_PORT'.encode()
cmd = (b'*3\r\n\$9\r\nREPLICAOF\r\n\$9\r\n127.0.0.1\r\n\$'
       + str(len(mport)).encode() + b'\r\n' + mport + b'\r\n')
try:
    s = socket.create_connection(('127.0.0.1', $REPLICA_PORT), timeout=2)
    s.sendall(cmd); s.recv(64); s.close()
except Exception:
    pass" 2>/dev/null
        }

        master_was_up=0
        while true; do
            sleep 0.2

            nc -z -w1 127.0.0.1 "$MASTER_PORT" 2>/dev/null \
                && master_up=1 || master_up=0

            if [[ $master_up -eq 1 && $master_was_up -eq 0 ]]; then
                start_fresh_replica
            elif [[ $master_up -eq 0 && $master_was_up -eq 1 ]]; then
                kill_replica
            fi

            master_was_up=$master_up
        done
    ) &
    WATCHDOG_PID=$!
    print_info "Lifecycle watchdog started (PID=$WATCHDOG_PID)."
    print_info "Fresh replica will start/stop together with each AFL++ master instance."
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    check_requirements
    run_lifecycle_watchdog

    echo ""
    print_info "Watchdog is running. Fresh replica starts as soon as AFL++ brings up the master."
    print_info "Starting instrumented master via run_fuzzer.sh (AFL_PROACTOR_THREADS=2)..."
    echo ""

    export AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-2}"
    export BUILD_DIR="$BUILD_DIR"

    export AFL_LOOP_LIMIT="${AFL_LOOP_LIMIT:-10000}"

    # Do NOT use exec here — we need the trap to fire on exit so that
    # the watchdog (and the replica it manages) are cleaned up when AFL++ stops.
    "$SCRIPT_DIR/run_fuzzer.sh" "$@"
}

main "$@"
