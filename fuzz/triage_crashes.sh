#!/usr/bin/env bash
# Triage AFL++ crash artifacts: replay each crash against a fresh Dragonfly
# instance and report whether it's confirmed or a false positive.
#
# Usage:
#   ./fuzz/triage_crashes.sh <dragonfly_binary> <mode> <crashes.zip>
#
#   dragonfly_binary  Path to Dragonfly binary
#   mode              Protocol: 'resp' or 'memcache'
#   crashes.zip       .zip downloaded from CI artifacts (contains crash-*.tar.gz files)
#
# Examples:
#   ./fuzz/triage_crashes.sh ./build-dbg/dragonfly resp fuzz-long-resp-crashes-35.zip
#   ./fuzz/triage_crashes.sh ./build-dbg/dragonfly memcache fuzz-long-memcache-crashes-35.zip

set -euo pipefail

# ─── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Config ───────────────────────────────────────────────────────────────────
RESP_PORT=6379
MC_PORT=11211
STARTUP_TIMEOUT=5   # seconds to wait for Dragonfly to accept connections
POST_REPLAY_WAIT=3  # seconds to wait after replay for Dragonfly to crash

print_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }

usage() {
    echo -e "${BOLD}Usage:${NC} $0 <dragonfly_binary> <mode> <crashes.zip>"
    echo ""
    echo "  dragonfly_binary  Path to Dragonfly binary"
    echo "  mode              Protocol: 'resp' or 'memcache'"
    echo "  crashes.zip       .zip downloaded from CI artifacts"
    echo ""
    echo "Examples:"
    echo "  $0 ./build-dbg/dragonfly resp fuzz-long-resp-crashes-35.zip"
    echo "  $0 ./build-dbg/dragonfly memcache fuzz-long-memcache-crashes-35.zip"
    exit 1
}

# ─── Args ─────────────────────────────────────────────────────────────────────
if [[ $# -lt 3 ]]; then
    usage
fi

DRAGONFLY_BIN="$(realpath "$1")"
MODE="$2"
CRASHES_ZIP="$(realpath "$3")"

if [[ ! -f "$DRAGONFLY_BIN" ]]; then
    print_error "Dragonfly binary not found: $DRAGONFLY_BIN"
    exit 1
fi
if [[ "$MODE" != "resp" && "$MODE" != "memcache" ]]; then
    print_error "Mode must be 'resp' or 'memcache', got: $MODE"
    exit 1
fi
if [[ ! -f "$CRASHES_ZIP" ]]; then
    print_error "Crashes zip not found: $CRASHES_ZIP"
    exit 1
fi
if [[ "$CRASHES_ZIP" != *.zip ]]; then
    print_error "Expected a .zip file (CI artifact), got: $CRASHES_ZIP"
    exit 1
fi

# ─── Working directory ────────────────────────────────────────────────────────
WORK_DIR=$(mktemp -d /tmp/triage_XXXXXX)
DF_PID=""
cleanup() {
    [[ -n "$DF_PID" ]] && kill -9 "$DF_PID" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

# ─── Extract zip ──────────────────────────────────────────────────────────────
print_info "Extracting $(basename "$CRASHES_ZIP")..."
unzip -q "$CRASHES_ZIP" -d "$WORK_DIR/input"
CRASHES_DIR="$WORK_DIR/input"

# ─── Find crash archives ──────────────────────────────────────────────────────
mapfile -t CRASH_ARCHIVES < <(find "$CRASHES_DIR" -name 'crash-*.tar.gz' | sort)
TOTAL=${#CRASH_ARCHIVES[@]}

if [[ $TOTAL -eq 0 ]]; then
    print_error "No crash-*.tar.gz files found in: $CRASHES_DIR"
    exit 1
fi

print_info "Found $TOTAL crash archive(s)  mode=$MODE  binary=$DRAGONFLY_BIN"
echo ""

# ─── Locate replay_crash.py ───────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLAY_SCRIPT="$SCRIPT_DIR/replay_crash.py"
if [[ ! -f "$REPLAY_SCRIPT" ]]; then
    print_error "replay_crash.py not found at: $REPLAY_SCRIPT"
    print_error "Run this script from the repository root or fuzz/ directory."
    exit 1
fi

# ─── Helpers ──────────────────────────────────────────────────────────────────
# Wait until a TCP port accepts connections
wait_for_port() {
    local host="$1" port="$2" timeout_sec="$3"
    local deadline=$((SECONDS + timeout_sec))
    while [[ $SECONDS -lt $deadline ]]; do
        if (>/dev/tcp/"$host"/"$port") 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# Wait until a TCP port stops accepting connections
wait_port_free() {
    local port="$1" timeout_sec="${2:-5}"
    local deadline=$((SECONDS + timeout_sec))
    while [[ $SECONDS -lt $deadline ]]; do
        if ! (>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# Show crash info from glog log directory.
show_crash_log() {
    local log_dir="$1"
    local fatal_link="$log_dir/dragonfly.FATAL"

    if [[ -f "$fatal_link" ]]; then
        # Skip the 4-line glog file header, show crash message + stack trace
        sed -n '5,$p' "$fatal_link" | head -40 | sed 's/^/    /'
        return
    fi

    # No FATAL file — fall back to tail of INFO log
    local info_log
    info_log=$(ls -t "$log_dir"/dragonfly.*.log.INFO.* 2>/dev/null | head -1 || true)
    if [[ -n "$info_log" ]]; then
        echo "    (no FATAL log — last INFO log lines:)"
        tail -20 "$info_log" | sed 's/^/    /'
    else
        echo "    (no log files found in $log_dir)"
    fi
}

# ─── Main loop ────────────────────────────────────────────────────────────────
CONFIRMED=0
FALSE_POSITIVE=0
FAILED=0

for CRASH_ARCHIVE in "${CRASH_ARCHIVES[@]}"; do
    CRASH_NAME=$(basename "$CRASH_ARCHIVE" .tar.gz)   # crash-000000
    CRASH_ID="${CRASH_NAME#crash-}"                    # 000000
    IDX=$((CONFIRMED + FALSE_POSITIVE + FAILED + 1))

    echo -e "${CYAN}${BOLD}─── [$IDX/$TOTAL] Crash ${CRASH_ID} ───${NC}"

    # Extract this crash archive
    EXTRACT_DIR="$WORK_DIR/current_crash"
    rm -rf "$EXTRACT_DIR"
    mkdir -p "$EXTRACT_DIR"
    tar -xzf "$CRASH_ARCHIVE" -C "$EXTRACT_DIR"

    CRASH_DATA_DIR="$EXTRACT_DIR/${CRASH_NAME}/crashes"
    if [[ ! -d "$CRASH_DATA_DIR" ]]; then
        print_warn "Expected directory not found: $CRASH_DATA_DIR — skipping"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    # Kill any leftover process on the port from a previous iteration
    if (>/dev/tcp/127.0.0.1/"$RESP_PORT") 2>/dev/null; then
        print_warn "Port $RESP_PORT still in use — waiting..."
        wait_port_free "$RESP_PORT" 5 || {
            print_error "Port $RESP_PORT still blocked after 5s — cannot start Dragonfly"
            FAILED=$((FAILED + 1))
            echo ""
            continue
        }
    fi

    # Start Dragonfly — use --log_dir so glog writes to separate per-level files
    # (dragonfly.FATAL symlink is created on crash and contains the fatal message)
    LOG_DIR="$WORK_DIR/logs_${CRASH_ID}"
    mkdir -p "$LOG_DIR"

    DF_ARGS=(
        --port "$RESP_PORT"
        --log_dir="$LOG_DIR"
        --proactor_threads 1
        '--dbfilename='
    )
    [[ "$MODE" == "memcache" ]] && DF_ARGS+=(--memcached_port="$MC_PORT")

    "$DRAGONFLY_BIN" "${DF_ARGS[@]}" >/dev/null 2>&1 &
    DF_PID=$!

    if ! wait_for_port 127.0.0.1 "$RESP_PORT" "$STARTUP_TIMEOUT"; then
        print_error "Dragonfly did not start within ${STARTUP_TIMEOUT}s (crash $CRASH_ID)"
        kill -9 "$DF_PID" 2>/dev/null || true
        wait "$DF_PID" 2>/dev/null && true || true
        DF_PID=""
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi
    # In memcache mode also verify the memcache listener is up before replaying
    if [[ "$MODE" == "memcache" ]] && ! wait_for_port 127.0.0.1 "$MC_PORT" 3; then
        print_error "Memcache port $MC_PORT not ready (crash $CRASH_ID)"
        kill -9 "$DF_PID" 2>/dev/null || true
        wait "$DF_PID" 2>/dev/null && true || true
        DF_PID=""
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    # Replay the crash
    REPLAY_PORT="$RESP_PORT"
    [[ "$MODE" == "memcache" ]] && REPLAY_PORT="$MC_PORT"

    python3 "$REPLAY_SCRIPT" \
        "$CRASH_DATA_DIR" "$CRASH_ID" 127.0.0.1 "$REPLAY_PORT" \
        >/dev/null 2>&1 || true

    # Wait for Dragonfly to die (poll every 100ms)
    DIED=false
    for _ in $(seq 1 $((POST_REPLAY_WAIT * 10))); do
        if ! kill -0 "$DF_PID" 2>/dev/null; then
            DIED=true
            break
        fi
        sleep 0.1
    done

    if ! $DIED; then
        echo -e "  ${YELLOW}FALSE POSITIVE${NC} — Dragonfly alive after replay"
        FALSE_POSITIVE=$((FALSE_POSITIVE + 1))
        kill -9 "$DF_PID" 2>/dev/null || true
        wait "$DF_PID" 2>/dev/null && true || true
        DF_PID=""
    else
        # Capture signal without triggering set -e (assignment always exits 0)
        wait "$DF_PID" 2>/dev/null && EXIT_CODE=0 || EXIT_CODE=$?
        DF_PID=""
        # Sanity check: exit code > 128 means killed by signal; otherwise not a signal death
        if [[ $EXIT_CODE -le 128 ]]; then
            echo -e "  ${YELLOW}FALSE POSITIVE${NC} — Dragonfly exited cleanly (code $EXIT_CODE)"
            FALSE_POSITIVE=$((FALSE_POSITIVE + 1))
            echo ""
            continue
        fi
        SIGNAL=$((EXIT_CODE - 128))
        CONFIRMED=$((CONFIRMED + 1))

        if [[ $SIGNAL -eq 6 ]]; then
            echo -e "  ${RED}CONFIRMED${NC} — SIGABRT (signal 6) — assertion / LOG(FATAL)"
            show_crash_log "$LOG_DIR"
        elif [[ $SIGNAL -eq 11 ]]; then
            echo -e "  ${RED}CONFIRMED${NC} — SIGSEGV (signal 11) — segmentation fault"
            show_crash_log "$LOG_DIR"
        else
            echo -e "  ${RED}CONFIRMED${NC} — signal $SIGNAL (exit code $EXIT_CODE)"
            show_crash_log "$LOG_DIR"
        fi
    fi
    echo ""
done

# ─── Summary ──────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}═══ Triage Summary ═══${NC}"
printf "  %-18s %d\n" "Total:" "$TOTAL"
printf "  ${RED}%-18s %d${NC}\n" "Confirmed:" "$CONFIRMED"
printf "  ${YELLOW}%-18s %d${NC}\n" "False positive:" "$FALSE_POSITIVE"
[[ $FAILED -gt 0 ]] && printf "  ${RED}%-18s %d${NC}\n" "Failed/skipped:" "$FAILED"

# Exit 1 if any confirmed crashes found
[[ $CONFIRMED -gt 0 ]] && exit 1
exit 0
