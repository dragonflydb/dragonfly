#!/usr/bin/env bash
# Triage AFL++ crash artifacts: replay each crash against a fresh Dragonfly
# instance and report whether it's confirmed or a false positive.
#
# Each crash gets up to two passes on a fresh server:
#   1. drain   - every recorded command executes (replies are read before the next input);
#                this reproduces deterministic crashes and names the input that ran the fatal
#                command, which is usually NOT the input the fuzzer saved (the process stays up
#                for tens of milliseconds after an assertion, so AFL blames the next input).
#   2. harness - only if pass 1 stayed alive: the exact fuzz-harness behaviour (send, one read,
#                close), for timing-dependent bugs.
#
# Usage:
#   ./fuzz/triage_crashes.sh <dragonfly_binary> <mode> <crashes.zip>
#
#   dragonfly_binary  Path to a plain (non-AFL) Dragonfly binary
#   mode              Protocol: 'resp' or 'memcache'
#   crashes.zip       .zip downloaded from CI artifacts (contains crash-*.tar.gz files)
#
# Exit code: 0 = every crash a false positive; 1 = at least one crash confirmed under the
# recorded configuration; 2 = some crash could not be triaged, was inconclusive, or crashed only
# under a changed/guessed configuration; 3 = bad arguments or the triage could not start at all.
#
# Tiering archives: the fuzz run keeps its backing files on the runner's own disk. Replaying
# them anywhere else is, by policy, always a configuration deviation (INCONCLUSIVE when the
# server survives, "crash under a different configuration" when it dies): tiering crashes
# are IO- and timing-dependent, and a local disk is not the fuzz machine's.
# TRIAGE_MEM_LIMIT_KB overrides the memory limit recorded in the archive's repro.env.
#
# Examples:
#   ./fuzz/triage_crashes.sh ./build-dbg-plain/dragonfly resp fuzz-long-resp-crashes-35.zip
#   ./fuzz/triage_crashes.sh ./build-dbg-plain/dragonfly memcache fuzz-long-memcache-crashes-35.zip

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
    echo "  $0 ./build-dbg-plain/dragonfly resp fuzz-long-resp-crashes-35.zip"
    echo "  $0 ./build-dbg-plain/dragonfly memcache fuzz-long-memcache-crashes-35.zip"
    exit 3
}

# ─── Args ─────────────────────────────────────────────────────────────────────
if [[ $# -lt 3 ]]; then
    usage
fi

MODE="$2"
# Existence is checked before realpath: under set -e a failing realpath would exit with its
# own code instead of the preflight code.
if [[ ! -f "$1" || ! -x "$1" ]]; then
    print_error "Dragonfly binary not found or not executable: $1"
    exit 3
fi
if [[ "$MODE" != "resp" && "$MODE" != "memcache" ]]; then
    print_error "Mode must be 'resp' or 'memcache', got: $MODE"
    exit 3
fi
if [[ ! -f "$3" ]]; then
    print_error "Crashes zip not found: $3"
    exit 3
fi
DRAGONFLY_BIN="$(realpath "$1")"
CRASHES_ZIP="$(realpath "$3")"
if [[ "$CRASHES_ZIP" != *.zip ]]; then
    print_error "Expected a .zip file (CI artifact), got: $CRASHES_ZIP"
    exit 3
fi
# A USE_AFL build runs the stdin fuzz loop and exits at once outside afl-fuzz; it cannot serve
# as the replay target.
if grep -q -a -m1 -e '__afl_area_ptr' -e '__afl_persistent_loop' "$DRAGONFLY_BIN"; then
    print_error "$DRAGONFLY_BIN is AFL-instrumented (USE_AFL=ON); build a plain Debug binary for triage"
    exit 3
fi

# ─── Working directory ────────────────────────────────────────────────────────
if ! WORK_DIR=$(mktemp -d /tmp/triage_XXXXXX); then
    print_error "Cannot create a working directory under /tmp"
    exit 3
fi
DF_PID=""
REPLAY_PID=""
cleanup() {
    [[ -n "$REPLAY_PID" ]] && kill "$REPLAY_PID" 2>/dev/null || true
    [[ -n "$DF_PID" ]] && kill -9 "$DF_PID" 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
# On INT/TERM stop right away: letting the script continue would classify our own kill of
# the server as a confirmed crash. The replay runs in the background under `wait` so the
# trap fires immediately instead of after the replay finishes.
on_signal() {
    trap - EXIT
    cleanup
    exit 130
}
trap cleanup EXIT
trap on_signal INT TERM

# ─── Extract zip ──────────────────────────────────────────────────────────────
print_info "Extracting $(basename "$CRASHES_ZIP")..."
if ! unzip -q "$CRASHES_ZIP" -d "$WORK_DIR/input"; then
    print_error "Cannot extract $CRASHES_ZIP"
    exit 3
fi
CRASHES_DIR="$WORK_DIR/input"

# ─── Find crash archives ──────────────────────────────────────────────────────
mapfile -t CRASH_ARCHIVES < <(find "$CRASHES_DIR" -name 'crash-*.tar.gz' | sort)
TOTAL=${#CRASH_ARCHIVES[@]}

if [[ $TOTAL -eq 0 ]]; then
    print_error "No crash-*.tar.gz files found in: $CRASHES_DIR"
    exit 3
fi

print_info "Found $TOTAL crash archive(s)  mode=$MODE  binary=$DRAGONFLY_BIN"
echo ""

# ─── Locate replay_crash.py ───────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPLAY_SCRIPT="$SCRIPT_DIR/replay_crash.py"
if [[ ! -f "$REPLAY_SCRIPT" ]]; then
    print_error "replay_crash.py not found at: $REPLAY_SCRIPT"
    print_error "Run this script from the repository root or fuzz/ directory."
    exit 3
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

# Show the fatal message and stack trace. The server's stderr is captured to
# $log_dir/stderr.log: an absl CHECK failure, a C assert() and the failure-signal handler all
# write there (the file log sink never creates a FATAL file).
show_crash_log() {
    local log_dir="$1"
    local stderr_log="$log_dir/stderr.log"
    local marker

    if [[ -s "$stderr_log" ]]; then
        # Earliest of: a FATAL log line (LOG(FATAL) or CHECK), a C assert, the failure-signal
        # handler, or an uncaught exception.
        marker=$(grep -n -m1 -E '^F[0-9]{4} |Check fail|assert\(|\*\*\* SIG|terminate called|what\(\)' \
            "$stderr_log" | cut -d: -f1 || true)
        if [[ -n "$marker" ]]; then
            sed -n "$((marker > 1 ? marker - 1 : 1)),$((marker + 40))p" "$stderr_log" | sed 's/^/    /'
            return
        fi
        echo "    (no fatal marker in stderr — last lines:)"
        tail -20 "$stderr_log" | sed 's/^/    /'
        return
    fi

    # No stderr capture — fall back to tail of INFO log
    local info_log
    info_log=$(ls -t "$log_dir"/dragonfly.*.log.INFO.* 2>/dev/null | head -1 || true)
    if [[ -n "$info_log" ]]; then
        echo "    (no stderr log — last INFO log lines:)"
        tail -20 "$info_log" | sed 's/^/    /'
    else
        echo "    (no log files found in $log_dir)"
    fi
}

# Start Dragonfly with DF_ARGS under MEM_LIMIT_KB on a clean --dir; sets DF_PID.
# Returns 1 if it did not come up.
start_dragonfly() {
    local log_dir="$1" db_dir="$2"
    # A previous pass may have run SAVE/BGSAVE; the fuzz harness always starts from an empty dir.
    # Every step is checked explicitly: `if ! start_dragonfly` disables set -e inside.
    if ! rm -rf "$db_dir" || ! mkdir -p "$db_dir"; then
        print_error "Cannot prepare an empty --dir at $db_dir"
        return 1
    fi
    if [[ -n "$(ls -A "$db_dir")" ]]; then
        print_error "--dir $db_dir is not empty after cleanup"
        return 1
    fi
    # The memory limit is part of the reproduction: never start without it.
    (
        if ! ulimit -v "$MEM_LIMIT_KB" 2>"$log_dir/ulimit.err"; then
            exit 99
        fi
        exec "$DRAGONFLY_BIN" "${DF_ARGS[@]}" >"$log_dir/stderr.log" 2>&1
    ) &
    DF_PID=$!

    if ! wait_for_port 127.0.0.1 "$RESP_PORT" "$STARTUP_TIMEOUT"; then
        if [[ -s "$log_dir/ulimit.err" ]]; then
            print_error "Cannot apply memory limit $MEM_LIMIT_KB KB: $(cat "$log_dir/ulimit.err")"
        else
            print_error "Dragonfly did not start within ${STARTUP_TIMEOUT}s; last stderr lines:"
            tail -5 "$log_dir/stderr.log" 2>/dev/null | sed 's/^/    /'
        fi
        stop_dragonfly
        return 1
    fi
    # In memcache mode also verify the memcache listener is up before replaying
    if [[ "$MODE" == "memcache" ]] && ! wait_for_port 127.0.0.1 "$MC_PORT" 3; then
        print_error "Memcache port $MC_PORT not ready"
        stop_dragonfly
        return 1
    fi
    return 0
}

stop_dragonfly() {
    [[ -n "$DF_PID" ]] || return 0
    kill -9 "$DF_PID" 2>/dev/null || true
    wait "$DF_PID" 2>/dev/null && true || true
    DF_PID=""
    wait_port_free "$RESP_PORT" 5 || true
}

# Returns 0 once DF_PID is gone, 1 if it is still alive after POST_REPLAY_WAIT seconds. Used
# after every pass: an abort raised by the last input takes tens of milliseconds to exit.
wait_for_death() {
    for _ in $(seq 1 $((POST_REPLAY_WAIT * 10))); do
        kill -0 "$DF_PID" 2>/dev/null || return 0
        sleep 0.1
    done
    return 1
}

# Replay one crash in the given mode; output goes to $3. Returns the replay exit code:
# 0 = server alive, 3 = server died (the killing input is in the log), 4 = server alive but some
# inputs could not be verified (listed in the log), other = replay failed.
run_replay() {
    local mode="$1" crash_data_dir="$2" replay_log="$3" replay_port="$4" rc
    echo "=== pass: $mode ===" >>"$replay_log"
    python3 "$REPLAY_SCRIPT" "$crash_data_dir" "$CRASH_ID" 127.0.0.1 "$replay_port" \
        --mode "$mode" --pid "$DF_PID" --protocol "$MODE" >>"$replay_log" 2>&1 &
    REPLAY_PID=$!
    wait "$REPLAY_PID" && rc=0 || rc=$?
    REPLAY_PID=""
    return "$rc"
}

# ─── Main loop ────────────────────────────────────────────────────────────────
CONFIRMED=0
DIFF_CONFIRMED=0  # signal death, but under guessed or changed flags: a crash, not the artifact
FALSE_POSITIVE=0
FAILED=0

for CRASH_ARCHIVE in "${CRASH_ARCHIVES[@]}"; do
    CRASH_NAME=$(basename "$CRASH_ARCHIVE" .tar.gz)   # crash-000000
    CRASH_ID="${CRASH_NAME#crash-}"                    # 000000
    IDX=$((CONFIRMED + DIFF_CONFIRMED + FALSE_POSITIVE + FAILED + 1))
    # Reset per-archive port defaults (may be overridden from repro.env below)
    RESP_PORT=6379
    MC_PORT=11211

    echo -e "${CYAN}${BOLD}─── [$IDX/$TOTAL] Crash ${CRASH_ID} ───${NC}"

    # Extract this crash archive
    EXTRACT_DIR="$WORK_DIR/current_crash"
    rm -rf "$EXTRACT_DIR"
    mkdir -p "$EXTRACT_DIR"
    if ! tar -xzf "$CRASH_ARCHIVE" -C "$EXTRACT_DIR" 2>"$WORK_DIR/tar.err"; then
        print_warn "Cannot extract $(basename "$CRASH_ARCHIVE"): $(tr '\n' ' ' <"$WORK_DIR/tar.err" | cut -c1-160) — skipping"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    CRASH_DATA_DIR="$EXTRACT_DIR/${CRASH_NAME}/crashes"
    if [[ ! -d "$CRASH_DATA_DIR" ]]; then
        print_warn "Expected directory not found: $CRASH_DATA_DIR — skipping"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    # Load exact Dragonfly flags and memory limit from repro.env bundled in the archive.
    # Done before the port-in-use check so RESP_PORT reflects the actual fuzz port.
    # repro.env is written by run_fuzzer.sh so flags stay in sync with the fuzz run.
    # Fallback to safe defaults for older archives that don't include repro.env.
    REPRO_ENV="$EXTRACT_DIR/${CRASH_NAME}/repro.env"
    if [[ -f "$REPRO_ENV" ]]; then
        # The archive knows its protocol (PROTOCOL=, or --memcached_port in older archives);
        # replaying it under the wrong one would silently produce a false positive.
        ARCHIVE_PROTOCOL=$(grep '^PROTOCOL=' "$REPRO_ENV" | cut -d= -f2 || true)
        if [[ -z "$ARCHIVE_PROTOCOL" ]]; then
            grep -q '^--memcached_port=' "$REPRO_ENV" && ARCHIVE_PROTOCOL=memcache || ARCHIVE_PROTOCOL=resp
        fi
        if [[ "$ARCHIVE_PROTOCOL" != "$MODE" ]]; then
            print_error "Crash $CRASH_ID was recorded for protocol '$ARCHIVE_PROTOCOL' but mode is '$MODE' — skipping"
            FAILED=$((FAILED + 1))
            echo ""
            continue
        fi
        # package_crash.sh writes GUESSED=1 when the fuzz run's repro.env was missing: the
        # flags are defaults, so survival proves nothing about the real configuration.
        GUESSED=0
        grep -q '^GUESSED=1' "$REPRO_ENV" && GUESSED=1
        MEM_LIMIT_KB=$(grep '^MEM_LIMIT_KB=' "$REPRO_ENV" | cut -d= -f2 || true)
        MEM_LIMIT_KB="${MEM_LIMIT_KB:-$((4 * 1024 * 1024))}"
        # Only flags go to the server; PROTOCOL=, GUESSED= and MEM_LIMIT_KB= are metadata.
        mapfile -t DF_ARGS < <(grep '^--' "$REPRO_ENV" || true)
        RESP_PORT=$(grep '^--port=' "$REPRO_ENV" | cut -d= -f2 || true)
        RESP_PORT="${RESP_PORT:-6379}"
        MC_PORT=$(grep '^--memcached_port=' "$REPRO_ENV" | cut -d= -f2 || true)
        MC_PORT="${MC_PORT:-11211}"
    else
        print_warn "repro.env not found — using default flags (older crash archive)"
        GUESSED=1
        MEM_LIMIT_KB=$((4 * 1024 * 1024))
        DF_ARGS=(
            --port="$RESP_PORT"
            --proactor_threads=1
            --dbfilename=
            --omit_basic_usage
            --restricted_commands=SHUTDOWN,DEBUG,FLUSHALL,FLUSHDB
            --max_bulk_len=1048576
        )
        [[ "$MODE" == "memcache" ]] && DF_ARGS+=(--memcached_port="$MC_PORT")
    fi
    # Material deviations from the fuzz run's configuration: a surviving server then proves
    # nothing (an OOM- or O_DIRECT-dependent crash would not reproduce), so the verdict becomes
    # INCONCLUSIVE instead of FALSE POSITIVE.
    CONFIG_CHANGED=""
    # The archived limit is what the fuzz machine used; raise it if the server cannot even
    # start under it on this machine (TRIAGE_MEM_LIMIT_KB=unlimited disables it).
    if [[ -n "${TRIAGE_MEM_LIMIT_KB:-}" && "$TRIAGE_MEM_LIMIT_KB" != "$MEM_LIMIT_KB" ]]; then
        CONFIG_CHANGED+="memory limit ${MEM_LIMIT_KB} KB overridden to ${TRIAGE_MEM_LIMIT_KB}; "
        MEM_LIMIT_KB="$TRIAGE_MEM_LIMIT_KB"
    fi

    # Both ports must be free before starting: a foreign service already listening there would
    # pass the readiness check and receive the fuzz input if Dragonfly failed to bind.
    PORTS_BUSY=0
    for port in "$RESP_PORT" $([[ "$MODE" == "memcache" ]] && echo "$MC_PORT"); do
        if (>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            print_warn "Port $port still in use — waiting..."
            wait_port_free "$port" 5 || {
                print_error "Port $port still in use after 5s — cannot start Dragonfly"
                PORTS_BUSY=1
            }
        fi
    done
    if [[ $PORTS_BUSY -eq 1 ]]; then
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    # Per-crash log dir (stderr capture + glog files) and a clean --dir for save-enabled runs
    LOG_DIR="$WORK_DIR/logs_${CRASH_ID}"
    DB_DIR="$WORK_DIR/db_${CRASH_ID}"
    rm -rf "$DB_DIR"
    mkdir -p "$LOG_DIR" "$DB_DIR"

    # If the fuzz run used tiered storage, repro.env carries a cwd-relative --tiered_prefix.
    # Rewrite it into this crash's temp dir so the backing files are isolated and cleaned with it.
    HAS_TIERING=0
    for i in "${!DF_ARGS[@]}"; do
        if [[ "${DF_ARGS[$i]}" == --tiered_prefix=* ]]; then
            DF_ARGS[$i]="--tiered_prefix=$DB_DIR/backing"
            HAS_TIERING=1
            # Deliberate policy (see the header): tiering is never replayed as recorded.
            CONFIG_CHANGED+="tiering backing files relocated to $DB_DIR (the fuzz run used its own disk); "
        fi
    done

    # repro.env may carry --backing_file_direct=true from the fuzz machine; force buffered IO
    # when this machine's filesystem rejects O_DIRECT (tmpfs/overlayfs) so the server can start.
    if [[ "$HAS_TIERING" == "1" ]] && \
        ! dd if=/dev/zero of="$DB_DIR/odirect_probe" bs=4096 count=1 oflag=direct \
            status=none 2>/dev/null; then
        for i in "${!DF_ARGS[@]}"; do
            if [[ "${DF_ARGS[$i]}" == --backing_file_direct=true ]]; then
                DF_ARGS[$i]="--backing_file_direct=false"
                CONFIG_CHANGED+="tiering backing file on a filesystem without O_DIRECT, buffered IO forced; "
            fi
        done
    fi
    rm -f "$DB_DIR/odirect_probe"

    # Triage-specific flags (not part of the fuzz run):
    # --logtostderr so the fatal message lands in stderr.log; --dir provides a clean writable
    # directory for save-enabled runs (each crash gets its own dir to avoid stale dumps);
    # --bind=127.0.0.1 keeps the unauthenticated replay server off the network (the harness
    # itself only ever talks to 127.0.0.1, so this is not a configuration deviation)
    DF_ARGS+=(--logtostderr --dir="$DB_DIR" --bind=127.0.0.1)

    REPLAY_PORT="$RESP_PORT"
    [[ "$MODE" == "memcache" ]] && REPLAY_PORT="$MC_PORT"
    REPLAY_LOG="$WORK_DIR/replay_${CRASH_ID}.log"
    : >"$REPLAY_LOG"

    # Pass 1: drain. Pass 2 (harness) only if the server survived pass 1. A pass counts as
    # "died" when the replay reports it (exit 3) or the process exits shortly afterwards.
    DIED=false
    PASS="drain"
    UNVERIFIED=""
    if ! start_dragonfly "$LOG_DIR" "$DB_DIR"; then
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi
    run_replay drain "$CRASH_DATA_DIR" "$REPLAY_LOG" "$REPLAY_PORT" && REPLAY_RC=0 || REPLAY_RC=$?
    if [[ $REPLAY_RC -eq 0 || $REPLAY_RC -eq 3 || $REPLAY_RC -eq 4 ]] && wait_for_death; then
        DIED=true
    fi
    if [[ $REPLAY_RC -eq 4 ]]; then
        UNVERIFIED=$(grep -m1 '\[WARN\]' "$REPLAY_LOG" | sed 's/.*\[WARN\][^ ]* //' || true)
        REPLAY_RC=0
    fi

    if [[ $REPLAY_RC -eq 0 ]] && ! $DIED; then
        stop_dragonfly
        PASS="harness"
        if ! start_dragonfly "$LOG_DIR" "$DB_DIR"; then
            FAILED=$((FAILED + 1))
            echo ""
            continue
        fi
        run_replay harness "$CRASH_DATA_DIR" "$REPLAY_LOG" "$REPLAY_PORT" && REPLAY_RC=0 || REPLAY_RC=$?
        if [[ $REPLAY_RC -eq 0 || $REPLAY_RC -eq 3 ]] && wait_for_death; then
            DIED=true
        fi
    fi

    if [[ $REPLAY_RC -ne 0 && $REPLAY_RC -ne 3 ]]; then
        print_warn "Replay script failed for crash $CRASH_ID (exit $REPLAY_RC) — skipping"
        tail -5 "$REPLAY_LOG" | sed 's/^/    /'
        stop_dragonfly
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi

    if ! $DIED; then
        if [[ $GUESSED -eq 1 ]]; then
            echo -e "  ${YELLOW}INCONCLUSIVE${NC} — Dragonfly alive, but the archive has no repro.env: the server ran with guessed flags (tiering, save, memory limit, threads may differ from the fuzz run)"
            [[ -n "$UNVERIFIED" ]] && echo "    $UNVERIFIED"
            FAILED=$((FAILED + 1))
        elif [[ -n "$CONFIG_CHANGED" ]]; then
            echo -e "  ${YELLOW}INCONCLUSIVE${NC} — Dragonfly alive, but not under the fuzz run's configuration: ${CONFIG_CHANGED}"
            [[ -n "$UNVERIFIED" ]] && echo "    $UNVERIFIED"
            FAILED=$((FAILED + 1))
        elif [[ -n "$UNVERIFIED" ]]; then
            echo -e "  ${YELLOW}FALSE POSITIVE${NC} — Dragonfly alive after drain and harness replays, but the drain pass could not verify every input:"
            echo "    $UNVERIFIED"
            FALSE_POSITIVE=$((FALSE_POSITIVE + 1))
        else
            echo -e "  ${YELLOW}FALSE POSITIVE${NC} — Dragonfly alive after drain and harness replays (every input verified)"
            FALSE_POSITIVE=$((FALSE_POSITIVE + 1))
        fi
        stop_dragonfly
    else
        # Capture signal without triggering set -e (assignment always exits 0)
        wait "$DF_PID" 2>/dev/null && EXIT_CODE=0 || EXIT_CODE=$?
        DF_PID=""
        KILLER=$(grep '\[DEAD\]' "$REPLAY_LOG" | sed 's/.*\[DEAD\][^ ]* //' | sed '2,$s/^/    /' || true)
        [[ -n "$KILLER" ]] || KILLER="Server died after the replay finished (delayed abort from the last input)"
        # Sanity check: exit code > 128 means killed by signal; otherwise not a signal death
        if [[ $EXIT_CODE -le 128 ]]; then
            if [[ $GUESSED -eq 1 || -n "$CONFIG_CHANGED" ]]; then
                echo -e "  ${YELLOW}INCONCLUSIVE${NC} — Dragonfly exited cleanly (code $EXIT_CODE) in ${PASS} pass, but not under the fuzz run's configuration: ${CONFIG_CHANGED:-guessed flags}"
                FAILED=$((FAILED + 1))
            else
                echo -e "  ${YELLOW}FALSE POSITIVE${NC} — Dragonfly exited cleanly (code $EXIT_CODE) in ${PASS} pass"
                FALSE_POSITIVE=$((FALSE_POSITIVE + 1))
            fi
            [[ -n "$KILLER" ]] && echo "    $KILLER"
            echo ""
            continue
        fi
        SIGNAL=$((EXIT_CODE - 128))
        # A signal death is a real crash of the server on these inputs, but under guessed or
        # changed flags it may not be the crash the fuzzer saw: counted apart, exit code 2.
        VERDICT="CONFIRMED"
        if [[ $GUESSED -eq 1 || -n "$CONFIG_CHANGED" ]]; then
            VERDICT="CRASH UNDER A DIFFERENT CONFIGURATION"
            DIFF_CONFIRMED=$((DIFF_CONFIRMED + 1))
        else
            CONFIRMED=$((CONFIRMED + 1))
        fi

        if [[ $SIGNAL -eq 6 ]]; then
            echo -e "  ${RED}${VERDICT}${NC} — SIGABRT (signal 6) — assertion / LOG(FATAL) — ${PASS} pass"
        elif [[ $SIGNAL -eq 11 ]]; then
            echo -e "  ${RED}${VERDICT}${NC} — SIGSEGV (signal 11) — segmentation fault — ${PASS} pass"
        else
            echo -e "  ${RED}${VERDICT}${NC} — signal $SIGNAL (exit code $EXIT_CODE) — ${PASS} pass"
        fi
        if [[ $GUESSED -eq 1 || -n "$CONFIG_CHANGED" ]]; then
            echo "    not the fuzz run's configuration: ${CONFIG_CHANGED:-guessed flags}"
        fi
        [[ -n "$KILLER" ]] && echo "    $KILLER"
        show_crash_log "$LOG_DIR"
    fi
    echo ""
done

# ─── Summary ──────────────────────────────────────────────────────────────────
echo -e "${CYAN}${BOLD}═══ Triage Summary ═══${NC}"
printf "  %-18s %d\n" "Total:" "$TOTAL"
printf "  ${RED}%-18s %d${NC}\n" "Confirmed:" "$CONFIRMED"
[[ $DIFF_CONFIRMED -gt 0 ]] && printf "  ${RED}%-18s %d${NC}\n" "Crash, other cfg:" "$DIFF_CONFIRMED"
printf "  ${YELLOW}%-18s %d${NC}\n" "False positive:" "$FALSE_POSITIVE"
[[ $FAILED -gt 0 ]] && printf "  ${RED}%-18s %d${NC}\n" "Failed/skipped:" "$FAILED"

# Exit 1 if any crash was confirmed as recorded; 2 if some crash could not be triaged or crashed
# only under a changed/guessed configuration; 0 otherwise
[[ $CONFIRMED -gt 0 ]] && exit 1
[[ $FAILED -gt 0 || $DIFF_CONFIRMED -gt 0 ]] && exit 2
exit 0
