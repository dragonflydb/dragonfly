#!/usr/bin/env bash

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET="resp"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-dbg}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/$TARGET}"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/$TARGET}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/$TARGET}"
DICT_FILE="${DICT_FILE:-$FUZZ_DIR/dict/$TARGET.dict}"
TIMEOUT="500"
FUZZ_TARGET="$BUILD_DIR/dragonfly"
AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-1}"

# Persistent record: restart server every N iterations and record the last N inputs.
# This ensures that on crash, ALL inputs that built the current server state are available
# for replay. Without this, state from earlier iterations is lost and crashes become
# non-reproducible. Max recommended by AFL++: 10000.
AFL_LOOP_LIMIT="${AFL_LOOP_LIMIT:-10000}"

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_note() {
    echo -e "${BLUE}[NOTE]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

check_requirements() {
    if [[ ! -f "${FUZZ_TARGET}" ]]; then
        print_warning "Dragonfly not found at ${FUZZ_TARGET}"
        print_warning "Build with: -DUSE_AFL=ON"
        exit 1
    fi
}

setup_system() {
    # AFL++ requires core dumps to go to a file, not a pipe (like systemd-coredump).
    local core_pattern
    core_pattern=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)
    if [[ "$core_pattern" == "|"* ]]; then
        print_info "Setting core_pattern to 'core' (was piped to external utility)..."
        echo core | sudo tee /proc/sys/kernel/core_pattern > /dev/null 2>&1 || {
            print_warning "Could not set core_pattern. Run: echo core | sudo tee /proc/sys/kernel/core_pattern"
            exit 1
        }
    fi

    # Set CPU governor to performance if possible, otherwise skip the check.
    if [[ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
        local gov
        gov=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true)
        if [[ "$gov" != "performance" ]]; then
            echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor > /dev/null 2>&1 || {
                print_note "Could not set CPU governor, using AFL_SKIP_CPUFREQ=1"
                export AFL_SKIP_CPUFREQ=1
            }
        fi
    fi
}

setup_directories() {
    print_info "Setting up directories..."
    mkdir -p "${OUTPUT_DIR}"
    mkdir -p "${CORPUS_DIR}"

    if [[ -z "$(ls -A "$CORPUS_DIR" 2>/dev/null)" ]]; then
        if [[ -d "${SEEDS_DIR}" ]] && [[ -n "$(ls -A "${SEEDS_DIR}" 2>/dev/null)" ]]; then
            print_info "Copying seeds to corpus..."
            cp "${SEEDS_DIR}"/* "${CORPUS_DIR}/" 2>/dev/null || true
        else
            print_warning "No seeds found, creating minimal seed"
            echo -e '*1\r\n$4\r\nPING\r\n' > "${CORPUS_DIR}/ping"
        fi
    fi
}

show_config() {
    echo ""
    print_info "AFL++ Persistent Mode Configuration:"
    echo "  Binary:           ${FUZZ_TARGET}"
    echo "  Corpus:           ${CORPUS_DIR}"
    echo "  Output:           ${OUTPUT_DIR}"
    echo "  Dictionary:       ${DICT_FILE}"
    echo "  Timeout:          ${TIMEOUT}ms"
    echo "  Proactor threads: ${AFL_PROACTOR_THREADS}"
    echo "  Loop limit:      ${AFL_LOOP_LIMIT} (= AFL_PERSISTENT_RECORD)"
    echo ""
    print_note "Fuzzing integrated in dragonfly (USE_AFL + persistent mode)"
    print_note "To change proactor threads: export AFL_PROACTOR_THREADS=N (default: 1)"
    print_note "To change loop limit: export AFL_LOOP_LIMIT=N (default: 10000)"
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++ persistent mode fuzzing..."
    print_info "Press Ctrl+C to stop"
    echo ""

    AFL_CMD=(
        afl-fuzz
        -o "${OUTPUT_DIR}"
        -t "${TIMEOUT}"
        -m 4096
        -i "${CORPUS_DIR}"
    )

    if [[ -f "${DICT_FILE}" ]]; then
        AFL_CMD+=(-x "${DICT_FILE}")
    fi

    AFL_CMD+=(
        --
        "${FUZZ_TARGET}"
        --port=6379
        --logtostderr
        --proactor_threads=${AFL_PROACTOR_THREADS}
        --afl_loop_limit=${AFL_LOOP_LIMIT}
        --bind=0.0.0.0
        --bind=::
        --dbfilename=""
        --omit_basic_usage
        --rename_command=SHUTDOWN=
        --rename_command=DEBUG=
        --rename_command=FLUSHALL=
        --rename_command=FLUSHDB=
        --max_bulk_len=1048576
    )

    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    cd "${OUTPUT_DIR}"

    # Run AFL++ - fuzzing integrated in dragonfly via USE_AFL
    # AFL_HANG_TMOUT: Only consider it a hang if no response for 60 seconds
    # This prevents false positives from slow but legitimate operations
    export AFL_HANG_TMOUT=60000

    # Dragonfly has ~350K edges, default AFL++ bitmap is 64KB (massive collisions).
    # Use 512KB bitmap to reduce hash collisions and improve stability.
    export AFL_MAP_SIZE=524288

    # Record the last N inputs before a crash for replay.
    # Synced with afl_loop_limit so the full server state history is always captured.
    export AFL_PERSISTENT_RECORD=${AFL_LOOP_LIMIT}

    # Even with 1 proactor thread, some coverage instability is expected.
    # Tell AFL++ to continue despite unstable coverage — don't bail on flaky edges.
    export AFL_IGNORE_PROBLEMS=1

    # More aggressive havoc mutations from the start — don't wait for deterministic
    # stages to finish. Useful for protocol fuzzing where random mutations find new paths.
    export AFL_EXPAND_HAVOC_NOW=1

    # Custom RESP protocol mutator — mutates at command/argument level
    # instead of random bytes, keeping RESP framing valid.
    export PYTHONPATH="$FUZZ_DIR"
    export AFL_PYTHON_MODULE=resp_mutator

    exec "${AFL_CMD[@]}"
}

main() {
    check_requirements
    setup_system
    setup_directories
    show_config
    run_fuzzer
}

main "$@"
