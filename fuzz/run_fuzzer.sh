#!/usr/bin/env bash

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Target: "resp" (default) or "memcache"
TARGET="${1:-resp}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-dbg}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/$TARGET}"
# The composite action reads this marker after afl-fuzz exits and removes the recorded directories.
TEMP_DIRS_FILE="${OUTPUT_DIR}/temp_dirs.txt"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/$TARGET}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/$TARGET}"
DICT_FILE="${DICT_FILE:-$FUZZ_DIR/dict/$TARGET.dict}"
TIMEOUT="5000"
FUZZ_TARGET="$BUILD_DIR/dragonfly"
AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-1}"
AFL_MEM_MB="${AFL_MEM_MB:-4096}"  # Memory limit (MB) passed to afl-fuzz -m; also written to repro.env

# Tiering (disk-backed storage) fuzzing. When AFL_ENABLE_TIERING=1, Dragonfly is launched with
# tiered storage enabled so the fuzzer exercises the offload/fetch code paths. Used in the nightly
# (long) tiering leg. offload_threshold=1.0 offloads eligible values eagerly regardless of memory
# pressure, so tiering is stressed even with the small values the mutator produces.
AFL_TIER_MAXMEMORY="${AFL_TIER_MAXMEMORY:-1G}"
AFL_TIER_OFFLOAD_THRESHOLD="${AFL_TIER_OFFLOAD_THRESHOLD:-1.0}"
# Where tiering backing files live. CI points this at the runner temp dir (a real disk mounted
# from the host); the /tmp default may be tmpfs/overlayfs with unrealistic IO timing.
AFL_TIER_DIR="${AFL_TIER_DIR:-/tmp}"
# tiered_experimental_cooling ("experimental" is a legacy name, the feature is stable): pin with
# AFL_TIER_COOLING=true|false; otherwise alternate by AFL_RUN_NUMBER parity so nightly campaigns
# cover both code paths, falling back to the server default (true) when neither is set.
AFL_TIER_COOLING="${AFL_TIER_COOLING:-}"

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

    if [[ "$TARGET" != "resp" && "$TARGET" != "memcache" ]]; then
        print_warning "Unknown target: $TARGET (use 'resp' or 'memcache')"
        exit 1
    fi
}

setup_directories() {
    print_info "Setting up directories..."
    mkdir -p "${OUTPUT_DIR}"
    mkdir -p "${CORPUS_DIR}"
    touch "${TEMP_DIRS_FILE}"

    # When AFL_ENABLE_SAVE=1, enable SAVE/BGSAVE by pointing --dbfilename and --dir
    # to a temp directory. Used in nightly (long) fuzzing to test snapshot serialization.
    if [[ "${AFL_ENABLE_SAVE:-}" == "1" ]]; then
        DB_DIR=$(mktemp -d /tmp/dragonfly-fuzz-db.XXXXXX)
        printf '%s\n' "${DB_DIR}" >> "${TEMP_DIRS_FILE}"
        DB_FILENAME="dump"
        print_info "Save mode enabled — database directory: ${DB_DIR}"
    else
        DB_DIR=""
        DB_FILENAME=""
    fi

    # When AFL_ENABLE_TIERING=1, enable tiered storage by pointing --tiered_prefix at a temp
    # backing directory. Dragonfly opens "<prefix>-NNNN.dts" per shard with O_TRUNC, so each
    # server (re)start gets a fresh backing file. O_DIRECT (the production default) is kept when
    # the backing filesystem supports it; tmpfs/overlayfs reject it, which would abort the server
    # on startup, so fall back to buffered IO there.
    if [[ "${AFL_ENABLE_TIERING:-}" == "1" ]]; then
        TIER_DIR=$(mktemp -d "${AFL_TIER_DIR}/dragonfly-fuzz-tier.XXXXXX")
        printf '%s\n' "${TIER_DIR}" >> "${TEMP_DIRS_FILE}"
        TIER_PREFIX="${TIER_DIR}/backing"
        if dd if=/dev/zero of="${TIER_DIR}/odirect_probe" bs=4096 count=1 oflag=direct \
            status=none 2>/dev/null; then
            TIER_DIRECT=true
        else
            TIER_DIRECT=false
        fi
        rm -f "${TIER_DIR}/odirect_probe"
        if [[ -n "${AFL_TIER_COOLING}" ]]; then
            TIER_COOLING="${AFL_TIER_COOLING}"
        elif [[ "${AFL_RUN_NUMBER:-}" =~ ^[0-9]+$ ]]; then
            # 10# forces decimal: leading-zero values like 08 would otherwise be parsed as octal
            TIER_COOLING=$([[ $((10#$AFL_RUN_NUMBER % 2)) -eq 0 ]] && echo true || echo false)
        else
            TIER_COOLING=true
        fi
        print_info "Tiering enabled — backing prefix: ${TIER_PREFIX} (direct=${TIER_DIRECT}, cooling=${TIER_COOLING})"
    else
        TIER_DIR=""
        TIER_PREFIX=""
        TIER_DIRECT=""
        TIER_COOLING=""
    fi

    if [[ -z "$(ls -A "$CORPUS_DIR" 2>/dev/null)" ]]; then
        if [[ -d "${SEEDS_DIR}" ]] && [[ -n "$(ls -A "${SEEDS_DIR}" 2>/dev/null)" ]]; then
            print_info "Copying seeds to corpus..."
            cp "${SEEDS_DIR}"/* "${CORPUS_DIR}/" 2>/dev/null || true
        else
            print_warning "No seeds found, creating minimal seed"
            if [[ "$TARGET" == "memcache" ]]; then
                printf 'version\r\n' > "${CORPUS_DIR}/version"
            else
                echo -e '*1\r\n$4\r\nPING\r\n' > "${CORPUS_DIR}/ping"
            fi
        fi
    fi
}

show_config() {
    echo ""
    print_info "AFL++ Persistent Mode Configuration:"
    echo "  Target:           ${TARGET}"
    echo "  Binary:           ${FUZZ_TARGET}"
    echo "  Corpus:           ${CORPUS_DIR}"
    echo "  Output:           ${OUTPUT_DIR}"
    echo "  Dictionary:       ${DICT_FILE}"
    echo "  Timeout:          ${TIMEOUT}ms"
    echo "  Proactor threads: ${AFL_PROACTOR_THREADS}"
    echo "  Memory limit:     ${AFL_MEM_MB}MB"
    echo "  Loop limit:      ${AFL_LOOP_LIMIT} (= AFL_PERSISTENT_RECORD)"
    echo "  Save mode:       ${AFL_ENABLE_SAVE:-off}"
    echo "  Tiering:         ${TIER_PREFIX:-off}${TIER_PREFIX:+ (direct=${TIER_DIRECT}, cooling=${TIER_COOLING})}"
    echo ""
    print_note "Fuzzing integrated in dragonfly (USE_AFL + persistent mode)"
    print_note "Usage: ./run_fuzzer.sh [resp|memcache]"
    print_note "To change proactor threads: export AFL_PROACTOR_THREADS=N (default: 1)"
    print_note "To change memory limit: export AFL_MEM_MB=N (default: 4096)"
    print_note "To change loop limit: export AFL_LOOP_LIMIT=N (default: 10000)"
    echo ""
}

write_repro_env() {
    # Write Dragonfly flags + memory limit so package_crash.sh can bundle them
    # with every crash archive. triage_crashes.sh reads this file to start
    # Dragonfly with the exact same configuration as during fuzzing.
    mkdir -p "${OUTPUT_DIR}"
    local out="${OUTPUT_DIR}/repro.env"
    {
        echo "# Dragonfly reproduction environment — generated by run_fuzzer.sh"
        echo "MEM_LIMIT_KB=$((AFL_MEM_MB * 1024))"
        echo "--port=6379"
        echo "--proactor_threads=${AFL_PROACTOR_THREADS}"
        echo "--dbfilename=${DB_FILENAME}"
        echo "--omit_basic_usage"
        echo "--restricted_commands=SHUTDOWN,DEBUG,FLUSHALL,FLUSHDB"
        echo "--max_bulk_len=1048576"
        if [[ -n "$TIER_PREFIX" ]]; then
            # cwd-relative prefix so repro works from any directory (parent dir = cwd, which always
            # exists). triage_crashes.sh rewrites this to its per-crash temp dir.
            echo "--tiered_prefix=tiered_backing"
            echo "--maxmemory=${AFL_TIER_MAXMEMORY}"
            echo "--tiered_offload_threshold=${AFL_TIER_OFFLOAD_THRESHOLD}"
            echo "--backing_file_direct=${TIER_DIRECT}"
            echo "--tiered_experimental_cooling=${TIER_COOLING}"
        fi
        [[ "$TARGET" == "memcache" ]] && echo "--memcached_port=11211"
    } > "$out"
    print_info "Reproduction environment: ${out}"
}

run_fuzzer() {
    print_info "Starting AFL++ persistent mode fuzzing (target: $TARGET)..."
    print_info "Press Ctrl+C to stop"
    echo ""

    AFL_CMD=(
        afl-fuzz
        -o "${OUTPUT_DIR}"
        -t "${TIMEOUT}"
        -m "${AFL_MEM_MB}"
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
        --dbfilename="${DB_FILENAME}"
        --omit_basic_usage
        --restricted_commands=SHUTDOWN,DEBUG,FLUSHALL,FLUSHDB
        --max_bulk_len=1048576
    )

    [[ -n "$DB_DIR" ]] && AFL_CMD+=(--dir="${DB_DIR}")

    if [[ -n "$TIER_PREFIX" ]]; then
        AFL_CMD+=(
            --tiered_prefix="${TIER_PREFIX}"
            --maxmemory="${AFL_TIER_MAXMEMORY}"
            --tiered_offload_threshold="${AFL_TIER_OFFLOAD_THRESHOLD}"
            --backing_file_direct="${TIER_DIRECT}"
            --tiered_experimental_cooling="${TIER_COOLING}"
        )
    fi

    if [[ "$TARGET" == "memcache" ]]; then
        AFL_CMD+=(--memcached_port=11211 --afl_target_port=11211)
    fi

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

    # Custom protocol mutator — mutates at command/argument level
    # instead of random bytes, keeping protocol framing valid.
    export PYTHONPATH="$FUZZ_DIR"
    if [[ "$TARGET" == "memcache" ]]; then
        export AFL_PYTHON_MODULE=memcache_mutator
    else
        export AFL_PYTHON_MODULE=resp_mutator
    fi

    exec "${AFL_CMD[@]}"
}

main() {
    check_requirements
    setup_directories
    write_repro_env
    show_config
    run_fuzzer
}

main "$@"
