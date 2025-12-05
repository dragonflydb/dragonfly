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
AFL_PROACTOR_THREADS="${AFL_PROACTOR_THREADS:-2}"

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
    echo ""
    print_note "Fuzzing integrated in dragonfly (USE_AFL + persistent mode)"
    print_note "To change proactor threads: export AFL_PROACTOR_THREADS=N (default: 2)"
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++ persistent mode fuzzing..."
    print_info "Press Ctrl+C to stop"
    echo ""

    AFL_CMD=(
        afl-fuzz -D
        -l 2
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
        --bind=0.0.0.0
        --bind=::
        --dbfilename=""
        --omit_basic_usage
        --rename_command=SHUTDOWN=
        --rename_command=DEBUG=
        --rename_command=FLUSHALL=
        --rename_command=FLUSHDB=
    )

    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    cd "${OUTPUT_DIR}"

    # Run AFL++ - fuzzing integrated in dragonfly via USE_AFL
    # AFL_HANG_TMOUT: Only consider it a hang if no response for 60 seconds
    # This prevents false positives from slow but legitimate operations
    export AFL_HANG_TMOUT=60000
    exec "${AFL_CMD[@]}"
}

main() {
    check_requirements
    setup_directories
    show_config
    run_fuzzer
}

main "$@"
