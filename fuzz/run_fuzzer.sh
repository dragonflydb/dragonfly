#!/usr/bin/env bash
# AFL++ Socket Fuzzing для Dragonfly
# Використовує socketfuzz.so (LD_PRELOAD) для перехоплення socket calls

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
TIMEOUT="15000+"
FUZZ_TARGET="$BUILD_DIR/dragonfly"
SOCKETFUZZ_LIB="$BUILD_DIR/libsocketfuzz.so"

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
    if [[ ! -f "$FUZZ_TARGET" ]]; then
        print_warning "Dragonfly not found at $FUZZ_TARGET"
        print_warning "Build with: cmake -B build-dbg -DUSE_AFL=ON -GNinja && ninja -C build-dbg dragonfly"
        exit 1
    fi

    if [[ ! -f "$SOCKETFUZZ_LIB" ]]; then
        print_warning "socketfuzz.so not found at $SOCKETFUZZ_LIB"
        print_warning "It should be built automatically with dragonfly when USE_AFL=ON"
        print_warning "Rebuild with: ninja -C $BUILD_DIR"
        exit 1
    fi
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
            print_warning "No seeds found, creating minimal seed"
            echo -e '*1\r\n$4\r\nPING\r\n' > "$CORPUS_DIR/ping"
        fi
    fi
}

show_config() {
    echo ""
    print_info "Configuration:"
    echo "  Binary:      $FUZZ_TARGET"
    echo "  SocketFuzz:  $SOCKETFUZZ_LIB"
    echo "  Corpus:      $CORPUS_DIR"
    echo "  Output:      $OUTPUT_DIR"
    echo "  Dictionary:  $DICT_FILE"
    echo "  Timeout:     ${TIMEOUT}ms"
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++ with socket fuzzing..."
    print_info "Press Ctrl+C to stop"
    echo ""

    AFL_CMD=(
        afl-fuzz
        -o "$OUTPUT_DIR"
        -t "$TIMEOUT"
        -m none
        -i "$CORPUS_DIR"
    )

    if [[ -f "$DICT_FILE" ]]; then
        AFL_CMD+=(-x "$DICT_FILE")
    fi

    AFL_CMD+=(
        --
        "$FUZZ_TARGET"
        --port=6379
        --logtostderr
        --proactor_threads=2
        --dbfilename=""
        --omit_basic_usage
    )

    print_info "Running: AFL_PRELOAD=libsocketfuzz.so ${AFL_CMD[*]}"
    echo ""

    cd "$OUTPUT_DIR"

    # Run AFL++ with socketfuzz LD_PRELOAD
    # Use AFL_PRELOAD instead of LD_PRELOAD for better AFL++ compatibility
    export AFL_PRELOAD="$SOCKETFUZZ_LIB"

    exec "${AFL_CMD[@]}"
}

main() {
    check_requirements
    setup_directories
    show_config
    run_fuzzer
}

main "$@"
