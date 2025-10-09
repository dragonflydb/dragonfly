#!/usr/bin/env bash
# AFL++ Fuzzer runner for Dragonfly

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration (single target)
TARGET="resp"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-fuzz}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/$TARGET}"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/$TARGET}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/$TARGET}"
DICT_FILE="${DICT_FILE:-$FUZZ_DIR/dict/$TARGET.dict}"
TIMEOUT="1000"    # milliseconds (constant)
MEM_LIMIT="2048"  # MB (constant)
JOBS="1"          # single instance for determinism
TIME_LIMIT="0"    # 0 = unlimited (constant)
FUZZ_TARGET="$BUILD_DIR/fuzz/${TARGET}_fuzz"

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

setup_directories() {
    print_info "Setting up directories..."

    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$CORPUS_DIR"

    # If corpus is empty, use seeds
    if [[ -z "$(ls -A "$CORPUS_DIR" 2>/dev/null)" ]]; then
        if [[ -d "$SEEDS_DIR" ]] && [[ -n "$(ls -A "$SEEDS_DIR" 2>/dev/null)" ]]; then
            print_info "Copying seeds to corpus..."
            cp "$SEEDS_DIR"/* "$CORPUS_DIR/" 2>/dev/null || true
        else
            print_warning "No seeds found in $SEEDS_DIR"
            # Create a minimal seed
            echo -e '*1\r\n$4\r\nPING\r\n' > "$CORPUS_DIR/ping"
        fi
    fi

    print_info "Directories ready."
}

show_config() {
    echo ""
    print_info "Configuration:"
    echo "  Target:      $TARGET"
    echo "  Binary:      $FUZZ_TARGET"
    echo "  Seeds:       $SEEDS_DIR"
    echo "  Corpus:      $CORPUS_DIR"
    echo "  Output:      $OUTPUT_DIR"
    echo "  Dictionary:  $DICT_FILE"
    echo "  Timeout:     ${TIMEOUT}ms"
    echo "  Memory:      ${MEM_LIMIT}MB"
    echo "  Jobs:        $JOBS"
    echo "  Time limit:  ${TIME_LIMIT}s (0=unlimited)"
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++ fuzzer..."
    print_info "Press Ctrl+C to stop"
    echo ""

    # Build AFL++ command (no resume; always use corpus directory)
    AFL_CMD=(
        afl-fuzz
        -o "$OUTPUT_DIR"
        -t "$TIMEOUT"
        -m "$MEM_LIMIT"
        -i "$CORPUS_DIR"
    )

    # Add dictionary if it exists
    if [[ -f "$DICT_FILE" ]]; then
        AFL_CMD+=(-x "$DICT_FILE")
    fi

    # Add time limit if specified
    if [[ "$TIME_LIMIT" -gt 0 ]]; then
        AFL_CMD+=(-V "$TIME_LIMIT")
    fi

    # Single job: no -M/-S cluster

    # Add the target binary (harness has fixed flags)
    AFL_CMD+=("$FUZZ_TARGET")

    # Display command
    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    # Execute fuzzer
    exec "${AFL_CMD[@]}"
}

main() {
    setup_directories
    show_config
    run_fuzzer
}

main "$@"
