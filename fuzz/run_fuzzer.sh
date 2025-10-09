#!/usr/bin/env bash
# AFL++ Fuzzer runner for Dragonfly
# Usage: ./run_fuzzer.sh <target> [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration
TARGET="${1:-resp}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-fuzz}"
FUZZ_DIR="$SCRIPT_DIR"
OUTPUT_DIR="${OUTPUT_DIR:-$FUZZ_DIR/artifacts/$TARGET}"
CORPUS_DIR="${CORPUS_DIR:-$FUZZ_DIR/corpus/$TARGET}"
SEEDS_DIR="${SEEDS_DIR:-$FUZZ_DIR/seeds/$TARGET}"
DICT_FILE="${DICT_FILE:-$FUZZ_DIR/dict/$TARGET.dict}"
TIMEOUT="${TIMEOUT:-1000}"  # milliseconds
MEM_LIMIT="${MEM_LIMIT:-2048}"  # MB
JOBS="${JOBS:-$(nproc)}"
TIME_LIMIT="${TIME_LIMIT:-0}"  # 0 = unlimited
USE_EPOLL="${USE_EPOLL:-0}"  # 0=IoUring (default), 1=Epoll
MONITOR="${MONITOR:-0}"  # 0=off, 1=show commands like redis-cli MONITOR
MONITOR_FILE="${MONITOR_FILE:-$FUZZ_DIR/artifacts/$TARGET/commands.log}"  # Where to write commands
FLUSH_BETWEEN="${FLUSH_BETWEEN:-0}"  # 0=off, 1=run FLUSHALL after each test case

print_header() {
    echo -e "${GREEN}--------------------------------${NC}"
    echo -e "${GREEN}  Dragonfly AFL++ Fuzzer${NC}"
    echo -e "${GREEN}--------------------------------${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

check_requirements() {
    print_info "Checking requirements..."

    # Check if AFL++ is installed
    if ! command -v afl-fuzz &> /dev/null; then
        print_error "afl-fuzz not found. Please install AFL++:"
        echo "  Ubuntu/Debian: sudo apt-get install afl++"
        echo "  Or build from source: https://github.com/AFLplusplus/AFLplusplus"
        exit 1
    fi

    # Check if fuzz target exists
    FUZZ_TARGET="$BUILD_DIR/fuzz/${TARGET}_fuzz"
    if [[ ! -f "$FUZZ_TARGET" ]]; then
        print_error "Fuzz target not found: $FUZZ_TARGET"
        print_info "Please build with: ./build_fuzzer.sh $TARGET"
        exit 1
    fi

    # Check if target was compiled with AFL++
    if ! strings "$FUZZ_TARGET" | grep -q "__AFL"; then
        print_warning "Target may not be instrumented with AFL++"
        print_info "Build with: CC=afl-clang-fast CXX=afl-clang-fast++ ./build_fuzzer.sh"
    fi

    print_info "Requirements check passed ✓"
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

    print_info "Directories ready ✓"
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
    echo "  IO Engine:   $([ "$USE_EPOLL" = "1" ] && echo "Epoll" || echo "IoUring (default)")"
    if [ "$MONITOR" = "1" ]; then
        echo "  Monitor:     ON → $MONITOR_FILE"
    else
        echo "  Monitor:     OFF"
    fi
    if [ "$FLUSH_BETWEEN" = "1" ]; then
        echo "  FlushEach:   ON (FLUSHALL between testcases)"
    else
        echo "  FlushEach:   OFF"
    fi
    echo ""
}

run_fuzzer() {
    print_info "Starting AFL++ fuzzer..."
    print_info "Press Ctrl+C to stop"
    echo ""

    # Build AFL++ command
    # Detect resume mode: if an existing AFL++ queue is present, use -i -
    RESUME=0
    if [[ -d "$OUTPUT_DIR/queue" ]] || [[ -d "$OUTPUT_DIR/fuzzer01/queue" ]]; then
        RESUME=1
    fi

    AFL_CMD=(
        afl-fuzz
        -o "$OUTPUT_DIR"
        -t "$TIMEOUT"
        -m "$MEM_LIMIT"
    )

    if [[ "$RESUME" -eq 1 ]]; then
        AFL_CMD+=(-i -)
        print_info "Resuming from existing output dir → $OUTPUT_DIR"
    else
        AFL_CMD+=(-i "$CORPUS_DIR")
    fi

    # Add dictionary if it exists
    if [[ -f "$DICT_FILE" ]]; then
        AFL_CMD+=(-x "$DICT_FILE")
    fi

    # Add time limit if specified
    if [[ "$TIME_LIMIT" -gt 0 ]]; then
        AFL_CMD+=(-V "$TIME_LIMIT")
    fi

    # Add parallel jobs
    if [[ "$JOBS" -gt 1 ]]; then
        AFL_CMD+=(-M "fuzzer01")
    fi

    # Add the target binary with flags
    AFL_CMD+=("$FUZZ_TARGET")

    # Add Epoll flag if requested
    if [[ "$USE_EPOLL" = "1" ]]; then
        AFL_CMD+=(--force_epoll=true)
        print_info "Using Epoll"
    else
        print_info "Using IoUring (default)"
    fi

    # Add monitor flag if requested
    if [[ "$MONITOR" = "1" ]]; then
        AFL_CMD+=(--fuzzer_monitor=true --fuzzer_monitor_file="$MONITOR_FILE")
        print_info "Monitor mode ON → $MONITOR_FILE"
    fi

    # Enable deterministic per-testcase state by flushing DB
    if [[ "$FLUSH_BETWEEN" = "1" ]]; then
        AFL_CMD+=(--fuzzer_flush_all_between_tests=true)
        print_info "FLUSHALL between testcases enabled"
    fi

    # Display command
    print_info "Running: ${AFL_CMD[*]}"
    echo ""

    # Execute fuzzer
    exec "${AFL_CMD[@]}"
}

show_help() {
    cat << EOF
Usage: $0 <target> [options]

Targets:
  resp          RESP protocol fuzzer (default)

Environment Variables:
  BUILD_DIR     Build directory (default: build-fuzz)
  OUTPUT_DIR    AFL++ output directory (default: fuzz/artifacts/<target>)
  CORPUS_DIR    Input corpus directory (default: fuzz/corpus/<target>)
  SEEDS_DIR     Initial seeds directory (default: fuzz/seeds/<target>)
  DICT_FILE     Dictionary file (default: fuzz/dict/<target>.dict)
  TIMEOUT       Execution timeout in ms (default: 1000)
  MEM_LIMIT     Memory limit in MB (default: 2048)
  JOBS          Number of parallel jobs (default: nproc)
  TIME_LIMIT    Stop after N seconds (default: 0 = unlimited)
  USE_EPOLL     Use Epoll instead of IoUring (default: 0)
  MONITOR       Show commands like redis-cli MONITOR (default: 0)
  MONITOR_FILE  Where to write monitored commands (default: artifacts/<target>/commands.log)

Examples:
  # Basic usage
  $0 resp

  # Custom timeout and memory
  TIMEOUT=500 MEM_LIMIT=4096 $0 resp

  # 5 minute run with 4 parallel jobs
  TIME_LIMIT=300 JOBS=4 $0 resp

  # Resume from previous run
  OUTPUT_DIR=fuzz/artifacts/resp $0 resp

  # Test with Epoll instead of IoUring
  USE_EPOLL=1 $0 resp

  # Monitor mode - see all commands being executed
  MONITOR=1 TIME_LIMIT=10 $0 resp

  # Custom monitor file location
  MONITOR=1 MONITOR_FILE=./my_commands.log $0 resp

For more information, see: fuzz/README.md
EOF
}

# Main execution
main() {
    if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
        show_help
        exit 0
    fi

    print_header
    check_requirements
    setup_directories
    show_config
    run_fuzzer
}

main "$@"
