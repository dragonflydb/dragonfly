#!/usr/bin/env bash
# Build script for AFL++ fuzz targets
# Usage: ./build_fuzzer.sh [target]

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET="${1:-all}"
BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-fuzz}"

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_header() {
    echo -e "${GREEN}--------------------------------${NC}"
    echo -e "${GREEN}  Building Dragonfly Fuzzers${NC}"
    echo -e "${GREEN}--------------------------------${NC}"
}

check_afl() {
    print_info "Checking for AFL++ installation..."

    if ! command -v afl-clang-fast &> /dev/null; then
        print_error "AFL++ compilers not found!"
        echo ""
        echo "Please install AFL++:"
        echo "  Ubuntu/Debian: sudo apt-get install afl++"
        echo "  macOS: brew install afl++"
        echo "  Or build from source: https://github.com/AFLplusplus/AFLplusplus"
        exit 1
    fi

    print_info "AFL++ found: $(which afl-clang-fast)"
}

configure_build() {
    print_info "Configuring build with AFL++ instrumentation..."

    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    # Configure with AFL++ compilers
    CC=afl-clang-fast \
    CXX=afl-clang-fast++ \
    cmake \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DDFLY_FUZZ=ON \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        "$PROJECT_ROOT"

    print_info "Configuration complete ✓"
}

build_targets() {
    print_info "Building fuzz targets..."

    cd "$BUILD_DIR"

    if [[ "$TARGET" == "all" ]]; then
        cmake --build . --target fuzz_targets -j$(nproc)
    else
        cmake --build . --target "${TARGET}_fuzz" -j$(nproc)
    fi

    print_info "Build complete ✓"
}

show_summary() {
    echo ""
    print_info "Fuzz targets built successfully!"
    echo ""
    echo "Built targets:"
    ls -lh "$BUILD_DIR/fuzz/"*_fuzz 2>/dev/null || echo "  (none)"
    echo ""
    echo "Next steps:"
    echo "  1. Run fuzzer:     $SCRIPT_DIR/run_fuzzer.sh resp"
    echo "  2. View docs:      cat $SCRIPT_DIR/README.md"
    echo ""
}

main() {
    print_header
    check_afl
    configure_build
    build_targets
    show_summary
}

main "$@"
