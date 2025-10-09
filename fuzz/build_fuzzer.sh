#!/usr/bin/env bash
# Build script for AFL++ fuzz targets

set -e

# Colors
GREEN='\033[0;32m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_DIR="${BUILD_DIR:-$PROJECT_ROOT/build-fuzz}"

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

configure_build() {
    print_info "Configuring build with AFL++ instrumentation..."

    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    CC=afl-clang-fast \
    CXX=afl-clang-fast++ \
    cmake \
        -DCMAKE_BUILD_TYPE=Debug \
        -GNinja \
        -DDFLY_FUZZ=ON \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DWITH_AWS:BOOL=OFF \
        -DWITH_GCP:BOOL=OFF \
        "$PROJECT_ROOT"

    print_info "Configuration complete."
}

build_targets() {
    print_info "Building fuzz targets..."

    cd "$BUILD_DIR"
    cmake --build . --target fuzz_targets -j$(nproc)

    print_info "Build complete."
}

main() {
    configure_build
    build_targets
}

main "$@"
