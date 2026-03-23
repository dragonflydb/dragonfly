#!/usr/bin/env bash

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }

usage() {
    echo "Usage: $0 <crash_id> [crashes_dir]"
    echo ""
    echo "Packages a crash and its RECORD files into a self-contained archive"
    echo "that can be sent to another developer for reproduction."
    echo ""
    echo "Arguments:"
    echo "  crash_id      Crash ID (e.g. 000000)"
    echo "  crashes_dir   Path to crashes directory (default: fuzz/artifacts/resp/default/crashes)"
    echo ""
    echo "Example:"
    echo "  $0 000000"
    echo "  $0 000001 /path/to/crashes"
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
fi

CRASH_ID="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRASHES_DIR="${2:-$SCRIPT_DIR/artifacts/resp/default/crashes}"

if [[ ! -d "$CRASHES_DIR" ]]; then
    print_error "Crashes directory not found: $CRASHES_DIR"
    exit 1
fi

# Find the crash input file
CRASH_FILE=$(find "$CRASHES_DIR" -maxdepth 1 -name "id:${CRASH_ID},*" ! -name "RECORD:*" | head -1)
if [[ -z "$CRASH_FILE" ]]; then
    print_error "Crash input not found for id:${CRASH_ID} in $CRASHES_DIR"
    exit 1
fi

# Count RECORD files
RECORD_COUNT=$(find "$CRASHES_DIR" -maxdepth 1 -name "RECORD:${CRASH_ID},cnt:*" | wc -l)

ARCHIVE_NAME="crash-${CRASH_ID}"
TMPDIR=$(mktemp -d)
DEST="$TMPDIR/$ARCHIVE_NAME"
mkdir -p "$DEST/crashes"

print_info "Packaging crash ${CRASH_ID}..."
print_info "Crash input: $(basename "$CRASH_FILE")"
print_info "RECORD files: ${RECORD_COUNT}"

# Copy crash input and RECORD files into crashes/ subdirectory
cp "$CRASH_FILE" "$DEST/crashes/"
if [[ $RECORD_COUNT -gt 0 ]]; then
    find "$CRASHES_DIR" -maxdepth 1 -name "RECORD:${CRASH_ID},cnt:*" -exec cp {} "$DEST/crashes/" \;
fi

# Copy replay_crash.py
cp "$SCRIPT_DIR/replay_crash.py" "$DEST/"

# Copy repro.env — contains exact Dragonfly flags + memory limit used during fuzzing.
# triage_crashes.sh reads this to start Dragonfly identically to the fuzz run.
# repro.env lives one level above the fuzzer instance dir (i.e. OUTPUT_DIR):
#   crashes_dir  = .../artifacts/<target>/default/crashes
#   repro.env    = .../artifacts/<target>/repro.env
REPRO_ENV="$(dirname "$(dirname "$CRASHES_DIR")")/repro.env"
if [[ -f "$REPRO_ENV" ]]; then
    cp "$REPRO_ENV" "$DEST/"
    print_info "Reproduction environment: repro.env included"
else
    print_warn "repro.env not found at $REPRO_ENV — crash archive won't include exact fuzz flags"
fi

REPLAY_PORT=6379
if [[ -f "$DEST/repro.env" ]] && grep -q '^--memcached_port=' "$DEST/repro.env"; then
    REPLAY_PORT=$(grep '^--memcached_port=' "$DEST/repro.env" | cut -d= -f2)
fi

# Create archive
OUTPUT="$(pwd)/${ARCHIVE_NAME}.tar.gz"
tar -czf "$OUTPUT" -C "$TMPDIR" "$ARCHIVE_NAME"
rm -rf "$TMPDIR"

SIZE=$(du -h "$OUTPUT" | cut -f1)
print_info "Archive created: ${OUTPUT} (${SIZE})"
echo ""

echo "To reproduce:"
echo "  1. Extract the archive:"
echo "     tar xzf ${ARCHIVE_NAME}.tar.gz && cd ${ARCHIVE_NAME}"
echo "  2. Start Dragonfly with the exact flags from the fuzz run (repro.env):"
echo "     MEM_KB=\$(grep '^MEM_LIMIT_KB=' repro.env | cut -d= -f2)"
echo "     readarray -t DF_FLAGS < <(grep -v '^#' repro.env | grep -v '^MEM_LIMIT_KB=' | grep -v '^\$')"
echo "     (ulimit -v \"\$MEM_KB\"; exec ./build-dbg/dragonfly \"\${DF_FLAGS[@]}\") &"
echo "  3. Replay:"
echo "     python3 replay_crash.py crashes ${CRASH_ID} 127.0.0.1 ${REPLAY_PORT}"
