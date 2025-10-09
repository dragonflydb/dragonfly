#!/usr/bin/env bash
# Script to reproduce AFL++ crashes with full debugging

set -e

CRASH_FILE="$1"
BUILD_DIR="${BUILD_DIR:-build-fuzz}"

if [[ -z "$CRASH_FILE" ]]; then
    echo "Usage: $0 <crash_file>"
    echo ""
    echo "Example:"
    echo "  $0 fuzz/artifacts/resp/fuzzer01/crashes/id:000000*"
    echo ""
    echo "This will:"
    echo "  1. Show crash input (hex dump)"
    echo "  2. Show monitored commands"
    echo "  3. Show Dragonfly logs"
    echo "  4. Run with GDB for stack trace"
    exit 1
fi

if [[ ! -f "$CRASH_FILE" ]]; then
    echo "Error: Crash file not found: $CRASH_FILE"
    exit 1
fi

FUZZ_BIN="$BUILD_DIR/fuzz/resp_fuzz"
if [[ ! -f "$FUZZ_BIN" ]]; then
    echo "Error: Fuzzer binary not found: $FUZZ_BIN"
    echo "Build with: cd fuzz && ./build_fuzzer.sh"
    exit 1
fi

echo "========================================"
echo "  Dragonfly AFL++ Crash Reproducer"
echo "========================================"
echo ""
echo "Crash file: $CRASH_FILE"
echo "Size: $(wc -c < "$CRASH_FILE") bytes"
echo ""

echo "=== 1. Crash Input (hex) ==="
hexdump -C "$CRASH_FILE" | head -20
echo ""

echo "=== 2. Monitored Commands ==="
rm -f crash_commands.log dragonfly.{INFO,WARNING,ERROR}*
$FUZZ_BIN \
    --fuzzer_monitor=true \
    --fuzzer_monitor_file=crash_commands.log \
    < "$CRASH_FILE" 2>&1 | head -50 || true

if [[ -f crash_commands.log ]]; then
    cat crash_commands.log
else
    echo "(no commands logged - crash before execution)"
fi
echo ""

echo "=== 3. Dragonfly Logs ==="
echo "INFO:"
cat dragonfly.INFO* 2>/dev/null | tail -20 || echo "(no INFO logs)"
echo ""
echo "WARNING:"
cat dragonfly.WARNING* 2>/dev/null | tail -20 || echo "(no WARNING logs)"
echo ""
echo "ERROR:"
cat dragonfly.ERROR* 2>/dev/null | tail -20 || echo "(no ERROR logs)"
echo ""

echo "=== 4. GDB Stack Trace ==="
echo "Running with GDB..."
gdb -batch -ex "run < $CRASH_FILE" -ex "bt" -ex "quit" --args $FUZZ_BIN 2>&1 | tail -50

echo ""
echo "========================================"
echo "To debug interactively:"
echo "  gdb --args $FUZZ_BIN --fuzzer_monitor=true"
echo "  (gdb) run < $CRASH_FILE"
echo "  (gdb) bt"
echo "  (gdb) info locals"
echo "========================================"
