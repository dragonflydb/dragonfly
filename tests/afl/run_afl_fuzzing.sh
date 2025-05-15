#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# Parameters
REDIS_HOST=${REDIS_HOST:-"127.0.0.1"}
REDIS_PORT=${REDIS_PORT:-"6379"}
OUTPUT_DIR=${OUTPUT_DIR:-"$SCRIPT_DIR/output"}
INPUT_DIR=${INPUT_DIR:-"$SCRIPT_DIR/input"}
DICT_FILE=${DICT_FILE:-"$SCRIPT_DIR/redis.dict"}
MAX_COMMANDS=${MAX_COMMANDS:-30}

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Always create or update dictionary using the separate dictionary generator
echo "Generating command dictionary..."
cd "$SCRIPT_DIR" && python3 "$SCRIPT_DIR/redis_dict_generator.py" --output "$DICT_FILE" && cd -

# Set environment variables for the fuzzer
export REDIS_HOST="$REDIS_HOST"
export REDIS_PORT="$REDIS_PORT"

# Ignore core dump and instrumentation errors for AFL++
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
export AFL_SKIP_BIN_CHECK=1
export AFL_NO_ARITH=1

echo "Starting AFL++ fuzzing with comprehensive command testing..."
echo "Target: $REDIS_HOST:$REDIS_PORT"

# Run AFL++
afl-fuzz -i "$INPUT_DIR" -o "$OUTPUT_DIR" -n -m none -x "$DICT_FILE" -t 5000 \
    -- python3 "$SCRIPT_DIR/redis_fuzzer.py" --host "$REDIS_HOST" --port "$REDIS_PORT" --commands "$MAX_COMMANDS"
