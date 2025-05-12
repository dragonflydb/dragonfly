#!/bin/bash
# Enhanced script for fuzzing dragonfly using afl++ with comprehensive command testing

# Parameters
REDIS_HOST=${REDIS_HOST:-"127.0.0.1"}
REDIS_PORT=${REDIS_PORT:-"6379"}
OUTPUT_DIR=${OUTPUT_DIR:-"$(pwd)/tests/afl/output"}
INPUT_DIR=${INPUT_DIR:-"$(pwd)/tests/afl/input"}
DICT_FILE=${DICT_FILE:-"$(pwd)/tests/afl/redis.dict"}
MAX_COMMANDS=${MAX_COMMANDS:-30}

# Check if directories exist
mkdir -p "$OUTPUT_DIR"
mkdir -p "$INPUT_DIR"

# Check for AFL++ availability
if ! command -v afl-fuzz &> /dev/null; then
    echo "AFL++ needs to be installed"
    exit 1
fi

# Check for fuzzing script
if [ ! -f "tests/afl/redis_fuzzer.py" ]; then
    echo "File redis_fuzzer.py not found"
    exit 1
fi

# Set execution permissions
chmod +x tests/afl/redis_fuzzer.py

# Check if dragonfly is running (without using nc)
if timeout 1 bash -c "cat < /dev/null > /dev/tcp/$REDIS_HOST/$REDIS_PORT" 2>/dev/null; then
    echo "Connection to Dragonfly on $REDIS_HOST:$REDIS_PORT successful!"
else
    echo "Dragonfly is not running on $REDIS_HOST:$REDIS_PORT"
    echo "Please start dragonfly before testing"
    exit 1
fi

# Always create or update dictionary
echo "Generating Redis command dictionary..."
cd $(dirname "$0") && python3 redis_fuzzer.py --create-dict && cd - > /dev/null

# Make sure there are input case files
if [ ! -f "$INPUT_DIR/init_case.txt" ]; then
    echo "Creating initial test cases..."
    cat > "$INPUT_DIR/init_case.txt" << EOF
SET key1 value1
GET key1
HSET hash1 field1 value1
LPUSH list1 element1
SADD set1 member1
ZADD zset1 1.0 member1
PING
INFO
EOF
fi

# Create some input cases with special characters
if [ ! -f "$INPUT_DIR/special_chars.txt" ]; then
    echo "Creating special character test cases..."
    cat > "$INPUT_DIR/special_chars.txt" << EOF
SET key! "value with spaces"
SET "key with \"quotes\"" "value with \\backslashes\\"
HSET complex@key "weird!field" "value\\nwith\\tescape sequences"
LPUSH emoji🔥 "unicode😊value"
EOF
fi

# Ignore core dump and instrumentation errors for AFL++
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1
export AFL_SKIP_BIN_CHECK=1
export AFL_NO_ARITH=1

echo "Starting enhanced AFL++ fuzzing with comprehensive command testing..."

# Clean previous results
rm -rf "$OUTPUT_DIR"

# Run AFL++ with enhanced options
afl-fuzz -i "$INPUT_DIR" -o "$OUTPUT_DIR" -n -m none -x "$DICT_FILE" -t 5000 \
    -- python3 tests/afl/redis_fuzzer.py --host "$REDIS_HOST" --port "$REDIS_PORT" --commands "$MAX_COMMANDS"

echo "Fuzzing completed!"
