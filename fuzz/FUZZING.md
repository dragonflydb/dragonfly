# AFL++ Fuzzing for Dragonfly

## Install AFL++

For effective fuzzing with crash replay support, AFL++ must be built from source with `AFL_PERSISTENT_RECORD` enabled.

```bash
# Install dependencies
sudo apt update
sudo apt install llvm-18-dev clang-18 lld-18 gcc-13-plugin-dev

# Build AFL++ with AFL_PERSISTENT_RECORD support
git clone --depth=1 --branch v4.34c https://github.com/AFLplusplus/AFLplusplus.git
cd AFLplusplus

# Enable AFL_PERSISTENT_RECORD (required for stateful crash replay)
sed -i 's|// #define AFL_PERSISTENT_RECORD|#define AFL_PERSISTENT_RECORD|' include/config.h

make distrib
sudo make install
```

## Prepare System

```bash
sudo afl-system-config
```

Sets core_pattern and CPU governors for optimal AFL++ performance.

## Build Dragonfly

The build automatically enables ASAN and UBSAN for maximum bug detection.

```bash
cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -C build-dbg dragonfly
```

## Run Fuzzer

```bash
cd fuzz
./run_fuzzer.sh
```

The fuzzer uses 2 proactor threads by default to maximize race condition
detection. Override with `AFL_PROACTOR_THREADS=N` if needed.

## Crash Replay (AFL_PERSISTENT_RECORD)

Dragonfly uses AFL++ persistent mode where the server accumulates state between
iterations. A crash at iteration 5000 may depend on state built by inputs 1-4999.

**Solution:** `run_fuzzer.sh` syncs `AFL_PERSISTENT_RECORD` with `afl_loop_limit`
(default: 10000). The server restarts every N iterations, and the last N inputs
are always recorded. This guarantees that on any crash, the **full state history**
of the current process is available for replay.

```bash
# Default: 10000 iterations, all recorded
./run_fuzzer.sh

# Custom limit (lower = more restarts, faster replay; higher = deeper state)
AFL_LOOP_LIMIT=5000 ./run_fuzzer.sh
```

When a crash occurs, AFL++ saves files in the crashes directory:
```
crashes/RECORD:000000,cnt:000000  (first input after server start)
crashes/RECORD:000000,cnt:000001  (second input)
...
crashes/RECORD:000000,cnt:000NNN  (crashing input)
```

### Replay Recorded Crash

```bash
# Set directory containing RECORD files
export AFL_PERSISTENT_DIR=./artifacts/resp/default/crashes

# Replay specific record (e.g., record 000000)
AFL_PERSISTENT_REPLAY=000000 ./build-dbg/dragonfly --port=6379
```

This replays all recorded inputs in sequence, reproducing the exact state that led to the crash.

### Manual Replay (Alternative)

If AFL_PERSISTENT_REPLAY doesn't work, replay manually:

```bash
# Start dragonfly
./build-dbg/dragonfly --port=6379 &

# Send each recorded input in order
for f in $(ls crashes/RECORD:000000,cnt:* | sort); do
    nc localhost 6379 < "$f"
done
```

## Replay Simple Crash

For crashes that don't depend on accumulated state:

```bash
./build-dbg/dragonfly --port=6379 &
nc localhost 6379 < artifacts/resp/default/crashes/id:000000,...
```

## Seed Corpus

The `seeds/resp/` directory contains 54 seed files covering all major command families:
string, list, hash, set, sorted set, stream, JSON, bitfield, bitops, geo,
HyperLogLog, Bloom filter, transactions, pub/sub, scripting, search (FT.*),
ACL, scan, key/expire operations, and server introspection.

Seeds use multi-command sequences that first create state then exercise
operations on it, ensuring deep code path coverage beyond the parser.

To add a new seed, create a file with valid RESP-encoded commands using `\n` line endings:

```
*3
$3
SET
$3
key
$5
value
```
