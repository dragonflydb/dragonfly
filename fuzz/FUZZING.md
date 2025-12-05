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

```bash
cmake -B build-dbg -DUSE_AFL=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
ninja -C build-dbg dragonfly
```

## Run Fuzzer

```bash
cd fuzz
./run_fuzzer.sh
```

## AFL_PERSISTENT_RECORD (Stateful Crash Replay)

Dragonfly uses AFL++ persistent mode for performance. This means multiple fuzzing iterations run within the same process, and the server accumulates state between iterations.

**Problem:** When a crash occurs, AFL++ only saves the last input. But the crash may depend on state accumulated from previous inputs.

**Solution:** `AFL_PERSISTENT_RECORD` saves the last N inputs before a crash, enabling replay of the full sequence.

### Enable Recording

```bash
# Set number of inputs to record before crash (e.g., last 100 inputs)
AFL_PERSISTENT_RECORD=100 ./run_fuzzer.sh
```

When a crash occurs, AFL++ saves files in the crashes directory:
```
crashes/RECORD:000000,cnt:000000  (input N-99)
crashes/RECORD:000000,cnt:000001  (input N-98)
...
crashes/RECORD:000000,cnt:000099  (crashing input)
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

## Fuzzer Configuration

The `run_fuzzer.sh` script configures:

- **Timeout:** 500ms per input (adjust with `TIMEOUT` variable)
- **CMPLOG:** `-l 2` for automatic command discovery
- **Memory limit:** 4GB per instance
- **Disabled commands:** SHUTDOWN, DEBUG, FLUSHALL, FLUSHDB

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AFL_PROACTOR_THREADS` | Dragonfly worker threads | 2 |
| `AFL_PERSISTENT_RECORD` | Inputs to save before crash | disabled |
| `BUILD_DIR` | Build directory path | `build-dbg` |
| `OUTPUT_DIR` | Fuzzer output directory | `artifacts/resp` |
| `CORPUS_DIR` | Corpus directory | `corpus/resp` |

## Debugging Crashes

1. **With ASAN (recommended for memory bugs):**
   ```bash
   # Build separate ASAN binary (not for fuzzing, only for replay)
   cmake -B build-asan -DWITH_ASAN=ON -DCMAKE_BUILD_TYPE=Debug -GNinja
   ninja -C build-asan dragonfly

   # Replay crash with ASAN
   ./build-asan/dragonfly --port=6379 &
   nc localhost 6379 < crashes/id:000000,...
   ```

2. **With GDB:**
   ```bash
   gdb --args ./build-dbg/dragonfly --port=6379
   (gdb) run
   # In another terminal: nc localhost 6379 < crash_file
   ```

## Parallel Fuzzing

Run multiple fuzzer instances for faster coverage:

```bash
# Terminal 1 - Main instance
AFL_FINAL_SYNC=1 afl-fuzz -M main -o output ... -- ./dragonfly ...

# Terminal 2-N - Secondary instances
afl-fuzz -S secondary1 -o output ... -- ./dragonfly ...
afl-fuzz -S secondary2 -o output ... -- ./dragonfly ...
```
