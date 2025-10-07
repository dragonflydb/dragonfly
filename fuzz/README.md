# Dragonfly AFL++ Fuzzing

AFL++ fuzzing infrastructure for Dragonfly. This implementation provides **tight integration** with AFL++ while running Dragonfly with full service initialization and **multiple concurrent virtual connections**.

## Key Features

- **IoUring by default** - Uses Linux io_uring for maximum performance (Epoll as fallback)
- **Multiple virtual connections** - Simulates 4 concurrent clients for realistic testing
- **Full Dragonfly initialization** - Real Service, ProactorPool, EngineShardSet, all shards
- **AFL++ coverage-guided** - Intelligent input generation based on code coverage
- **Parallel command execution** - Tests concurrent command processing and race conditions
- **Monitor mode** - Watch commands in real-time, saved to file

## Prerequisites

### Install AFL++

**Ubuntu/Debian:**
```bash
sudo apt-get install afl++
```

**macOS:**
```bash
brew install afl++
```

## Quick Start

### 1. Build Fuzzer

```bash
cd fuzz
./build_fuzzer.sh
```

### 2. Run Fuzzer

```bash
./run_fuzzer.sh resp
```

### 3. Monitor Mode (See Commands)

```bash
# Run with monitor
MONITOR=1 TIME_LIMIT=60 ./run_fuzzer.sh resp

# Watch in real-time in another terminal
tail -f artifacts/resp/commands.log
```

Output example:
```
=== Fuzzer Monitor Session ===
[conn0] "SET" "key" "value"
[conn1] "GET" "key"
[conn2] "DEL" "key"
```

## Configuration

### Environment Variables

```bash
# Monitor mode - save all executed commands to file
MONITOR=1 ./run_fuzzer.sh resp
# Commands → artifacts/resp/commands.log

# Custom monitor file location
MONITOR=1 MONITOR_FILE=./my_commands.log ./run_fuzzer.sh resp

# Use Epoll instead of IoUring
USE_EPOLL=1 ./run_fuzzer.sh resp

# Custom timeout (default: 1000ms)
TIMEOUT=500 ./run_fuzzer.sh resp

# Memory limit (default: 2048MB)
MEM_LIMIT=4096 ./run_fuzzer.sh resp

# Time limit in seconds (0=unlimited)
TIME_LIMIT=3600 ./run_fuzzer.sh resp  # 1 hour
```

### Testing Both IO Engines

```bash
# Test IoUring (default, faster)
./run_fuzzer.sh resp

# Test Epoll (different code paths)
USE_EPOLL=1 OUTPUT_DIR=fuzz/artifacts/resp-epoll ./run_fuzzer.sh resp
```

## Analyzing Crashes

When AFL++ finds a crash:

```bash
# List crashes
ls -lh artifacts/resp/crashes/

# Reproduce crash with monitor
./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true \
  --fuzzer_monitor_file=crash_commands.log \
  < artifacts/resp/crashes/id:000000*

# See what commands caused crash
cat crash_commands.log

# Debug with GDB
gdb --args ./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true --fuzzer_monitor_file=debug.log
(gdb) run < artifacts/resp/crashes/id:000000*
(gdb) bt
```

## Monitor Mode Usage

### Real-time monitoring

```bash
# Terminal 1: Start fuzzer with monitor
MONITOR=1 ./run_fuzzer.sh resp

# Terminal 2: Watch commands
tail -f artifacts/resp/commands.log
```

### Analyze fuzzer behavior

```bash
# What commands are being tested?
grep -o '"[A-Z]*"' artifacts/resp/commands.log | sort | uniq -c | sort -rn | head -20

# How many sessions?
grep -c "Monitor Session" artifacts/resp/commands.log
```

## Advanced Usage

### Parallel Fuzzing

```bash
# Terminal 1 (master)
./run_fuzzer.sh resp

# Terminal 2+ (secondary)
AFL_SECONDARY=1 OUTPUT_DIR=artifacts/resp ./run_fuzzer.sh resp
```

## Best Practices

1. **Test both IoUring and Epoll** - Different code paths
2. **Use monitor** for debugging crashes
3. **Run long sessions** - Bugs appear after hours
4. **Analyze monitor logs** - See coverage gaps

## Resources

- [AFL++ Documentation](https://github.com/AFLplusplus/AFLplusplus/tree/stable/docs)

## Troubleshooting

### "No instrumentation detected"

```bash
CC=afl-clang-fast CXX=afl-clang-fast++ ./build_fuzzer.sh
```

### Want to see what's being tested?

```bash
MONITOR=1 TIME_LIMIT=60 ./run_fuzzer.sh resp
tail -f artifacts/resp/commands.log
```

### Slow speed

- Disable monitor mode (adds overhead)
- Reduce timeout: `TIMEOUT=100`
- Use IoUring (default)

## Success Metrics

- **Stability**: >95%
- **Exec speed**: >5000 exec/sec (IoUring), >2000 (Epoll)
- **New paths**: Growing
- **Crashes**: Finding bugs!
