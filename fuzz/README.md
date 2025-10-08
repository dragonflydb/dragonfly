# Dragonfly AFL++ Fuzzing

AFL++ fuzzing infrastructure for Dragonfly. This implementation provides **tight integration** with AFL++ while running Dragonfly with full service initialization and **multiple concurrent virtual connections**.

## Key Features

- **IoUring by default** - Uses Linux io_uring for maximum performance (Epoll as fallback)
- **4 concurrent connections** - Simulates realistic multi-client load
- **Full Dragonfly initialization** - Real Service, ProactorPool, EngineShardSet, all shards
- **AFL++ persistent mode** - One process, 10,000 iterations (10-100x faster)
- **Parallel command execution** - Tests concurrent command processing and race conditions
- **Monitor mode** - Watch commands + Dragonfly logs (INFO/WARNING/ERROR)

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

### 3. Monitor Mode (See Commands + Logs)

Watch what's happening:
```bash
# Run with monitor
MONITOR=1 TIME_LIMIT=60 ./run_fuzzer.sh resp

# In another terminal - watch commands
tail -f artifacts/resp/commands.log

# Watch Dragonfly logs (INFO, WARNING, ERROR)
tail -f artifacts/resp/dragonfly*.INFO
tail -f artifacts/resp/dragonfly*.WARNING
tail -f artifacts/resp/dragonfly*.ERROR
```

Output files:
- `artifacts/resp/commands.log` - All executed commands
- `artifacts/resp/dragonfly*.INFO` - Info logs from Dragonfly
- `artifacts/resp/dragonfly*.WARNING` - Warnings
- `artifacts/resp/dragonfly*.ERROR` - Errors

## Configuration

### Environment Variables

```bash
# Monitor mode - save commands + logs
MONITOR=1 ./run_fuzzer.sh resp

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

# Check what commands caused crash
cat crash_commands.log

# Check Dragonfly logs for errors
cat dragonfly*.ERROR
cat dragonfly*.WARNING

# Debug with GDB
gdb --args ./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true --fuzzer_monitor_file=debug.log
(gdb) run < artifacts/resp/crashes/id:000000*
(gdb) bt
```

## Monitor Mode Details

Monitor mode creates multiple log files:

```
artifacts/resp/
├── commands.log           # All commands from 4 connections
├── dragonfly*.INFO        # Info logs from Dragonfly
├── dragonfly*.WARNING     # Warning logs
├── dragonfly*.ERROR       # Error logs
└── crashes/               # AFL++ crash reproducers
```

Watch in real-time:
```bash
# Commands
tail -f artifacts/resp/commands.log | grep "conn[1-4]"

# Errors
tail -f artifacts/resp/dragonfly*.ERROR

# All logs
tail -f artifacts/resp/dragonfly*.{INFO,WARNING,ERROR}
```

## Advanced Usage

### Parallel Fuzzing

```bash
# Terminal 1 (master)
./run_fuzzer.sh resp

# Terminal 2+ (secondary)
AFL_SECONDARY=1 OUTPUT_DIR=artifacts/resp ./run_fuzzer.sh resp
```

### Analyze What Was Tested

```bash
# What commands?
grep -o '"[A-Z]*"' artifacts/resp/commands.log | sort | uniq -c | sort -rn

# Any errors?
cat artifacts/resp/dragonfly*.ERROR

# Any warnings?
cat artifacts/resp/dragonfly*.WARNING
```

## Best Practices

1. **Test both IoUring and Epoll** - Different code paths
2. **Use monitor** for debugging - Commands + logs
3. **Check logs regularly** - Catch warnings early
4. **Run long sessions** - Bugs appear after hours

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
tail -f artifacts/resp/dragonfly*.INFO
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
- **Logs**: Clean (no unexpected ERRORs)
