# Dragonfly AFL++ Fuzzing

AFL++ fuzzing infrastructure for Dragonfly. This implementation provides **tight integration** with AFL++ while running Dragonfly with full service initialization and **multiple concurrent virtual connections**.

## Key Features

- **IoUring by default** - Uses Linux io_uring for maximum performance (Epoll as fallback)
- **Multiple virtual connections** - Simulates 4 concurrent clients for realistic testing
- **Full Dragonfly initialization** - Real Service, ProactorPool, EngineShardSet, all shards
- **AFL++ coverage-guided** - Intelligent input generation based on code coverage
- **Parallel command execution** - Tests concurrent command processing and race conditions

## Architecture

Input from AFL++ is split across 4 virtual connections, each executing commands concurrently:

```
AFL++ Input
    ↓
Split into chunks
    ↓
┌───────────┬───────────┬───────────┬───────────┐
│ Conn 1    │ Conn 2    │ Conn 3    │ Conn 4    │
│ (Thread0) │ (Thread1) │ (Thread2) │ (Thread3) │
└─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┘
      │           │           │           │
      └───────────┴───────────┴───────────┘
                      ↓
            Full Dragonfly Service
         (IoUring/Epoll + All Shards)
```

This tests:
- **Concurrent command execution** from multiple clients
- **Race conditions** in transaction handling
- **Shard contention** and locking
- **Parser under load** with interleaved commands

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

**From source:**
```bash
git clone https://github.com/AFLplusplus/AFLplusplus
cd AFLplusplus
make
sudo make install
```

## Quick Start

### 1. Build Fuzzer

```bash
cd fuzz
./build_fuzzer.sh
```

### 2. Run Fuzzer (IoUring)

```bash
./run_fuzzer.sh resp
```

### 3. Test with Epoll

```bash
USE_EPOLL=1 ./run_fuzzer.sh resp
```

## Configuration

### Environment Variables

```bash
# Use Epoll instead of IoUring
USE_EPOLL=1 ./run_fuzzer.sh resp

# Custom timeout (default: 1000ms)
TIMEOUT=500 ./run_fuzzer.sh resp

# Memory limit (default: 2048MB)
MEM_LIMIT=4096 ./run_fuzzer.sh resp

# Parallel jobs (default: nproc)
JOBS=8 ./run_fuzzer.sh resp

# Time limit in seconds (0=unlimited)
TIME_LIMIT=3600 ./run_fuzzer.sh resp  # 1 hour
```

### Testing Both IO Engines

It's important to test both IoUring and Epoll:

```bash
# Test IoUring (default, faster)
./run_fuzzer.sh resp

# Test Epoll (backup, different code paths)
USE_EPOLL=1 OUTPUT_DIR=fuzz/artifacts/resp-epoll ./run_fuzzer.sh resp
```

## Analyzing Crashes

When AFL++ finds a crash:

```bash
# List crashes
ls -lh fuzz/artifacts/resp/crashes/

# Reproduce with IoUring
./build-fuzz/fuzz/resp_fuzz < fuzz/artifacts/resp/crashes/id:000000*

# Reproduce with Epoll
FLAGS_force_epoll=true ./build-fuzz/fuzz/resp_fuzz < fuzz/artifacts/resp/crashes/id:000000*

# Debug with GDB
gdb --args ./build-fuzz/fuzz/resp_fuzz
(gdb) run < fuzz/artifacts/resp/crashes/id:000000*
```

## Coverage Analysis

```bash
# Build with coverage
CC=afl-clang-fast CXX=afl-clang-fast++ \
  cmake -DDFLY_FUZZ=ON \
        -DCMAKE_CXX_FLAGS="-fprofile-instr-generate -fcoverage-mapping"

# Run fuzzer
TIME_LIMIT=300 ./run_fuzzer.sh resp

# Generate report
llvm-profdata merge -sparse default.profraw -o default.profdata
llvm-cov show ./build-fuzz/fuzz/resp_fuzz \
  -instr-profile=default.profdata \
  -format=html -output-dir=coverage
```

## Advanced Usage

### Parallel Fuzzing

```bash
# Terminal 1 (master)
./run_fuzzer.sh resp

# Terminal 2+ (secondary)
AFL_SECONDARY=1 OUTPUT_DIR=fuzz/artifacts/resp ./run_fuzzer.sh resp
```

## Adding New Fuzz Targets

1. Create `fuzz/targets/mycommand_fuzz.cc`
2. Add to `src/server/CMakeLists.txt`:

```cmake
if(DFLY_FUZZ)
  add_executable(mycommand_fuzz ${CMAKE_SOURCE_DIR}/fuzz/targets/mycommand_fuzz.cc)
  target_link_libraries(mycommand_fuzz PRIVATE dfly_test_lib)
endif()
```

3. Create seeds and dictionary
4. Build and run

## Best Practices

1. **Test both IoUring and Epoll** - Different code paths
2. **Run long sessions** - Many bugs only appear after hours
3. **Parallelize** - Use all CPU cores
4. **Monitor crashes** - Check regularly
5. **Minimize inputs** - Before reporting bugs

## Resources

- [AFL++ Documentation](https://github.com/AFLplusplus/AFLplusplus/tree/stable/docs)

## Troubleshooting

### "No instrumentation detected"

```bash
CC=afl-clang-fast CXX=afl-clang-fast++ ./build_fuzzer.sh
```

### "Timeout too small"

```bash
TIMEOUT=2000 ./run_fuzzer.sh resp
```

### "Out of memory"

```bash
MEM_LIMIT=4096 ./run_fuzzer.sh resp
```

### Slow speed

- Disable CPU frequency scaling
- Use IoUring (default): `USE_EPOLL=0`
- Reduce timeout: `TIMEOUT=100`

## Success Metrics

Good indicators:
- **Stability**: >95%
- **Exec speed**: >5000 exec/sec (IoUring), >2000 (Epoll)
- **New paths**: Growing over time
- **Crashes**: Finding bugs!
