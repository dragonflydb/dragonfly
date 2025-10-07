# AFL++ Fuzzing - Quick Start

## 5-Minute Setup

### 1. Install AFL++

```bash
# Ubuntu/Debian
sudo apt-get install afl++

# macOS
brew install afl++
```

### 2. Build & Run

```bash
cd fuzz
./build_fuzzer.sh                    # Build with AFL++ instrumentation
TIME_LIMIT=300 ./run_fuzzer.sh resp  # Fuzz for 5 minutes
```

### 3. Check Results

```bash
# Any crashes found?
ls -lh artifacts/resp/crashes/

# View crash
./build-fuzz/fuzz/resp_fuzz < artifacts/resp/crashes/id:000000*
```

## Next Steps

- Read full docs: [README.md](README.md)

## Need Help?

```bash
./run_fuzzer.sh --help  # Show all options
```

Common issues:
- **"No AFL found"** → Install AFL++ (see step 1)
- **"Not instrumented"** → Use `afl-clang-fast` compiler
- **Slow speed** → Disable CPU frequency scaling
