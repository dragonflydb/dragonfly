# Dragonfly Development Guide for Coding Agents

## Project Overview

**Dragonfly** is a high-performance in-memory data store written in **C++20**, fully compatible with Redis and Memcached APIs. It delivers 25X more throughput than Redis through a shared-nothing multi-threaded architecture.

**Key Tech Stack:**
- Language: C++20 (Google C++ Style Guide 2020 version, clang-format v14.0.6)
- Build: CMake + Ninja via `helio/blaze.sh` wrapper
- Platform: Linux (kernel 5.11+ for io_uring recommended, 5.1+ for basic support, <5.1 uses epoll fallback)
- Architecture: Shared-nothing design with fiber-based cooperative multitasking
- Size: ~1000 build targets, significant C++ codebase

## Critical Build & Test Commands

### Setup (Required Once)
```bash
# Install pre-commit hooks (MANDATORY before any commits)
pipx install pre-commit clang-format black
cd /path/to/dragonfly && pre-commit install
```

### Build Commands (ALWAYS use these exact commands)

**Debug build (for development - MOST COMMON):**
```bash
./helio/blaze.sh                    # Configure (creates build-dbg/)
cd build-dbg && ninja dragonfly     # Build main binary (~5-15 min first time)
cd build-dbg && ninja <test_name>   # Build specific test (e.g. generic_family_test)
```

**Release build (for production/benchmarking):**
```bash
./helio/blaze.sh -release           # Configure (creates build-opt/)
cd build-opt && ninja dragonfly
```

**Common build options** (passed to blaze.sh):
- `-DWITH_ASAN=ON` / `-DWITH_USAN=ON` - Enable sanitizers for debugging
- `-DWITH_SEARCH=OFF` - Disable search module for faster builds
- `-DWITH_AWS=OFF` `-DWITH_GCP=OFF` - Disable cloud libraries

**CRITICAL BUILD WARNINGS:**
- ⚠️ DO NOT use `make` for incremental builds - use `ninja` instead
- ⚠️ DO NOT use `./tools/docker/build.sh` for local development
- ⚠️ Build times: First build ~10-15 min, incremental ~30s-2min
- ⚠️ Builds may timeout if < 60 seconds wait time

### Testing Commands

**C++ Unit Tests:**
```bash
cd build-dbg
ctest -V -L DFLY                              # Run all tests (~10-20 min)
./generic_family_test                         # Run specific test binary
./generic_family_test --gtest_filter="Set.*"  # Run specific test case
```

**Python Integration Tests:**
```bash
# Setup (once)
cd tests/dragonfly
pip3 install -r requirements.txt

# Run tests
pytest -xv dragonfly                          # All tests
pytest -xv dragonfly -k <substring>           # Selective tests
pytest dragonfly/connection_test.py -s --df logtostdout  # With custom args
```

### Code Formatting (MANDATORY before commit)
```bash
pre-commit run --files <files>       # Format specific files
pre-commit run --all-files           # Format all files
```

## Project Structure & Key Files

```
dragonfly/
├── src/server/            # Core server (START HERE for server changes)
│   ├── dfly_main.cc       # Main entry point
│   ├── main_service.cc    # Command routing & service lifecycle
│   ├── db_slice.cc        # Per-thread database shard
│   ├── engine_shard_set.cc # Shard management
│   ├── *_family.cc        # Command implementations (set, string, list, etc.)
│   ├── cluster/           # Cluster mode
│   ├── journal/           # Replication
│   └── tiering/           # Disk storage
├── src/core/              # Core data structures
│   ├── dash.h             # DashTable hash table
│   └── dense_set.h        # Compact set
├── src/facade/            # Network & protocol handling
│   ├── dragonfly_connection.cc # Connection management
│   └── redis_parser.cc    # RESP protocol parser
├── helio/                 # Git submodule: I/O & threading library (DO NOT EDIT)
│   ├── util/fibers/       # Fiber support
│   └── blaze.sh           # Build configuration script
├── tests/dragonfly/       # Python pytest integration tests
├── docs/                  # Architecture docs
│   ├── build-from-source.md  # Detailed build instructions
│   ├── df-share-nothing.md   # Architecture overview
│   └── transaction.md     # Transaction model
├── .clang-format          # C++ formatting (clang-format v14.0.6, 100 char)
├── .pre-commit-config.yaml # Pre-commit hooks
└── AGENTS.md              # Comprehensive development guide (READ THIS!)
```

**Key Configuration Files:**
- `.clang-format` - C++ style (Google 2020, 100 char limit)
- `.pre-commit-config.yaml` - clang-format v14.0.6, black v25.1.0
- `CMakeLists.txt` - Root build config
- `Makefile` - Production release builds only

## CI/CD Pipeline (.github/workflows/ci.yml)

**Pre-commit checks:** clang-format, black, conventional commits, signed commits
**Build matrix:** Ubuntu 20/24, Alpine, GCC/Clang, Debug/Release, ASAN/UBSAN
**Tests:** C++ unit tests (ctest), Python integration tests (pytest)

**To replicate CI locally:**
```bash
# Pre-commit checks
pre-commit run --all-files

# Build with sanitizers (like CI)
./helio/blaze.sh -DWITH_ASAN=ON -DWITH_USAN=ON
cd build-dbg && ninja dragonfly
ctest -V -L DFLY
```

## Critical Architecture Patterns

**✅ DO:**
- Use fiber-aware primitives: `util::fb2::Mutex`, `util::fb2::Fiber` (NOT `std::thread`, `std::mutex`)
- Follow per-shard design (no global mutable state)
- Use `OpStatus` enum for error handling (see `src/server/common.h`)
- Add tests for all changes (see `tests/dragonfly/conftest.py`)
- Read files before editing
- Format code with pre-commit hooks
- Build and test incrementally

**❌ DON'T:**
- Use `std::thread` or `std::mutex` (causes deadlocks in fiber context)
- Edit `helio/` submodule (changes go upstream)
- Skip pre-commit hooks
- Remove/modify working code unless necessary
- Run codeql_checker during development (slow, CI-only)

## Common Pitfalls & Solutions

1. **Pre-commit fails:** Run `pipx install pre-commit clang-format black && pre-commit install`
2. **Build fails with missing deps:** Check `docs/build-from-source.md` for system dependencies
3. **Test timeout:** Use longer wait times: `timeout 20m ctest -V -L DFLY`
4. **Wrong binary directory:** Debug builds: `build-dbg/`, Release: `build-opt/`
5. **Fiber deadlocks:** Use `util::fb2` primitives, never `std::mutex`
6. **ASAN leaks:** Check `helio/util/asan_suppressions.txt` for known suppressions
7. **Docker security:** May need `--security-opt seccomp=unconfined` for io_uring

## Quick Reference: File Locations

| Need to... | File/Directory |
|------------|---------------|
| Add Redis command | `src/server/*_family.cc` |
| Modify networking | `src/facade/dragonfly_connection.cc` |
| Change storage | `src/server/db_slice.cc` |
| Update docs | `docs/` |
| Add tests | `tests/dragonfly/` |
| Fix formatting | `.clang-format`, `.pre-commit-config.yaml` |
| Configure build | `CMakeLists.txt`, `helio/blaze.sh` |

## Validation Checklist

Before completing any task:
- [ ] Code builds: `cd build-dbg && ninja dragonfly`
- [ ] Code formatted: `pre-commit run --files <files>`
- [ ] Tests pass: `cd build-dbg && ctest -V -L DFLY` (or specific test)
- [ ] No new ASAN/UBSAN violations (if using sanitizers)
- [ ] Changes follow shared-nothing architecture patterns
- [ ] DO NOT run codeql_checker (unnecessary for development)

## Additional Resources

- **Comprehensive Dev Guide:** [AGENTS.md](../AGENTS.md) - Read this for detailed workflows
- **Build Instructions:** [docs/build-from-source.md](../docs/build-from-source.md)
- **Architecture:** [docs/df-share-nothing.md](../docs/df-share-nothing.md)
- **Contributing:** [CONTRIBUTING.md](../CONTRIBUTING.md)
- **Test Guide:** [tests/README.md](../tests/README.md)

**Trust these instructions.** Only search for additional information if something here is incomplete or incorrect.
