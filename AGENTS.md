# Dragonfly Development Guide

> **Essential reference for working with the Dragonfly codebase**
> Architecture, build system, testing infrastructure, and development workflows.

---

## Table of Contents

1. [Critical Workflow Rules](#critical-workflow-rules)
2. [Quick Command Reference](#quick-command-reference)
3. [Project Overview](#project-overview)
4. [Repository Structure](#repository-structure)
5. [Build Instructions](#build-instructions)
6. [Testing](#testing)
7. [CI/CD Pipeline](#cicd-pipeline)
8. [Code Style & Pre-commit Hooks](#code-style--pre-commit-hooks)
9. [Third-Party Dependencies](#third-party-dependencies)
10. [Platform Support](#platform-support)
11. [CMake Build Options](#cmake-build-options)
12. [Key Files Reference](#key-files-reference)
13. [Common Pitfalls](#common-pitfalls)
14. [Debugging Tips](#debugging-tips)
15. [Validation Checklist](#validation-checklist)

---

## Critical Workflow Rules

**MANDATORY - Always Follow This Order:**

1. ✅ **Read Before Edit** - Always read files before modifying
2. ✅ **Use Correct Build Commands** - See [Quick Command Reference](#quick-command-reference) below
3. ✅ **Test After Changes** - Build and run a relevant unit test -
   `ninja <unit_test> && ./unit_test`
4. ✅ **Format Code** - `pre-commit run --files <files>`
5. ✅ **Follow Architecture** - See [Architecture Patterns](#architecture-patterns) below

### Pull Request Guidelines

**Conciseness is Key**: PR descriptions should be short, focused, and easy to scan.
- **Title**: Imperative, descriptive (e.g., "Fix fiber stack overflow in test_reply_guard_oom")
- **Summary**: 1-2 sentences explaining *what* changed and *why*
- **Changes**: Bullet points for key changes
- **Fixes**: Link issues (e.g., "Fixes #123")
- **Commit messages**: Keep every line (subject and body) <= 100 characters; wrap long descriptions

---

## Quick Command Reference

**CRITICAL: Read the full sections below for context. These are shortcuts only.**

### Building (see [Build Instructions](#build-instructions) for details)

```bash
# Debug build (for development)
./helio/blaze.sh
cd build-dbg && ninja dragonfly              # Build main binary
cd build-dbg && ninja generic_family_test    # Build specific test

# Release build (for production/benchmarking)
./helio/blaze.sh -release
cd build-opt && ninja dragonfly
```

### Testing (see [Testing](#testing) for details)

```bash
# C++ Unit Tests
cd build-dbg
ctest -V -L DFLY                                    # Run all tests
./generic_family_test                               # Run specific test binary
./generic_family_test --gtest_filter="Set.*"        # Run specific test case
```

### Code Formatting

```bash
# Setup (once)
pipx install pre-commit clang-format black
pre-commit install

# Format code
pre-commit run --files <files>              # Format specific files
pre-commit run --all-files                  # Format all files
```

### Common Operations

```bash
# Check git status
git status

# Check current branch
git branch

# View recent commits
git log --oneline -10
```

---

## Architecture Patterns

**Code Style**: [.clang-format](.clang-format) - snake_case vars, PascalCase functions, kPascalCase constants

**DO ✅**:
- Fiber-aware: `util::fb2::Mutex`, `util::fb2::Fiber` → [helio/util/fibers/](helio/util/fibers/)
- Per-shard ops (no global state) → [docs/df-share-nothing.md](docs/df-share-nothing.md)
- Command pattern → [src/server/set_family.cc](src/server/set_family.cc)
- Error handling: `OpStatus` → [src/server/common.h](src/server/common.h)
- Test patterns → [tests/dragonfly/conftest.py](tests/dragonfly/conftest.py)

**DON'T ❌**:
- `std::thread`, `std::mutex` (deadlocks!)
- Global mutable state
- Edit without reading
- Skip tests
- Use `./tools/docker/build.sh` for local development (use `ninja` instead)
- Use `make` for incremental builds (use `ninja` instead)

---

## Project Overview

**Dragonfly** is a high-performance, Redis and Memcached compatible in-memory data store written in C++20. It delivers significantly higher throughput than traditional single-threaded Redis implementations through innovative architectural choices.

### Key Characteristics

- **Language**: C++20 (Google C++ Style Guide 2020 version)
- **Architecture**: Shared-nothing multi-threaded design (via `helio` library)
- **Performance**: Uses io_uring (Linux 5.11+) for high-performance async I/O, with epoll fallback
- **Threading Model**: Fiber-based cooperative multitasking with lock-free data structures
- **Build System**: CMake + Ninja via `helio/blaze.sh` wrapper script
- **Target Platform**: Linux (kernel 5.11+ recommended), FreeBSD support available
- **Protocols**: Redis RESP2/RESP3, Memcached binary protocol
- **Compatibility**: Drop-in replacement for Redis API coverage

### Architectural Highlights

**For detailed architecture documentation, see [docs/df-share-nothing.md](docs/df-share-nothing.md)**

1. **Shared-Nothing Design**: Each thread operates independently with its own data structures, minimizing lock contention
2. **Helio Framework**: Custom I/O and threading library built on io_uring/epoll with fiber support
3. **DashTable**: Novel hash table implementation optimized for multi-core systems - see [docs/dashtable.md](docs/dashtable.md)
4. **Transaction Model**: Non-blocking optimistic transactions - see [docs/transaction.md](docs/transaction.md)
5. **Tiering Support**: Optional disk-backed storage for large datasets
6. **Search Module**: Full-text search capabilities (when enabled with WITH_SEARCH)

---

## Repository Structure

```
dragonfly/
├── src/                      # Main C++ source code
│   ├── server/               # Core server implementation
│   │   ├── dfly_main.cc      # Main entry point
│   │   ├── main_service.cc   # Service lifecycle & command routing
│   │   ├── db_slice.cc       # Per-thread database shard
│   │   ├── engine_shard_set.cc # Shard management
│   │   ├── cluster/          # Cluster mode implementation
│   │   ├── journal/          # Replication journal
│   │   ├── tiering/          # Tiered storage
│   │   ├── search/           # Search module
│   │   └── acl/              # Access control lists
│   ├── core/                 # Core data structures
│   │   ├── dash.h            # DashTable hash table
│   │   ├── dense_set.h       # Compact set implementation
│   │   ├── string_map.h      # Optimized string-keyed maps
│   │   ├── search/           # Search core algorithms
│   │   └── json/             # JSON support
│   ├── facade/               # Network & command handling
│   │   ├── dragonfly_connection.cc # Connection management
│   │   ├── redis_parser.cc   # RESP protocol parser
│   │   └── memcache_parser.cc # Memcached protocol
│   └── redis/                # Redis-specific implementations
│       └── lua/              # Lua scripting support
│
├── helio/                    # Git submodule: I/O and threading library
│   │                         # ** DO NOT EDIT unless contributing to helio **
│   ├── util/                 # Utilities: fibers, I/O, synchronization
│   ├── io/                   # io_uring & epoll abstraction
│   └── blaze.sh              # Build configuration wrapper
│
├── tests/                    # Test suite
│   ├── dragonfly/            # Python pytest integration/regression tests
│   │   ├── conftest.py       # Pytest fixtures & configuration
│   │   ├── requirements.txt  # Python test dependencies
│   │   └── *.py              # Test files
│   └── pytest.ini            # Pytest configuration & markers
│
├── docs/                     # Documentation
│   ├── build-from-source.md  # Build instructions
│   ├── dashtable.md          # DashTable internals
│   ├── transaction.md        # Transaction model
│   ├── df-share-nothing.md   # Shared-nothing architecture
│   └── differences.md        # Differences from Redis
│
├── contrib/                  # Utilities
│   ├── docker/               # Docker configurations
│   └── charts/dragonfly/     # Helm chart for Kubernetes
│
├── tools/                    # Benchmarking & utility tools
│   └── packaging/            # Packaging scripts
│
├── CMakeLists.txt            # Root CMake configuration
├── .clang-format             # C++ formatting rules (clang-format v14.0.6)
├── .pre-commit-config.yaml   # Pre-commit hooks configuration
├── pyproject.toml            # Python formatting (Black, 100 chars)
└── CONTRIBUTING.md           # Contribution guidelines
```

### Critical Paths to Remember

- **Main entry**: `src/server/dfly_main.cc`
- **Command dispatch**: `src/server/main_service.cc`
- **Data storage**: `src/server/db_slice.cc`
- **Networking**: `src/facade/dragonfly_connection.cc`
- **Helio library**: `helio/` (I/O and threading library)

---

## Build Instructions

**For complete build instructions, see [docs/build-from-source.md](docs/build-from-source.md)**

### Quick Start

**Debug build** (for development):
```bash
./helio/blaze.sh
cd build-dbg && ninja dragonfly
./dragonfly --alsologtostderr
```

**Release build** (for production/benchmarking):
```bash
./helio/blaze.sh -release
cd build-opt && ninja dragonfly
```

**Production release build** (static linking, optimized):
```bash
make release           # Configure + build
make package           # Create release packages with debug symbols
```

The [Makefile](Makefile) builds production releases with:
- Static linking: libstdc++, libgcc, Boost, OpenSSL
- Architecture optimizations (x86_64: `-march=core2 -msse4.1 -mtune=skylake`)
- Debug symbols (compressed)
- Output: `build-release/dragonfly-{arch}.tar.gz`

**Common build options**:
- See [docs/build-from-source.md](docs/build-from-source.md) for all options

---

## Testing

**For complete testing documentation, see [tests/README.md](tests/README.md)**

### Quick Reference

**C++ Unit Tests**:
```bash
cd build-dbg
ctest -V -L DFLY                                    # Run all tests
./generic_family_test                               # Run specific test binary
./generic_family_test --gtest_filter="Set.*"        # Run specific test case
```

---

## CI/CD Pipeline

**For complete CI configuration, see [.github/workflows/ci.yml](.github/workflows/ci.yml)**

The CI workflow runs on all PRs and includes:
- **Pre-commit checks**: clang-format, black formatters
- **Build matrix**: Multiple OS/compiler/sanitizer combinations (Ubuntu 20/24, Alpine, GCC/Clang, ASAN/UBSAN)
- **Test execution**: C++ unit tests, Python integration tests, cluster mode tests
- **Additional validations**: Helm charts, Docker image builds

---

## Code Style & Pre-commit Hooks

**For complete contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md)**

**Code style configuration files**:
- **C++**: [.clang-format](.clang-format) - Google C++ Style Guide (2020), clang-format v14.0.6, 100 char limit
- **Python**: [pyproject.toml](pyproject.toml) - Black formatter, 100 char limit, PEP 8 compliant
- **Pre-commit hooks**: [.pre-commit-config.yaml](.pre-commit-config.yaml) - Automated formatting checks

**Quick setup**:
```bash
pipx install pre-commit clang-format black
pre-commit install
pre-commit run --all-files                          # Run all formatters
```

---

## Third-Party Dependencies

**Key Libraries**: Abseil (strings/flags), Boost 1.71+ (context/intrusive), mimalloc (allocator), jsoncons (JSON), OpenSSL (TLS), libunwind (traces)

**Build artifacts**: `build-dbg/third_party/` - DO NOT edit

**For complete dependency info, see [docs/build-from-source.md](docs/build-from-source.md)**

---

## Platform Support

**Linux**: Primary platform. Kernel 5.11+ (io_uring), 5.1+ (basic), < 5.1 (epoll fallback)
- Check: `uname -r`
- Force epoll: `--proactor_type=epoll`
- Docker: `--security-opt seccomp=unconfined`

**FreeBSD**: Supported (kqueue backend)

**macOS**: Not supported for production (use Docker/Linux)

**For complete platform info, see [docs/build-from-source.md](docs/build-from-source.md)**

---

## CMake Build Options

**For complete list of build options, see [docs/build-from-source.md](docs/build-from-source.md)**

### Common Options

Pass options to `helio/blaze.sh` with `-D` prefix:

```bash
./helio/blaze.sh -DWITH_SEARCH=OFF -DWITH_AWS=ON
```

**Most useful options**:
- `WITH_ASAN=ON` / `WITH_USAN=ON` - Enable sanitizers for debugging
- `WITH_SEARCH=OFF` - Disable search module for faster builds
- `WITH_AWS=OFF` / `WITH_GCP=OFF` - Disable cloud libraries
- `WITH_TIERING=OFF` - Disable disk storage
- `USE_MOLD=ON` - Faster linking with LTO (production builds)

**Quick configurations**:
```bash
# Minimal build (fast compilation)
./helio/blaze.sh -DWITH_GPERF=OFF -DWITH_AWS=OFF -DWITH_GCP=OFF -DWITH_TIERING=OFF -DWITH_SEARCH=OFF

# Full-featured (all options ON by default)
./helio/blaze.sh

# Production optimized
./helio/blaze.sh -release -DUSE_MOLD=ON
```

---

## Key Files Reference

Quick reference to the most important files in the codebase.

| Purpose | File Path |
|---------|-----------|
| **Entry Points & Core** | |
| Main entry point | `src/server/dfly_main.cc` |
| Server lifecycle & command routing | `src/server/main_service.cc` |
| Per-thread database shard | `src/server/db_slice.cc` |
| Shard management | `src/server/engine_shard_set.cc` |
| **Data Structures** | |
| DashTable hash table | `src/core/dash.h` |
| Dense set implementation | `src/core/dense_set.h` |
| String map | `src/core/string_map.h` |
| **Networking** | |
| Connection handling | `src/facade/dragonfly_connection.cc` |
| Redis protocol parser | `src/facade/redis_parser.cc` |
| Memcached protocol parser | `src/facade/memcache_parser.cc` |
| **Build System** | |
| Root CMake config | `CMakeLists.txt` |
| Build script wrapper | `helio/blaze.sh` |
| Server CMake config | `src/server/CMakeLists.txt` |
| **CI/CD** | |
| Main CI workflow | `.github/workflows/ci.yml` |
| Pre-commit config | `.pre-commit-config.yaml` |
| **Code Style** | |
| C++ formatting | `.clang-format` |
| Python formatting | `pyproject.toml` |
| **Testing** | |
| Pytest configuration | `tests/pytest.ini` |
| Pytest fixtures | `tests/dragonfly/conftest.py` |
| Test requirements | `tests/dragonfly/requirements.txt` |
| **Documentation** | |
| Build instructions | `docs/build-from-source.md` |
| Architecture overview | `docs/df-share-nothing.md` |
| DashTable internals | `docs/dashtable.md` |
| Transaction model | `docs/transaction.md` |
| **Configuration** | |
| Contributing guide | `CONTRIBUTING.md` |
| CLA agreement | `CLA.txt` |

---

## Common Pitfalls

1. **Pre-commit not installed**: `pipx install pre-commit clang-format black && pre-commit install`
2. **Wrong binary**: Debug: `build-dbg/dragonfly`, Release: `build-opt/dragonfly`
3. **Wrong build command**: Use `cd build-dbg && ninja <target>`, NOT `./tools/docker/build.sh`
4. **Test timeouts**: `timeout 20m ctest -V -L DFLY`
5. **ASAN leaks**: Check CI, suppress in `helio/util/asan_suppressions.txt`
6. **Helio modifications**: DON'T edit `helio/` (it's a git submodule - changes go upstream)
7. **CodeQL checks**: DON'T run codeql_checker when testing changes - it's slow and unnecessary for development

---

## Debugging Tips

**Logging**: `--alsologtostderr --v=1 --vmodule=module=2`

**ASAN**: `ASAN_OPTIONS=detect_leaks=1:symbolize=1`, suppressions: `helio/util/asan_suppressions.txt`

**CI reproduction**: See [.github/workflows/ci.yml](.github/workflows/ci.yml)

**Troubleshooting**: Check fiber deadlocks (use `util::fb2` not `std::mutex`), timeout issues (`--test_timeout`), ASAN reports

---

## Validation Checklist

Before claiming a task is complete, verify:

### Code Quality

- [ ] Code compiles without errors: `cd build-dbg && ninja dragonfly`
- [ ] Code compiles without warnings (CI uses `-Werror`)
- [ ] Code follows Google C++ Style Guide (run `clang-format`)
- [ ] No new ASAN/UBSAN violations

### Testing

- [ ] All existing C++ unit tests pass: `ctest -V -L DFLY`
- [ ] New feature has corresponding test coverage
- [ ] Tests pass in both Debug and Release builds
- [ ] Tests pass with ASAN/UBSAN enabled (if applicable)
- [ ] **DO NOT run codeql_checker** - it's slow and unnecessary for development testing

### Pre-commit & Style

- [ ] Pre-commit hooks installed: `pre-commit install`
- [ ] Code formatted with clang-format (C++) and black (Python)

### Documentation

- [ ] Public APIs have comments explaining purpose
- [ ] Complex algorithms have explanatory comments
- [ ] README or docs updated if behavior changes
- [ ] No commented-out code left in final commit

### Performance

- [ ] No obvious performance regressions (run benchmarks if needed)
- [ ] No unnecessary allocations in hot paths
- [ ] Lock-free data structures used where appropriate
