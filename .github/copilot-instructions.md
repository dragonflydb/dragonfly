# Copilot Coding Agent Instructions for Dragonfly

## Repository Overview

Dragonfly is a high-performance, Redis and Memcached compatible in-memory data store written in C++17. It uses a shared-nothing architecture for efficient multi-threaded operation, delivering significantly higher throughput than traditional single-threaded Redis implementations.

**Key characteristics:**
- Large C++ codebase (~200k+ LOC) with Python integration tests
- Uses CMake/Ninja build system with the `helio` submodule for I/O and threading
- Targets Linux (kernel 5.11+ recommended), with io_uring and epoll support
- Requires signed commits with conventional commit messages

## Repository Structure

```
src/                    # Main C++ source code
├── server/             # Core server implementation (main entry: dfly_main.cc)
├── core/               # Core data structures
├── facade/             # Network and command handling
├── redis/              # Redis protocol implementation
helio/                  # Git submodule: I/O and threading library (must be initialized)
tests/dragonfly/        # Python pytest integration/regression tests
contrib/                # Docker configs, Helm charts, utility scripts
docs/                   # Documentation including build-from-source.md
tools/                  # Benchmarking and utility tools
```

## Build Instructions

### Prerequisites (Ubuntu/Debian)

```bash
sudo apt install ninja-build libunwind-dev libboost-context-dev libssl-dev \
     autoconf-archive libtool cmake g++ bison zlib1g-dev
```

### Alternative: Build with Docker (all prerequisites included)

Use pre-built development containers that have all dependencies installed:

```bash
docker run -it --rm -v $(pwd):/src -w /src ghcr.io/romange/ubuntu-dev:24 bash
# Or use Ubuntu 22.04:
docker run -it --rm -v $(pwd):/src -w /src ghcr.io/romange/ubuntu-dev:22 bash
```

### Initialize Submodules (REQUIRED)

```bash
git submodule update --init --recursive
```

### Configure and Build

**Debug build (recommended for development):**
```bash
./helio/blaze.sh                    # Creates build-dbg/
cd build-dbg && ninja dragonfly
```

**Release build:**
```bash
./helio/blaze.sh -release           # Creates build-opt/
cd build-opt && ninja dragonfly
```

**Minimal debug build (faster compilation):**
```bash
./helio/blaze.sh -DWITH_GPERF=OFF -DWITH_AWS=OFF -DWITH_GCP=OFF \
    -DWITH_TIERING=OFF -DWITH_SEARCH=OFF -DWITH_COLLECTION_CMDS=OFF \
    -DWITH_EXTENSION_CMDS=OFF
```

### Build Specific Test

```bash
cd build-dbg && ninja generic_family_test
./generic_family_test
```

## Testing

### C++ Unit Tests

```bash
cd build-dbg
ninja src/all                       # Build all targets in src/
ctest -V -L DFLY                    # Run all tests labeled DFLY
```

Individual test execution with environment variables:
```bash
GLOG_alsologtostderr=1 FLAGS_fiber_safety_margin=4096 timeout 20m ctest -V -L DFLY
```

### Python Regression Tests

```bash
cd tests
pip3 install -r dragonfly/requirements.txt
export DRAGONFLY_PATH="../build-dbg/dragonfly"
pytest dragonfly/ --log-cli-level=INFO
```

**Test markers:**
- `slow`: Long-running tests
- `opt_only`: Only run on release builds
- `debug_only`: Only run on debug builds
- `exclude_epoll`: Skip when using epoll

### Helm Chart Tests

```bash
cd contrib/charts/dragonfly
go test -v ./... -update            # Update golden files if needed
```

## Code Style and Pre-commit Hooks

**ALWAYS install pre-commit hooks before making changes:**

```bash
pipx install pre-commit clang-format black
pre-commit install
```

**Style guidelines:**
- C++: [Google C++ Style Guide (2020 version)](https://github.com/google/styleguide/blob/505ba68c74eb97e6966f60907ce893001bedc706/cppguide.html), clang-format v14.0.6, 100 column limit
- Python: Black formatter, 100 character line length
- Commits: Conventional Commits format, must be signed (`git commit -s`)

**Commit message format:**
```
<type>(<scope>): <description>

Valid types: feat, fix, build, chore, ci, docs, perf, refactor, revert, style, test
```

Example: `fix(server): correct memory leak in db_slice #123`

## CI/CD Pipeline

The main CI workflow (`.github/workflows/ci.yml`) runs on PRs to main:

1. **pre-commit**: Validates formatting and commit messages
2. **build**: Tests on ghcr.io/romange containers (ubuntu-dev:20, ubuntu-dev:24, alpine-dev)
   - Debug and Release configurations
   - GCC and Clang compilers
   - With/without ASAN/UBSAN sanitizers
3. **Unit tests**: IoUring mode, Epoll mode, Cluster mode variants
4. **Regression tests**: Python pytest suite
5. **Helm chart tests**: Lint and installation tests

## Key Files for Reference

| Purpose | File |
|---------|------|
| Main entry point | `src/server/dfly_main.cc` |
| Server configuration | `src/server/main_service.cc` |
| CMake root | `CMakeLists.txt` |
| Build script | `helio/blaze.sh` |
| CI workflow | `.github/workflows/ci.yml` |
| Pre-commit config | `.pre-commit-config.yaml` |
| C++ formatting | `.clang-format` |
| Python formatting | `pyproject.toml` |
| Pytest config | `tests/pytest.ini` |
| Test requirements | `tests/dragonfly/requirements.txt` |

## Common Pitfalls

1. **Submodule not initialized**: Always run `git submodule update --init --recursive` after cloning
2. **Missing pre-commit hooks**: Run `pre-commit install` to avoid commit failures
3. **Unsigned commits**: Always use `git commit -s` for signed commits
4. **Build directory confusion**: Debug builds go to `build-dbg/`, release to `build-opt/`
5. **Test timeouts**: Use `timeout` command for long-running tests to avoid hangs

## Validation Checklist

Before submitting changes:

1. ✅ Code compiles: `cd build-dbg && ninja dragonfly`
2. ✅ Unit tests pass: `ctest -V -L DFLY`
3. ✅ Pre-commit checks pass: `pre-commit run --all-files`
4. ✅ Commits are signed and follow conventional format
5. ✅ For Python changes: `pytest tests/dragonfly/ -k <test_name>`

Trust these instructions. Only search for additional information if these instructions are incomplete or produce errors.
