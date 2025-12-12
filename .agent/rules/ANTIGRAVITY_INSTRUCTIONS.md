# Antigravity Agent Instructions for Dragonfly

## Project Overview
Dragonfly is a high-performance, Redis and Memcached compatible in-memory data store written in C++17. It uses a shared-nothing architecture (via the `helio` library) for efficient multi-threaded operation.

## Core Guidelines

### 1. Build System
- **Build Tool**: CMake + Ninja.
- **Wrapper Script**: `helio/blaze.sh` is the primary entry point for configuring builds.
- **Debug Build (Default)**:
  ```bash
  ./helio/blaze.sh
  cd build-dbg
  ninja dragonfly
  ```
- **Release Build**:
  ```bash
  ./helio/blaze.sh -release
  cd build-opt
  ninja dragonfly
  ```
- **Specific Targets**: To save time, build only what you need (e.g., `ninja generic_family_test`).

### 2. Testing
**Always run tests before verifying a task.**

- **C++ Unit Tests**:
  Run from `build-dbg/` or `build-opt/`:
  ```bash
  ctest -V -L DFLY
  ```
  To run a specific test binary:
  ```bash
  ./<test_binary_name>
  ```

- **Python Integration Tests**:
  Located in `tests/dragonfly`.
  ```bash
  cd tests
  export DRAGONFLY_PATH="../build-dbg/dragonfly"  # Point to the binary you built
  pytest dragonfly/
  ```
  Use `-k <keyword>` to run specific tests.

### 3. Code Style & Conventions
- **C++**: Google C++ Style Guide.
  - Indentation: 2 spaces.
  - Column Limit: 100.
  - Use `clang-format` to enforce style.
- **Commits**:
  - **Must be signed**: `git commit -s`.
  - **Format**: Conventional Commits (`type(scope): description`).
    - Types: `feat`, `fix`, `chore`, `test`, `refactor`, `perf`, `docs`.

### 4. Key Directories
- `src/server`: Core server logic (`dfly_main.cc`, `main_service.cc`).
- `src/core`: Core data structures.
- `src/facade`: Network/Command handling.
- `helio/`: I/O and threading submodule (do not edit unless necessary).
- `tests/dragonfly`: Python regression tests.

### 5. Workflow Tips
- **Submodules**: If build fails with missing headers, ensure submodules are updated: `git submodule update --init --recursive`.
- **Dependencies**: The environment should have dependencies pre-installed. If not, refer to `copilot-instructions.md` for the `apt` command.
- **Sanitizers**: Debug builds often include ASAN/UBSAN. Watch out for memory leaks or undefined behavior in output.

## Agent Behavior
- **Proactive Testing**: Write a test case for every bug fix or new feature.
- **Verification**: Use `task_boundary` to track progress and `notify_user` for reviews.
- **Documentation**: Update `README.md` or relevant docs if behavior changes.
