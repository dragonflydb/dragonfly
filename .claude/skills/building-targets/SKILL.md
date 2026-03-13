---
name: building-targets
description: >
  Configures and builds the Dragonfly project and its components (main binary,
  tests, etc.) using blaze.sh and ninja. Use when the user asks to "build",
  "compile", "make dragonfly", "run tests", or "configure the build".
allowed-tools: Bash(ninja *), Bash(./helio/blaze.sh *)
---

# Building Dragonfly Targets

Instructions for configuring and building Dragonfly and its components.

## Build Efficiency Rules

> [!IMPORTANT]
> **Skip Configuration**: If the build directory (e.g., `build-dbg/` or `build-opt/`) already exists, skip running `./helio/blaze.sh`. Proceed directly to the **Building** phase.
>
> **No -C Flag**: Always `cd` into the build directory before running `ninja`. Do not use the `ninja -C <dir>` flag.

## Configuration

Dragonfly uses `helio/blaze.sh` as a wrapper around CMake for project configuration. **Only run this if the build directory does not exist.**
   ```bash
   ./helio/blaze.sh
   ```

2. **Release Build**:
   ```bash
   ./helio/blaze.sh -release
   ```

3. **Custom Options**:
   Pass CMake flags via `-D` prefix:
   ```bash
   ./helio/blaze.sh -DWITH_SEARCH=OFF -DWITH_AWS=OFF
   ```
   See CLAUDE.md for more options.

## Building

After configuration, use `ninja` within the corresponding build directory.

1. **Build Main Binary**:
   ```bash
   cd build-dbg && ninja dragonfly
   # OR for release:
   cd build-opt && ninja dragonfly
   ```

2. **Build Specific Test**:
   ```bash
   cd build-dbg && ninja <test_name>
   ```

3. **Build Everything**:
   ```bash
   cd build-dbg && ninja
   ```

## Best Practices

- Use **Debug builds** for development and debugging (includes symbols and sanitizers can be enabled).
- Use **Release builds** (`-release`) for performance measuring and production.
- If you encounter build errors after switching branches, try a clean build: `rm -rf build-dbg` and re-run `blaze.sh`.
- Use `USE_MOLD=ON` for faster linking in production builds if `mold` is available.
