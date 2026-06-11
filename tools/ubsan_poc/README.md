# UBSan POC — PR #7562 implicit-conversion bug class

Standalone C++ reproducer for the integer truncation bug fixed in
[dragonflydb/dragonfly#7562](https://github.com/dragonflydb/dragonfly/pull/7562).

No Dragonfly build required. Just a compiler.

## Files

| File | Purpose |
|---|---|
| `repro_all.cc` | 6 self-contained cases covering the main integer bug classes |
| `flag_matrix.sh` | Builds each case under 5 compiler × flag combos and prints PASS/MISS |

## Quick start

By default UBSan **continues running** after each finding (recoverable mode) — it
prints a diagnostic and keeps going. Adding `-fno-sanitize-recover=all` makes it
**abort on the first error** (what CI uses so a job fails immediately).

```bash
# Build in recoverable mode — runs all 6 cases and reports every finding
clang++ -O0 -g \
  -fsanitize=undefined,implicit-conversion,integer \
  tools/ubsan_poc/repro_all.cc -o /tmp/repro

/tmp/repro          # all 6 cases, reports each UBSan finding and continues
/tmp/repro 1        # only case 1 (the #7562 pattern)
```

```bash
# Build in abort-on-first-error mode (CI style)
# UBSan fires on case 1, prints the diagnostic, and stops — cases 2-6 never run
clang++ -O0 -g \
  -fsanitize=undefined,implicit-conversion,integer \
  -fno-sanitize-recover=all \
  tools/ubsan_poc/repro_all.cc -o /tmp/repro_strict

/tmp/repro_strict   # aborts at case 1 with exit code 1
/tmp/repro_strict 4 # only case 4 (signed overflow — also UB, also aborts)
```

```bash
# Full compiler × flag matrix (PASS/MISS table across 5 configs)
bash tools/ubsan_poc/flag_matrix.sh
```

## The 6 cases

| # | Bug class | UB per standard? | Requires |
|---|---|:---:|---|
| 1 | `uint64_t(1ULL<<32)` → `unsigned` truncates to **0** — the #7562 pattern | No | `-fsanitize=implicit-conversion` |
| 2 | `int64_t(70000)` → `int16_t` truncation | No | `-fsanitize=implicit-conversion` |
| 3 | `int(-1)` → `size_t` sign change → `SIZE_MAX` | No | `-fsanitize=implicit-conversion` |
| 4 | `INT_MAX + 1` signed overflow | **Yes** | `-fsanitize=undefined` (default) |
| 5 | `UINT_MAX + 1` unsigned overflow (wrap) | No | `-fsanitize=integer` |
| 6 | `int8_t(127) + 1` → promoted to `int(128)`, truncated to `-128` | No | `-fsanitize=implicit-conversion` |

Key insight: `-fsanitize=undefined` alone (Dragonfly's CI default before this change)
only catches case 4. Cases 1–3 and 6 require `-fsanitize=implicit-conversion`.
Case 5 requires `-fsanitize=integer`.

## Platform compatibility

| Platform | Works? | Notes |
|---|---|---|
| Linux x86_64 (Clang 7+) | ✅ | Fully tested |
| Linux aarch64 (Clang 7+) | ✅ | Same integer widths |
| macOS Intel / Apple Silicon | ✅ | Apple Clang ships `-fsanitize=implicit-conversion` since Xcode 10 |
| Windows | ⚠️ | LLVM/Clang on Windows should work; not tested |
| GCC (any platform) | ⚠️ | GCC does not support `-fsanitize=implicit-conversion`; cases 1–3, 5–6 will be **silent** (MISS) even with `-fsanitize=undefined` |

The bug is about C++ **integer type widths** (`uint64_t` = 64 bits, `unsigned` = 32 bits),
which are fixed by the standard regardless of CPU architecture or operating system.
The results are identical on any 64-bit machine.

## Why GCC misses it

GCC's `-fsanitize=undefined` does not include implicit-integer-truncation checks.
This is a Clang-only extension (added in Clang 7 / 2018, tracked at
[google/sanitizers#940](https://github.com/google/sanitizers/issues/940)).
The `CMakeLists.txt` patch in this repo guards the extra flags behind
`CMAKE_CXX_COMPILER_ID STREQUAL "Clang"` for this reason.
