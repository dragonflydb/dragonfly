# UBSan tooling

Helpers for running Dragonfly under Clang's UndefinedBehaviorSanitizer (UBSan).
See [docs/build-from-source.md](../../../docs/build-from-source.md) ("Running
UBSan locally") for how to configure a sanitized build and the full list of
`UBSAN_OPTIONS`.

## Files

| File | Purpose |
|---|---|
| `ubsan_selftest.cc` | `RunUbsanSelfTest()` — deliberately triggers one example of every enabled UBSan check. Not compiled into dragonfly by default. |
| `inject_ubsan_selftest.patch` | Wires a hidden `--run_ubsan_selftest` flag into `dfly_main.cc` and adds `ubsan_selftest.cc` to the dragonfly target. Applied, run, then reverted. |
| `ubsan-ignorelist.txt` | Compile-time `-fsanitize-ignorelist`: excludes third-party / vendored code so findings point at first-party code. |
| `ubsan-suppressions.txt` | Runtime `UBSAN_OPTIONS=suppressions=`: silences specific false positives. |

## Verifying UBSan is live

Build dragonfly with UBSan, then prove every enabled check actually fires:

```bash
# 1. configure a sanitized clang build (extended checks default ON)
./helio/blaze.sh -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DWITH_UBSAN=ON -DWITH_AWS=OFF -DWITH_GCP=OFF

# 2. inject the self-test hook and build
git apply --whitespace=nowarn tools/sanitizers/ubsan/inject_ubsan_selftest.patch
cd build-dbg && ninja dragonfly && cd ..

# 3. run it — prints one finding per enabled check (recoverable mode)
UBSAN_OPTIONS="print_stacktrace=1:report_error_type=1" \
  ./build-dbg/dragonfly --run_ubsan_selftest

# 4. revert the hook
git apply -R --whitespace=nowarn tools/sanitizers/ubsan/inject_ubsan_selftest.patch
```

`RunUbsanSelfTest(0)` runs all cases; pass a case number (1..12) to run just one.

## Checks covered

| # | Check | Tier | Notes |
|---|---|---|---|
| 1 | implicit-unsigned-integer-truncation | strict | the PR #7562 pattern |
| 2 | implicit-signed-integer-truncation | strict | |
| 3 | implicit-integer-sign-change | strict | |
| 4 | local-bounds | strict | |
| 5 | nullability | strict | |
| 6 | float-divide-by-zero | strict | |
| 7 | signed-integer-overflow | `-fsanitize=undefined` | true UB |
| 8 | unsigned-integer-overflow | strict (`integer`) | |
| 9 | unsigned-shift-base | strict (`integer`) | |
| 10 | function | strict | needs RTTI |
| 11 | vptr | strict | needs RTTI |
| 12 | object-size | strict | only at `-O1+` |

The extended (strict) checks come from `WITH_UBSAN_STRICT` and are Clang-only —
GCC implements none of them.
