# UBSan tooling

This directory holds everything for running Dragonfly under Clang's
**UndefinedBehaviorSanitizer (UBSan)**: a self-test that proves the checks are
live, the ignore/suppression lists, and the scripts that turn a run into a
report. A scheduled CI job ([`.github/workflows/ubsan.yml`](../../../.github/workflows/ubsan.yml))
builds Dragonfly with UBSan and runs the whole test suite under it on both
`x86_64` and `aarch64`.

UBSan is used here in two tiers:

- **Base** (`WITH_UBSAN`): the stock `-fsanitize=undefined` group (~18 real-UB
  checks: alignment, array-bounds, null, shift, signed-integer-overflow, ...).
  Works on Clang and GCC.
- **Strict** (`WITH_UBSAN_STRICT`, default ON, **Clang only**): extra checks that
  are not always UB but catch real bugs - `implicit-conversion` (silent integer
  truncation / sign-change, the PR #7562 class), `integer` (unsigned overflow and
  shift), `nullability`, `float-divide-by-zero`, `function`, `vptr`, and
  `object-size` (needs `-O1`).

## Build and run locally

**Use Clang.** UBSan (and sanitizers in general) are developed against Clang, so it
runs cleaner and reports more precisely than GCC, and the **strict** tier is
Clang-only. GCC works for the **base** tier but is not recommended; if you must use
it, pass `-DWITH_UBSAN_STRICT=OFF` (otherwise configure fails with a clear error).
The strict build sets `-O1` itself, which the `object-size` check needs. Findings can
vary slightly by Clang version, so match CI's compiler - the job's **Show toolchain**
step prints the exact `clang++ --version` from the `ubuntu-dev:24` container.

```bash
# configure a debug UBSan build (WITH_UBSAN_STRICT is ON by default)
./helio/blaze.sh -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DWITH_UBSAN=ON -DWITH_UBSAN_STRICT=ON -DWITH_AWS=OFF -DWITH_GCP=OFF

cd build-dbg && ninja dragonfly
```

Run the server (or any binary) with `UBSAN_OPTIONS` to control reporting:

```bash
UBSAN_OPTIONS="print_stacktrace=1:report_error_type=1:halt_on_error=0:\
log_path=$(pwd)/ubsan-logs/dfly:\
suppressions=$(git rev-parse --show-toplevel)/tools/sanitizers/ubsan/ubsan-suppressions.txt" \
  ./build-dbg/dragonfly --alsologtostderr
```

| `UBSAN_OPTIONS` | Effect |
| --- | --- |
| `print_stacktrace=1` | Print a symbolized stack for every finding. |
| `report_error_type=1` | Name the exact check (e.g. `implicit-integer-sign-change`) in the SUMMARY line. |
| `halt_on_error=0` | Keep running after each finding (default). Set `1` to stop at the first one. |
| `log_path=DIR/PREFIX` | Write findings to `PREFIX.<pid>` instead of stderr. Keep them inside the build dir, not `/tmp`. |
| `strip_path_prefix=PATH` | Trim `PATH` from file names so locations read as `src/...` / `helio/...`. |
| `suppressions=FILE` | Skip findings listed in `ubsan-suppressions.txt` (recoverable checks only). |
| `silence_unsigned_overflow=1` | Mute the (often intentional) unsigned-overflow check. |

The table above is only what we use; UBSan has many more runtime options. To see
the complete list (more detailed than the official docs), run any UBSan-linked
binary with `help=1` - it prints them at startup:

```bash
UBSAN_OPTIONS=help=1 ./build-dbg/dragonfly   # prints the option list, then starts the server
```

## Run the pytest (regression) tests under UBSan

These are the normal Python integration tests, just run against a UBSan build with
`UBSAN_OPTIONS` set - that env var is the only UBSan-specific part. Activate your
pytest virtualenv first (assumed already set up; it needs the deps from
`tests/dragonfly/requirements.txt`), then point `DRAGONFLY_PATH` at the UBSan
`dragonfly` and export `UBSAN_OPTIONS`.

Run one test file (drop the path for the whole suite):

```bash
ROOT="$(pwd)"
mkdir -p build-dbg/ubsan-logs
DRAGONFLY_PATH="$ROOT/build-dbg/dragonfly" \
UBSAN_OPTIONS="halt_on_error=0:print_stacktrace=1:report_error_type=1:\
strip_path_prefix=$ROOT/:\
suppressions=$ROOT/tools/sanitizers/ubsan/ubsan-suppressions.txt:\
log_path=$ROOT/build-dbg/ubsan-logs/ubsan" \
  python3 -m pytest -m "not large" tests/dragonfly/connection_test.py
```

For a **single test case**, add `-k` (and the asyncio loop-scope flag pytest wants
for a lone async test):

```bash
DRAGONFLY_PATH="$ROOT/build-dbg/dragonfly" \
UBSAN_OPTIONS="halt_on_error=0:print_stacktrace=1:report_error_type=1:log_path=$ROOT/build-dbg/ubsan-logs/ubsan" \
  python3 -m pytest -xvs tests/dragonfly/connection_test.py \
    -o asyncio_default_fixture_loop_scope=function -k "test_reply_count"
```

Findings land in **one folder per test**:
`build-dbg/ubsan-logs/<suite>/<case>/ubsan.<pid>`. The test harness
([`tests/dragonfly/ubsan_helper.py`](../../../tests/dragonfly/ubsan_helper.py))
rewrites `log_path` for each Dragonfly instance so tests never overwrite each
other's findings. A failing test also drops a `test-failure.log` (its traceback
plus the server logs) in the same folder.

## Files

| File | Purpose |
| --- | --- |
| `ubsan_selftest.cc` | `RunUbsanSelfTest()` - one example of every enabled check. Not built into dragonfly by default. |
| `inject_ubsan_selftest.patch` | Adds `ubsan_selftest.cc` to the dragonfly target and makes `main()` run `RunUbsanSelfTest()` at startup, then exit. Apply, run, revert. |
| `ubsan-ignorelist.txt` | Compile-time `-fsanitize-ignorelist`: which code is NOT instrumented (third-party, toolchain). |
| `ubsan-suppressions.txt` | Runtime `UBSAN_OPTIONS=suppressions=`: hides specific findings at run time. |
| `ubsan_summarize_findings.sh` | Turns a logs folder into the markdown findings report (the CI job summary). |
| `ubsan_test_report.py` | Turns the pytest JUnit into a timings + failures report, and flags tests that bailed out early. |
| `ubsan_trace.sh` | Triage helper: from an unzipped artifact, list which tests hit a `file:line` and print its stack. |
| `ci_steps.sh` | Step bodies for the CI job. Each big workflow step sources this and calls one `step_*` function. |

## Verify UBSan is live (self-test)

Phase 1 of the CI job proves every enabled check actually fires - a build with
broken flags would otherwise report a falsely "clean" run.

```bash
# 1. inject the self-test into the real binary
git apply --whitespace=nowarn tools/sanitizers/ubsan/inject_ubsan_selftest.patch

# 2. build (clang, strict; the strict build sets -O1 for object-size itself)
./helio/blaze.sh -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DWITH_UBSAN=ON -DWITH_UBSAN_STRICT=ON -DWITH_AWS=OFF -DWITH_GCP=OFF
cd build-dbg && ninja dragonfly && cd ..

# 3. run it: the patched binary runs the self-test at startup and exits (no flag)
UBSAN_OPTIONS="print_stacktrace=1:report_error_type=1" ./build-dbg/dragonfly

# 4. revert the injection
git apply -R --whitespace=nowarn tools/sanitizers/ubsan/inject_ubsan_selftest.patch
```

Expect **one `runtime error:` finding per case** in `ubsan_selftest.cc` (one row of
the table below - 12 at the time of writing). The CI job derives the expected count
from the case table in `ubsan_selftest.cc` and fails if any check is missing.

## Checks covered

| # | Check | Tier | Notes |
| --- | --- | --- | --- |
| 1 | implicit-unsigned-integer-truncation | strict | the PR #7562 pattern |
| 2 | implicit-signed-integer-truncation | strict | |
| 3 | implicit-integer-sign-change | strict | |
| 4 | array-bounds | base | `local-bounds` is deliberately off - it traps and cannot be recoverable |
| 5 | nullability | strict | |
| 6 | float-divide-by-zero | strict | |
| 7 | signed-integer-overflow | base | true UB |
| 8 | unsigned-integer-overflow | strict (`integer`) | legal wrap, still flagged |
| 9 | unsigned-shift-base | strict (`integer`) | |
| 10 | function | strict | needs RTTI |
| 11 | vptr | strict | needs RTTI |
| 12 | object-size | strict | only at `-O1+` |

"base" = stock `-fsanitize=undefined`; "strict" = `WITH_UBSAN_STRICT` (Clang only).

## Suppressions vs ignore-list

Two different tools - pick by what you want to happen:

- **Ignore-list** (`ubsan-ignorelist.txt`, compile-time): the code is **not
  instrumented at all**. Use it to skip whole vendored / toolchain files, or to
  mute one noisy check for a file or function permanently. Needs a rebuild. This
  is the preferred, durable choice.
- **Suppressions** (`ubsan-suppressions.txt`, runtime): the code is instrumented
  but the **report is hidden**. Use it to silence one confirmed-benign
  *recoverable* finding quickly, with no rebuild. Keep it empty by default.

"Recoverable" means the check prints and lets the program keep running (this run
keeps everything recoverable). A suppression only works on recoverable checks; the
ignore-list works on everything.

## The CI job (`ubsan.yml`)

Runs on a schedule and on demand (**Actions -> Run workflow**), on `x86_64` and
`aarch64`. Two phases: **(1)** the self-test above, then **(2)** revert the patch,
rebuild incrementally, and run the whole suite (minus `large`) under UBSan.
Findings and timings go to the job summary and to an `ubsan-logs-<arch>` artifact.
Findings are informational - they do not fail the job unless `fail_on_new` is on.

Manual-run inputs:

| Input | Meaning |
| --- | --- |
| `enable_helio` | Instrument the helio submodule too (default ON). Off excludes it via the ignore-list. |
| `compare` | Show a "new vs existing" diff against a baseline run (default ON). |
| `compare_ref` | Baseline to diff against: empty = last successful scheduled run; a git ref; or `R<run-number>` (e.g. `R3`). Must be green and an ancestor of HEAD. |
| `fail_on_new` | Fail the job on NEW real-UB / implicit-conversion findings vs the baseline (default OFF). |
| `ubsan_runtime_options` | Extra **runtime** `UBSAN_OPTIONS`, colon-separated, appended to the phase-2 defaults (later keys win). Example: `halt_on_error=1:silence_unsigned_overflow=1`. |

> **Sanity-check the input** with a harmless, obvious effect:
> `ubsan_runtime_options=silence_unsigned_overflow=1` - the large
> unsigned-integer-overflow group vanishes from the summary.

## Read the results

The **job summary** starts with a Notes banner (run config + totals + how-to),
then lists the findings split into **undefined behavior** (real C++ violations)
and **suspicious / defined-but-flagged** (the noisy integer / implicit-conversion
checks), each grouped by check type with locations deduplicated by
`file:line:column`. With a baseline it also shows what is newly added.

Render the findings report yourself:

```bash
# from a local run
bash tools/sanitizers/ubsan/ubsan_summarize_findings.sh build-dbg/ubsan-logs local | less

# or from a downloaded ubsan-logs-<arch> artifact
bash tools/sanitizers/ubsan/ubsan_summarize_findings.sh ./ubsan-logs-x86_64 x86_64 > findings.md
```

The summary shows `file:line:column` only. For the **full stack and which test hit
it**, use the bundled `ubsan_trace.sh` helper on the per-test `ubsan.<pid>` logs.
Give it a `file:line` from the summary; point it at the logs folder with a 2nd
argument (or run it from inside that folder, where it defaults to `.`).

```bash
# from a downloaded artifact: unzip ubsan-logs-<arch>, then pass its path
bash tools/sanitizers/ubsan/ubsan_trace.sh 'src/core/dense_set.cc:494' ~/Downloads/ubsan-logs-aarch64

# the helper is also bundled INSIDE the artifact, so from its unzipped root you can run
cd ~/Downloads/ubsan-logs-aarch64 && bash ubsan_trace.sh 'src/core/dense_set.cc:494'

# from a local run
bash tools/sanitizers/ubsan/ubsan_trace.sh 'src/core/dense_set.cc:494' build-dbg/ubsan-logs
```

`ubsan_test_report.py` adds a timings + failures section and, when a regression
timing baseline is available, flags failing tests that finished far faster than
normal - under UBSan (which is slower) that usually means they crashed or bailed
out early and covered less code.
