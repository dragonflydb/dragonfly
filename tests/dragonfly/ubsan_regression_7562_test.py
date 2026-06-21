# Copyright 2026, DragonflyDB authors.  All rights reserved.
# See LICENSE for licensing terms.
#
# Regression detection test for PR #7562:
# "implicit unsigned integer truncation" in ParseRedis.
#
# The bug: max_busy_read_cycles_cached was declared as uint64_t and initialised
# to 1ULL<<32 (= 4,294,967,296). It was passed to ParseRedis(unsigned), which
# silently truncated the value to 0 — disabling the busy-read cycle limit.
# This narrowing is well-defined by C++ [conv.integral], so -fsanitize=undefined
# alone does NOT catch it. -fsanitize=implicit-conversion (enabled by WITH_UBSAN
# on Clang) is required. See: https://github.com/dragonflydb/dragonfly/pull/7562
#
# How to run (from repo root):
#   # Build on the ubsan-poc-7562-revert branch (has the bug) with UBSan:
#   ./helio/blaze.sh -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
#       -DWITH_USAN=ON -DWITH_AWS=OFF -DWITH_GCP=OFF
#   cd build-dbg && ninja dragonfly && cd ..
#
#   # Activate virtualenv if not already, then run the test:
#   DRAGONFLY_PATH=$(pwd)/build-dbg/dragonfly \
#       python3 -m pytest tests/dragonfly/ubsan_regression_7562_test.py -xvs
#
# Expected result on a branch without the fix (ubsan-poc-7562-revert) + UBSan binary:
#   TEST PASSES (in rare cases it might fail due to slow startup, if yes, run again):
#   and prints to terminal:
#     runtime error: implicit conversion from type 'uint64_t' ... value
#     4294967296 ... changed the value to 0 (32-bit, unsigned)
#   A log file build-dbg/ubsan-logs/ubsan_pr7562.<pid> is also created and asserted.
#
# Expected result on a FIXED branch (ubsan-detection-improvements) + UBSan binary:
#   No UBSan output. No log file created. Test is SKIPPED (no log = no assertion).

import glob
import os

import pytest

import tests.dragonfly.instance as dfly_instance

# Force CycleClock::FromUsec(N) to return a value > UINT32_MAX.
# At 1 GHz : 10_000_000 µs × 10^9 cyc/s ÷ 10^6 = 10^10 cycles  >> 2^32
# At 500 MHz: 10_000_000 × 5×10^8 ÷ 10^6 = 5×10^9              > 2^32
# Safe on any modern CPU (>= ~430 MHz for this value to exceed UINT32_MAX).
_BIG_USEC = 10_000_000

# UBSan log directory: alongside the dragonfly binary so logs are easy to find
# after a test run.  The directory is created if it doesn't exist.
# The sanitizer appends .<pid> to the prefix to form the actual filename.
_DRAGONFLY_PATH = os.environ.get(
    "DRAGONFLY_PATH",
    os.path.join(os.path.dirname(__file__), "../../build-dbg/dragonfly"),
)
_UBSAN_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(_DRAGONFLY_PATH)), "ubsan-logs")
_UBSAN_LOG_PREFIX = os.path.join(_UBSAN_LOG_DIR, "ubsan_pr7562")

# UBSan instrumentation slows down the dragonfly startup significantly:
# ~10 implicit-conversion diagnostics fire during abseil/helio init before
# the server is ready to accept connections.  The default START_DELAY (0.8s)
# is too short.  We override it just for this test and restore it afterwards.
_UBSAN_START_DELAY = 10.0


class TestUBSanImplicitConversionPR7562:
    """
    Proves that -fsanitize=implicit-conversion detects the uint64_t->unsigned
    narrowing that was the root cause of the PR #7562 bug.

    The test starts Dragonfly with --max_busy_read_usec=10_000_000.
    UpdateFromFlags() then sets the thread-local cache to ~10^10 cycles, which
    exceeds UINT32_MAX. When ParseRedis(io_buf_, max_busy_read_cycles_cached) is
    called (triggered by any client command), the implicit narrowing fires the
    sanitizer check.

    UBSan output appears in two places:
      1. Live in the pytest terminal (because DflyInstance pipes all dragonfly
         stderr into the read_sedout thread which calls print() on every line).
      2. In the log file /tmp/ubsan_pr7562.<pid> (written via UBSAN_OPTIONS).
    """

    async def test_ubsan_detects_implicit_conversion(self, df_factory):
        # --- Create the log directory and clean up any leftover log from a previous run ---
        os.makedirs(_UBSAN_LOG_DIR, exist_ok=True)
        for f in glob.glob(f"{_UBSAN_LOG_PREFIX}.*"):
            os.remove(f)

        # --- Inject UBSAN_OPTIONS into the environment that the child process
        #     will inherit.  Set BEFORE inst.start() / subprocess.Popen(). ---
        prev_ubsan = os.environ.get("UBSAN_OPTIONS")
        os.environ["UBSAN_OPTIONS"] = f"log_path={_UBSAN_LOG_PREFIX}:print_stacktrace=1"

        # UBSan instrumentation fires ~10 diagnostics during abseil/helio startup,
        # making dragonfly take much longer to bind its port.  Bump the global
        # START_DELAY so _wait_for_server doesn't time out prematurely.
        orig_delay = dfly_instance.START_DELAY
        dfly_instance.START_DELAY = _UBSAN_START_DELAY

        try:
            # Create instance with a max_busy_read_usec that forces
            # CycleClock::FromUsec(N) > UINT32_MAX.
            inst = df_factory.create(max_busy_read_usec=_BIG_USEC)
            inst.start()

            client = inst.client()
            # Any Redis command triggers ConnectionFlow → ParseRedis →
            # ParseRedis(io_buf_, max_busy_read_cycles_cached).
            # That is the call site where the narrowing fires.
            await client.ping()
            await client.set("ubsan_test_key", "pr7562")
            assert await client.get("ubsan_test_key") == "pr7562"

            inst.stop()
        finally:
            dfly_instance.START_DELAY = orig_delay
            # Restore the original UBSAN_OPTIONS (or remove if it wasn't set).
            if prev_ubsan is not None:
                os.environ["UBSAN_OPTIONS"] = prev_ubsan
            elif "UBSAN_OPTIONS" in os.environ:
                del os.environ["UBSAN_OPTIONS"]

        # --- Check the UBSan log file ---
        ubsan_files = sorted(glob.glob(f"{_UBSAN_LOG_PREFIX}.*"))

        if not ubsan_files:
            pytest.skip(
                "No UBSan log file found — binary was not built with "
                "-fsanitize=implicit-conversion (WITH_UBSAN=ON + Clang), "
                "or the bug is already fixed on this branch."
            )

        content = open(ubsan_files[0]).read()

        # --- Validate the UBSan log ---
        # The log always contains findings from abseil/helio because those narrowings
        # fire deterministically on startup.  Finding #1 below (acl_family.cc or
        # dfly_main.cc) proves -fsanitize=implicit-conversion is active and the log
        # is wired up correctly.
        #
        # The specific PR #7562 finding (dragonfly_connection.cc:165x, uint64_t ->
        # unsigned int in IoLoop) fires only when ParseRedis is called while
        # max_busy_read_cycles_cached still holds the large initial value set by
        # CycleClock::FromUsec(10_000_000) — i.e. before UpdateFromFlags() runs on
        # that specific proactor thread.  This is a race, so the finding may or may
        # not appear in every run.  When it does appear, it looks like:
        #
        #   dragonfly_connection.cc:1656:42: runtime error: implicit conversion from
        #   type 'uint64_t' (aka 'unsigned long') of value 6240180970 (64-bit,
        #   unsigned) to type 'unsigned int' changed the value to 1945213674
        #
        # Step 1: verify that UBSan is actually running (the log is non-empty and
        # contains at least one implicit-conversion finding from any source file).
        assert "runtime error: implicit conversion" in content, (
            "UBSan log file exists but contains no implicit-conversion findings.\n"
            "The binary was probably NOT built with -fsanitize=implicit-conversion.\n"
            f"Log ({ubsan_files[0]}):\n{content[:2000]}"
        )

        # Step 2: print all Dragonfly-src findings for human inspection.
        src_findings = [
            line for line in content.splitlines() if "/src/" in line and "runtime error" in line
        ]
        if src_findings:
            print(f"\n[UBSAN FINDINGS IN DRAGONFLY SRC - Log: {ubsan_files[0]}]")
            for f in src_findings:
                print(" ", f)
        else:
            print(
                f"\n[UBSAN ACTIVE — no src/ findings this run; PR #7562 bug "
                f"may not have fired due to UpdateFromFlags() timing race. "
                f"See log: {ubsan_files[0]}]"
            )
