#!/usr/bin/env python3
"""Summarize a pytest JUnit XML from the UBSan run as GitHub-flavored markdown.

Usage:
    ubsan_test_report.py <ubsan-junit.xml> <arch> [baseline-junit-path]

Generates (to stdout, meant to be appended to $GITHUB_STEP_SUMMARY):
  * an outcome line (passed / failed / errored / skipped, total wall time);
  * every failure/error with its reason (first message line);
  * a `slowest-tests.txt` file (full per-test timings) written into the artifact;
  * if a baseline is given, "reduced-coverage suspects": tests that failed or
    errored AND finished much faster than their regression baseline. UBSan is
    strictly slower than a normal run, so a test that finishes far quicker than
    baseline almost certainly bailed out early -> it exercised less code.

The baseline path may be a single JUnit file or a directory; every *.xml under a
directory is parsed and merged (regression uploads e.g. pytest-iouring.xml).
Missing inputs degrade gracefully -- the relevant section is simply omitted.
"""

import os
import sys
import glob
import xml.etree.ElementTree as ET

# A test that finishes below this fraction of its baseline time under UBSan (which
# is always slower) most likely exited early and covered less code.
EARLY_EXIT_RATIO = 0.8


def _key(case):
    """Stable join key across runs: pytest classname + test name (with params)."""
    cls = case.get("classname", "")
    name = case.get("name", "")
    return f"{cls}::{name}" if cls else name


def _outcome(case):
    child = case.find("failure")
    if child is not None:
        return "failed", child
    child = case.find("error")
    if child is not None:
        return "errored", child
    if case.find("skipped") is not None:
        return "skipped", None
    return "passed", None


def _reason(node):
    """First meaningful line of a failure/error message."""
    if node is None:
        return ""
    msg = (node.get("message") or "").strip()
    if not msg:
        msg = (node.text or "").strip()
    for line in msg.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _parse(path):
    """Return {key: (time, outcome, reason)} for one JUnit file."""
    out = {}
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return out
    for case in root.iter("testcase"):
        try:
            t = float(case.get("time", "0") or 0)
        except ValueError:
            t = 0.0
        outcome, node = _outcome(case)
        out[_key(case)] = (t, outcome, _reason(node))
    return out


def _parse_baseline(path):
    """Merge every JUnit under `path` (file or dir) into {key: time}."""
    files = []
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "**", "*.xml"), recursive=True))
    elif os.path.isfile(path):
        files = [path]
    times = {}
    for f in files:
        for key, (t, _outcome, _reason) in _parse(f).items():
            # Keep the largest observed baseline time (most coverage) per test.
            if t > times.get(key, 0.0):
                times[key] = t
    return times


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(__doc__)
        return 2
    junit_path, arch = sys.argv[1], sys.argv[2]
    baseline_path = sys.argv[3] if len(sys.argv) > 3 else ""

    print(f"## Test timings & failures ({arch})\n")

    if not os.path.isfile(junit_path):
        print("> [!NOTE]")
        print("> No JUnit report was produced (pytest may not have started).")
        return 0

    cases = _parse(junit_path)
    if not cases:
        print("> [!NOTE]")
        print("> JUnit report contained no test cases.")
        return 0

    counts = {"passed": 0, "failed": 0, "errored": 0, "skipped": 0}
    total_time = 0.0
    for _t, outcome, _r in cases.values():
        counts[outcome] = counts.get(outcome, 0) + 1
        total_time += _t

    print(
        f"**{len(cases)} tests** — "
        f"{counts['passed']} passed, "
        f"{counts['failed']} failed, "
        f"{counts['errored']} errored, "
        f"{counts['skipped']} skipped. "
        f"Total test time: {total_time:.1f}s.\n"
    )

    # ---- Failures / errors with reasons -----------------------------------
    problems = [(k, t, o, r) for k, (t, o, r) in cases.items() if o in ("failed", "errored")]
    if problems:
        print("> [!CAUTION]")
        print(f"> {len(problems)} test(s) failed or errored under UBSan.\n")
        print("```")
        for key, t, outcome, reason in sorted(problems, key=lambda x: x[0]):
            print(f"[{outcome}] {key}  ({t:.1f}s)")
            if reason:
                print(f"    {reason}")
        print("```\n")

    # ---- Slowest tests: written to a file in the artifact, not the summary ---
    slowest = sorted(cases.items(), key=lambda kv: kv[1][0], reverse=True)
    slowest_path = os.path.join(os.path.dirname(os.path.abspath(junit_path)), "slowest-tests.txt")
    try:
        with open(slowest_path, "w") as fh:
            fh.write(f"# Tests by wall time, slowest first ({arch}) — {len(slowest)} tests\n")
            for key, (t, _o, _r) in slowest:
                fh.write(f"{t:8.1f}s  {key}\n")
        print(
            f"⏱️ Full per-test timings (slowest first) are in "
            f"`{os.path.basename(slowest_path)}` at the root of the `ubsan-logs-{arch}` artifact.\n"
        )
    except OSError:
        pass

    # ---- Reduced-coverage suspects vs regression baseline -----------------
    if baseline_path:
        baseline = _parse_baseline(baseline_path)
        if not baseline:
            print("> [!NOTE]")
            print("> No regression baseline was available for a timing comparison.")
            return 0
        suspects = []
        for key, t, outcome, _r in problems:
            b = baseline.get(key)
            if b and b > 0 and t < EARLY_EXIT_RATIO * b:
                suspects.append((key, t, b))
        if suspects:
            print("> [!WARNING]")
            print(
                "> These failing/erroring tests finished far faster than their "
                "regression baseline — under UBSan (slower than normal) that means "
                "they likely bailed out early and covered less code:\n"
            )
            print("```")
            for key, t, b in sorted(suspects, key=lambda x: x[1]):
                print(f"ubsan={t:.1f}s  baseline={b:.1f}s  ({t / b:.0%})  {key}")
            print("```\n")
        else:
            print("> [!TIP]")
            print(
                "> No reduced-coverage suspects: every failing/erroring test ran "
                "at least as long as its regression baseline."
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
