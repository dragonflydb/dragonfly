#!/usr/bin/env python3
"""Extract the fatal output of the fuzz target from the fuzzer log.

run_fuzzer.sh keeps the afl-fuzz output in fuzz.log. With AFL_DEBUG_CHILD=1 it also holds the
target's stderr, so the CHECK failure, assertion or signal report of every crash the fuzzer hit is
there, mixed with AFL's own status lines.

  extract_fatal.py <fuzz.log> --crash-time <epoch>  the crash dump logged just before that time
  extract_fatal.py <fuzz.log> --summary             distinct fatal messages with their counts

Exit code 1 when the log holds no crash dump at all.
"""

import argparse
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
AFL_STATUS = re.compile(r"^\[[*!+-]\] ")
# A crash dump starts with a fatal log line (LOG(FATAL), CHECK), a failed C assert or an uncaught
# exception; the failure-signal handler then reports the signal with its time. A signal report
# without such a line before it (e.g. SIGSEGV) starts a dump on its own.
DUMP_START = re.compile(r"^F\d{4} |Assertion `|terminate called")
SIGNAL = re.compile(r"\*\*\* (SIG[A-Z]+) received at time=(\d+)")
DUMP_LINE = re.compile(r"^\*\*\* |^PC: @ |^\s+@ |what\(\)")
MAX_LINES = 40
# The target logs its dump as it dies, AFL saves the crash file a second or two later and only then
# starts the next run, so the crash's own dump is logged at most a second (rounding) after the crash
# time; the window before it covers a slow save. In a crash loop the next death can fall into the
# window too, so dumps of different failures there leave the match open.
MATCH_WINDOW = 10


class Dump:
    def __init__(self, first_line):
        self.lines = [first_line]
        self.time = None

    def head(self):
        """First line without the log timestamp, thread id, signal time, CHECK_EQ operand values or
        addresses, which differ between hits of the same failure."""
        line = re.sub(r"^F\d{4} [\d:.]+\s+\d+\s+", "", self.lines[0])
        line = re.sub(r" at time=\d+.*", "", line)
        line = re.sub(r" \([^()]* vs\. [^()]*\)", "", line, count=1)
        return re.sub(r"0x[0-9a-fA-F]+", "0x?", line)


def parse(path):
    dumps = []
    with open(path, errors="replace") as log:
        for raw in log:
            line = ANSI.sub("", raw).rstrip()
            if not line.strip() or AFL_STATUS.match(line):
                continue
            signal = SIGNAL.search(line)
            current = dumps[-1] if dumps else None
            if DUMP_START.search(line) or (signal and (current is None or current.time)):
                dumps.append(Dump(line))
                current = dumps[-1]
            elif current is None or len(current.lines) >= MAX_LINES:
                continue
            elif signal or DUMP_LINE.search(line):
                current.lines.append(line)
            else:
                continue
            if signal:
                current.time = int(signal.group(2))
    return dumps


def summary(dumps):
    counts = {}
    for dump in dumps:
        counts[dump.head()] = counts.get(dump.head(), 0) + 1
    return [f"{count:5d}  {head}" for head, count in counts.items()]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--crash-time", type=int, help="seconds since the epoch")
    mode.add_argument("--summary", action="store_true")
    args = parser.parse_args()

    dumps = parse(args.log)
    if not dumps:
        return 1
    if args.summary:
        print("\n".join(summary(dumps)))
        return 0

    near = [
        d for d in dumps if d.time is not None and -MATCH_WINDOW <= d.time - args.crash_time <= 1
    ]
    if not near:
        print(
            f"# No fatal output logged within {MATCH_WINDOW}s of the crash; fatal messages of the run:"
        )
        print("\n".join(summary(dumps)))
    elif len({d.head() for d in near}) > 1:
        print(
            "# Dumps of different failures logged around the crash, which one is its own is unknown:"
        )
        print("\n".join(f"{d.time - args.crash_time:+4d}s  {d.head()}" for d in near))
    else:
        own = near[-1]
        print(
            f"# Fatal output of the fuzz target, logged {own.time - args.crash_time:+d}s from the crash"
        )
        print("\n".join(own.lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
