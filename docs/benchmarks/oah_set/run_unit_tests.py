#!/usr/bin/env python3
"""Run the OAHSet and StringSet unit-test benchmarks and preserve every sample.

Each --variant LABEL=DIR adds the oah_set_test binary from DIR as another OAHSet build, for
example one compiled with a different probe window or overload factor.
"""

import argparse
import csv
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import resource
import statistics
import subprocess
import tempfile
import time

# StringSetTest initializes the thread-local allocator in SetUpTestSuite, so string_set_test
# must run one test of that suite before its benchmarks; oah_set_test initializes it globally.
OAH_FILTER = "NONE"
STRING_SET_FILTER = "StringSetTest.Basic"
NAME = re.compile(r"(BM_\w+)/elements:(\d+)/KeySize:(\d+)")


def output(command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=Path("build-opt"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--passes", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--filter", default="BM_(Add|Get|Erase|Grow|Shrink|Scan|Clear)/")
    parser.add_argument("--variant", action="append", default=[], metavar="LABEL=DIR")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    build = args.build.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    tests = [("OAHSet", build / "oah_set_test", OAH_FILTER)]
    for variant in args.variant:
        label, directory = variant.split("=", 1)
        tests.append((label, Path(directory).resolve() / "oah_set_test", OAH_FILTER))
    tests.append(("StringSet", build / "string_set_test", STRING_SET_FILTER))

    metadata = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": output(["git", "rev-parse", "HEAD"], repo),
        "source_status": output(["git", "status", "--short", "--", "src/core"], repo),
        "source_sha256": {
            str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
            for pattern in ("oah*", "string_set*", "dense_set*")
            for path in sorted((repo / "src/core").glob(pattern))
            if path.is_file()
        },
        "binaries": {
            name: {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            for name, binary, _ in tests
        },
        "cpu": args.cpu,
        "cpufreq": cpufreq(args.cpu),
        "commands": [],
        "contexts": [],
    }
    rows = []
    # Alternating the binaries spreads slow changes in machine load over all sets.
    for run in range(args.passes):
        for name, binary, gtest_filter in tests:
            with tempfile.TemporaryDirectory() as scratch:
                report_path = Path(scratch) / "report.json"
                command = [
                    "taskset",
                    "-c",
                    str(args.cpu),
                    str(binary),
                    f"--gtest_filter={gtest_filter}",
                    "--bench",
                    f"--benchmark_filter={args.filter}",
                    f"--benchmark_repetitions={args.repetitions}",
                    f"--benchmark_out={report_path}",
                    "--benchmark_out_format=json",
                ]
                print(" ".join(command), flush=True)
                _, usage = run_measured(command, repo, stdout=subprocess.DEVNULL)
                metadata["commands"].append(usage)
                report = json.loads(report_path.read_text())

            # host_name identifies the machine, and library_version is the configure-time
            # `git describe`; source_commit and source_sha256 identify the measured code.
            dropped = ("host_name", "library_version")
            context = {key: value for key, value in report["context"].items() if key not in dropped}
            metadata["contexts"].append(context)
            for bench in report["benchmarks"]:
                if bench["run_type"] != "iteration":
                    continue
                if bench["time_unit"] != "ns":
                    raise RuntimeError(f"Unexpected time unit: {bench}")
                benchmark, elements, key_size = NAME.fullmatch(bench["run_name"]).groups()
                rows.append(
                    {
                        "run": run,
                        "set": name,
                        "benchmark": benchmark,
                        "elements": int(elements),
                        "key_size": int(key_size),
                        "repetition": bench["repetition_index"],
                        # Each iteration processes every element once.
                        "ns_per_op": round(bench["real_time"] / int(elements), 3),
                        "ns_per_iteration": round(bench["real_time"], 1),
                        # Real time well above CPU time means another process took the core.
                        "cpu_ns_per_iteration": round(bench["cpu_time"], 1),
                        # BM_Add reports the heap bytes of the filled set.
                        "memory_bytes": int(bench.get("Memory_Used", 0)),
                    }
                )
    metadata["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (args.output / "unit_tests.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (args.output / "unit_tests.csv").write_text(as_csv(rows))

    groups = {}
    for row in rows:
        key = (row["benchmark"], row["elements"], row["key_size"])
        groups.setdefault(key, {}).setdefault(row["set"], []).append(row["ns_per_op"])
    names = [name for name, _, _ in tests]
    print("benchmark elements key_size " + " ".join(f"{name}_ns" for name in names))
    for (benchmark, elements, key_size), sets in groups.items():
        medians = " ".join(f"{statistics.median(sets[name]):.1f}" for name in names)
        print(f"{benchmark} {elements} {key_size} {medians}")


def run_measured(command, cwd, **kwargs):
    """Runs command; returns its result and its wall time, CPU time and preemption count."""
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.perf_counter()
    result = subprocess.run(command, cwd=cwd, check=True, **kwargs)
    wall = time.perf_counter() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
    return result, {
        "command": command,
        "wall_s": round(wall, 3),
        "cpu_s": round(cpu, 3),
        "involuntary_switches": after.ru_nivcsw - before.ru_nivcsw,
    }


def cpufreq(cpu):
    settings = {}
    for name in ("scaling_driver", "scaling_governor", "energy_performance_preference"):
        path = Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/{name}")
        if path.exists():
            settings[name] = path.read_text().strip()
    return settings


def as_csv(rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0], lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


if __name__ == "__main__":
    main()
