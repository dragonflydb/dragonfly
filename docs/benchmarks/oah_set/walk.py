#!/usr/bin/env python3
"""Time full walks over large sets with the walk.cc driver and preserve every sample.

A walk visits every member once, either with iterators, as RDB saving does, or with Scan cursors,
as SSCAN does. Each --binary LABEL=PATH adds a build of walk.cc. OAHSet runs in every build;
StringSet and std::unordered_set run only in the first one, std::unordered_set without Scan.
"""

import argparse
import csv
import datetime
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import re
import resource
import statistics
import subprocess
import time

CONTAINERS = {"oah": "OAHSet", "ss": "StringSet", "std": "std::unordered_set"}
RESULT = re.compile(r"([\d.]+) ns per member")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--count", type=int, default=1_000_000)
    parser.add_argument("--key-size", type=int, default=32)
    parser.add_argument("--passes", type=int, default=5)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    builds = [
        (label, Path(path).resolve()) for label, path in (b.split("=", 1) for b in args.binary)
    ]
    args.output.mkdir(parents=True, exist_ok=True)

    cases = []
    for index, (label, binary) in enumerate(builds):
        for container in ["oah"] + (["ss", "std"] if index == 0 else []):
            for scan in (False, True) if container != "std" else (False,):
                cases.append((label, binary, container, scan))
    metadata = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip(),
        "binaries": {
            label: {"path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            for label, binary in builds
        },
        "cpu": args.cpu,
        "cpufreq": cpufreq(args.cpu),
        "commands": [],
    }
    rows = []
    # Interleaving the cases spreads slow changes in machine load over all of them.
    for run, binary_keys, (label, binary, container, scan) in itertools.product(
        range(args.repeats), (False, True), cases
    ):
        command = [
            "taskset",
            "-c",
            str(args.cpu),
            str(binary),
            f"--set={container}",
            f"--count={args.count}",
            f"--key_size={args.key_size}",
            f"--binary={str(binary_keys).lower()}",
            "--seed=20261116",
            f"--passes={args.passes}",
            f"--scan={str(scan).lower()}",
            "--logtostderr",
        ]
        print(" ".join(command), flush=True)
        result, usage = run_measured(command, repo, capture_output=True, text=True)
        metadata["commands"].append(usage)
        rows.append(
            {
                "run": run,
                "build": label,
                "container": CONTAINERS[container],
                "encoding": "binary" if binary_keys else "ascii",
                "walk": "scan" if scan else "iterator",
                "ns_per_member": float(RESULT.fullmatch(result.stdout.strip()).group(1)),
            }
        )
    metadata["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (args.output / "walk.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (args.output / "walk.csv").write_text(as_csv(rows))

    groups = {}
    for row in rows:
        key = (row["build"], row["container"], row["encoding"], row["walk"])
        groups.setdefault(key, []).append(row["ns_per_member"])
    for key, values in groups.items():
        print(" ".join(key), f"{statistics.median(values):.1f}")


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
