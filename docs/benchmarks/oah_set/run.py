#!/usr/bin/env python3
"""Run the OAHSet article's single-core benchmark matrix and preserve every sample.

Each --binary LABEL=PATH adds a build of oah_set_bench; the builds alternate per scenario.
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
import platform
import resource
import statistics
import subprocess
import time


def output(command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", metavar="[LABEL=]PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    builds = []
    for spec in args.binary or ["build-opt/oah_set_bench"]:
        label, _, path = spec.rpartition("=")
        builds.append((label or "default", Path(path).resolve()))
    args.output.mkdir(parents=True, exist_ok=True)

    # count, key size, binary strings, reserve before insertion
    scenarios = [(1000, 32, False, False)]
    for binary_keys in (False, True):
        scenarios += [(100000, length, binary_keys, False) for length in (8, 16, 32, 100, 256)]
    scenarios += [(1000000, 32, False, False), (1000000, 32, True, False)]
    scenarios += [(100000, 32, False, True), (1000000, 32, False, True)]
    metadata = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": output(["git", "rev-parse", "HEAD"], repo),
        "source_status": output(["git", "status", "--short", "--", "src/core"], repo),
        "source_sha256": {
            str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((repo / "src/core").glob("oah*"))
            if path.is_file()
        },
        "platform": platform.platform(),
        "cpu": args.cpu,
        "cpufreq": cpufreq(args.cpu),
        "lscpu": output(["lscpu"], repo),
        "compiler": output(["g++", "--version"], repo),
        "builds": {label: describe(binary) for label, binary in builds},
        "rounds": args.rounds,
        "repeats": args.repeats,
        "warmup_rounds": 1,
        "lookup_passes": 5,
        "seed": 20261116,
        "scenarios": scenarios,
        "commands": [],
    }
    rows = []
    # Each pass runs the whole matrix, so the repeats of a scenario are minutes apart and a burst
    # of outside load on a shared host disturbs at most a minority of its pooled rounds.
    for run, (count, key_size, binary_keys, reserve), (label, binary) in itertools.product(
        range(args.repeats), scenarios, builds
    ):
        command = [
            "taskset",
            "-c",
            str(args.cpu),
            str(binary),
            f"--count={count}",
            f"--key_size={key_size}",
            f"--rounds={args.rounds}",
            "--lookup_passes=5",
            "--seed=20261116",
            f"--binary={str(binary_keys).lower()}",
            f"--reserve={str(reserve).lower()}",
        ]
        print(" ".join(command), flush=True)
        result, usage = run_measured(command, repo, text=True, capture_output=True)
        metadata["commands"].append(usage)
        batch = list(csv.DictReader(io.StringIO(result.stdout)))
        if len(batch) != 3 * args.rounds:
            raise RuntimeError(f"Expected {3 * args.rounds} samples, got {len(batch)}")
        for row in batch:
            if int(row["heap_bytes"]) != int(row["structure_bytes"]) + int(row["key_bytes"]):
                raise RuntimeError(f"Inconsistent memory accounting: {row}")
        rows.extend({"run": run, "build": label, **row} for row in batch)
        (args.output / "raw.csv").write_text(as_csv(rows))
        if result.stderr:
            encoding = "binary" if binary_keys else "ascii"
            name = f"{label}_{count}_{key_size}_{encoding}_{int(reserve)}_run{run}.log"
            (args.output / name).write_text(result.stderr)

    metadata["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (args.output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")

    groups = {}
    fields = ("count", "key_size", "encoding", "reserved", "build", "container")
    for row in rows:
        key = tuple(row[field] for field in fields)
        groups.setdefault(key, []).append(row)
    summary = []
    for key, samples in groups.items():
        row = dict(zip(fields, key))
        row["samples"] = len(samples)
        for metric in samples[0]:
            if metric in fields or metric in ("run", "round"):
                continue
            values = [float(sample[metric]) for sample in samples]
            row[metric] = statistics.median(values)
            if metric.endswith("_ns"):
                quartiles = values * 3
                if len(values) > 1:
                    quartiles = statistics.quantiles(values, n=4, method="inclusive")
                row[f"{metric}_min"] = min(values)
                row[f"{metric}_p25"] = quartiles[0]
                row[f"{metric}_p75"] = quartiles[2]
                row[f"{metric}_max"] = max(values)
        summary.append(row)
    (args.output / "summary.csv").write_text(as_csv(summary))


def describe(binary):
    # The build directory's compile_commands.json records how oah_set_bench.cc was compiled.
    compile_commands = json.loads((binary.parent / "compile_commands.json").read_text())
    return {
        "binary": str(binary),
        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "compile_command": next(
            entry["command"]
            for entry in compile_commands
            if entry["file"].endswith("/core/oah_set_bench.cc")
        ),
    }


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
