#!/usr/bin/env python3
"""Collect health metrics for AFL++ fuzzing runs.

The experimental persistent workflow uses this helper in two phases:
  1. snapshot: capture the restored AFL state before the run starts.
  2. report: compare the final state with the snapshot and write JSON/Markdown reports.

The script is intentionally best-effort. Missing AFL stats should produce warnings, not hide
crashes or fail an experimental fuzzing run.
"""

import argparse
import json
import os
import time
from pathlib import Path

STALE_FIND_SECONDS = 3 * 24 * 60 * 60


def _parse_number(value):
    value = value.strip()
    if value.endswith("%"):
        value = value[:-1]
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def read_stats(path):
    stats = {}
    stats_path = Path(path)
    if not stats_path.is_file():
        return stats

    with stats_path.open(encoding="utf-8", errors="replace") as file:
        for line in file:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            stats[key.strip()] = _parse_number(value)
    return stats


def queue_metrics(queue_dir):
    root = Path(queue_dir)
    count = 0
    size_bytes = 0
    if not root.is_dir():
        return {"queue_files": 0, "queue_size_bytes": 0}

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if ".state" in path.parts:
            continue
        count += 1
        try:
            size_bytes += path.stat().st_size
        except OSError:
            pass

    return {"queue_files": count, "queue_size_bytes": size_bytes}


def collect_state(stats_file, queue_dir):
    stats = read_stats(stats_file)
    state = {
        "captured_at": int(time.time()),
        "stats_file": str(stats_file),
        "queue_dir": str(queue_dir),
        "stats": stats,
    }
    state.update(queue_metrics(queue_dir))
    return state


def _num(stats, key, default=0):
    value = stats.get(key, default)
    return value if isinstance(value, (int, float)) else default


def _first_num(stats, *keys, default=0):
    for key in keys:
        value = stats.get(key)
        if isinstance(value, (int, float)):
            return value
    return default


def _delta(after, before, key):
    return _num(after, key) - _num(before, key)


def _load_before_state(path):
    if not path:
        return {"stats": {}}, []

    before_path = Path(path)
    if not before_path.is_file():
        return {"stats": {}}, []

    try:
        with before_path.open(encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        return {"stats": {}}, [f"Could not read pre-run health snapshot: {exc}"]

    if not isinstance(data, dict):
        return {"stats": {}}, ["Pre-run health snapshot did not contain a JSON object"]

    if not isinstance(data.get("stats", {}), dict):
        data["stats"] = {}
    return data, []


def _format_seconds(seconds):
    if seconds is None:
        return "unknown"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def build_report(args):
    before, warnings = _load_before_state(args.before)

    after = collect_state(args.stats_file, args.queue_dir)
    before_stats = before.get("stats", {})
    after_stats = after["stats"]
    now = int(time.time())

    last_find = _num(after_stats, "last_find", 0)
    last_find_age = None if last_find <= 0 else max(0, now - int(last_find))
    execs_done = _num(after_stats, "execs_done")
    corpus_count = _first_num(after_stats, "corpus_count", "paths_total")
    before_corpus_count = _first_num(before_stats, "corpus_count", "paths_total")
    saved_crashes = _first_num(after_stats, "saved_crashes", "unique_crashes")
    saved_hangs = _first_num(after_stats, "saved_hangs", "unique_hangs")
    unique_crashes = _first_num(after_stats, "unique_crashes", "saved_crashes")
    unique_hangs = _first_num(after_stats, "unique_hangs", "saved_hangs")
    execs_per_sec = _num(after_stats, "execs_per_sec")

    metrics = {
        "target": args.target,
        "duration_minutes": args.duration_minutes,
        "reset_state": args.reset_state,
        "cache_hit": args.cache_hit,
        "commit": args.commit,
        "run_url": args.run_url,
        "captured_at": now,
        "stats_file": str(args.stats_file),
        "queue_dir": str(args.queue_dir),
        "execs_done": execs_done,
        "execs_done_delta": _delta(after_stats, before_stats, "execs_done"),
        "execs_per_sec": execs_per_sec,
        "corpus_count": corpus_count,
        "corpus_count_delta": corpus_count - before_corpus_count,
        "paths_total": _first_num(after_stats, "paths_total", "corpus_count"),
        "saved_crashes": saved_crashes,
        "saved_hangs": saved_hangs,
        "unique_crashes": unique_crashes,
        "unique_hangs": unique_hangs,
        "last_find": last_find,
        "last_find_age_seconds": last_find_age,
        "bitmap_cvg": after_stats.get("bitmap_cvg", ""),
        "stability": after_stats.get("stability", ""),
        "cycles_done": _num(after_stats, "cycles_done"),
        "cycles_wo_finds": _num(after_stats, "cycles_wo_finds"),
        "queue_files": after["queue_files"],
        "queue_files_delta": after["queue_files"] - int(before.get("queue_files", 0)),
        "queue_size_bytes": after["queue_size_bytes"],
        "queue_size_bytes_delta": after["queue_size_bytes"]
        - int(before.get("queue_size_bytes", 0)),
    }

    if not after_stats:
        warnings.append("AFL fuzzer_stats was not found or was empty")
    if metrics["execs_done"] <= 0:
        warnings.append("No AFL executions were recorded")
    if metrics["queue_files"] <= 0:
        warnings.append("AFL queue is empty")
    if "execs_per_sec" in after_stats and metrics["execs_per_sec"] < 1:
        warnings.append("AFL execs_per_sec is below 1")
    if last_find_age is not None and last_find_age > STALE_FIND_SECONDS:
        warnings.append(f"No new path was found for {_format_seconds(last_find_age)}")

    stability = metrics["stability"]
    if isinstance(stability, (int, float)) and stability < 80:
        warnings.append(f"AFL stability is low: {metrics['stability']}")

    if metrics["saved_crashes"] > 0:
        warnings.append(f"AFL reported {metrics['saved_crashes']} saved crash(es)")
    if metrics["saved_hangs"] > 0:
        warnings.append(f"AFL reported {metrics['saved_hangs']} saved hang(s)")

    metrics["warnings"] = warnings
    return metrics


def write_json(path, data):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, sort_keys=True)
        file.write("\n")


def write_markdown(path, data):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)

    rows = [
        ("target", data["target"]),
        ("duration_minutes", data["duration_minutes"]),
        ("reset_state", data["reset_state"]),
        ("cache_hit", data["cache_hit"]),
        ("execs_done", data["execs_done"]),
        ("execs_done_delta", data["execs_done_delta"]),
        ("execs_per_sec", data["execs_per_sec"]),
        ("corpus_count", data["corpus_count"]),
        ("corpus_count_delta", data["corpus_count_delta"]),
        ("queue_files", data["queue_files"]),
        ("queue_files_delta", data["queue_files_delta"]),
        ("last_find_age", _format_seconds(data["last_find_age_seconds"])),
        ("bitmap_cvg", data["bitmap_cvg"]),
        ("stability", data["stability"]),
        ("saved_crashes", data["saved_crashes"]),
        ("saved_hangs", data["saved_hangs"]),
        ("unique_crashes", data["unique_crashes"]),
        ("unique_hangs", data["unique_hangs"]),
        ("queue_size_bytes", data["queue_size_bytes"]),
    ]

    with output.open("w", encoding="utf-8") as file:
        file.write(f"### Experimental persistent fuzzing health ({data['target']})\n\n")
        file.write(f"- Commit: `{data['commit']}`\n")
        file.write(f"- Run: {data['run_url']}\n")
        file.write("\n| Metric | Value |\n| --- | --- |\n")
        for key, value in rows:
            file.write(f"| `{key}` | `{value}` |\n")

        file.write("\n#### Health warnings\n\n")
        if data["warnings"]:
            for warning in data["warnings"]:
                file.write(f"- {warning}\n")
        else:
            file.write("- None\n")


def print_report(data):
    print("Experimental persistent fuzzing health")
    for key in (
        "target",
        "duration_minutes",
        "reset_state",
        "cache_hit",
        "execs_done",
        "execs_done_delta",
        "execs_per_sec",
        "corpus_count",
        "corpus_count_delta",
        "queue_files",
        "queue_files_delta",
        "last_find_age_seconds",
        "bitmap_cvg",
        "stability",
        "saved_crashes",
        "saved_hangs",
        "unique_crashes",
        "unique_hangs",
    ):
        print(f"  {key}: {data[key]}")

    if data["warnings"]:
        for warning in data["warnings"]:
            print(f"HEALTH WARNING: {warning}")
            if os.environ.get("GITHUB_ACTIONS"):
                print(f"::warning::{warning}")
    else:
        print("HEALTH WARNING: none")


def snapshot_cmd(args):
    write_json(args.output, collect_state(args.stats_file, args.queue_dir))


def report_cmd(args):
    report = build_report(args)
    write_json(args.output_json, report)
    write_markdown(args.output_md, report)
    print_report(report)


def main():
    parser = argparse.ArgumentParser(description="Collect AFL++ fuzzing health metrics")
    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot = subparsers.add_parser("snapshot", help="Capture current AFL state")
    snapshot.add_argument("--stats-file", required=True)
    snapshot.add_argument("--queue-dir", required=True)
    snapshot.add_argument("--output", required=True)
    snapshot.set_defaults(func=snapshot_cmd)

    report = subparsers.add_parser("report", help="Write health report after a run")
    report.add_argument("--target", required=True)
    report.add_argument("--stats-file", required=True)
    report.add_argument("--queue-dir", required=True)
    report.add_argument("--before", required=True)
    report.add_argument("--output-json", required=True)
    report.add_argument("--output-md", required=True)
    report.add_argument("--duration-minutes", required=True)
    report.add_argument("--reset-state", required=True)
    report.add_argument("--cache-hit", required=True)
    report.add_argument("--commit", required=True)
    report.add_argument("--run-url", required=True)
    report.set_defaults(func=report_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
