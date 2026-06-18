#!/usr/bin/env python3
"""Extract headline benchmark metrics from dfly_bench / memtier JSON output and
compute memory-per-entry.

The dfly_bench --json_out_file (and memtier --json-out-file) reports carry
per-operation aggregate stats and a per-second time series. This pulls the
numbers that go into the report tables (QPS, P99) and, given an INFO memory
snapshot plus DBSIZE, computes both logical and RSS bytes-per-entry.

Usage:
    collect_metrics.py --bench-json run.json [--bench-json run2.json ...] \
        [--info info_memory.txt] [--dbsize N] [--label "Dragonfly m7g.2xlarge"] \
        [--json]

Each --bench-json is summarized independently (e.g. pass the write run and the
read run). Memory metrics are computed once from --info/--dbsize and apply to
the labeled configuration as a whole.
"""

import argparse
import json
import re
import sys

from bench_util import find_first, percentile_latency_us


def summarize_bench(path):
    """Pull QPS, op count, and P99 per operation from one bench JSON file."""
    with open(path) as f:
        data = json.load(f)

    stats = find_first(data, "ALL STATS") or data
    runtime = find_first(stats, "Runtime") or {}
    total_ms = find_first(runtime, "Total duration") or 0

    ops = {}
    for op_name, op in stats.items():
        if not isinstance(op, dict) or op_name.lower() == "runtime":
            continue
        count = find_first(op, "Count")
        if count is None:
            continue  # not an operation block
        if count == 0:
            continue
        p99 = percentile_latency_us(op)
        qps = (count * 1000.0 / total_ms) if total_ms else None
        ops[op_name] = {
            "count": count,
            "qps": round(qps) if qps is not None else None,
            "qps_k": round(qps / 1000.0, 1) if qps is not None else None,
            "p99_us": p99,
        }

    return {
        "file": path,
        "total_ms": total_ms,
        "config": find_first(data, "configuration") or {},
        "operations": ops,
    }


_INFO_RE = re.compile(r"^([a-zA-Z0-9_]+):(.+)$")


def parse_info_memory(path):
    """Parse a redis-cli INFO memory dump into a dict."""
    out = {}
    with open(path) as f:
        for line in f:
            m = _INFO_RE.match(line.strip())
            if m:
                out[m.group(1)] = m.group(2)
    return out


def memory_metrics(info_path, dbsize):
    info = parse_info_memory(info_path)
    used = int(info.get("used_memory", 0))
    rss = int(info.get("used_memory_rss", 0))
    result = {
        "used_memory": used,
        "used_memory_rss": rss,
        "dbsize": dbsize,
    }
    if dbsize:
        result["bytes_per_entry_logical"] = round(used / dbsize, 1)
        result["bytes_per_entry_rss"] = round(rss / dbsize, 1) if rss else None
    return result


def format_latency_us(value):
    if value is None:
        return "?"
    if isinstance(value, float):
        return f"{value:.1f}us"
    return f"{value}us"


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--bench-json",
        action="append",
        default=[],
        help="dfly_bench/memtier JSON report (repeatable)",
    )
    ap.add_argument("--info", help="redis-cli INFO memory dump")
    ap.add_argument("--dbsize", type=int, help="number of keys (DBSIZE)")
    ap.add_argument("--label", default="", help="configuration label for the report")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of text")
    args = ap.parse_args()

    if not args.bench_json and not args.info:
        ap.error("provide at least one --bench-json or --info")

    result = {"label": args.label, "runs": [], "memory": None}
    for path in args.bench_json:
        try:
            result["runs"].append(summarize_bench(path))
        except Exception as e:  # keep going; a bad file shouldn't kill the batch
            print(f"warning: could not parse {path}: {e}", file=sys.stderr)
    if args.info:
        result["memory"] = memory_metrics(args.info, args.dbsize or 0)

    if args.json:
        print(json.dumps(result, indent=2))
        return

    if args.label:
        print(f"# {args.label}\n")
    for run in result["runs"]:
        print(f"{run['file']}  (duration {run['total_ms']} ms)")
        for op, m in run["operations"].items():
            qk = f"{m['qps_k']}k" if m["qps_k"] is not None else "?"
            p99 = format_latency_us(m["p99_us"])
            print(f"  {op:<8} QPS {qk:<10} P99 {p99}")
        print()
    mem = result["memory"]
    if mem:
        print("Memory:")
        print(f"  used_memory     {mem['used_memory']:,} bytes")
        if mem.get("used_memory_rss"):
            print(f"  used_memory_rss {mem['used_memory_rss']:,} bytes")
        print(f"  DBSIZE          {mem['dbsize']:,} keys")
        if mem.get("bytes_per_entry_logical") is not None:
            print(f"  bytes/entry (logical) {mem['bytes_per_entry_logical']}")
        if mem.get("bytes_per_entry_rss") is not None:
            print(f"  bytes/entry (rss)     {mem['bytes_per_entry_rss']}")


if __name__ == "__main__":
    main()
