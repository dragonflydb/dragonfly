#!/usr/bin/env python3
"""Generate a QPS/latency-over-time PNG chart from dfly_bench / memtier JSON
time-series output, overlaying one line per datastore.

The persuasive story in these reports is throughput *stability* over time —
e.g. Dragonfly holding a flat line while a competitor degrades as its main
table grows. This plots the per-second time series from each run's JSON.

Usage:
    make_charts.py --series "Dragonfly=df_write.json" \
                   --series "Valkey=valkey_write.json" \
                   --metric qps --title "Write throughput, m7g.2xlarge" \
                   --out charts/write_m7g2xl.png

--metric: qps (default) or p99.  --op picks the operation block (Sets/Gets);
default is the first op found.  Requires matplotlib.
"""

import argparse
import json
import sys

from bench_util import (
    find_first,
    import_matplotlib,
    parse_series_specs,
    percentile_latency_us,
    save_figure,
)


def _bucket_value(sample, metric):
    """Pull qps (Count, since buckets are 1s) or p99 from a time bucket."""
    if not isinstance(sample, dict):
        return None
    if metric == "qps":
        # 1-second buckets, so count per bucket is already a per-second rate.
        return find_first(sample, "Count")
    return percentile_latency_us(sample)


def load_series(path, metric, op_hint):
    """Return (seconds[], values[]) for the chosen op from one JSON file."""
    with open(path) as f:
        data = json.load(f)
    stats = find_first(data, "ALL STATS") or data

    op_block = None
    if op_hint:
        op_block = find_first(stats, op_hint)
    if op_block is None:
        # dfly_bench emits a Sets block before Gets, but a read-only run leaves Sets
        # at zero count (and vice versa). Picking the first op would plot the empty
        # series, so choose the op whose time series actually has traffic.
        def _activity(blk):
            ts = find_first(blk, "Time-Serie") or {}
            return sum(find_first(v, "Count") or 0 for v in ts.values() if isinstance(v, dict))

        candidates = [
            blk
            for blk in stats.values()
            if isinstance(blk, dict) and find_first(blk, "Time-Serie") is not None
        ]
        op_block = max(candidates, key=_activity, default=None)
    if op_block is None:
        raise ValueError(f"no time series found in {path}")

    ts = find_first(op_block, "Time-Serie") or {}
    secs = sorted(int(s) for s in ts.keys())
    xs, ys = [], []
    for s in secs:
        val = _bucket_value(ts[str(s)], metric)
        if val is not None:
            xs.append(s)
            ys.append(val)
    return xs, ys


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--series",
        action="append",
        default=[],
        required=True,
        help="LABEL=path.json (repeatable, one line per datastore)",
    )
    ap.add_argument("--metric", choices=["qps", "p99"], default="qps")
    ap.add_argument("--op", default="", help="operation block hint, e.g. Sets or Gets")
    ap.add_argument("--title", default="")
    ap.add_argument("--out", required=True, help="output PNG path")
    args = ap.parse_args()

    plt = import_matplotlib()

    fig, ax = plt.subplots(figsize=(9, 5))
    plotted = 0
    for label, path in parse_series_specs(args.series):
        try:
            xs, ys = load_series(path, args.metric, args.op)
        except Exception as e:
            print(f"warning: {path}: {e}", file=sys.stderr)
            continue
        if args.metric == "qps":
            ys = [y / 1000.0 for y in ys]  # plot in thousands
        ax.plot(xs, ys, label=label, linewidth=2)
        plotted += 1

    if not plotted:
        print("no series plotted", file=sys.stderr)
        sys.exit(1)

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("QPS (k)" if args.metric == "qps" else "P99 latency (us)")
    if args.title:
        ax.set_title(args.title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)

    save_figure(fig, args.out)


if __name__ == "__main__":
    main()
