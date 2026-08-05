#!/usr/bin/env python3
"""Plot memory-fill-over-time from monitor_fill.sh output.

For `--command` workloads (e.g. SETEX), dfly_bench writes no JSON time series, so
make_charts.py can't be used. monitor_fill.sh already emits per-poll lines like:

    poll 7: RUN dbsize=172018929 rss=18.37GiB (60%)

This parses one or more such logs (one per binary/datastore) and produces two
panels: RSS vs time, and RSS vs keyspace size — the visual for how memory fills
and where it plateaus.

Usage:
    plot_fill.py --series "prebuilt=prebuilt/poll.txt" \
                 --series "release=release/poll.txt" \
                 --interval 10 --ram-gib 30 --out charts/memory_fill.png
"""
import argparse
import re
import sys

from bench_util import import_matplotlib, parse_series_specs, save_figure

LINE = re.compile(r"dbsize=(\d+)\s+rss=([\d.]+)GiB")


def parse(path, interval):
    t, db, rss = [], [], []
    with open(path) as f:
        for line in f:
            m = LINE.search(line)
            if m:
                t.append(len(t) * interval)
                db.append(int(m.group(1)) / 1e6)
                rss.append(float(m.group(2)))
    return t, db, rss


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--series", action="append", required=True, help="LABEL=poll.txt (repeatable)")
    ap.add_argument("--interval", type=int, default=10, help="seconds between polls")
    ap.add_argument(
        "--ram-gib", type=float, default=0, help="host RAM for the abort-line annotation"
    )
    ap.add_argument(
        "--abort-pct",
        type=float,
        default=92,
        help="RSS abort threshold for the guard line (match monitor_fill.sh)",
    )
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    plt = import_matplotlib()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    markers = "os^Dv"
    for i, (label, path) in enumerate(parse_series_specs(args.series)):
        t, db, rss = parse(path, args.interval)
        if not t:
            print(f"warning: no poll lines in {path}", file=sys.stderr)
            continue
        m = markers[i % len(markers)]
        ax1.plot(t, rss, m + "-", label=label, lw=2)
        ax2.plot(db, rss, m + "-", label=label, lw=2)

    if args.ram_gib:
        ax1.axhline(
            args.ram_gib * args.abort_pct / 100,
            ls="--",
            color="r",
            alpha=0.6,
            label=f"{args.abort_pct:g}% RSS guard",
        )
        ax1.set_ylim(0, args.ram_gib)
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Server RSS (GiB)")
    ax1.set_title("Memory fill over time")
    ax1.legend()
    ax1.grid(alpha=0.3)
    ax2.set_xlabel("Keys (millions)")
    ax2.set_ylabel("Server RSS (GiB)")
    ax2.set_title("RSS vs keyspace size")
    ax2.legend()
    ax2.grid(alpha=0.3)

    save_figure(fig, args.out)


if __name__ == "__main__":
    main()
