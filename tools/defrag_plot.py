#!/usr/bin/env python3

import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

MIB = 1024 * 1024


def field(row, side, name):
    v = row.get(side)
    if isinstance(v, dict):
        return v.get(name)
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl")
    parser.add_argument("--x", choices=["cycle", "time"], default="cycle")
    parser.add_argument("--out", default="defrag_progress.png")
    args = parser.parse_args()

    df = pd.read_json(args.jsonl, lines=True)

    for side in ("before", "after"):
        for key in ("reserved", "committed", "used", "wasted", "waste_pct"):
            df[f"{side}_{key}"] = df.apply(lambda r: field(r, side, key), axis=1)

    df["time_s"] = (df["ts_ns"] - df["ts_ns"].iloc[0]) / 1e9
    df["committed_drop_mib"] = (df["before_committed"] - df["after_committed"]) / MIB
    df["committed_drop_cumulative_mib"] = (
        df["before_committed"].iloc[0] - df["after_committed"]
    ) / MIB

    x = df["cycle"] if args.x == "cycle" else df["time_s"]
    xlabel = "Cycle" if args.x == "cycle" else "Wall time (s)"
    markevery = max(1, len(df) // 90)

    fig, axes = plt.subplots(3, 1, figsize=(18, 8), sharex=True, constrained_layout=True)

    axes[0].plot(
        x,
        df["after_waste_pct"],
        linewidth=2.4,
        marker="o",
        markersize=3,
        markevery=markevery,
    )
    axes[0].set_ylabel("Waste %")
    axes[0].grid(True, alpha=0.25)

    axes[1].plot(x, df["after_committed"] / MIB, label="committed", linewidth=2.4)
    axes[1].plot(
        x,
        df["after_used"] / MIB,
        label="used",
        linewidth=1.8,
        linestyle="--",
        alpha=0.75,
    )
    axes[1].set_ylabel("MiB")
    axes[1].legend()
    axes[1].grid(True, alpha=0.25)

    if args.x == "cycle":
        bar_width = 0.8
    elif len(x) > 1:
        bar_width = max((x.iloc[-1] - x.iloc[0]) / len(x) * 0.7, 0.001)
    else:
        bar_width = 0.8

    axes[2].plot(
        x,
        df["committed_drop_cumulative_mib"],
        color="tab:blue",
        linewidth=2.4,
        marker="o",
        markersize=3,
        markevery=markevery,
        label="cumulative drop",
    )
    axes[2].set_ylabel("Cumulative drop MiB")
    axes[2].set_xlabel(xlabel)
    axes[2].grid(True, alpha=0.25)

    per_cycle_axis = axes[2].twinx()
    per_cycle_axis.bar(
        x,
        df["committed_drop_mib"],
        width=bar_width,
        color="tab:orange",
        alpha=0.25,
        label="per-cycle drop",
    )
    per_cycle_axis.set_ylabel("Per-cycle drop MiB")

    lines, labels = axes[2].get_legend_handles_labels()
    bars, bar_labels = per_cycle_axis.get_legend_handles_labels()
    axes[2].legend(lines + bars, labels + bar_labels, loc="upper left")

    for ax in axes:
        ax.margins(x=0.01)
    axes[-1].xaxis.set_major_locator(MaxNLocator(nbins=18, integer=args.x == "cycle"))

    fig.suptitle("Defrag Progress")
    fig.savefig(args.out, dpi=160)


if __name__ == "__main__":
    main()
