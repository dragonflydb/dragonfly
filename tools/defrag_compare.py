#!/usr/bin/env python3

import argparse
import os


def field(row, side, name):
    v = row.get(side)
    if isinstance(v, dict):
        return v.get(name)
    return None


def default_label(path: str, df) -> str:
    labels = df.get("label")
    if labels is not None:
        non_null = labels.dropna()
        if not non_null.empty:
            return str(non_null.iloc[0])
    return os.path.splitext(os.path.basename(path))[0]


def load_run(path: str, pd):
    df = pd.read_json(path, lines=True)
    label = default_label(path, df)
    df["waste_pct"] = df.apply(lambda r: field(r, "after", "waste_pct"), axis=1)
    df = df[["cycle", "waste_pct"]].dropna()
    return label, df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("runs", nargs="+", help="defrag driver JSONL files")
    parser.add_argument("--out", default="defrag_compare.png")
    parser.add_argument("--title", default="Defrag Fragmentation Progress")
    parser.add_argument(
        "--guide",
        type=float,
        action="append",
        default=[20.0, 10.0, 5.0],
        help="horizontal waste percentage guide line; may be repeated",
    )
    args = parser.parse_args()

    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib.ticker import MaxNLocator

    fig, ax = plt.subplots(figsize=(16, 7), constrained_layout=True)

    for path in args.runs:
        label, df = load_run(path, pd)
        if df.empty:
            print(f"skipping empty run: {path}")
            continue
        markevery = max(1, len(df) // 90)
        ax.plot(
            df["cycle"],
            df["waste_pct"],
            linewidth=2.5,
            marker="o",
            markersize=3,
            markevery=markevery,
            label=label,
        )

    for guide in args.guide:
        ax.axhline(guide, color="gray", linewidth=1, alpha=0.25)
        ax.text(
            0.995,
            guide,
            f"{guide:g}%",
            transform=ax.get_yaxis_transform(),
            ha="right",
            va="bottom",
            color="gray",
            fontsize=9,
        )

    ax.set_title(args.title)
    ax.set_xlabel("Cycle")
    ax.set_ylabel("Waste %")
    ax.grid(True, alpha=0.25)
    ax.legend()
    ax.margins(x=0.01)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=18, integer=True))

    fig.savefig(args.out, dpi=160)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
