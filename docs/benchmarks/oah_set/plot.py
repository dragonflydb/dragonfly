#!/usr/bin/env python3
"""Render the article figures from summary.csv, unit_tests.csv, and whole_set_commands.csv.

Requires matplotlib and numpy.
"""

import argparse
import csv
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Label, harness build, harness container, and unit-test set. StringSet and std::unordered_set do
# not depend on the OAHSet constants, so their samples come from the default build.
SETS = (
    ("OAHSet 16/2", "default", "OAHSet", "OAHSet"),
    ("OAHSet 4/1", "w4o1", "OAHSet", "OAHSet_w4o1"),
    ("StringSet", "default", "StringSet", "StringSet"),
    ("std::unordered_set", "default", "std::unordered_set", None),
)
COLORS = {
    "OAHSet 16/2": "#087F8C",
    "OAHSet 4/1": "#C8741E",
    "StringSet": "#3764A8",
    "std::unordered_set": "#87929D",
}
# Lighter shades mark binary keys, which OAHSet stores raw.
RAW_COLORS = {"OAHSet 16/2": "#86CBD0", "OAHSet 4/1": "#E9B98A"}
COUNT = 100000
KEY_SIZE = 32
METRICS = (
    ("insert_ns", "Insert new key"),
    ("hit_ns", "Lookup hit"),
    ("miss_ns", "Lookup miss"),
    ("erase_ns", "Erase by key"),
)
# Label, unit benchmark, elements, and key size. One benchmark iteration is one whole operation.
OPERATIONS = (
    ("Grow: double 2,097,152 members", "BM_Grow", 2097152, 16),
    ("Grow: double 262,144 members", "BM_Grow", 262144, 16),
    ("Load: add 100,000 to a reserved set", "BM_Add", 100000, 10),
    ("Scan: walk 100,000 members", "BM_Scan", 100000, 10),
    ("Shrink: 100,000 members", "BM_Shrink", 100000, 10),
    ("Clear: free 32,000 members", "BM_Clear", 32000, 10),
)
# Label and operation in whole_set_commands.csv.
SERVER_OPERATIONS = (
    ("Growth: longest SADD batch", "slow_sadd"),
    ("SAVE", "save"),
    ("Load the snapshot", "load"),
    ("SSCAN: the set built by SADD, server CPU", "sscan_built"),
    ("SSCAN: a loaded set, server CPU", "sscan_loaded"),
    ("DEL: the set built by SADD", "del_built"),
    ("DEL: a loaded set", "del_loaded"),
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True, help="the harness summary.csv")
    parser.add_argument("--unit", type=Path, required=True, help="unit_tests.csv")
    parser.add_argument("--server", type=Path, required=True, help="whole_set_commands.csv")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.data.open() as source:
        rows = list(csv.DictReader(source))
    with args.unit.open() as source:
        unit = list(csv.DictReader(source))
    with args.server.open() as source:
        server = list(csv.DictReader(source))
    args.output.mkdir(parents=True, exist_ok=True)
    builds = {label: (build, container) for label, build, container, _ in SETS}

    def sample(label, key_size, encoding):
        build, container = builds[label]
        return next(
            row
            for row in rows
            if row["build"] == build
            and row["container"] == container
            and row["count"] == str(COUNT)
            and row["key_size"] == str(key_size)
            and row["encoding"] == encoding
            and row["reserved"] == "0"
        )

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.labelcolor": "#243647",
            "text.color": "#243647",
            "svg.fonttype": "none",
            "svg.hashsalt": "oah-set-article",
            "savefig.facecolor": "white",
        }
    )
    plot_memory(sample, args.output / "memory")
    plot_operations(unit_medians(unit), server_medians(server), args.output / "operations")
    plot_performance(sample, args.output / "performance")


def unit_medians(rows):
    """Medians in nanoseconds, and the fewest samples behind any of them."""
    groups = {}
    for row in rows:
        key = (row["set"], row["benchmark"], int(row["elements"]), int(row["key_size"]))
        groups.setdefault(key, []).append(float(row["ns_per_iteration"]))
    medians = {key: statistics.median(values) for key, values in groups.items()}
    return medians, min(len(values) for values in groups.values())


def server_medians(rows):
    """Medians in nanoseconds, and the number of repeats: wall time, except server CPU time for
    SSCAN, whose wall time is mostly client round trips; for growth, the longest SADD batch of each
    repeat. The first DEL and the first SSCAN of a repeat work on the set that SADD built, the
    later ones on loaded sets."""
    groups, seen = {}, set()
    for row in rows:
        operation = row["operation"]
        if operation in ("del", "sscan"):
            run = (row["repeat"], row["build"], row["set"], operation)
            operation += "_loaded" if run in seen else "_built"
            seen.add(run)
        groups.setdefault((row["build"], row["set"], operation), []).append(row)
    medians = {}
    for key, group in groups.items():
        if key[2] == "slow_sadd":
            longest = {}
            for row in group:
                longest[row["repeat"]] = max(longest.get(row["repeat"], 0), float(row["wall_ms"]))
            values = list(longest.values())
        else:
            column = "server_cpu_ms" if key[2].startswith("sscan") else "wall_ms"
            values = [float(row[column]) for row in group]
        medians[key] = statistics.median(values) * 1e6
    return medians, len({row["repeat"] for row in rows})


def plot_memory(sample, stem):
    def per_member(label, key_size, encoding):
        row = sample(label, key_size, encoding)
        return (float(row["heap_bytes"]) + float(row["object_bytes"])) / COUNT

    sizes = (8, 16, 32, 100)

    # Only OAHSet stores ASCII and binary keys differently. Elsewhere the keys change only the
    # number of collision links slightly, so one bar stands for both encodings.
    for label in ("StringSet", "std::unordered_set"):
        for size in sizes:
            ascii_bytes = per_member(label, size, "ascii")
            if abs(ascii_bytes - per_member(label, size, "binary")) > 0.1:
                raise ValueError(f"{label} memory depends on the encoding at {size} bytes")
    series = []
    for label in ("OAHSet 16/2", "OAHSet 4/1"):
        series.append((f"{label}, ASCII keys", COLORS[label], label, "ascii"))
        series.append((f"{label}, binary keys", RAW_COLORS[label], label, "binary"))
    series += [
        (label, COLORS[label], label, "ascii") for label in ("StringSet", "std::unordered_set")
    ]
    positions = np.arange(len(sizes))
    width = 0.14
    fig, ax = plt.subplots(figsize=(11.5, 5.8), layout="constrained")
    for index, (name, color, label, encoding) in enumerate(series):
        values = [per_member(label, size, encoding) for size in sizes]
        offset = (index - (len(series) - 1) / 2) * width
        bars = ax.bar(positions + offset, values, width, label=name, color=color)
        ax.bar_label(bars, fmt="%.0f", padding=2, fontsize=8)
    ax.set_xticks(positions, [f"{size}-byte keys" for size in sizes])
    ax.set_ylabel("Allocated bytes per key · lower is better")
    ax.set_ylim(0, 190)
    fig.suptitle("Owning the same strings: the memory cost", x=0.01, ha="left", weight="bold")
    fig.supxlabel(
        "100,000 unique keys · usable allocation sizes + container object · growth included",
        fontsize=10,
    )
    ax.legend(loc="upper left", frameon=False, ncol=3, fontsize=10)
    ax.yaxis.grid(True, alpha=0.15)
    ax.set_axisbelow(True)
    save(fig, stem)


def plot_operations(unit_results, server_results, stem):
    (unit, unit_runs), (server, server_runs) = unit_results, server_results
    sets = [(label, build, container, unit_set) for label, build, container, unit_set in SETS]
    sets = [entry for entry in sets if entry[3]]
    panels = (
        (
            "In a Dragonfly server",
            "1,000,000 32-byte ASCII members · one proactor thread · "
            f"medians of {server_runs} runs",
            [
                (name, [server[(build, container, operation)] for _, build, container, _ in sets])
                for name, operation in SERVER_OPERATIONS
            ],
        ),
        (
            "In the container",
            "Unit-test benchmarks · 10-byte keys, 16 bytes for Grow · "
            f"medians of {unit_runs} runs",
            [
                (name, [unit[(unit_set, *case)] for *_, unit_set in sets])
                for name, *case in OPERATIONS
            ],
        ),
    )
    baseline = [label for label, *_ in sets].index("StringSet")
    width = 0.26
    fig, axes = plt.subplots(
        2,
        1,
        figsize=(11.5, 10),
        sharex=True,
        height_ratios=[len(rows) for *_, rows in panels],
        layout="constrained",
    )
    upper = 1.2 * max(
        times[index] / times[baseline]
        for *_, rows in panels
        for _, times in rows
        for index in range(len(sets))
    )
    for ax, (title, subtitle, rows) in zip(axes, panels):
        positions = np.arange(len(rows))
        for index, (label, *_) in enumerate(sets):
            times = [row_times[index] for _, row_times in rows]
            ratios = [row_times[index] / row_times[baseline] for _, row_times in rows]
            bars = ax.barh(
                positions + (index - 1) * width, ratios, width, label=label, color=COLORS[label]
            )
            for bar, ratio, time in zip(bars, ratios, times):
                ax.text(
                    ratio + 0.015,
                    bar.get_y() + width / 2,
                    duration(time),
                    va="center",
                    fontsize=8,
                )
        ax.set_yticks(positions, [name for name, _ in rows])
        ax.invert_yaxis()
        ax.set_xlim(0, upper)
        ax.axvline(1, color="#243647", linewidth=0.8, alpha=0.35)
        ax.xaxis.grid(True, alpha=0.15)
        ax.set_axisbelow(True)
        ax.set_title(f"{title} · {subtitle}", loc="left", fontsize=10)
    fig.suptitle("Whole-set operations", x=0.01, ha="left", weight="bold")
    fig.supxlabel(
        "Time relative to StringSet · lower is better · labels show the time of one operation",
        fontsize=10,
    )
    handles, names = axes[0].get_legend_handles_labels()
    fig.legend(handles, names, loc="outside upper right", frameon=False, ncol=3)
    save(fig, stem)


def duration(ns):
    if ns >= 1e8:
        return f"{ns / 1e6:.0f} ms"
    if ns >= 1e6:
        return f"{ns / 1e6:.1f} ms"
    return f"{ns / 1e3:.0f} µs"


def plot_performance(sample, stem):
    panels = (("ascii", "32-byte ASCII keys, packed"), ("binary", "32-byte binary keys, raw"))
    labels = [label for label, *_ in SETS]
    upper = 1.15 * max(
        float(sample(label, KEY_SIZE, encoding)[f"{metric}_p75"])
        for label in labels
        for encoding, _ in panels
        for metric, _ in METRICS
    )
    positions = np.arange(len(METRICS))
    width = 0.2
    fig, axes = plt.subplots(1, 2, figsize=(13, 6.5), sharey=True, layout="constrained")
    for ax, (encoding, title) in zip(axes, panels):
        for index, label in enumerate(labels):
            row = sample(label, KEY_SIZE, encoding)
            values = np.array([float(row[metric]) for metric, _ in METRICS])
            low = values - [float(row[f"{metric}_p25"]) for metric, _ in METRICS]
            high = np.array([float(row[f"{metric}_p75"]) for metric, _ in METRICS]) - values
            bars = ax.barh(
                positions + (index - (len(labels) - 1) / 2) * width,
                values,
                width,
                label=label,
                color=COLORS[label],
                xerr=[low, high],
                error_kw={"elinewidth": 0.8, "capsize": 2, "alpha": 0.8},
            )
            for bar, value, error in zip(bars, values, high):
                ax.text(
                    value + error + upper * 0.01,
                    bar.get_y() + width / 2,
                    f"{value:.0f}",
                    va="center",
                    fontsize=8,
                )
        ax.set_title(title, loc="left", fontsize=11)
        ax.set_xlim(0, upper)
        ax.xaxis.grid(True, alpha=0.15)
        ax.set_axisbelow(True)
    axes[0].set_yticks(positions, [label for _, label in METRICS])
    axes[0].invert_yaxis()
    fig.suptitle("Single-member operations by key format", x=0.01, ha="left", weight="bold")
    fig.supxlabel(
        "Nanoseconds per operation · lower is better · 100,000 unique keys · medians of 21 rounds · "
        "whiskers show the interquartile range",
        fontsize=10,
    )
    handles, names = axes[0].get_legend_handles_labels()
    fig.legend(handles, names, loc="outside upper right", frameon=False, ncol=4)
    save(fig, stem)


def save(figure, stem):
    svg = stem.with_suffix(".svg")
    figure.savefig(svg, metadata={"Date": None})
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    figure.savefig(stem.with_suffix(".png"), dpi=200)
    plt.close(figure)


if __name__ == "__main__":
    main()
