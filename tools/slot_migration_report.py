#!/usr/bin/env python3
"""Summarize the raw four-node migration experiments without hiding failed runs."""

import argparse
import csv
import json
from pathlib import Path
import statistics
import sys

from slot_migration_benchmark import Audit, OPTIONS, parameters, write_json


def worst_latency_ratio(row, phases=("migration", "after")):
    return max(
        node[phase][metric] / node["before"][metric]
        for node in row["latency"].values()
        for phase in phases
        for metric in ("p95_us", "p99_us")
    )


def qps_fraction(row):
    return min(node["migration"]["qps"] / node["before"]["qps"] for node in row["latency"].values())


def aggregate(rows):
    result = {
        "runs": len(rows),
        "batch_seconds": statistics.median(row["batch_seconds"] for row in rows),
        "batch_min": min(row["batch_seconds"] for row in rows),
        "batch_max": max(row["batch_seconds"] for row in rows),
        "worst_latency_ratio": statistics.median(worst_latency_ratio(row) for row in rows),
        "qps_fraction": statistics.median(qps_fraction(row) for row in rows),
        "parameters": rows[0]["parameters"],
        "nodes": {},
    }
    for node in "ABCD":
        result["nodes"][node] = {}
        for phase in ("before", "migration", "after"):
            result["nodes"][node][phase] = {
                key: statistics.median(row["latency"][node][phase][key] for row in rows)
                for key in ("p95_us", "p99_us", "p999_us", "max_us", "qps", "peak_interval_p99_us")
            }
            result["nodes"][node][phase]["cpu_mean"] = statistics.median(
                row["cpu"][node][phase]["cpu_mean"]
                for row in rows
                if row["cpu"][node][phase]["cpu_mean"] is not None
            )
    return result


def collect(root):
    successes = []
    failures = []
    for run_dir in sorted((root / "runs").iterdir()):
        if (run_dir / "result.json").exists():
            successes.append(json.loads((run_dir / "result.json").read_text()))
        elif (run_dir / "failure.json").exists():
            failure = json.loads((run_dir / "failure.json").read_text())
            parts = run_dir.name.split("-")
            failure.setdefault("case", parts[1])
            failure.setdefault("config", int(parts[2]))
            failure.setdefault("stage", parts[0])
            failure["path"] = str(run_dir)
            failures.append(failure)
    return successes, failures


def summarize(root):
    rows, failures = collect(root)
    groups = {}
    for row in rows:
        key = (row["stage"], row["case"], row["config"])
        groups.setdefault(key, []).append(row)
    stats = {key: aggregate(group) for key, group in groups.items()}
    result = {
        "success_count": len(rows),
        "failure_count": len(failures),
        "failures": failures,
        "cases": {},
        "parameter_effects": {},
    }
    for case in ("ring", "fanin"):
        default = stats[("default", case, 0)]
        screened = {key[2]: value for key, value in stats.items() if key[:2] == ("grid", case)}
        finalists = {key[2]: value for key, value in stats.items() if key[:2] == ("finalist", case)}
        case_failures = [failure for failure in failures if failure["case"] == case]
        failed_configs = {failure["config"] for failure in case_failures}
        stable = {
            number: value
            for number, value in finalists.items()
            if value["runs"] >= 3 and number not in failed_configs
        }
        best = min(stable, key=lambda number: stable[number]["batch_seconds"]) if stable else None
        preferred = None
        if best is not None:
            tied = [
                number
                for number in stable
                if stable[number]["batch_seconds"] <= stable[best]["batch_seconds"] + 0.12
            ]
            preferred = min(
                tied,
                key=lambda number: max(
                    node["migration"]["p99_us"] for node in stable[number]["nodes"].values()
                ),
            )
        common = {key[2]: value for key, value in stats.items() if key[:2] == ("common", case)}
        result["cases"][case] = {
            "defaults": default,
            "screened": screened,
            "finalists": finalists,
            "best_repeated": best,
            "preferred_repeated": preferred,
            "common_validation": common,
            "failed_configs": sorted(failed_configs),
            "passing_read_control": [
                number
                for number, value in screened.items()
                if value["worst_latency_ratio"] <= 1 and value["qps_fraction"] >= 0.95
            ],
            "lowest_latency_screen": (
                min(screened, key=lambda number: screened[number]["worst_latency_ratio"])
                if screened
                else None
            ),
        }
        effects = {}
        for bit, parameter in enumerate(OPTIONS):
            pairs = [
                (screened[number], screened[number | (1 << bit)])
                for number in screened
                if not number & (1 << bit) and number | (1 << bit) in screened
            ]
            effects[parameter] = {
                "paired_successes": len(pairs),
                "median_time_change_percent": (
                    statistics.median(
                        (alternative["batch_seconds"] / default_value["batch_seconds"] - 1) * 100
                        for default_value, alternative in pairs
                    )
                    if pairs
                    else None
                ),
                "median_latency_ratio_change_percent": (
                    statistics.median(
                        (
                            alternative["worst_latency_ratio"]
                            / default_value["worst_latency_ratio"]
                            - 1
                        )
                        * 100
                        for default_value, alternative in pairs
                    )
                    if pairs
                    else None
                ),
                "default_value_failed_configs": sum(
                    not number & (1 << bit) for number in failed_configs
                ),
                "alternative_value_failed_configs": sum(
                    bool(number & (1 << bit)) for number in failed_configs
                ),
            }
        result["parameter_effects"][case] = effects
    common_candidates = []
    for number in set(result["cases"]["ring"]["screened"]) & set(
        result["cases"]["fanin"]["screened"]
    ):
        if any(number in result["cases"][case]["failed_configs"] for case in ("ring", "fanin")):
            continue
        compatible = True
        improvements = []
        for case in ("ring", "fanin"):
            baseline = result["cases"][case]["defaults"]
            candidate = result["cases"][case]["screened"][number]
            improvements.append(1 - candidate["batch_seconds"] / baseline["batch_seconds"])
            for node in "ABCD":
                for phase in ("migration", "after"):
                    for metric in ("p95_us", "p99_us"):
                        if (
                            candidate["nodes"][node][phase][metric]
                            > baseline["nodes"][node][phase][metric] * 1.05
                        ):
                            compatible = False
                if (
                    candidate["nodes"][node]["migration"]["qps"]
                    < baseline["nodes"][node]["migration"]["qps"] * 0.95
                ):
                    compatible = False
        if compatible:
            common_candidates.append(
                {"config": number, "worst_case_time_improvement": min(improvements)}
            )
    result["common_candidates_vs_default_migration"] = sorted(
        common_candidates, key=lambda item: -item["worst_case_time_improvement"]
    )
    write_json(root / "summary.json", result)
    with (root / "runs.csv").open("w") as stream:
        fields = [
            "stage",
            "case",
            "config",
            "repetition",
            "status",
            "batch_seconds",
            "logical_mib_per_second",
            "worst_latency_ratio",
            "minimum_read_qps_fraction",
            *OPTIONS,
        ]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **{key: row[key] for key in fields if key in row},
                    "status": "success",
                    "worst_latency_ratio": worst_latency_ratio(row),
                    "minimum_read_qps_fraction": qps_fraction(row),
                    **row["parameters"],
                }
            )
        for failure in failures:
            writer.writerow(
                {
                    "stage": failure["stage"],
                    "case": failure["case"],
                    "config": failure["config"],
                    "status": "failed",
                    **parameters(failure["config"]),
                }
            )
    with (root / "node-metrics.csv").open("w") as stream:
        fields = [
            "stage",
            "case",
            "config",
            "repetition",
            "node",
            "phase",
            "p95_us",
            "p99_us",
            "p999_us",
            "max_us",
            "qps",
            "peak_interval_p99_us",
            "cpu_mean",
            "cpu_peak",
            "client_cpu_mean",
            "rss_peak",
        ]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            for node in "ABCD":
                for phase in ("before", "migration", "after"):
                    writer.writerow(
                        {
                            **{key: row[key] for key in ("stage", "case", "config", "repetition")},
                            "node": node,
                            "phase": phase,
                            **{
                                key: row["latency"][node][phase][key]
                                for key in fields
                                if key in row["latency"][node][phase]
                            },
                            **{
                                key: row["cpu"][node][phase][key]
                                for key in fields
                                if key in row["cpu"][node][phase]
                            },
                        }
                    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument(
        "--charts", action="store_true", help="Render plots after timed workloads end"
    )
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()
    Audit(args.root.resolve()).command([sys.executable, *sys.argv])
    summary = summarize(args.root.resolve())
    if args.charts:
        charts(args.root.resolve(), summary)
    if args.report:
        write_report(args.root.resolve(), summary)
    print(f"Successful runs: {summary['success_count']}; failed runs: {summary['failure_count']}")
    for case, data in summary["cases"].items():
        print(
            case,
            "grid",
            len(data["screened"]),
            "failed configurations",
            len(data["failed_configs"]),
            "best repeated",
            data["best_repeated"],
        )


def charts(root, summary):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    chart_dir = root / "charts"
    chart_dir.mkdir(exist_ok=True)
    plt.rcParams.update({"font.size": 10, "svg.fonttype": "none"})
    figure, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    for axis, case in zip(axes, ("ring", "fanin")):
        data = summary["cases"][case]
        for number, row in data["screened"].items():
            number = int(number)
            axis.scatter(
                row["worst_latency_ratio"],
                row["batch_seconds"],
                marker="^" if number & 32 else "o",
                color="#d95f02" if number & 8 else "#1b75bc",
                alpha=0.8,
                s=45,
            )
            if row["batch_seconds"] == min(
                value["batch_seconds"] for value in data["screened"].values()
            ):
                axis.annotate(
                    f"config {number}",
                    (row["worst_latency_ratio"], row["batch_seconds"]),
                    xytext=(5, 6),
                    textcoords="offset points",
                )
        baseline = data["defaults"]
        axis.scatter(
            baseline["worst_latency_ratio"],
            baseline["batch_seconds"],
            marker="*",
            color="black",
            s=150,
            label="Default median, 3 runs",
        )
        axis.axvline(1, color="gray", linestyle="--", linewidth=1)
        axis.set_title(
            f"{'Concurrency 1: ring' if case == 'ring' else 'Concurrency 2: fan-in'}\n{len(data['screened'])} completed / {len(data['failed_configs'])} failed configurations"
        )
        axis.set_xlabel("Worst P95/P99 ratio to the preceding GET-only window")
        axis.set_ylabel("Migration batch duration (s)")
        axis.grid(alpha=0.2)
        axis.legend(loc="best")
    figure.suptitle(
        "64 combinations per case — orange: destination throttle 20µs; triangle: 4-MiB buffer"
    )
    figure.savefig(chart_dir / "parameter-search.svg")
    plt.close(figure)
    figure, axes = plt.subplots(1, 2, figsize=(13, 8), constrained_layout=True)
    for axis, case in zip(axes, ("ring", "fanin")):
        data = summary["cases"][case]
        image = [[float("nan")] * 4 for _ in range(16)]
        for number, value in data["screened"].items():
            image[int(number) % 16][int(number) // 16] = value["batch_seconds"]
        rendered = axis.imshow(image, aspect="auto", cmap="viridis_r")
        for number in data["failed_configs"]:
            axis.text(
                number // 16,
                number % 16,
                "timeout",
                ha="center",
                va="center",
                color="#b71c1c",
                fontsize=8,
            )
        for number, value in data["screened"].items():
            axis.text(
                int(number) // 16,
                int(number) % 16,
                f"{value['batch_seconds']:.1f}",
                ha="center",
                va="center",
                color="black",
                fontsize=8,
            )
        axis.set_xticks(
            range(4), ["no cap\n1 MiB", "100mb\n1 MiB", "no cap\n4 MiB", "100mb\n4 MiB"]
        )
        axis.set_yticks(
            range(16), [" / ".join(list(parameters(number).values())[:4]) for number in range(16)]
        )
        axis.set_title(case)
        axis.set_xlabel("Egress cap / output buffer")
        axis.set_ylabel("CPU budget / threshold / sleep µs / destination throttle µs")
        figure.colorbar(rendered, ax=axis, label="Completed batch duration (s)")
    figure.suptitle("All 64 combinations per case; failed transfers have no duration ranking")
    figure.savefig(chart_dir / "combination-matrix.svg")
    plt.close(figure)
    rows, _ = collect(root)
    for case in ("ring", "fanin"):
        best = summary["cases"][case]["preferred_repeated"]
        stages = [("default", 0)] + ([("finalist", best)] if best is not None else [])
        for stage, number in stages:
            candidates = [
                row
                for row in rows
                if row["case"] == case and row["stage"] == stage and row["config"] == number
            ]
            median_time = statistics.median(row["batch_seconds"] for row in candidates)
            row = min(candidates, key=lambda item: abs(item["batch_seconds"] - median_time))
            run_dir = Path(row["path"])
            migration_start = row["phases"]["migration"][0]
            finish = row["phases"]["after"][0] - migration_start
            metrics = [
                json.loads(line) for line in (run_dir / "metrics.jsonl").read_text().splitlines()
            ]
            figure, axes = plt.subplots(
                4, 1, figsize=(11, 10), sharex=True, constrained_layout=True
            )
            for index, name in enumerate("ABCD"):
                intervals = json.loads((run_dir / f"client-{name}-intervals.json").read_text())
                x = [
                    (interval["start"] + interval["end"]) / 2 - migration_start
                    for interval in intervals
                ]
                label = name + (
                    " (destination)"
                    if case == "fanin" and name == "D"
                    else " (source)" if case == "fanin" else ""
                )
                axes[0].plot(x, [interval["p99_us"] for interval in intervals], label=label)
                axes[1].plot(x, [interval["max_us"] / 1000 for interval in intervals])
                axes[2].plot(
                    x,
                    [
                        interval["count"] / max(interval["end"] - interval["start"], 0.001) / 1000
                        for interval in intervals
                    ],
                )
                axes[3].plot(
                    [sample["time"] - migration_start for sample in metrics],
                    [sample["nodes"][index].get("cpu_percent", 0) for sample in metrics],
                )
            for axis in axes:
                axis.axvspan(0, finish, alpha=0.08, color="gray")
                axis.axvline(0, color="black", linewidth=0.8)
                axis.axvline(finish, color="black", linewidth=0.8, linestyle="--")
                axis.grid(alpha=0.2)
            axes[0].set_ylabel("GET P99 (µs)")
            axes[1].set_ylabel("Maximum GET (ms)")
            axes[1].set_yscale("log")
            axes[2].set_ylabel("GETs/s (thousands)")
            axes[3].set_ylabel("Server CPU (% of core)")
            axes[3].set_xlabel("Seconds relative to the opening configuration")
            axes[0].legend(ncols=4)
            figure.suptitle(
                f"{case}, {stage}, config {number}: {row['batch_seconds']:.2f}s — representative repeat"
            )
            figure.savefig(chart_dir / f"{case}-{stage}-{number:02d}.svg")
            plt.close(figure)


def write_report(root, summary):
    environment = json.loads((root / "environment.json").read_text())
    calibration = json.loads((root / "calibration.json").read_text())
    lines = [
        "# Dragonfly 1.40.8 — local slot migration experiments",
        "",
        "Four primary processes, one proactor and one database shard each. "
        "Continuous cluster GETs run before, during and after migration.",
        "",
        "## Conditions",
        "",
        "- Release build from source tag `i1.40.8`, commit "
        "`c13712901762329149b13bd50090a2f18f210493`; server implementation unchanged.",
        "- AMD Ryzen AI 7 350, 30 GiB RAM, Linux 7.0.0-38-generic, io_uring, loopback networking.",
        "- Nodes A/B/C/D pinned to CPUs 0/1/2/3; cluster clients pinned to CPUs 4/5/6/7. "
        "No SMT siblings shared between these eight processes.",
        "- Each node uses `--proactor_threads=1 --num_shards=1 --maxmemory=6gb`. "
        "Every server thread's affinity is checked at launch.",
        "- 1,048,576 migrating string keys × 1,024 value bytes = 1 GiB per job; "
        "16,384 resident GET keys per node outside migrating ranges. No TTLs or workload writes.",
        "- Ten GET connections per client, pipeline one, go-redis/v9 v9.12.1; "
        "every returned value is validated. Three-significant-figure HDR histograms record every GET.",
        "- CPU calibration: "
        + ", ".join(f"{name} {cpu:.1f}%" for name, cpu in zip("ABCD", calibration["cpu_percent"]))
        + ". "
        "Calibrated client settings are unchanged during and across migration runs.",
        "- Ten-second GET-only windows before and after migration; fresh original snapshots "
        "loaded before every run, outside measurement windows. Automatic snapshots disabled.",
        "- Concurrency 1: A→B, B→C, C→D, D→A simultaneously (4 GiB total). "
        "Concurrency 2: A/B/C→D simultaneously (3 GiB total).",
        "",
        "## Results",
        "",
        f"Recorded {summary['success_count']} completed runs and {summary['failure_count']} failed runs, "
        "including default repetitions, the combination screen and finalist repetitions. "
        "Failed runs are excluded from speed rankings and retained in the raw data.",
        "",
        "Times below are medians of completed repetitions; latency numbers are medians of each "
        "run's independently merged phase histogram percentiles, not averages of per-second percentiles.",
        "",
        "| Case | Default median (s) | Selected repeated configuration | Median (s) | Time change |",
        "| --- | :---: | :---: | :---: | :---: |",
    ]
    for case, name in (("ring", "Concurrency 1"), ("fanin", "Concurrency 2")):
        data = summary["cases"][case]
        default = data["defaults"]["batch_seconds"]
        best = data["preferred_repeated"]
        if best is None:
            lines.append(f"| {name} | {default:.2f} | No validated finalist | — | — |")
        else:
            candidate = data["finalists"][best]
            lines.append(
                f"| {name} | {default:.2f} | {best} | {candidate['batch_seconds']:.2f} | "
                f"{(candidate['batch_seconds']/default-1)*100:+.1f}% |"
            )
    lines += [
        "",
        "The selected combinations are within 0.12 seconds of the fastest repeated median; "
        "among these near-ties, the smaller worst-node P99 is preferred. Status polling is at 100 ms. "
        "These improve existing migration behavior; they do not preserve the GET-only read tail.",
        "",
        "| Parameter | Concurrency 1 selected | Concurrency 2 selected |",
        "| --- | :---: | :---: |",
    ]
    for parameter in OPTIONS:
        values = []
        for case in ("ring", "fanin"):
            data = summary["cases"][case]
            best = data["preferred_repeated"]
            values.append(
                data["finalists"][best]["parameters"][parameter] if best is not None else "—"
            )
        lines.append(f"| `{parameter}` | {values[0]} | {values[1]} |")
    lines += [
        "",
        "## Read tails",
        "",
        "| Case / node | Default GET-only P95 / P99 (µs) | Default migration P95 / P99 (µs) | Selected migration P95 / P99 (µs) |",
        "| --- | :---: | :---: | :---: |",
    ]
    for case in ("ring", "fanin"):
        data = summary["cases"][case]
        best = data["preferred_repeated"]
        for name in "ABCD":
            default = data["defaults"]["nodes"][name]
            candidate = (
                data["finalists"][best]["nodes"][name]["migration"] if best is not None else None
            )
            pair = lambda value: f"{value['p95_us']:.0f} / {value['p99_us']:.0f}"
            lines.append(
                f"| {case} / {name} | {pair(default['before'])} | {pair(default['migration'])} | "
                f"{pair(candidate) if candidate else '—'} |"
            )
    lines += [
        "",
        "Strict screening compares both P95 and P99 on every node with that run's GET-only "
        "window, includes the cleanup window, and requires at least 95% of preceding read QPS. "
        "No absolute production latency budget was supplied; this criterion is provisional.",
        "",
    ]
    for case in ("ring", "fanin"):
        data = summary["cases"][case]
        lines.append(
            f"- {case}: {len(data['passing_read_control'])} screened configurations met this strict criterion; "
            f"{len(data['failed_configs'])} configurations had an observed failure."
        )
    lines += [
        "",
        "The separate common-setting validation used configuration 60 "
        "(budget 0.2, threshold 10, sleep 250µs, destination throttle 20µs, egress 100mb, buffer 4 MiB). "
        "Its three fresh repetitions per case are recorded separately:",
        "",
        "| Case | Common-setting median (s) | Range (s) | Change versus default |",
        "| --- | :---: | :---: | :---: |",
    ]
    for case in ("ring", "fanin"):
        data = summary["cases"][case]
        common = data["common_validation"].get(60)
        if common:
            delta = (common["batch_seconds"] / data["defaults"]["batch_seconds"] - 1) * 100
            lines.append(
                f"| {case} | {common['batch_seconds']:.2f} | {common['batch_min']:.2f}–{common['batch_max']:.2f} | {delta:+.1f}% |"
            )
    lines += [
        "",
        "The common setting reduced read tails but its ring speed gain did not hold in "
        "the repetitions. Use the case-specific profiles for the measured speed improvements.",
        "",
        "Observed ring failures were journal-write timeouts with stalled streams and low CPU, "
        "while host memory remained available. All 32 capped ring screens completed; 29 of 32 "
        "uncapped ring screens failed. The three default ring repetitions had completed earlier, "
        "so this is timing-dependent behavior in the tested version/workload. The mechanism is "
        "consistent with backpressure but was not established by a stack trace.",
        "",
    ]
    lines += [
        "",
        "Source cutover pauses ordinary reads. Maximum latency and one-second peaks are saved "
        "alongside P95/P99 because a brief pause can disappear from whole-run percentiles.",
        "",
        "![Parameter search](charts/parameter-search.svg)",
        "",
        "![Every tested combination](charts/combination-matrix.svg)",
        "",
        "## Parameter effects",
        "",
        "Each percentage is the median of paired alternative/default results with the other five "
        "parameters held fixed. Only pairs where both runs completed are included; failure counts "
        "remain separate, so a failed run is never represented as a fast migration.",
        "",
        "| Alternative | Ring time change | Fan-in time change | Successful pairs (ring / fan-in) |",
        "| --- | :---: | :---: | :---: |",
    ]
    for parameter, values in OPTIONS.items():
        a, b = (summary["parameter_effects"][case][parameter] for case in ("ring", "fanin"))
        show = lambda value: "—" if value is None else f"{value:+.1f}%"
        lines.append(
            f"| `{parameter}={values[1]}` | {show(a['median_time_change_percent'])} | "
            f"{show(b['median_time_change_percent'])} | {a['paired_successes']} / {b['paired_successes']} |"
        )
    lines += [
        "",
        "## Appendix: reproduction and raw data",
        "",
        "- [Exact command audit](commands_audit.log), [environment and binary hashes](environment.json), "
        "[CPU calibration](calibration.json).",
        "- [All run outcomes and parameters](runs.csv), [per-node phase metrics](node-metrics.csv), "
        "[structured summary, effects and failures](summary.json).",
        "- Each run directory contains server logs, all opening/closing configurations, process/thread "
        "affinities, one-second CPU/RSS/host-memory samples, migration status history and full client histograms.",
        "- The SSH benchmark scripts share a single server PID/port and cannot manage four local pinned "
        "processes. The local harness tracks each owned PID, verifies teardown and ports, and enforces "
        "zero eviction/expiry and data correctness. This deviation was recorded before workloads.",
        "- Docker monitoring was unavailable because access to its socket was denied. Local process metrics "
        "and client HDR logs replace Grafana/Prometheus; charts come from these saved measurements.",
        "- Prefill uses `DEBUG POPULATE` (internal batches of 32), then fixed snapshots. "
        "These writes and snapshots occur before measured GET windows.",
        "- A preliminary memtier run produced substantial MOVED traffic with resident hash tags and only "
        "whole-run HDR output. It was excluded; the main runs use the bundled Go cluster client.",
        "- Results describe this one-proactor, loopback, string workload. The tested parameter grid has two "
        "values per flag; untested values and production hardware may behave differently.",
        "",
        f"Server SHA256: `{environment['binary_sha256']}`.",
        "",
    ]
    (root / "report.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
