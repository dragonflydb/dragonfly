#!/usr/bin/env python3
"""
Redis Stream Benchmark Analysis Tool
Compares results across runs, generates visualizations, and provides insights

Written with Claude Code (https://claude.com/claude-code)
"""

import pandas as pd
import argparse
from pathlib import Path
from typing import Dict, List
import statistics


class BenchmarkAnalyzer:
    def __init__(self, results_files: List[str]):
        """Load and parse benchmark results"""
        self.data = []
        self.labels = []

        for filepath in results_files:
            label = Path(filepath).stem
            self.data.append(pd.read_csv(filepath))
            self.labels.append(label)

        print(f"Loaded {len(self.data)} result file(s)")

    def compare_throughput(self):
        """Compare throughput across runs"""
        print("\n" + "=" * 100)
        print("THROUGHPUT COMPARISON (ops/sec)")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")
            print(
                df[["scenario", "command", "num_operations", "throughput_ops_sec"]].to_string(
                    index=False
                )
            )

    def compare_latency(self, percentile: str = "p95"):
        """Compare latency at specific percentile"""
        print(f"\n{'='*100}")
        print(f"LATENCY COMPARISON (P{percentile[1:].upper()} - ms)")
        print("=" * 100)

        col_name = f"{percentile}_latency_ms"

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")
            print(df[["scenario", "command", "num_operations", col_name]].to_string(index=False))

    def compare_memory(self):
        """Compare memory usage"""
        print("\n" + "=" * 100)
        print("MEMORY USAGE COMPARISON (MB)")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")
            cols = [
                "scenario",
                "command",
                "num_operations",
                "memory_before_mb",
                "memory_after_mb",
                "memory_delta_mb",
            ]
            print(df[cols].to_string(index=False))

    def efficiency_analysis(self):
        """Analyze efficiency: throughput per MB"""
        print("\n" + "=" * 100)
        print("EFFICIENCY ANALYSIS (throughput / memory delta)")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")

            # Calculate efficiency metric
            df_copy = df.copy()
            df_copy["efficiency"] = df_copy.apply(
                lambda row: (
                    row["throughput_ops_sec"] / abs(row["memory_delta_mb"])
                    if row["memory_delta_mb"] != 0
                    else row["throughput_ops_sec"]
                ),
                axis=1,
            )

            print(
                df_copy[
                    ["scenario", "command", "throughput_ops_sec", "memory_delta_mb", "efficiency"]
                ].to_string(index=False)
            )

    def identify_bottlenecks(self):
        """Identify performance bottlenecks"""
        print("\n" + "=" * 100)
        print("BOTTLENECK ANALYSIS")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")

            # Highest latency scenarios
            print("\n  Highest P99 Latency:")
            top_latency = df.nlargest(3, "p99_latency_ms")[
                ["scenario", "command", "p99_latency_ms"]
            ]
            print(top_latency.to_string(index=False))

            # Lowest throughput scenarios
            print("\n  Lowest Throughput:")
            low_throughput = df.nsmallest(3, "throughput_ops_sec")[
                ["scenario", "command", "throughput_ops_sec"]
            ]
            print(low_throughput.to_string(index=False))

            # Highest memory overhead
            print("\n  Highest Memory Delta:")
            high_memory = df.nlargest(3, "memory_delta_mb")[
                ["scenario", "command", "memory_delta_mb"]
            ]
            print(high_memory.to_string(index=False))

    def cross_run_comparison(self):
        """Compare same scenarios across different runs"""
        if len(self.data) < 2:
            print("\nNeed at least 2 result files for cross-run comparison")
            return

        print("\n" + "=" * 100)
        print("CROSS-RUN COMPARISON")
        print("=" * 100)

        # Get common scenarios
        scenarios = set(self.data[0]["scenario"].unique())
        for df in self.data[1:]:
            scenarios &= set(df["scenario"].unique())

        for scenario in sorted(scenarios):
            print(f"\n{scenario.upper()}:")

            # Compare throughput
            print("\n  Throughput (ops/sec):")
            for i, df in enumerate(self.data):
                scenario_data = df[df["scenario"] == scenario]
                avg_throughput = scenario_data["throughput_ops_sec"].mean()
                print(f"    {self.labels[i]}: {avg_throughput:,.0f}")

            # Compare latency
            print("\n  P95 Latency (ms):")
            for i, df in enumerate(self.data):
                scenario_data = df[df["scenario"] == scenario]
                avg_latency = scenario_data["p95_latency_ms"].mean()
                print(f"    {self.labels[i]}: {avg_latency:.3f}")

            # Compare memory delta
            print("\n  Memory Delta (MB):")
            for i, df in enumerate(self.data):
                scenario_data = df[df["scenario"] == scenario]
                avg_memory = scenario_data["memory_delta_mb"].mean()
                print(f"    {self.labels[i]}: {avg_memory:+.2f}")

    def regression_detection(self):
        """Detect performance regressions between runs"""
        if len(self.data) < 2:
            print("\nNeed at least 2 result files for regression detection")
            return

        print("\n" + "=" * 100)
        print("PERFORMANCE REGRESSION DETECTION")
        print("=" * 100)

        baseline = self.data[0]

        for i in range(1, len(self.data)):
            print(f"\n{self.labels[0]} vs {self.labels[i]}:")

            current = self.data[i]

            for scenario in baseline["scenario"].unique():
                baseline_rows = baseline[baseline["scenario"] == scenario]
                current_rows = current[current["scenario"] == scenario]

                if baseline_rows.empty or current_rows.empty:
                    continue

                baseline_throughput = baseline_rows["throughput_ops_sec"].mean()
                current_throughput = current_rows["throughput_ops_sec"].mean()

                baseline_latency = baseline_rows["p95_latency_ms"].mean()
                current_latency = current_rows["p95_latency_ms"].mean()

                baseline_memory = baseline_rows["memory_delta_mb"].mean()
                current_memory = current_rows["memory_delta_mb"].mean()

                throughput_change = (
                    (current_throughput - baseline_throughput) / baseline_throughput
                ) * 100
                latency_change = ((current_latency - baseline_latency) / baseline_latency) * 100
                memory_change = current_memory - baseline_memory

                print(f"\n  {scenario}:")
                print(
                    f"    Throughput: {baseline_throughput:,.0f} → {current_throughput:,.0f} "
                    f"({throughput_change:+.1f}%)"
                )
                print(
                    f"    P95 Latency: {baseline_latency:.3f}ms → {current_latency:.3f}ms "
                    f"({latency_change:+.1f}%)"
                )
                print(
                    f"    Memory Delta: {baseline_memory:+.2f}MB → {current_memory:+.2f}MB "
                    f"({memory_change:+.2f}MB)"
                )

                if throughput_change < -5:
                    print(f"    REGRESSION: Throughput decreased significantly")
                elif latency_change > 10:
                    print(f"    REGRESSION: Latency increased significantly")
                elif throughput_change > 10:
                    print(f"    IMPROVEMENT: Throughput increased")

    def percentile_distribution(self):
        """Show latency percentile distribution"""
        print("\n" + "=" * 100)
        print("LATENCY PERCENTILE DISTRIBUTION")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")
            print(
                df[
                    [
                        "scenario",
                        "command",
                        "min_latency_ms",
                        "p50_latency_ms",
                        "p95_latency_ms",
                        "p99_latency_ms",
                        "max_latency_ms",
                    ]
                ].to_string(index=False)
            )

    def scenario_analysis(self):
        """Analyze performance by scenario"""
        print("\n" + "=" * 100)
        print("SCENARIO-BASED ANALYSIS")
        print("=" * 100)

        for i, df in enumerate(self.data):
            print(f"\n{self.labels[i]}:")

            for scenario in sorted(df["scenario"].unique()):
                scenario_data = df[df["scenario"] == scenario]

                print(f"\n  {scenario}:")
                print(
                    f"    Avg Throughput: {scenario_data['throughput_ops_sec'].mean():,.0f} ops/sec"
                )
                print(f"    Avg P95 Latency: {scenario_data['p95_latency_ms'].mean():.3f}ms")
                print(f"    Avg Memory Delta: {scenario_data['memory_delta_mb'].mean():+.2f}MB")
                print(f"    Commands: {', '.join(scenario_data['command'].unique())}")

    def export_comparison_csv(self, output_file: str = "comparison_results.csv"):
        """Export comparison to CSV"""
        if len(self.data) < 2:
            print("Need at least 2 files for comparison export")
            return

        baseline = self.data[0]
        comparison_rows = []

        for scenario in baseline["scenario"].unique():
            baseline_rows = baseline[baseline["scenario"] == scenario]

            for _, baseline_row in baseline_rows.iterrows():
                row = {
                    "scenario": baseline_row["scenario"],
                    "command": baseline_row["command"],
                    "baseline_throughput": baseline_row["throughput_ops_sec"],
                    "baseline_p95_latency": baseline_row["p95_latency_ms"],
                    "baseline_memory_delta": baseline_row["memory_delta_mb"],
                }

                for i in range(1, len(self.data)):
                    current = self.data[i]
                    current_rows = current[
                        (current["scenario"] == baseline_row["scenario"])
                        & (current["command"] == baseline_row["command"])
                    ]

                    if not current_rows.empty:
                        current_row = current_rows.iloc[0]
                        prefix = self.labels[i]

                        row[f"{prefix}_throughput"] = current_row["throughput_ops_sec"]
                        row[f"{prefix}_p95_latency"] = current_row["p95_latency_ms"]
                        row[f"{prefix}_memory_delta"] = current_row["memory_delta_mb"]
                        row[f"{prefix}_throughput_change_%"] = (
                            (current_row["throughput_ops_sec"] - baseline_row["throughput_ops_sec"])
                            / baseline_row["throughput_ops_sec"]
                            * 100
                        )
                        row[f"{prefix}_memory_delta_change_MB"] = (
                            current_row["memory_delta_mb"] - baseline_row["memory_delta_mb"]
                        )

                comparison_rows.append(row)

        comparison_df = pd.DataFrame(comparison_rows)
        comparison_df.to_csv(output_file, index=False)
        print(f"\nComparison exported to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Analyze Redis benchmark results")
    parser.add_argument("files", nargs="+", help="Result CSV files to analyze")
    parser.add_argument("--throughput", action="store_true", help="Show throughput comparison")
    parser.add_argument("--latency", action="store_true", help="Show latency comparison")
    parser.add_argument("--memory", action="store_true", help="Show memory comparison")
    parser.add_argument("--efficiency", action="store_true", help="Show efficiency analysis")
    parser.add_argument("--bottlenecks", action="store_true", help="Identify bottlenecks")
    parser.add_argument("--regression", action="store_true", help="Detect regressions")
    parser.add_argument("--percentiles", action="store_true", help="Show percentile distribution")
    parser.add_argument("--scenarios", action="store_true", help="Analyze by scenario")
    parser.add_argument("--cross", action="store_true", help="Cross-run comparison")
    parser.add_argument("--all", action="store_true", help="Run all analyses")
    parser.add_argument("--export", help="Export comparison CSV")

    args = parser.parse_args()

    analyzer = BenchmarkAnalyzer(args.files)

    if args.all or not any(
        [
            args.throughput,
            args.latency,
            args.memory,
            args.efficiency,
            args.bottlenecks,
            args.regression,
            args.percentiles,
            args.scenarios,
            args.cross,
        ]
    ):
        analyzer.compare_throughput()
        analyzer.compare_latency()
        analyzer.compare_memory()
        analyzer.efficiency_analysis()
        analyzer.identify_bottlenecks()
        analyzer.percentile_distribution()
        analyzer.scenario_analysis()
        analyzer.cross_run_comparison()
        analyzer.regression_detection()
    else:
        if args.throughput:
            analyzer.compare_throughput()
        if args.latency:
            analyzer.compare_latency()
        if args.memory:
            analyzer.compare_memory()
        if args.efficiency:
            analyzer.efficiency_analysis()
        if args.bottlenecks:
            analyzer.identify_bottlenecks()
        if args.percentiles:
            analyzer.percentile_distribution()
        if args.scenarios:
            analyzer.scenario_analysis()
        if args.cross:
            analyzer.cross_run_comparison()
        if args.regression:
            analyzer.regression_detection()

    if args.export:
        analyzer.export_comparison_csv(args.export)


if __name__ == "__main__":
    main()
