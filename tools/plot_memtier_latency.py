#!/usr/bin/env python3
"""
Script to read memtier_benchmark JSON output and generate interactive latency charts.

The script generates interactive HTML charts (using Plotly) where you can:
- Click on legend items to show/hide time series
- Zoom in/out and pan
- Hover over data points for detailed information

To generate the JSON file, run memtier_benchmark with the --json-out-file option:

    memtier_benchmark --server <host> --port <port> \\
        --json-out-file memtier_out.json \\
        [other options...]

Example:
    memtier_benchmark --json-out-file memtier_out.json \\
        --clients 25 --threads 4 --test-time 120 \\
        --ratio 1:10

Then run this script to visualize the results:
    ./plot_memtier_latency.py memtier_out.json

Requirements:
    pip install plotly matplotlib numpy

Note: If plotly is not available, falls back to static SVG charts.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import webbrowser
import tempfile
import os

# Try to import plotly for interactive charts
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Warning: plotly not available. Install with: pip install plotly")


def load_json_data(filepath):
    """Load JSON data from file."""
    with open(filepath, "r") as f:
        return json.load(f)


def extract_latency_timeseries(data, operation, ignore_last_seconds=3):
    """
    Extract latency time series data from memtier output.

    Args:
        data: Parsed JSON data
        operation: Operation type (e.g., 'Mgets', 'Sets', 'Gets', etc.)
        ignore_last_seconds: Number of seconds to ignore from the end

    Returns:
        Dictionary with time series data
    """
    time_serie = data["ALL STATS"][operation]["Time-Serie"]

    times = []
    avg_latencies = []
    p50_latencies = []
    p99_latencies = []
    p99_9_latencies = []
    min_latencies = []
    max_latencies = []
    ops_per_sec = []

    # Sort time points and determine cutoff
    sorted_times = sorted(time_serie.keys(), key=lambda x: int(x))
    if ignore_last_seconds > 0 and len(sorted_times) > ignore_last_seconds:
        # Remove last N seconds
        sorted_times = sorted_times[:-ignore_last_seconds]

    for time_point in sorted_times:
        interval_data = time_serie[time_point]
        times.append(int(time_point))
        avg_latencies.append(interval_data["Average Latency"])
        p50_latencies.append(interval_data.get("p50.00", 0))
        p99_latencies.append(interval_data.get("p99.00", 0))
        p99_9_latencies.append(interval_data.get("p99.90", 0))
        min_latencies.append(interval_data["Min Latency"])
        max_latencies.append(interval_data["Max Latency"])

        # Calculate ops/sec for this interval (count per second)
        ops_per_sec.append(interval_data["Count"])

    return {
        "times": times,
        "avg": avg_latencies,
        "p50": p50_latencies,
        "p99": p99_latencies,
        "p99.9": p99_9_latencies,
        "min": min_latencies,
        "max": max_latencies,
        "ops_per_sec": ops_per_sec,
    }


def plot_latency_chart_interactive(data, output_file="latency_chart.html", open_browser=True):
    """
    Generate interactive latency chart using Plotly.

    Args:
        data: Parsed JSON data
        output_file: Output filename for the chart
        open_browser: If True, open the chart in the browser
    """
    if not PLOTLY_AVAILABLE:
        print("Plotly not available. Falling back to matplotlib...")
        # Change extension to svg for matplotlib fallback
        svg_file = output_file.replace(".html", ".svg")
        return plot_latency_chart(data, svg_file, open_browser)

    # Get all available operations from ALL STATS (excluding 'Runtime')
    all_stats = data["ALL STATS"]
    operations = [
        key
        for key in all_stats.keys()
        if key != "Runtime" and isinstance(all_stats[key], dict) and "Time-Serie" in all_stats[key]
    ]

    if not operations:
        print("Error: No operation data found in JSON")
        return

    # Extract data for all operations
    ops_data = {}
    for op in operations:
        ops_data[op] = extract_latency_timeseries(data, op, ignore_last_seconds=3)

    # Determine subplot layout
    num_ops = len(operations)
    if num_ops == 1:
        rows, cols = 2, 1
        specs = [[{"secondary_y": False}], [{"secondary_y": False}]]
        subplot_titles = [f"{operations[0]} Latency", "Throughput"]
    elif num_ops == 2:
        rows, cols = 2, 2
        specs = [
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ]
        subplot_titles = [
            f"{operations[0]} Latency",
            f"{operations[1]} Latency",
            "Latency Comparison",
            "Throughput",
        ]
    else:
        rows = num_ops + 1
        cols = 1
        specs = [[{"secondary_y": False}] for _ in range(rows)]
        subplot_titles = [f"{op} Latency" for op in operations] + ["Throughput"]

    # Create subplots
    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        specs=specs,
        vertical_spacing=0.12,
        horizontal_spacing=0.1,
    )

    # Plot individual operation latencies
    for idx, op in enumerate(operations):
        if num_ops == 2:
            row = (idx // cols) + 1
            col = (idx % cols) + 1
        else:
            row = idx + 1
            col = 1

        op_data = ops_data[op]

        # Add traces with independent visibility toggle (no legendgroup)
        fig.add_trace(
            go.Scatter(
                x=op_data["times"],
                y=op_data["avg"],
                name=f"{op} Avg",
                mode="lines",
                line=dict(width=2),
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=op_data["times"],
                y=op_data["p50"],
                name=f"{op} p50",
                mode="lines",
                line=dict(width=2),
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=op_data["times"],
                y=op_data["p99"],
                name=f"{op} p99",
                mode="lines",
                line=dict(width=2),
            ),
            row=row,
            col=col,
        )
        fig.add_trace(
            go.Scatter(
                x=op_data["times"],
                y=op_data["p99.9"],
                name=f"{op} p99.9",
                mode="lines",
                line=dict(width=2),
            ),
            row=row,
            col=col,
        )

        fig.update_xaxes(title_text="Time (seconds)", row=row, col=col)
        fig.update_yaxes(title_text="Latency (ms)", row=row, col=col)

    # Add comparison plot if multiple operations and layout allows
    if num_ops == 2:
        comp_row, comp_col = 2, 1
        for op in operations:
            op_data = ops_data[op]
            fig.add_trace(
                go.Scatter(
                    x=op_data["times"],
                    y=op_data["p99"],
                    name=f"{op} p99 (comp)",
                    mode="lines",
                    line=dict(width=2, dash="solid"),
                ),
                row=comp_row,
                col=comp_col,
            )
            fig.add_trace(
                go.Scatter(
                    x=op_data["times"],
                    y=op_data["avg"],
                    name=f"{op} Avg (comp)",
                    mode="lines",
                    line=dict(width=2, dash="dash"),
                ),
                row=comp_row,
                col=comp_col,
            )
        fig.update_xaxes(title_text="Time (seconds)", row=comp_row, col=comp_col)
        fig.update_yaxes(title_text="Latency (ms)", row=comp_row, col=comp_col)

    # Add throughput plot
    if num_ops == 2:
        tp_row, tp_col = 2, 2
    else:
        tp_row = rows
        tp_col = 1

    for op in operations:
        op_data = ops_data[op]
        fig.add_trace(
            go.Scatter(
                x=op_data["times"],
                y=op_data["ops_per_sec"],
                name=f"{op} ops/sec",
                mode="lines",
                line=dict(width=2),
            ),
            row=tp_row,
            col=tp_col,
        )

    fig.update_xaxes(title_text="Time (seconds)", row=tp_row, col=tp_col)
    fig.update_yaxes(title_text="Operations per Second", row=tp_row, col=tp_col)

    # Update layout
    fig.update_layout(
        title_text="Memtier Benchmark - Latency Analysis (Interactive - Click legend to toggle)",
        height=300 * rows,
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
    )

    # Add annotation with statistics
    stats_lines = ["<b>Overall Statistics (last 3 seconds excluded):</b><br>"]
    for op in operations:
        op_stats = all_stats[op]
        stats_lines.append(
            f"{op}: Avg={op_stats['Average Latency']:.3f}ms, "
            f"p99={op_stats['Percentile Latencies']['p99.00']:.3f}ms, "
            f"Ops/sec={op_stats['Ops/sec']:.2f}<br>"
        )
    stats_lines.append(f"Duration: {all_stats['Runtime']['Total duration'] / 1000:.1f}s")

    fig.add_annotation(
        text="".join(stats_lines),
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.05,
        showarrow=False,
        font=dict(size=10),
        bgcolor="wheat",
        bordercolor="black",
        borderwidth=1,
        xanchor="center",
        yanchor="top",
    )

    # Save to HTML
    fig.write_html(output_file)
    print(f"Interactive chart saved to: {output_file}")

    # Open in browser
    if open_browser:
        abs_path = os.path.abspath(output_file)
        file_url = f"file://{abs_path}"
        print(f"Opening chart in browser: {file_url}")
        webbrowser.open(file_url)


def plot_latency_chart(data, output_file="latency_chart.svg", open_browser=True):
    """
    Generate latency chart from memtier data.

    Args:
        data: Parsed JSON data
        output_file: Output filename for the chart
        open_browser: If True, open the chart in the browser
    """
    # Get all available operations from ALL STATS (excluding 'Runtime')
    all_stats = data["ALL STATS"]
    operations = [
        key
        for key in all_stats.keys()
        if key != "Runtime" and isinstance(all_stats[key], dict) and "Time-Serie" in all_stats[key]
    ]

    if not operations:
        print("Error: No operation data found in JSON")
        return

    # Extract data for all operations
    ops_data = {}
    for op in operations:
        ops_data[op] = extract_latency_timeseries(data, op, ignore_last_seconds=3)

    # Determine number of subplots needed
    num_ops = len(operations)
    if num_ops == 1:
        # Single operation: 2x2 grid with detailed views
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        axes = axes.flatten()
    elif num_ops == 2:
        # Two operations: 2x2 grid
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        axes = axes.flatten()
    else:
        # Multiple operations: dynamic grid
        rows = (num_ops + 1) // 2 + 1
        fig, axes = plt.subplots(rows, 2, figsize=(16, 4 * rows))
        axes = axes.flatten()

    fig.suptitle("Memtier Benchmark - Latency Analysis", fontsize=16, fontweight="bold")

    # Plot each operation's latency percentiles
    for idx, op in enumerate(operations):
        if idx >= len(axes) - 1:  # Save last plot for throughput
            break

        ax = axes[idx]
        op_data = ops_data[op]

        ax.plot(op_data["times"], op_data["avg"], label="Average", linewidth=2)
        ax.plot(op_data["times"], op_data["p50"], label="p50", linewidth=2)
        ax.plot(op_data["times"], op_data["p99"], label="p99", linewidth=2)
        ax.plot(op_data["times"], op_data["p99.9"], label="p99.9", linewidth=2)
        ax.set_xlabel("Time (seconds)", fontsize=12)
        ax.set_ylabel("Latency (ms)", fontsize=12)
        ax.set_title(f"{op} Operations - Latency Percentiles", fontsize=14, fontweight="bold")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)

    # Comparison plot (if multiple operations)
    if num_ops > 1:
        comparison_idx = min(num_ops, len(axes) - 2)
        ax_comp = axes[comparison_idx]

        for op in operations:
            op_data = ops_data[op]
            ax_comp.plot(op_data["times"], op_data["p99"], label=f"{op} p99", linewidth=2)
            ax_comp.plot(
                op_data["times"],
                op_data["avg"],
                label=f"{op} Avg",
                linewidth=2,
                linestyle="--",
                alpha=0.7,
            )

        ax_comp.set_xlabel("Time (seconds)", fontsize=12)
        ax_comp.set_ylabel("Latency (ms)", fontsize=12)
        ax_comp.set_title("Operations Comparison - Latency", fontsize=14, fontweight="bold")
        ax_comp.legend(loc="best")
        ax_comp.grid(True, alpha=0.3)

    # Throughput plot
    throughput_idx = min(num_ops + 1, len(axes) - 1) if num_ops > 1 else len(axes) - 1
    ax_throughput = axes[throughput_idx]

    for op in operations:
        op_data = ops_data[op]
        ax_throughput.plot(
            op_data["times"], op_data["ops_per_sec"], label=f"{op} ops/sec", linewidth=2
        )

    ax_throughput.set_xlabel("Time (seconds)", fontsize=12)
    ax_throughput.set_ylabel("Operations per Second", fontsize=12)
    ax_throughput.set_title("Throughput Over Time", fontsize=14, fontweight="bold")
    ax_throughput.legend(loc="best")
    ax_throughput.grid(True, alpha=0.3)

    # Hide any unused subplots
    for idx in range(throughput_idx + 1, len(axes)):
        axes[idx].set_visible(False)

    # Add overall statistics as text
    stats_lines = ["Overall Statistics (last 3 seconds excluded):"]
    for op in operations:
        op_stats = all_stats[op]
        stats_lines.append(
            f"{op}: Avg={op_stats['Average Latency']:.3f}ms, "
            f"p99={op_stats['Percentile Latencies']['p99.00']:.3f}ms, "
            f"Ops/sec={op_stats['Ops/sec']:.2f}"
        )
    stats_lines.append(f"Duration: {all_stats['Runtime']['Total duration'] / 1000:.1f}s")
    stats_text = "\n".join(stats_lines)

    fig.text(
        0.5,
        0.02,
        stats_text,
        ha="center",
        fontsize=10,
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.97])
    plt.savefig(output_file, dpi=300, bbox_inches="tight", format="svg")
    print(f"Chart saved to: {output_file}")

    # Open in browser
    if open_browser:
        abs_path = os.path.abspath(output_file)
        file_url = f"file://{abs_path}"
        print(f"Opening chart in browser: {file_url}")
        webbrowser.open(file_url)

    plt.close()


def print_summary(data):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("MEMTIER BENCHMARK SUMMARY")
    print("=" * 60)

    config = data["configuration"]
    runtime = data["ALL STATS"]["Runtime"]

    print(f"\nConfiguration:")
    print(f"  Server: {config['server']}:{config['port']}")
    print(f"  Clients: {config['clients']}")
    print(f"  Threads: {config['threads']}")
    print(f"  Duration: {runtime['Total duration'] / 1000:.1f}s")
    print(f"  Pipeline: {config['pipeline']}")
    print(f"  Ratio (SET:GET): {config['ratio']}")

    # Get all operations dynamically
    all_stats = data["ALL STATS"]
    operations = [
        key
        for key in all_stats.keys()
        if key != "Runtime" and isinstance(all_stats[key], dict) and "Count" in all_stats[key]
    ]

    for op in operations:
        op_stats = all_stats[op]
        print(f"\n{op} Operations:")
        print(f"  Total: {op_stats['Count']:,}")
        print(f"  Ops/sec: {op_stats['Ops/sec']:.2f}")
        print(f"  Avg Latency: {op_stats['Average Latency']:.3f} ms")
        print(f"  Min Latency: {op_stats['Min Latency']:.3f} ms")
        print(f"  Max Latency: {op_stats['Max Latency']:.3f} ms")
        print(f"  p50: {op_stats['Percentile Latencies']['p50.00']:.3f} ms")
        print(f"  p99: {op_stats['Percentile Latencies']['p99.00']:.3f} ms")
        print(f"  p99.9: {op_stats['Percentile Latencies']['p99.90']:.3f} ms")

    print("\n" + "=" * 60 + "\n")


def main():
    """Main function."""
    import sys

    # Get input file from command line or use default
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "memtier_out.json"

    # Get output file from command line or use default
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    else:
        # Use .html for interactive charts by default
        output_file = "latency_chart.html" if PLOTLY_AVAILABLE else "latency_chart.svg"

    # Check if input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found!")
        print(f"\nUsage: {sys.argv[0]} [input_file.json] [output_file.html|.svg]")
        print(f"\nTo generate the JSON file, run memtier_benchmark with --json-out-file:")
        print(f"  memtier_benchmark --server <host> --port <port> \\")
        print(f"      --json-out-file memtier_out.json \\")
        print(f"      [other options...]")
        sys.exit(1)

    # Load and process data
    print(f"Loading data from: {input_file}")
    data = load_json_data(input_file)

    # Print summary
    print_summary(data)

    # Generate chart
    print(f"Generating latency chart...")

    # Use interactive chart if output is .html, otherwise use matplotlib
    if output_file.endswith(".html"):
        plot_latency_chart_interactive(data, output_file)
    else:
        plot_latency_chart(data, output_file)

    print(f"\nDone!")
    if PLOTLY_AVAILABLE:
        print(f"Tip: Use .html extension for interactive charts with toggleable series")


if __name__ == "__main__":
    main()
