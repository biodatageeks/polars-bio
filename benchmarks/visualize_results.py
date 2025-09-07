#!/usr/bin/env python3
"""
Visualization script for benchmark results.
Creates bar plots showing performance comparisons between Pandas, Polars, and polars-bio.
"""

from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Set style
plt.style.use("seaborn-v0_8")
sns.set_palette("husl")


def load_and_aggregate_results(csv_path: str) -> pd.DataFrame:
    """Load CSV results and compute min, max, avg statistics"""
    if not Path(csv_path).exists():
        print(f"Warning: {csv_path} not found, skipping...")
        return None

    df = pd.read_csv(csv_path)

    # Group by all columns except 'run' and the metric columns
    metric_cols = [
        col
        for col in df.columns
        if col
        in [
            "total_time",
            "read_time",
            "filter_time",
            "time",
            "full_scan_time",
            "peak_memory_mb",
            "max_memory_mb",
        ]
    ]
    group_cols = [
        col
        for col in df.columns
        if col
        not in ["run"]
        + metric_cols
        + ["result_count", "full_result_count", "filtered_result_count"]
    ]

    if not metric_cols:
        return df

    # Aggregate metrics
    agg_dict = {}
    for col in metric_cols:
        agg_dict[f"{col}_min"] = (col, "min")
        agg_dict[f"{col}_max"] = (col, "max")
        agg_dict[f"{col}_avg"] = (col, "mean")

    # Also keep other columns (take first value since they should be same within group)
    for col in df.columns:
        if col not in metric_cols and col not in group_cols and col != "run":
            agg_dict[col] = (col, "first")

    result = df.groupby(group_cols).agg(**agg_dict).reset_index()
    return result


def plot_general_performance():
    """Plot general performance comparison"""
    df = load_and_aggregate_results("results/general_performance.csv")
    if df is None:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Total time comparison
    bars1 = ax1.bar(
        df["library"],
        df["total_time_avg"],
        yerr=[
            df["total_time_avg"] - df["total_time_min"],
            df["total_time_max"] - df["total_time_avg"],
        ],
        capsize=5,
        alpha=0.8,
    )
    ax1.set_title("General Performance: Total Time")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_xlabel("Library")

    # Add value labels on bars
    for bar, avg in zip(bars1, df["total_time_avg"]):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.1,
            f"{avg:.2f}s",
            ha="center",
            va="bottom",
        )

    # Read vs Filter time breakdown
    libraries = df["library"]
    read_times = df["read_time_avg"]
    filter_times = df["filter_time_avg"]

    bars2 = ax2.bar(libraries, read_times, label="Read Time", alpha=0.8)
    bars3 = ax2.bar(
        libraries, filter_times, bottom=read_times, label="Filter Time", alpha=0.8
    )

    ax2.set_title("Performance Breakdown: Read vs Filter")
    ax2.set_ylabel("Time (seconds)")
    ax2.set_xlabel("Library")
    ax2.legend()

    plt.tight_layout()
    plt.savefig("results/general_performance.png", dpi=300, bbox_inches="tight")
    print("Saved: results/general_performance.png")


def plot_memory_comparison():
    """Plot memory usage comparison"""
    df = load_and_aggregate_results("results/memory_profiling.csv")
    if df is None:
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    bars = ax.bar(df["library"], df["max_memory_mb"], alpha=0.8)
    ax.set_title("Memory Usage Comparison")
    ax.set_ylabel("Peak Memory (MB)")
    ax.set_xlabel("Library")

    # Add value labels
    for bar, mem in zip(bars, df["max_memory_mb"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 10,
            f"{mem:.0f} MB",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    plt.savefig("results/memory_comparison.png", dpi=300, bbox_inches="tight")
    print("Saved: results/memory_comparison.png")


def plot_thread_scalability():
    """Plot thread scalability results"""
    df = load_and_aggregate_results("results/thread_scalability.csv")
    if df is None:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Full scan scalability
    for library in df["library"].unique():
        lib_data = df[df["library"] == library]
        ax1.plot(
            lib_data["threads"],
            lib_data["full_scan_time_avg"],
            marker="o",
            label=library,
            linewidth=2,
            markersize=6,
        )
        ax1.fill_between(
            lib_data["threads"],
            lib_data["full_scan_time_min"],
            lib_data["full_scan_time_max"],
            alpha=0.2,
        )

    ax1.set_title("Thread Scalability: Full Scan")
    ax1.set_xlabel("Number of Threads")
    ax1.set_ylabel("Time (seconds)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Filter scalability
    for library in df["library"].unique():
        lib_data = df[df["library"] == library]
        ax2.plot(
            lib_data["threads"],
            lib_data["filter_time_avg"],
            marker="s",
            label=library,
            linewidth=2,
            markersize=6,
        )
        ax2.fill_between(
            lib_data["threads"],
            lib_data["filter_time_min"],
            lib_data["filter_time_max"],
            alpha=0.2,
        )

    ax2.set_title("Thread Scalability: Filtered Query")
    ax2.set_xlabel("Number of Threads")
    ax2.set_ylabel("Time (seconds)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("results/thread_scalability.png", dpi=300, bbox_inches="tight")
    print("Saved: results/thread_scalability.png")


def plot_projection_pruning():
    """Plot projection pruning results with memory usage"""
    df = load_and_aggregate_results("results/projection_pruning.csv")
    if df is None:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Performance comparison
    proj_types = df["projection_type"].unique()
    x = np.arange(len(proj_types))
    width = 0.25

    libraries = df["library"].unique()
    for i, library in enumerate(libraries):
        lib_data = df[df["library"] == library]

        if library == "polars-bio":
            # Show both pushdown configurations
            no_pushdown = lib_data[lib_data["projection_pushdown"] == False]
            with_pushdown = lib_data[lib_data["projection_pushdown"] == True]

            ax1.bar(
                x - width + i * width * 0.5,
                no_pushdown["total_time_avg"],
                width * 0.4,
                label=f"{library} (no pushdown)",
                alpha=0.8,
            )
            ax1.bar(
                x + i * width * 0.5,
                with_pushdown["total_time_avg"],
                width * 0.4,
                label=f"{library} (pushdown)",
                alpha=0.8,
            )
        else:
            ax1.bar(
                x + (i - len(libraries) / 2) * width,
                lib_data["total_time_avg"],
                width,
                label=library,
                alpha=0.8,
            )

    ax1.set_title("Projection Pruning: Performance")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_xlabel("Projection Type")
    ax1.set_xticks(x)
    ax1.set_xticklabels(proj_types, rotation=45)
    ax1.legend()

    # Memory usage comparison
    for i, library in enumerate(libraries):
        lib_data = df[df["library"] == library]

        if library == "polars-bio":
            no_pushdown = lib_data[lib_data["projection_pushdown"] == False]
            with_pushdown = lib_data[lib_data["projection_pushdown"] == True]

            ax2.bar(
                x - width + i * width * 0.5,
                no_pushdown["peak_memory_mb_avg"],
                width * 0.4,
                alpha=0.8,
            )
            ax2.bar(
                x + i * width * 0.5,
                with_pushdown["peak_memory_mb_avg"],
                width * 0.4,
                alpha=0.8,
            )
        else:
            ax2.bar(
                x + (i - len(libraries) / 2) * width,
                lib_data["peak_memory_mb_avg"],
                width,
                alpha=0.8,
            )

    ax2.set_title("Projection Pruning: Memory Usage")
    ax2.set_ylabel("Peak Memory (MB)")
    ax2.set_xlabel("Projection Type")
    ax2.set_xticks(x)
    ax2.set_xticklabels(proj_types, rotation=45)

    plt.tight_layout()
    plt.savefig("results/projection_pruning.png", dpi=300, bbox_inches="tight")
    print("Saved: results/projection_pruning.png")


def plot_predicate_pushdown():
    """Plot predicate pushdown results with memory usage"""
    df = load_and_aggregate_results("results/predicate_pushdown.csv")
    if df is None:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Performance comparison
    filter_types = df["filter_type"].unique()
    x = np.arange(len(filter_types))
    width = 0.25

    libraries = df["library"].unique()
    for i, library in enumerate(libraries):
        lib_data = df[df["library"] == library]

        if library == "polars-bio":
            no_pushdown = lib_data[lib_data["predicate_pushdown"] == False]
            with_pushdown = lib_data[lib_data["predicate_pushdown"] == True]

            ax1.bar(
                x - width + i * width * 0.5,
                no_pushdown["total_time_avg"],
                width * 0.4,
                label=f"{library} (no pushdown)",
                alpha=0.8,
            )
            ax1.bar(
                x + i * width * 0.5,
                with_pushdown["total_time_avg"],
                width * 0.4,
                label=f"{library} (pushdown)",
                alpha=0.8,
            )
        else:
            ax1.bar(
                x + (i - len(libraries) / 2) * width,
                lib_data["total_time_avg"],
                width,
                label=library,
                alpha=0.8,
            )

    ax1.set_title("Predicate Pushdown: Performance")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_xlabel("Filter Type")
    ax1.set_xticks(x)
    ax1.set_xticklabels(filter_types)
    ax1.legend()

    # Memory usage comparison
    for i, library in enumerate(libraries):
        lib_data = df[df["library"] == library]

        if library == "polars-bio":
            no_pushdown = lib_data[lib_data["predicate_pushdown"] == False]
            with_pushdown = lib_data[lib_data["predicate_pushdown"] == True]

            ax2.bar(
                x - width + i * width * 0.5,
                no_pushdown["peak_memory_mb_avg"],
                width * 0.4,
                alpha=0.8,
            )
            ax2.bar(
                x + i * width * 0.5,
                with_pushdown["peak_memory_mb_avg"],
                width * 0.4,
                alpha=0.8,
            )
        else:
            ax2.bar(
                x + (i - len(libraries) / 2) * width,
                lib_data["peak_memory_mb_avg"],
                width,
                alpha=0.8,
            )

    ax2.set_title("Predicate Pushdown: Memory Usage")
    ax2.set_ylabel("Peak Memory (MB)")
    ax2.set_xlabel("Filter Type")
    ax2.set_xticks(x)
    ax2.set_xticklabels(filter_types)

    plt.tight_layout()
    plt.savefig("results/predicate_pushdown.png", dpi=300, bbox_inches="tight")
    print("Saved: results/predicate_pushdown.png")


def plot_combined_optimizations():
    """Plot combined optimizations results"""
    df = load_and_aggregate_results("results/combined_optimizations.csv")
    if df is None:
        return

    fig, ax = plt.subplots(figsize=(12, 8))

    # Create speedup comparison
    test_cases = df["test_case"].unique()
    configurations = df["configuration"].unique()

    x = np.arange(len(test_cases))
    width = 0.2

    baseline_times = {}
    baseline_data = df[df["configuration"] == "polars-bio-baseline"]
    for _, row in baseline_data.iterrows():
        baseline_times[row["test_case"]] = row["time_avg"]

    colors = plt.cm.Set1(np.linspace(0, 1, len(configurations)))

    for i, config in enumerate(configurations):
        config_data = df[df["configuration"] == config]
        speedups = []

        for test_case in test_cases:
            test_data = config_data[config_data["test_case"] == test_case]
            if len(test_data) > 0 and test_case in baseline_times:
                speedup = baseline_times[test_case] / test_data.iloc[0]["time_avg"]
                speedups.append(speedup)
            else:
                speedups.append(0)

        bars = ax.bar(
            x + i * width,
            speedups,
            width,
            label=config.replace("-", " ").title(),
            alpha=0.8,
            color=colors[i],
        )

        # Add speedup labels
        for bar, speedup in zip(bars, speedups):
            if speedup > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.1,
                    f"{speedup:.1f}x",
                    ha="center",
                    va="bottom",
                    fontweight="bold",
                )

    ax.axhline(y=1.0, color="red", linestyle="--", alpha=0.7, label="Baseline (1x)")
    ax.set_title("Performance Speedup: Combined Optimizations vs Baseline")
    ax.set_ylabel("Speedup (higher is better)")
    ax.set_xlabel("Test Case")
    ax.set_xticks(x + width * (len(configurations) - 1) / 2)
    ax.set_xticklabels([case.replace("_", " ").title() for case in test_cases])
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig("results/combined_optimizations.png", dpi=300, bbox_inches="tight")
    print("Saved: results/combined_optimizations.png")


def create_summary_report():
    """Create a summary report with key findings"""
    report = []
    report.append("# Polars-Bio Benchmark Results Summary\\n")

    # General Performance
    df_gen = load_and_aggregate_results("results/general_performance.csv")
    if df_gen is not None:
        report.append("## General Performance")
        report.append("| Library | Total Time (s) | Read Time (s) | Filter Time (s) |")
        report.append("|---------|----------------|---------------|-----------------|")
        for _, row in df_gen.iterrows():
            report.append(
                f"| {row['library']} | {row['total_time_avg']:.3f} | {row['read_time_avg']:.3f} | {row['filter_time_avg']:.3f} |"
            )
        report.append("")

    # Memory Usage
    df_mem = load_and_aggregate_results("results/memory_profiling.csv")
    if df_mem is not None:
        report.append("## Memory Usage")
        report.append("| Library | Peak Memory (MB) |")
        report.append("|---------|------------------|")
        for _, row in df_mem.iterrows():
            report.append(f"| {row['library']} | {row['max_memory_mb']:.0f} |")
        report.append("")

    # Combined Optimizations Speedup
    df_combined = load_and_aggregate_results("results/combined_optimizations.csv")
    if df_combined is not None:
        baseline_data = df_combined[
            df_combined["configuration"] == "polars-bio-baseline"
        ]
        optimized_data = df_combined[
            df_combined["configuration"] == "polars-bio-optimized"
        ]

        if len(baseline_data) > 0 and len(optimized_data) > 0:
            report.append("## Optimization Speedups")
            report.append(
                "| Test Case | Baseline Time (s) | Optimized Time (s) | Speedup |"
            )
            report.append(
                "|-----------|-------------------|-------------------|---------|"
            )

            for test_case in baseline_data["test_case"].unique():
                baseline_time = baseline_data[baseline_data["test_case"] == test_case][
                    "time_avg"
                ].iloc[0]
                optimized_time = optimized_data[
                    optimized_data["test_case"] == test_case
                ]["time_avg"].iloc[0]
                speedup = baseline_time / optimized_time
                report.append(
                    f"| {test_case.replace('_', ' ').title()} | {baseline_time:.3f} | {optimized_time:.3f} | {speedup:.1f}x |"
                )
            report.append("")

    with open("results/summary_report.md", "w") as f:
        f.write("\\n".join(report))

    print("Saved: results/summary_report.md")


def main():
    """Generate all visualizations"""
    Path("results").mkdir(exist_ok=True)

    print("Generating visualizations...")

    try:
        plot_general_performance()
    except Exception as e:
        print(f"Error plotting general performance: {e}")

    try:
        plot_memory_comparison()
    except Exception as e:
        print(f"Error plotting memory comparison: {e}")

    try:
        plot_thread_scalability()
    except Exception as e:
        print(f"Error plotting thread scalability: {e}")

    try:
        plot_projection_pruning()
    except Exception as e:
        print(f"Error plotting projection pruning: {e}")

    try:
        plot_predicate_pushdown()
    except Exception as e:
        print(f"Error plotting predicate pushdown: {e}")

    try:
        plot_combined_optimizations()
    except Exception as e:
        print(f"Error plotting combined optimizations: {e}")

    try:
        create_summary_report()
    except Exception as e:
        print(f"Error creating summary report: {e}")

    print("\\nVisualization complete! Check the results/ directory for output files.")


if __name__ == "__main__":
    main()
