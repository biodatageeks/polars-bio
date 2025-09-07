#!/usr/bin/env python3
"""
Benchmark script for projection pruning performance comparison.
Tests the advantage of selecting only needed columns vs reading all columns.
"""

import csv
import os
import time
from pathlib import Path
from typing import List, Union

import polars as pl
import psutil
from gff_parsers import polars_scan_gff

import polars_bio as pb

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"


def benchmark_polars_projection(columns: List[str], label: str):
    """Benchmark vanilla Polars with specified column projection"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = memory_before

    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)

    if columns:
        result = lf.select(columns).collect()
    else:
        result = lf.collect()

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(peak_memory, memory_after)

    total_time = time.time() - start_time
    return (
        total_time,
        len(result),
        len(result.columns) if hasattr(result, "columns") else 0,
        peak_memory,
    )


def benchmark_polars_bio_projection(
    columns: List[str], label: str, projection_pushdown: bool = False
):
    """Benchmark polars-bio with specified column projection"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = memory_before

    start_time = time.time()

    if projection_pushdown:
        lf = pb.scan_gff(GFF_FILE, projection_pushdown=True)
    else:
        lf = pb.scan_gff(GFF_FILE)

    if columns:
        # Map seqid to chrom for polars-bio
        bio_columns = [col if col != "seqid" else "chrom" for col in columns]
        result = lf.select(bio_columns).collect()
    else:
        result = lf.collect()

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(peak_memory, memory_after)

    total_time = time.time() - start_time
    return (
        total_time,
        len(result),
        len(result.columns) if hasattr(result, "columns") else 0,
        peak_memory,
    )


def main():
    """Run projection pruning benchmarks and save results"""
    results = []

    # Set single thread for fair comparison
    os.environ["POLARS_MAX_THREADS"] = "1"
    pb.set_option("datafusion.execution.target_partitions", "1")

    print("Running projection pruning benchmarks...")

    # Test scenarios: all columns vs minimal columns vs medium selection
    test_cases = [
        (None, "all_columns"),
        (["seqid", "start", "end", "type"], "minimal_columns"),
        (["seqid", "source", "type", "start", "end", "strand"], "medium_columns"),
        (["seqid", "start", "end"], "three_columns"),
    ]

    for columns, label in test_cases:
        print(f"\\nTesting {label} projection:")

        # Benchmark vanilla Polars
        print(f"  Benchmarking Polars ({label})...")
        for run in range(5):
            try:
                total_time, row_count, col_count, peak_memory = (
                    benchmark_polars_projection(columns, label)
                )
                results.append(
                    {
                        "library": "polars",
                        "projection_type": label,
                        "projection_pushdown": False,
                        "run": run + 1,
                        "total_time": total_time,
                        "row_count": row_count,
                        "column_count": col_count,
                        "peak_memory_mb": peak_memory,
                        "threads": 1,
                    }
                )
                print(
                    f"    Run {run+1}: {total_time:.3f}s ({row_count} rows, {col_count} cols, {peak_memory:.1f}MB)"
                )
            except Exception as e:
                print(f"    Error in run {run+1}: {e}")

        # Benchmark polars-bio without projection pushdown
        print(f"  Benchmarking polars-bio ({label}, no pushdown)...")
        for run in range(5):
            try:
                total_time, row_count, col_count, peak_memory = (
                    benchmark_polars_bio_projection(columns, label, False)
                )
                results.append(
                    {
                        "library": "polars-bio",
                        "projection_type": label,
                        "projection_pushdown": False,
                        "run": run + 1,
                        "total_time": total_time,
                        "row_count": row_count,
                        "column_count": col_count,
                        "peak_memory_mb": peak_memory,
                        "threads": 1,
                    }
                )
                print(
                    f"    Run {run+1}: {total_time:.3f}s ({row_count} rows, {col_count} cols, {peak_memory:.1f}MB)"
                )
            except Exception as e:
                print(f"    Error in run {run+1}: {e}")

        # Benchmark polars-bio with projection pushdown
        print(f"  Benchmarking polars-bio ({label}, with pushdown)...")
        for run in range(5):
            try:
                total_time, row_count, col_count, peak_memory = (
                    benchmark_polars_bio_projection(columns, label, True)
                )
                results.append(
                    {
                        "library": "polars-bio",
                        "projection_type": label,
                        "projection_pushdown": True,
                        "run": run + 1,
                        "total_time": total_time,
                        "row_count": row_count,
                        "column_count": col_count,
                        "peak_memory_mb": peak_memory,
                        "threads": 1,
                    }
                )
                print(
                    f"    Run {run+1}: {total_time:.3f}s ({row_count} rows, {col_count} cols, {peak_memory:.1f}MB)"
                )
            except Exception as e:
                print(f"    Error in run {run+1}: {e}")

    # Save results
    Path("results").mkdir(exist_ok=True)
    with open("results/projection_pruning.csv", "w", newline="") as f:
        fieldnames = [
            "library",
            "projection_type",
            "projection_pushdown",
            "run",
            "total_time",
            "row_count",
            "column_count",
            "peak_memory_mb",
            "threads",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\\nResults saved to results/projection_pruning.csv")

    # Calculate and print average times by projection type
    print("\\nAverage times by projection type:")
    print("Library\\t\\t\\tProjection\\t\\tPushdown\\tTime")
    print("-" * 65)

    for library in ["polars", "polars-bio"]:
        for _, proj_type in test_cases:
            if library == "polars":
                # Polars only has one configuration
                lib_results = [
                    r
                    for r in results
                    if r["library"] == library and r["projection_type"] == proj_type
                ]
                if lib_results:
                    avg_time = sum(r["total_time"] for r in lib_results) / len(
                        lib_results
                    )
                    print(
                        f"{library}\\t\\t\\t{proj_type}\\t\\tN/A\\t\\t{avg_time:.3f}s"
                    )
            else:
                # polars-bio has two configurations
                for pushdown in [False, True]:
                    lib_results = [
                        r
                        for r in results
                        if r["library"] == library
                        and r["projection_type"] == proj_type
                        and r["projection_pushdown"] == pushdown
                    ]
                    if lib_results:
                        avg_time = sum(r["total_time"] for r in lib_results) / len(
                            lib_results
                        )
                        pushdown_str = "Yes" if pushdown else "No"
                        print(
                            f"{library}\\t\\t{proj_type}\\t\\t{pushdown_str}\\t\\t{avg_time:.3f}s"
                        )


if __name__ == "__main__":
    main()
