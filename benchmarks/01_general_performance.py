#!/usr/bin/env python3
"""
Benchmark script for general performance comparison between Pandas, Polars, and polars-bio.
Tests separate read-only and read+filter operations on compressed GFF files.

Features:
- Includes GFF attributes parsing for fair comparison (all libraries use List[Struct{key, value}])
- Configurable number of runs per benchmark (NUM_RUNS variable)
- Single-threaded execution for fair comparison

Split into two benchmark types:
1. Reading only (no filtering) - measures raw I/O performance (single config per library)
2. Reading with filtering applied - measures combined I/O + query performance

Notes:
- Polars has projection/predicate pushdown optimizations enabled by default
- polars-bio explicitly enables both optimizations for best performance comparison
- pandas doesn't have equivalent optimization concepts (eager evaluation)
- Detailed optimization testing is available in separate dedicated benchmark scripts
"""

import csv
import os
import time
from pathlib import Path
from typing import Union

import polars as pl
from gff_parsers import pandas_read_gff, polars_scan_gff

import polars_bio as pb

# Configuration
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"
NUM_RUNS = 3  # Number of runs per benchmark configuration


def benchmark_pandas_read_only():
    """Benchmark pandas reading only (no filtering)"""
    start_time = time.time()
    df = pandas_read_gff(GFF_FILE)
    read_time = time.time() - start_time
    return read_time, len(df)


def benchmark_pandas_read_with_filter():
    """Benchmark pandas reading with filtering applied"""
    start_time = time.time()
    df = pandas_read_gff(GFF_FILE)
    filtered = df[
        (df["seqid"] == "chrY") & (df["start"] < 500000) & (df["end"] > 510000)
    ]
    result = filtered[["seqid", "start", "end", "type"]]
    total_time = time.time() - start_time
    return total_time, len(result)


def benchmark_polars_read_only():
    """Benchmark vanilla Polars reading only (no filtering)"""
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result = lf.collect()
    read_time = time.time() - start_time
    return read_time, len(result)


def benchmark_polars_read_with_filter():
    """Benchmark vanilla Polars reading with filtering applied"""
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result = (
        lf.filter(
            (pl.col("seqid") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["seqid", "start", "end", "type"])
        .collect()
    )
    total_time = time.time() - start_time
    return total_time, len(result)


def benchmark_polars_bio_read_only(
    projection_pushdown: bool = False, predicate_pushdown: bool = False
):
    """Benchmark polars-bio reading only (no filtering)"""
    pb.set_option("datafusion.execution.target_partitions", "1")
    start_time = time.time()
    lf = pb.scan_gff(
        GFF_FILE,
        projection_pushdown=projection_pushdown,
        predicate_pushdown=predicate_pushdown,
    )
    result = lf.collect()
    read_time = time.time() - start_time
    return read_time, len(result)


def benchmark_polars_bio_read_with_filter(
    projection_pushdown: bool = False, predicate_pushdown: bool = False
):
    """Benchmark polars-bio reading with filtering applied"""
    os.environ["POLARS_MAX_THREADS"] = "1"
    pb.set_option("datafusion.execution.target_partitions", "1")
    start_time = time.time()
    # Use select().filter() order (not filter().select()) to avoid optimization bug
    lf = pb.scan_gff(
        GFF_FILE,
        projection_pushdown=projection_pushdown,
        predicate_pushdown=predicate_pushdown,
    )
    result = (
        lf.select(["chrom", "start", "end", "type"])
        .filter(
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .collect()
    )
    total_time = time.time() - start_time
    return total_time, len(result)


def main():
    """Run benchmarks and save results"""
    results = []

    # Set single thread for fair comparison
    os.environ["POLARS_MAX_THREADS"] = "1"
    pb.set_option("datafusion.execution.target_partitions", "1")

    print("Running general performance benchmarks...")

    # Test cases: read only and read with filter
    test_cases = [
        ("read_only", "Reading only (no filtering)"),
        ("read_with_filter", "Reading with filtering applied"),
    ]

    for test_type, description in test_cases:
        print(f"\n=== {description} ===")

        if test_type == "read_only":
            # Benchmark pandas read only
            print("Benchmarking Pandas (read only)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_pandas_read_only()
                results.append(
                    {
                        "library": "pandas",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} rows)")

            # Benchmark vanilla Polars read only
            print("Benchmarking Polars (read only)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_polars_read_only()
                results.append(
                    {
                        "library": "polars",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} rows)")

            # Benchmark polars-bio read only (single configuration - optimizations don't apply)
            print("Benchmarking polars-bio (read only, no optimizations needed)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_polars_bio_read_only(False, False)
                results.append(
                    {
                        "library": "polars-bio",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} rows)")

        else:  # read_with_filter
            # Benchmark pandas read with filter
            print("Benchmarking Pandas (read with filter)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_pandas_read_with_filter()
                results.append(
                    {
                        "library": "pandas",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} filtered rows)")

            # Benchmark vanilla Polars read with filter
            print("Benchmarking Polars (read with filter)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_polars_read_with_filter()
                results.append(
                    {
                        "library": "polars",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} filtered rows)")

            # Benchmark polars-bio read with filter (both optimizations enabled)
            print("Benchmarking polars-bio (read with filter, both optimizations)...")
            for i in range(NUM_RUNS):
                total_time, result_count = benchmark_polars_bio_read_with_filter(
                    True, True
                )
                results.append(
                    {
                        "library": "polars-bio",
                        "test_type": test_type,
                        "projection_pushdown": True,
                        "predicate_pushdown": True,
                        "run": i + 1,
                        "total_time": total_time,
                        "result_count": result_count,
                        "threads": 1,
                    }
                )
                print(f"  Run {i+1}: {total_time:.3f}s ({result_count} filtered rows)")

    # Save results
    Path("results").mkdir(exist_ok=True)
    with open("results/general_performance.csv", "w", newline="") as f:
        fieldnames = [
            "library",
            "test_type",
            "projection_pushdown",
            "predicate_pushdown",
            "run",
            "total_time",
            "result_count",
            "threads",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\nResults saved to results/general_performance.csv")

    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print("Library\t\t\tTest Type\t\tProj PD\tPred PD\tAvg Time")
    print("-" * 75)

    for test_type, _ in test_cases:
        for library in ["pandas", "polars", "polars-bio"]:
            lib_results = [
                r
                for r in results
                if r["library"] == library and r["test_type"] == test_type
            ]
            if lib_results:
                avg_time = sum(r["total_time"] for r in lib_results) / len(lib_results)
                if library == "pandas":
                    print(f"{library}\t\t\t{test_type}\t\tN/A\tN/A\t{avg_time:.3f}s")
                elif library == "polars":
                    if test_type == "read_with_filter":
                        print(
                            f"{library}\t\t\t{test_type}\t\tYes\tYes\t{avg_time:.3f}s (default)"
                        )
                    else:
                        print(
                            f"{library}\t\t\t{test_type}\t\tN/A\tN/A\t{avg_time:.3f}s"
                        )
                else:  # polars-bio
                    if test_type == "read_with_filter":
                        print(f"{library}\t\t{test_type}\t\tYes\tYes\t{avg_time:.3f}s")
                    else:
                        print(
                            f"{library}\t\t\t{test_type}\t\tN/A\tN/A\t{avg_time:.3f}s"
                        )


if __name__ == "__main__":
    main()
