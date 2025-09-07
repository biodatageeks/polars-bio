#!/usr/bin/env python3
"""
Thread scalability benchmark for Polars and polars-bio.
Tests performance scaling with 1, 2, 4, 6, 8, and 16 threads.

Split into two benchmark types:
1. Reading only (no filtering) - measures raw I/O thread scaling
2. Reading with filtering applied - measures combined I/O + query thread scaling

For polars-bio, uses both projection and predicate pushdown optimizations enabled
to test the best-case parallel performance.
"""

import csv
import os
import time
from pathlib import Path
from typing import Union

import polars as pl

import polars_bio as pb

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"


def polars_scan_gff(path: Union[str, Path]) -> pl.LazyFrame:
    """Read GFF file using vanilla Polars"""
    schema = pl.Schema(
        [
            ("seqid", pl.String),
            ("source", pl.String),
            ("type", pl.String),
            ("start", pl.UInt32),
            ("end", pl.UInt32),
            ("score", pl.Float32),
            ("strand", pl.String),
            ("phase", pl.UInt32),
            ("attributes", pl.String),
        ]
    )

    reader = pl.scan_csv(
        path,
        has_header=False,
        separator="\t",
        comment_prefix="#",
        schema=schema,
        null_values=["."],
    )
    return reader


def benchmark_polars_read_only(threads: int):
    """Benchmark vanilla Polars read only with specified thread count"""
    os.environ["POLARS_MAX_THREADS"] = str(threads)

    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result = lf.collect()
    total_time = time.time() - start_time

    return total_time, len(result)


def benchmark_polars_read_with_filter(threads: int):
    """Benchmark vanilla Polars read with filter with specified thread count"""
    os.environ["POLARS_MAX_THREADS"] = str(threads)

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


def benchmark_polars_bio_read_only(threads: int):
    """Benchmark polars-bio read only with specified thread count (both optimizations enabled)"""
    os.environ["POLARS_MAX_THREADS"] = str(threads)
    pb.set_option("datafusion.execution.target_partitions", str(threads))

    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE, projection_pushdown=True, predicate_pushdown=True)
    result = lf.collect()
    total_time = time.time() - start_time

    return total_time, len(result)


def benchmark_polars_bio_read_with_filter(threads: int):
    """Benchmark polars-bio read with filter with specified thread count (both optimizations enabled)"""
    os.environ["POLARS_MAX_THREADS"] = str(threads)
    pb.set_option("datafusion.execution.target_partitions", str(threads))

    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE, projection_pushdown=True, predicate_pushdown=True)
    result = (
        lf.filter(
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["chrom", "start", "end", "type"])
        .collect()
    )
    total_time = time.time() - start_time

    return total_time, len(result)


def main():
    """Run thread scalability benchmarks and save results"""
    thread_counts = [1, 2, 4, 6, 8, 16]
    results = []

    print("Running thread scalability benchmarks...")

    # Test cases: read only and read with filter
    test_cases = [
        ("read_only", "Reading only (no filtering)"),
        ("read_with_filter", "Reading with filtering applied"),
    ]

    for test_type, description in test_cases:
        print(f"\n=== {description} ===")

        for threads in thread_counts:
            print(f"\nTesting with {threads} threads:")

            if test_type == "read_only":
                # Benchmark vanilla Polars read only
                print(f"  Benchmarking Polars read only ({threads} threads)...")
                for run in range(3):  # 3 runs for statistics
                    try:
                        total_time, result_count = benchmark_polars_read_only(threads)
                        results.append(
                            {
                                "library": "polars",
                                "test_type": test_type,
                                "threads": threads,
                                "run": run + 1,
                                "total_time": total_time,
                                "result_count": result_count,
                            }
                        )
                        print(
                            f"    Run {run+1}: {total_time:.3f}s ({result_count} rows)"
                        )
                    except Exception as e:
                        print(f"    Error in run {run+1}: {e}")

                # Benchmark polars-bio read only
                print(
                    f"  Benchmarking polars-bio read only ({threads} threads, both optimizations)..."
                )
                for run in range(3):
                    try:
                        total_time, result_count = benchmark_polars_bio_read_only(
                            threads
                        )
                        results.append(
                            {
                                "library": "polars-bio",
                                "test_type": test_type,
                                "threads": threads,
                                "run": run + 1,
                                "total_time": total_time,
                                "result_count": result_count,
                            }
                        )
                        print(
                            f"    Run {run+1}: {total_time:.3f}s ({result_count} rows)"
                        )
                    except Exception as e:
                        print(f"    Error in run {run+1}: {e}")

            else:  # read_with_filter
                # Benchmark vanilla Polars read with filter
                print(f"  Benchmarking Polars read with filter ({threads} threads)...")
                for run in range(3):
                    try:
                        total_time, result_count = benchmark_polars_read_with_filter(
                            threads
                        )
                        results.append(
                            {
                                "library": "polars",
                                "test_type": test_type,
                                "threads": threads,
                                "run": run + 1,
                                "total_time": total_time,
                                "result_count": result_count,
                            }
                        )
                        print(
                            f"    Run {run+1}: {total_time:.3f}s ({result_count} filtered rows)"
                        )
                    except Exception as e:
                        print(f"    Error in run {run+1}: {e}")

                # Benchmark polars-bio read with filter
                print(
                    f"  Benchmarking polars-bio read with filter ({threads} threads, both optimizations)..."
                )
                for run in range(3):
                    try:
                        total_time, result_count = (
                            benchmark_polars_bio_read_with_filter(threads)
                        )
                        results.append(
                            {
                                "library": "polars-bio",
                                "test_type": test_type,
                                "threads": threads,
                                "run": run + 1,
                                "total_time": total_time,
                                "result_count": result_count,
                            }
                        )
                        print(
                            f"    Run {run+1}: {total_time:.3f}s ({result_count} filtered rows)"
                        )
                    except Exception as e:
                        print(f"    Error in run {run+1}: {e}")

    # Save results
    Path("results").mkdir(exist_ok=True)
    with open("results/thread_scalability.csv", "w", newline="") as f:
        fieldnames = [
            "library",
            "test_type",
            "threads",
            "run",
            "total_time",
            "result_count",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\nResults saved to results/thread_scalability.csv")

    # Calculate and print average times by thread count
    print("\n=== Thread Scalability Summary ===")
    print("Library\t\t\tTest Type\t\tThreads\tAvg Time\tSpeedup")
    print("-" * 75)

    for test_type, _ in test_cases:
        for library in ["polars", "polars-bio"]:
            baseline_time = None
            for threads in thread_counts:
                library_results = [
                    r
                    for r in results
                    if r["library"] == library
                    and r["test_type"] == test_type
                    and r["threads"] == threads
                ]
                if library_results:
                    avg_time = sum(r["total_time"] for r in library_results) / len(
                        library_results
                    )
                    if threads == 1:
                        baseline_time = avg_time
                        speedup_str = "1.00x"
                    else:
                        speedup = baseline_time / avg_time if baseline_time else 0
                        speedup_str = f"{speedup:.2f}x"
                    print(
                        f"{library}\t\t\t{test_type}\t\t{threads}\t{avg_time:.3f}s\t{speedup_str}"
                    )


if __name__ == "__main__":
    main()
