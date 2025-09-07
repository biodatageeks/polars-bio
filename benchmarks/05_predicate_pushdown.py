#!/usr/bin/env python3
"""
Benchmark script for predicate pushdown performance comparison.
Tests the advantage of pushing filters down to the file reader vs applying filters after reading.
"""

import csv
import os
import time
from pathlib import Path
from typing import Union

import polars as pl
import psutil
from gff_parsers import polars_scan_gff

import polars_bio as pb

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"


def benchmark_polars_filtering():
    """Benchmark vanilla Polars filtering (no predicate pushdown available for CSV)"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB

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

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(memory_before, memory_after)

    return total_time, len(result), peak_memory


def benchmark_polars_bio_filtering(predicate_pushdown: bool = False):
    """Benchmark polars-bio filtering with or without predicate pushdown"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB

    start_time = time.time()

    if predicate_pushdown:
        lf = pb.scan_gff(GFF_FILE, predicate_pushdown=True)
    else:
        lf = pb.scan_gff(GFF_FILE)

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
    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(memory_before, memory_after)

    return total_time, len(result), peak_memory


def benchmark_polars_bio_complex_filtering(predicate_pushdown: bool = False):
    """Benchmark polars-bio with more complex filtering"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB

    start_time = time.time()

    if predicate_pushdown:
        lf = pb.scan_gff(GFF_FILE, predicate_pushdown=True)
    else:
        lf = pb.scan_gff(GFF_FILE)

    # More complex filter: multiple chromosomes and conditions
    result = (
        lf.filter(
            (pl.col("chrom").is_in(["chrY", "chrX", "chr1"]))
            & (pl.col("start") < 1000000)
            & (pl.col("end") > 900000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["chrom", "start", "end", "type", "strand"])
        .collect()
    )

    total_time = time.time() - start_time
    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(memory_before, memory_after)

    return total_time, len(result), peak_memory


def benchmark_polars_complex_filtering():
    """Benchmark vanilla Polars with complex filtering"""
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024  # MB

    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result = (
        lf.filter(
            (pl.col("seqid").is_in(["chrY", "chrX", "chr1"]))
            & (pl.col("start") < 1000000)
            & (pl.col("end") > 900000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["seqid", "start", "end", "type", "strand"])
        .collect()
    )
    total_time = time.time() - start_time

    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    peak_memory = max(memory_before, memory_after)

    return total_time, len(result), peak_memory


def main():
    """Run predicate pushdown benchmarks and save results"""
    results = []

    # Set single thread for fair comparison
    os.environ["POLARS_MAX_THREADS"] = "1"
    pb.set_option("datafusion.execution.target_partitions", "1")

    print("Running predicate pushdown benchmarks...")

    # Test simple filtering
    print("\\nTesting simple filtering (chrY region):")

    # Benchmark vanilla Polars
    print("  Benchmarking Polars (simple filter)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = benchmark_polars_filtering()
            results.append(
                {
                    "library": "polars",
                    "filter_type": "simple",
                    "predicate_pushdown": False,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Benchmark polars-bio without predicate pushdown
    print("  Benchmarking polars-bio (simple filter, no pushdown)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = benchmark_polars_bio_filtering(
                False
            )
            results.append(
                {
                    "library": "polars-bio",
                    "filter_type": "simple",
                    "predicate_pushdown": False,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Benchmark polars-bio with predicate pushdown
    print("  Benchmarking polars-bio (simple filter, with pushdown)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = benchmark_polars_bio_filtering(True)
            results.append(
                {
                    "library": "polars-bio",
                    "filter_type": "simple",
                    "predicate_pushdown": True,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Test complex filtering
    print("\\nTesting complex filtering (multiple chromosomes, types):")

    # Benchmark vanilla Polars
    print("  Benchmarking Polars (complex filter)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = benchmark_polars_complex_filtering()
            results.append(
                {
                    "library": "polars",
                    "filter_type": "complex",
                    "predicate_pushdown": False,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Benchmark polars-bio without predicate pushdown
    print("  Benchmarking polars-bio (complex filter, no pushdown)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = (
                benchmark_polars_bio_complex_filtering(False)
            )
            results.append(
                {
                    "library": "polars-bio",
                    "filter_type": "complex",
                    "predicate_pushdown": False,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Benchmark polars-bio with predicate pushdown
    print("  Benchmarking polars-bio (complex filter, with pushdown)...")
    for run in range(5):
        try:
            total_time, result_count, peak_memory = (
                benchmark_polars_bio_complex_filtering(True)
            )
            results.append(
                {
                    "library": "polars-bio",
                    "filter_type": "complex",
                    "predicate_pushdown": True,
                    "run": run + 1,
                    "total_time": total_time,
                    "result_count": result_count,
                    "peak_memory_mb": peak_memory,
                    "threads": 1,
                }
            )
            print(
                f"    Run {run+1}: {total_time:.3f}s ({result_count} results, {peak_memory:.1f}MB)"
            )
        except Exception as e:
            print(f"    Error in run {run+1}: {e}")

    # Save results
    Path("results").mkdir(exist_ok=True)
    with open("results/predicate_pushdown.csv", "w", newline="") as f:
        fieldnames = [
            "library",
            "filter_type",
            "predicate_pushdown",
            "run",
            "total_time",
            "result_count",
            "peak_memory_mb",
            "threads",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\\nResults saved to results/predicate_pushdown.csv")

    # Calculate and print average times
    print("\\nAverage times by filter type:")
    print("Library\\t\\t\\tFilter\\t\\tPushdown\\tTime\\t\\tResults")
    print("-" * 70)

    for library in ["polars", "polars-bio"]:
        for filter_type in ["simple", "complex"]:
            if library == "polars":
                # Polars only has one configuration
                lib_results = [
                    r
                    for r in results
                    if r["library"] == library and r["filter_type"] == filter_type
                ]
                if lib_results:
                    avg_time = sum(r["total_time"] for r in lib_results) / len(
                        lib_results
                    )
                    avg_count = sum(r["result_count"] for r in lib_results) / len(
                        lib_results
                    )
                    print(
                        f"{library}\\t\\t\\t{filter_type}\\t\\tN/A\\t\\t{avg_time:.3f}s\\t\\t{avg_count:.0f}"
                    )
            else:
                # polars-bio has two configurations
                for pushdown in [False, True]:
                    lib_results = [
                        r
                        for r in results
                        if r["library"] == library
                        and r["filter_type"] == filter_type
                        and r["predicate_pushdown"] == pushdown
                    ]
                    if lib_results:
                        avg_time = sum(r["total_time"] for r in lib_results) / len(
                            lib_results
                        )
                        avg_count = sum(r["result_count"] for r in lib_results) / len(
                            lib_results
                        )
                        pushdown_str = "Yes" if pushdown else "No"
                        print(
                            f"{library}\\t\\t{filter_type}\\t\\t{pushdown_str}\\t\\t{avg_time:.3f}s\\t\\t{avg_count:.0f}"
                        )


if __name__ == "__main__":
    main()
