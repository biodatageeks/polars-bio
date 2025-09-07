#!/usr/bin/env python3
"""
Benchmark script for combined optimizations performance comparison.
Tests single thread baseline vs all optimizations with 16 threads for maximum performance.
"""

import csv
import os
import time
from pathlib import Path
from typing import Union

import polars as pl
from gff_parsers import polars_scan_gff

import polars_bio as pb

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"


def benchmark_baseline_single_thread():
    """Benchmark baseline performance: single thread, no optimizations"""
    os.environ["POLARS_MAX_THREADS"] = "1"
    pb.set_option("datafusion.execution.target_partitions", "1")

    # Test case 1: Simple filtering and projection
    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE)
    result1 = (
        lf.filter(
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["chrom", "start", "end", "type"])
        .collect()
    )
    test1_time = time.time() - start_time

    # Test case 2: Complex filtering and projection
    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE)
    result2 = (
        lf.filter(
            (pl.col("chrom").is_in(["chr1", "chr2", "chrX"]))
            & (pl.col("start") < 1000000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["chrom", "start", "end", "type", "strand"])
        .collect()
    )
    test2_time = time.time() - start_time

    # Test case 3: Full scan with projection
    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE)
    result3 = lf.select(["chrom", "start", "end", "type"]).collect()
    test3_time = time.time() - start_time

    return {
        "simple_filter_time": test1_time,
        "simple_filter_results": len(result1),
        "complex_filter_time": test2_time,
        "complex_filter_results": len(result2),
        "full_scan_time": test3_time,
        "full_scan_results": len(result3),
    }


def benchmark_polars_single_thread():
    """Benchmark vanilla Polars: single thread"""
    os.environ["POLARS_MAX_THREADS"] = "1"

    # Test case 1: Simple filtering and projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result1 = (
        lf.filter(
            (pl.col("seqid") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["seqid", "start", "end", "type"])
        .collect()
    )
    test1_time = time.time() - start_time

    # Test case 2: Complex filtering and projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result2 = (
        lf.filter(
            (pl.col("seqid").is_in(["chr1", "chr2", "chrX"]))
            & (pl.col("start") < 1000000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["seqid", "start", "end", "type", "strand"])
        .collect()
    )
    test2_time = time.time() - start_time

    # Test case 3: Full scan with projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result3 = lf.select(["seqid", "start", "end", "type"]).collect()
    test3_time = time.time() - start_time

    return {
        "simple_filter_time": test1_time,
        "simple_filter_results": len(result1),
        "complex_filter_time": test2_time,
        "complex_filter_results": len(result2),
        "full_scan_time": test3_time,
        "full_scan_results": len(result3),
    }


def benchmark_optimized_multi_thread():
    """Benchmark optimized performance: 16 threads, all optimizations enabled"""
    # Reset to default threads (16) or set explicitly
    if "POLARS_MAX_THREADS" in os.environ:
        del os.environ["POLARS_MAX_THREADS"]
    pb.set_option("datafusion.execution.target_partitions", "16")

    # Test case 1: Simple filtering and projection with all optimizations
    start_time = time.time()
    lf = pb.scan_gff(
        GFF_FILE, parallel=True, projection_pushdown=True, predicate_pushdown=True
    )
    result1 = (
        lf.filter(
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["chrom", "start", "end", "type"])
        .collect()
    )
    test1_time = time.time() - start_time

    # Test case 2: Complex filtering and projection with all optimizations
    start_time = time.time()
    lf = pb.scan_gff(
        GFF_FILE, parallel=True, projection_pushdown=True, predicate_pushdown=True
    )
    result2 = (
        lf.filter(
            (pl.col("chrom").is_in(["chr1", "chr2", "chrX"]))
            & (pl.col("start") < 1000000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["chrom", "start", "end", "type", "strand"])
        .collect()
    )
    test2_time = time.time() - start_time

    # Test case 3: Full scan with projection and parallelism
    start_time = time.time()
    lf = pb.scan_gff(GFF_FILE, parallel=True, projection_pushdown=True)
    result3 = lf.select(["chrom", "start", "end", "type"]).collect()
    test3_time = time.time() - start_time

    return {
        "simple_filter_time": test1_time,
        "simple_filter_results": len(result1),
        "complex_filter_time": test2_time,
        "complex_filter_results": len(result2),
        "full_scan_time": test3_time,
        "full_scan_results": len(result3),
    }


def benchmark_polars_multi_thread():
    """Benchmark vanilla Polars: 16 threads"""
    # Reset to default threads (16)
    if "POLARS_MAX_THREADS" in os.environ:
        del os.environ["POLARS_MAX_THREADS"]

    # Test case 1: Simple filtering and projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result1 = (
        lf.filter(
            (pl.col("seqid") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["seqid", "start", "end", "type"])
        .collect()
    )
    test1_time = time.time() - start_time

    # Test case 2: Complex filtering and projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result2 = (
        lf.filter(
            (pl.col("seqid").is_in(["chr1", "chr2", "chrX"]))
            & (pl.col("start") < 1000000)
            & (pl.col("type").is_in(["gene", "exon"]))
        )
        .select(["seqid", "start", "end", "type", "strand"])
        .collect()
    )
    test2_time = time.time() - start_time

    # Test case 3: Full scan with projection
    start_time = time.time()
    lf = polars_scan_gff(GFF_FILE)
    result3 = lf.select(["seqid", "start", "end", "type"]).collect()
    test3_time = time.time() - start_time

    return {
        "simple_filter_time": test1_time,
        "simple_filter_results": len(result1),
        "complex_filter_time": test2_time,
        "complex_filter_results": len(result2),
        "full_scan_time": test3_time,
        "full_scan_results": len(result3),
    }


def main():
    """Run combined optimizations benchmarks and save results"""
    results = []

    print("Running combined optimizations benchmarks...")

    configurations = [
        (
            "polars-bio-baseline",
            "1 thread, no optimizations",
            benchmark_baseline_single_thread,
        ),
        ("polars-baseline", "1 thread", benchmark_polars_single_thread),
        (
            "polars-bio-optimized",
            "16 threads, all optimizations",
            benchmark_optimized_multi_thread,
        ),
        ("polars-multi", "16 threads", benchmark_polars_multi_thread),
    ]

    for config_name, config_desc, benchmark_func in configurations:
        print(f"\\nBenchmarking {config_name} ({config_desc})...")

        for run in range(5):
            try:
                benchmark_results = benchmark_func()

                # Add results for each test case
                for test_case in ["simple_filter", "complex_filter", "full_scan"]:
                    results.append(
                        {
                            "configuration": config_name,
                            "test_case": test_case,
                            "run": run + 1,
                            "time": benchmark_results[f"{test_case}_time"],
                            "result_count": benchmark_results[f"{test_case}_results"],
                            "threads": (
                                16
                                if "multi" in config_name or "optimized" in config_name
                                else 1
                            ),
                            "optimizations": (
                                "all" if "optimized" in config_name else "none"
                            ),
                        }
                    )

                print(f"  Run {run+1}:")
                print(
                    f"    Simple filter: {benchmark_results['simple_filter_time']:.3f}s ({benchmark_results['simple_filter_results']} results)"
                )
                print(
                    f"    Complex filter: {benchmark_results['complex_filter_time']:.3f}s ({benchmark_results['complex_filter_results']} results)"
                )
                print(
                    f"    Full scan: {benchmark_results['full_scan_time']:.3f}s ({benchmark_results['full_scan_results']} results)"
                )

            except Exception as e:
                print(f"  Error in run {run+1}: {e}")

    # Save results
    Path("results").mkdir(exist_ok=True)
    with open("results/combined_optimizations.csv", "w", newline="") as f:
        fieldnames = [
            "configuration",
            "test_case",
            "run",
            "time",
            "result_count",
            "threads",
            "optimizations",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("\\nResults saved to results/combined_optimizations.csv")

    # Calculate and print average speedups
    print("\\nAverage times by configuration:")
    print("Configuration\\t\\t\\tTest Case\\t\\tTime\\t\\tResults")
    print("-" * 75)

    for config_name, _, _ in configurations:
        for test_case in ["simple_filter", "complex_filter", "full_scan"]:
            config_results = [
                r
                for r in results
                if r["configuration"] == config_name and r["test_case"] == test_case
            ]
            if config_results:
                avg_time = sum(r["time"] for r in config_results) / len(config_results)
                avg_count = sum(r["result_count"] for r in config_results) / len(
                    config_results
                )
                print(
                    f"{config_name}\\t\\t{test_case}\\t\\t{avg_time:.3f}s\\t\\t{avg_count:.0f}"
                )

    # Calculate speedups relative to baseline
    print("\\nSpeedups relative to polars-bio-baseline:")
    baseline_times = {}

    # Get baseline times
    for test_case in ["simple_filter", "complex_filter", "full_scan"]:
        baseline_results = [
            r
            for r in results
            if r["configuration"] == "polars-bio-baseline"
            and r["test_case"] == test_case
        ]
        if baseline_results:
            baseline_times[test_case] = sum(r["time"] for r in baseline_results) / len(
                baseline_results
            )

    for config_name, _, _ in configurations:
        if config_name != "polars-bio-baseline":
            print(f"\\n{config_name}:")
            for test_case in ["simple_filter", "complex_filter", "full_scan"]:
                config_results = [
                    r
                    for r in results
                    if r["configuration"] == config_name and r["test_case"] == test_case
                ]
                if config_results and test_case in baseline_times:
                    avg_time = sum(r["time"] for r in config_results) / len(
                        config_results
                    )
                    speedup = baseline_times[test_case] / avg_time
                    print(f"  {test_case}: {speedup:.2f}x speedup")


if __name__ == "__main__":
    main()
