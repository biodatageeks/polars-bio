#!/usr/bin/env python3
"""
Thread scalability benchmark for Polars and polars-bio.
Tests performance scaling with 1, 2, 4, 6, 8, and 16 threads.

Split into two benchmark types:
1. Reading only (no filtering) - measures raw I/O thread scaling
2. Reading with filtering applied - measures combined I/O + query thread scaling

Note: Polars reads thread pool settings on first initialization. To ensure
POLARS_MAX_THREADS is honored for each thread count, each configuration runs
in a fresh subprocess (like 02_memory_profiling).

For polars-bio, both projection and predicate pushdown are enabled to test
best-case parallel performance.
"""

import csv
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"
BENCH_DIR = Path(__file__).resolve().parent


def _create_polars_script(test_type: str, threads: int) -> str:
    """Create a temp script to run a polars workload in a fresh process."""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = polars_scan_gff("{GFF_FILE}")
    t0 = time.perf_counter()
    result = lf.collect()
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""
    else:
        main_content = f"""
if __name__ == "__main__":
    lf = polars_scan_gff("{GFF_FILE}")
    t0 = time.perf_counter()
    result = (
        lf.filter(
            (pl.col("seqid") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["seqid", "start", "end", "type"])
        .collect()
    )
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""

    script_content = f"""
import os
os.environ['POLARS_MAX_THREADS'] = "{threads}"
import sys
sys.path.insert(0, r"{BENCH_DIR}")
from gff_parsers import polars_scan_gff
import polars as pl
import time

{main_content}
"""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False)
    f.write(script_content)
    f.close()
    return f.name


def _create_polars_bio_script(test_type: str, threads: int) -> str:
    """Create a temp script to run a polars-bio workload in a fresh process."""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = pb.scan_gff(
        "{GFF_FILE}", projection_pushdown=True, predicate_pushdown=True, parallel=True
    )
    t0 = time.perf_counter()
    result = lf.collect()
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""
    else:
        main_content = f"""
if __name__ == "__main__":
    lf = pb.scan_gff(
        "{GFF_FILE}", projection_pushdown=True, predicate_pushdown=True, parallel=True
    )
    t0 = time.perf_counter()
    result = (
        lf.select(["chrom", "start", "end", "type"])
          .filter(
            (pl.col("chrom") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
          )
          .collect()
    )
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""

    script_content = f"""
import os
os.environ['POLARS_MAX_THREADS'] = "{threads}"
import polars_bio as pb
pb.set_option("datafusion.execution.target_partitions", "{threads}")
import polars as pl
import time

{main_content}
"""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False)
    f.write(script_content)
    f.close()
    return f.name


def _create_polars_streaming_script(test_type: str, threads: int) -> str:
    """Create a temp script to run a polars (streaming CSV) workload in a fresh process."""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = polars_streaming_scan_gff("{GFF_FILE}")
    t0 = time.perf_counter()
    result = lf.collect()
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""
    else:
        main_content = f"""
if __name__ == "__main__":
    lf = polars_streaming_scan_gff("{GFF_FILE}")
    t0 = time.perf_counter()
    result = (
        lf.filter(
            (pl.col("seqid") == "chrY")
            & (pl.col("start") < 500000)
            & (pl.col("end") > 510000)
        )
        .select(["seqid", "start", "end", "type"])
        .collect()
    )
    dt = time.perf_counter() - t0
    print(f"TIME:{{dt:.6f}}")
    print(f"COUNT:{{len(result)}}")
"""

    script_content = f"""
import os
os.environ['POLARS_MAX_THREADS'] = "{threads}"
import polars as pl
import polars_streaming_csv_decompression as pscd
import time

def polars_streaming_scan_gff(path: str) -> pl.LazyFrame:
    schema = pl.Schema([
        ("seqid", pl.String),
        ("source", pl.String),
        ("type", pl.String),
        ("start", pl.UInt32),
        ("end", pl.UInt32),
        ("score", pl.Float32),
        ("strand", pl.String),
        ("phase", pl.UInt32),
        ("attributes", pl.String),
    ])

    reader = pscd.streaming_csv(
        path,
        has_header=False,
        separator="\t",
        comment_prefix="#",
        schema=schema,
        null_values=["."],
    )

    reader = reader.with_columns(
        pl.col("attributes")
        .str.split(";")
        .list.eval(
            pl.element()
            .str.split("=")
            .list.to_struct(n_field_strategy="max_width", fields=["key", "value"])
        )
        .alias("attributes")
    )
    return reader

{main_content}
"""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False)
    f.write(script_content)
    f.close()
    return f.name


def _run_script(script_path: str) -> tuple[float, int]:
    """Run script with current interpreter; parse in-script time and count."""
    try:
        proc = subprocess.run(
            [sys.executable, script_path], capture_output=True, text=True, check=False
        )
        stdout = proc.stdout or ""
        m_count = re.search(r"COUNT:(\d+)", stdout)
        m_time = re.search(r"TIME:([0-9eE+\-.]+)", stdout)
        count = int(m_count.group(1)) if m_count else 0
        elapsed = float(m_time.group(1)) if m_time else 0.0
        return elapsed, count
    finally:
        try:
            Path(script_path).unlink(missing_ok=True)
        except Exception:
            pass


def benchmark_polars_read_only(threads: int):
    script = _create_polars_script("read_only", threads)
    return _run_script(script)


def benchmark_polars_read_with_filter(threads: int):
    script = _create_polars_script("read_with_filter", threads)
    return _run_script(script)


def benchmark_polars_bio_read_only(threads: int):
    script = _create_polars_bio_script("read_only", threads)
    return _run_script(script)


def benchmark_polars_bio_read_with_filter(threads: int):
    script = _create_polars_bio_script("read_with_filter", threads)
    return _run_script(script)


def benchmark_polars_streaming_read_only(threads: int):
    script = _create_polars_streaming_script("read_only", threads)
    return _run_script(script)


def benchmark_polars_streaming_read_with_filter(threads: int):
    script = _create_polars_streaming_script("read_with_filter", threads)
    return _run_script(script)


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

                # Benchmark Polars (streaming CSV) read only
                print(
                    f"  Benchmarking Polars (streaming) read only ({threads} threads)..."
                )
                for run in range(3):
                    try:
                        total_time, result_count = benchmark_polars_streaming_read_only(
                            threads
                        )
                        results.append(
                            {
                                "library": "polars-streaming",
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

                # Benchmark Polars (streaming CSV) read with filter
                print(
                    f"  Benchmarking Polars (streaming) read with filter ({threads} threads)..."
                )
                for run in range(3):
                    try:
                        total_time, result_count = (
                            benchmark_polars_streaming_read_with_filter(threads)
                        )
                        results.append(
                            {
                                "library": "polars-streaming",
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
        for library in ["polars", "polars-streaming", "polars-bio"]:
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
