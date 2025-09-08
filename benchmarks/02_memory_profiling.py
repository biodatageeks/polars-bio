#!/usr/bin/env python3
"""
Memory profiling benchmark for Pandas, Polars, and polars-bio.
Uses memory_profiler to track memory usage during GFF processing.

Split into two benchmark types:
1. Reading only (no filtering) - measures raw I/O memory usage
2. Reading with filtering applied - measures combined I/O + query memory usage

For polars-bio, tests 4 configurations:
- No optimizations
- Projection pushdown only
- Predicate pushdown only
- Both optimizations enabled
"""

import csv
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Union

import pandas as pd
import polars as pl

import polars_bio as pb

# Data file path
GFF_FILE = "/tmp/gencode.v49.annotation.gff3.bgz"
BENCH_DIR = Path(__file__).resolve().parent


def create_pandas_script(test_type: str):
    """Create a temporary script for pandas memory profiling"""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    df = pandas_read_gff("{GFF_FILE}")
    print(f"Result count: {{len(df)}}")
"""
    else:  # read_with_filter
        main_content = f"""
if __name__ == "__main__":
    df = pandas_read_gff("{GFF_FILE}")
    filtered = df[(df['seqid'] == 'chrY') & (df['start'] < 500000) & (df['end'] > 510000)]
    result = filtered[['seqid', 'start', 'end', 'type']]
    print(f"Result count: {{len(result)}}")
"""

    script_content = f"""
import sys
sys.path.insert(0, r"{BENCH_DIR}")
from gff_parsers import pandas_read_gff

{main_content}
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        return f.name


def create_polars_script(test_type: str):
    """Create a temporary script for polars memory profiling"""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = polars_scan_gff("{GFF_FILE}")
    result = lf.collect()
    print(f"Result count: {{len(result)}}")
"""
    else:  # read_with_filter
        main_content = f"""
if __name__ == "__main__":
    lf = polars_scan_gff("{GFF_FILE}")
    result = lf.filter(
        (pl.col("seqid") == "chrY") &
        (pl.col("start") < 500000) &
        (pl.col("end") > 510000)
    ).select(["seqid", "start", "end", "type"]).collect()
    print(f"Result count: {{len(result)}}")
"""

    script_content = f"""
import os
import sys
os.environ['POLARS_MAX_THREADS'] = "1"
sys.path.insert(0, r"{BENCH_DIR}")
from gff_parsers import polars_scan_gff
import polars as pl

{main_content}
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        return f.name


def create_polars_streaming_script(test_type: str):
    """Create a temporary script for polars using streaming CSV decompression"""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = polars_streaming_scan_gff("{GFF_FILE}")
    result = lf.collect()
    print(f"Result count: {{len(result)}}")
"""
    else:  # read_with_filter
        main_content = f"""
if __name__ == "__main__":
    lf = polars_streaming_scan_gff("{GFF_FILE}")
    result = lf.filter(
        (pl.col("seqid") == "chrY") &
        (pl.col("start") < 500000) &
        (pl.col("end") > 510000)
    ).select(["seqid", "start", "end", "type"]).collect()
    print(f"Result count: {{len(result)}}")
"""

    script_content = f"""
import os
import polars as pl
import polars_streaming_csv_decompression as pscd

os.environ['POLARS_MAX_THREADS'] = "1"

def polars_streaming_scan_gff(path):
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
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        return f.name


def create_polars_bio_script(
    test_type: str, projection_pushdown: bool = False, predicate_pushdown: bool = False
):
    """Create a temporary script for polars-bio memory profiling"""
    if test_type == "read_only":
        main_content = f"""
if __name__ == "__main__":
    lf = pb.scan_gff("{GFF_FILE}", projection_pushdown={projection_pushdown}, predicate_pushdown={predicate_pushdown})
    result = lf.collect()
    print(f"Result count: {{len(result)}}")
"""
    else:  # read_with_filter
        main_content = f"""
if __name__ == "__main__":
    lf = pb.scan_gff("{GFF_FILE}", projection_pushdown={projection_pushdown}, predicate_pushdown={predicate_pushdown})
    result = (
        lf.select(["chrom", "start", "end", "type"])
          .filter(
            (pl.col("chrom") == "chrY") &
            (pl.col("start") < 500000) &
            (pl.col("end") > 510000)
          ).collect()
    )
    print(f"Result count: {{len(result)}}")
"""

    script_content = f"""
import os
import polars as pl
import polars_bio as pb

os.environ['POLARS_MAX_THREADS'] = "1"
pb.set_option("datafusion.execution.target_partitions", "1")

{main_content}
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        return f.name


def run_memory_profile(script_path: str, library_name: str):
    """Run memory profiler on a script and parse results.

    Always returns (max_memory_mb, wall_time_s) even if underlying commands fail.
    Prints warnings instead of raising.
    """
    max_memory: float = 0.0
    wall_time: float = 0.0
    latest_mprof = None

    # Try to run with mprof to collect memory profile
    try:
        cmd = ["mprof", "run", "--python", script_path]
        mprof_start = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        mprof_elapsed = time.time() - mprof_start
        # mprof run time isn't returned; keep wall_time for plain run below
        if result.returncode != 0:
            print(
                f"Warning: mprof run failed for {library_name} (exit {result.returncode})."
            )

        mprof_files = list(Path(".").glob("mprofile_*.dat"))
        if mprof_files:
            latest_mprof = max(mprof_files, key=lambda x: x.stat().st_mtime)
            try:
                with open(latest_mprof, "r") as f:
                    for line in f:
                        if line.startswith("MEM"):
                            parts = line.strip().split()
                            if len(parts) >= 2:
                                try:
                                    memory_mb = float(parts[1])
                                    max_memory = max(max_memory, memory_mb)
                                except ValueError:
                                    continue
            except Exception as e:
                print(f"Warning: failed parsing mprof data for {library_name}: {e}")
        else:
            print(f"Warning: no mprofile_*.dat file produced for {library_name}.")
    except FileNotFoundError as e:
        # mprof not available or failed unexpectedly; continue
        print(f"Warning: mprof not available for {library_name}: {e}")
    except Exception as e:
        print(f"Warning: unexpected mprof error for {library_name}: {e}")

    # Run script normally to measure wall clock time (without mprof overhead)
    try:
        import sys as _sys

        start_time = time.time()
        normal = subprocess.run(
            [_sys.executable, script_path], capture_output=True, text=True, check=False
        )
        wall_time = time.time() - start_time
        if normal.returncode != 0:
            print(
                f"Warning: script returned non-zero exit ({normal.returncode}) for {library_name}."
            )
    except Exception as e:
        print(f"Warning: failed to measure wall time for {library_name}: {e}")

    # Cleanup temp mprof file if created
    try:
        if latest_mprof is not None and latest_mprof.exists():
            latest_mprof.unlink()
    except Exception:
        pass

    return max_memory, wall_time


def main():
    """Run memory profiling benchmarks and save results"""
    results = []

    print("Running memory profiling benchmarks...")

    # Check if mprof is available
    try:
        subprocess.run(["mprof", "--help"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("mprof not found. Installing memory_profiler...")
        subprocess.run(["pip", "install", "memory_profiler"], check=True)

    # Test cases: read only and read with filter
    test_cases = [
        ("read_only", "Reading only (no filtering)"),
        ("read_with_filter", "Reading with filtering applied"),
    ]

    for test_type, description in test_cases:
        print(f"\n=== {description} ===")

        # Pandas
        print(f"Profiling Pandas memory usage and wall time ({test_type})...")
        pandas_script = create_pandas_script(test_type)
        try:
            pandas_memory, pandas_time = run_memory_profile(pandas_script, "pandas")
            if pandas_memory is not None and pandas_time is not None:
                results.append(
                    {
                        "library": "pandas",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "max_memory_mb": pandas_memory,
                        "wall_time_s": pandas_time,
                        "threads": 1,
                    }
                )
                print(
                    f"  Max memory: {pandas_memory:.1f} MB, Wall time: {pandas_time:.3f}s"
                )
        finally:
            Path(pandas_script).unlink()

        # Polars
        print(f"Profiling Polars memory usage and wall time ({test_type})...")
        polars_script = create_polars_script(test_type)
        try:
            polars_memory, polars_time = run_memory_profile(polars_script, "polars")
            if polars_memory is not None and polars_time is not None:
                results.append(
                    {
                        "library": "polars",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "max_memory_mb": polars_memory,
                        "wall_time_s": polars_time,
                        "threads": 1,
                    }
                )
                print(
                    f"  Max memory: {polars_memory:.1f} MB, Wall time: {polars_time:.3f}s"
                )
        finally:
            Path(polars_script).unlink()

        # Polars (streaming CSV decompression)
        print(
            f"Profiling Polars (streaming) memory usage and wall time ({test_type})..."
        )
        polars_streaming_script = create_polars_streaming_script(test_type)
        try:
            polars_streaming_memory, polars_streaming_time = run_memory_profile(
                polars_streaming_script, "polars-streaming"
            )
            if (
                polars_streaming_memory is not None
                and polars_streaming_time is not None
            ):
                results.append(
                    {
                        "library": "polars-streaming",
                        "test_type": test_type,
                        "projection_pushdown": False,
                        "predicate_pushdown": False,
                        "max_memory_mb": polars_streaming_memory,
                        "wall_time_s": polars_streaming_time,
                        "threads": 1,
                    }
                )
                print(
                    f"  Max memory: {polars_streaming_memory:.1f} MB, Wall time: {polars_streaming_time:.3f}s"
                )
        finally:
            Path(polars_streaming_script).unlink()

        # Polars-bio with 4 configurations
        configs = [
            (False, False, "no pushdowns"),
            (True, False, "projection pushdown"),
            (False, True, "predicate pushdown"),
            (True, True, "both pushdowns"),
        ]

        for proj_pushdown, pred_pushdown, config_name in configs:
            print(
                f"Profiling polars-bio memory usage and wall time ({test_type}, {config_name})..."
            )
            polars_bio_script = create_polars_bio_script(
                test_type, proj_pushdown, pred_pushdown
            )
            try:
                polars_bio_memory, polars_bio_time = run_memory_profile(
                    polars_bio_script, f"polars-bio-{config_name}"
                )
                if polars_bio_memory is not None and polars_bio_time is not None:
                    results.append(
                        {
                            "library": "polars-bio",
                            "test_type": test_type,
                            "projection_pushdown": proj_pushdown,
                            "predicate_pushdown": pred_pushdown,
                            "max_memory_mb": polars_bio_memory,
                            "wall_time_s": polars_bio_time,
                            "threads": 1,
                        }
                    )
                    print(
                        f"  Max memory: {polars_bio_memory:.1f} MB, Wall time: {polars_bio_time:.3f}s"
                    )
            finally:
                Path(polars_bio_script).unlink()

    # Save results
    if results:
        Path("results").mkdir(exist_ok=True)
        with open("results/memory_profiling.csv", "w", newline="") as f:
            fieldnames = [
                "library",
                "test_type",
                "projection_pushdown",
                "predicate_pushdown",
                "max_memory_mb",
                "wall_time_s",
                "threads",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print("\nResults saved to results/memory_profiling.csv")

        # Print summary statistics
        print("\n=== Memory Usage Summary ===")
        print("Library\t\t\tTest Type\t\tProj PD\tPred PD\tMax Memory\tWall Time")
        print("-" * 85)

        for test_type, _ in test_cases:
            for library in ["pandas", "polars", "polars-streaming", "polars-bio"]:
                if library in ["pandas", "polars"]:
                    lib_results = [
                        r
                        for r in results
                        if r["library"] == library and r["test_type"] == test_type
                    ]
                    if lib_results:
                        r = lib_results[
                            0
                        ]  # Only one result per library for pandas/polars
                        print(
                            f"{library}\t\t\t{test_type}\t\tN/A\tN/A\t{r['max_memory_mb']:.1f}MB\t\t{r['wall_time_s']:.3f}s"
                        )
                elif library == "polars-streaming":
                    lib_results = [
                        r
                        for r in results
                        if r["library"] == library and r["test_type"] == test_type
                    ]
                    if lib_results:
                        r = lib_results[0]
                        print(
                            f"{library}\t{test_type}\t\tN/A\tN/A\t{r['max_memory_mb']:.1f}MB\t\t{r['wall_time_s']:.3f}s"
                        )
                else:  # polars-bio
                    for proj_pushdown, pred_pushdown, config_name in configs:
                        lib_results = [
                            r
                            for r in results
                            if r["library"] == library
                            and r["test_type"] == test_type
                            and r["projection_pushdown"] == proj_pushdown
                            and r["predicate_pushdown"] == pred_pushdown
                        ]
                        if lib_results:
                            r = lib_results[0]
                            proj_str = "Yes" if proj_pushdown else "No"
                            pred_str = "Yes" if pred_pushdown else "No"
                            print(
                                f"{library}\t\t{test_type}\t\t{proj_str}\t{pred_str}\t{r['max_memory_mb']:.1f}MB\t\t{r['wall_time_s']:.3f}s"
                            )
    else:
        print("No memory profiling results obtained")


if __name__ == "__main__":
    main()
