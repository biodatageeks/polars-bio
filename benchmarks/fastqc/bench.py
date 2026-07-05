"""Benchmark polars-bio fastqc against RastQC.

Reports per-run wall time, throughput, and a scan-only baseline so the QC
math is isolated from FASTQ decode. Run 1-thread and all-core.
"""

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import polars as pl

import polars_bio as pb


def _timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def scan_only(fastq: str) -> float:
    return _timed(lambda: pb.scan_fastq(fastq).select(pl.len()).collect())


def pb_fastqc(fastq: str, threads: int) -> float:
    pb.set_option("datafusion.execution.target_partitions", str(threads))
    return _timed(lambda: pb.fastqc(fastq).tidy.collect())


def rastqc(fastq: str, threads: int) -> float:
    if shutil.which("rastqc") is None:
        return float("nan")
    with tempfile.TemporaryDirectory() as d:
        return _timed(
            lambda: subprocess.run(
                ["rastqc", "-t", str(threads), "--nozip", "-o", d, fastq],
                check=True,
                capture_output=True,
            )
        )


def _all_cores() -> int:
    return os.cpu_count() or 1


def main(fastq: str):
    n_reads = pb.scan_fastq(fastq).select(pl.len()).collect().item()
    size_mb = Path(fastq).stat().st_size / 1e6
    print(f"file={fastq} reads={n_reads} size={size_mb:.1f}MB")
    for label, threads in (("1", 1), ("all", _all_cores())):
        base = scan_only(fastq)
        pbt = pb_fastqc(fastq, threads)
        rqt = rastqc(fastq, threads)
        print(
            f"[threads={label}] scan_only={base:.3f}s  pb.fastqc={pbt:.3f}s "
            f"(qc_only={pbt - base:.3f}s, {n_reads / pbt:,.0f} reads/s)  rastqc={rqt:.3f}s"
        )


if __name__ == "__main__":
    import sys

    main(sys.argv[1])
