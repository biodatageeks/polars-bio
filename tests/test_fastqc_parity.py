import shutil
import tempfile

import polars as pl
import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("fastqc") is None, reason="FastQC binary not installed"
)

FASTQ = "tests/data/io/fastq/example.fastq"

# Modules we match FastQC on bit-for-bit. Only dup_levels is excluded (FastQC
# uses different bin labels, e.g. "10-49" vs our ">10"); it is validated
# arithmetically in test_fastqc_correctness.py.
EXACT_MODULES = ["per_base_quality", "basic_stats", "per_seq_gc"]


def test_parity_against_fastqc():
    from benchmarks.fastqc.parity import (
        parity_report,
        parse_fastqc_data,
        pb_tidy,
        run_fastqc,
    )

    with tempfile.TemporaryDirectory() as d:
        ref = parse_fastqc_data(run_fastqc(FASTQ, d))
        got = pb_tidy(FASTQ, None)
        report = parity_report(got, ref).filter(pl.col("module").is_in(EXACT_MODULES))
        mism = report.filter(report["verdict"] == "mismatch")
        assert mism.height == 0, f"parity mismatches:\n{mism}"
