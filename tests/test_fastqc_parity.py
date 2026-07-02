import shutil
import tempfile

import polars as pl
import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("fastqc") is None, reason="FastQC binary not installed"
)

FASTQ = "tests/data/io/fastq/example.fastq"

# All four modules match FastQC 0.12.1. per_base_quality/per_seq_gc/dup_levels
# are bit-exact; basic_stats n_seq is exact and %GC is within FastQC's integer
# truncation (TOLERANCES allows 0.5; floor-exactness is asserted in the golden
# test). dup_levels only compares per-bin "pct" rows (labels align incl ">10k+").
EXACT_MODULES = ["per_base_quality", "per_seq_gc", "dup_levels", "basic_stats"]


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
