import shutil
import tempfile

import polars as pl
import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("fastqc") is None, reason="FastQC binary not installed"
)

FASTQ = "tests/data/io/fastq/example.fastq"

# Modules FastQC computes bit-identically to us. per_seq_gc (FastQC's GCModel
# display smoothing diverges even from its own published source in 0.12.1) and
# dup_levels (different bin labels) are validated separately: per_seq_gc/dup by
# test_fastqc_correctness.py, and per_base_quality/basic_stats also by the
# committed golden in test_fastqc_golden.py.
EXACT_MODULES = ["per_base_quality", "basic_stats"]


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
