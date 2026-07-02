"""Correctness gate vs a committed FastQC 0.12.1 reference (no Java at test time).

`tests/data/io/fastq/golden/example.nogroup.fastqc_data.txt` is real FastQC
`--nogroup` output for example.fastq, committed so CI can assert FastQC parity
without running FastQC.

We assert *exact* agreement on the metrics FastQC computes the same way we do
(per_base_quality, basic-stats counts). Modules where FastQC applies cosmetic
transforms are validated arithmetically in test_fastqc_correctness.py instead:
  - per_seq_gc: FastQC smooths the distribution into fractional counts.
  - dup_levels: FastQC uses different bin labels ("10-49" vs our ">10").
"""

import polars as pl

import polars_bio as pb

GOLDEN = "tests/data/io/fastq/golden/example.nogroup.fastqc_data.txt"
FASTQ = "tests/data/io/fastq/example.fastq"


def _golden() -> pl.DataFrame:
    from benchmarks.fastqc.parity import parse_fastqc_data

    return parse_fastqc_data(GOLDEN)


def _ours() -> pl.DataFrame:
    return (
        pb.fastqc(FASTQ)
        .tidy.collect()
        .select("module", "label", "position", "metric", "value")
    )


def test_per_base_quality_matches_fastqc_exactly():
    ref = _golden().filter(pl.col("module") == "per_base_quality")
    got = _ours().filter(pl.col("module") == "per_base_quality")
    joined = got.join(
        ref,
        on=["module", "label", "position", "metric"],
        how="inner",
        suffix="_ref",
        nulls_equal=True,
    )
    # Every reference row (mean/median/q1/q3/p10/p90 per position) must match.
    assert joined.height == ref.height > 0
    worst = joined.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
    assert worst <= 1e-6, f"max per_base_quality diff vs FastQC = {worst}"


def test_per_seq_gc_matches_fastqc_exactly():
    # Exact after replicating FastQC's truncateSequence (101bp -> first 100bp)
    # and GCModel interpolation.
    ref = _golden().filter(pl.col("module") == "per_seq_gc")
    got = _ours().filter(pl.col("module") == "per_seq_gc")
    joined = got.join(
        ref,
        on=["module", "label", "position", "metric"],
        how="inner",
        suffix="_ref",
        nulls_equal=True,
    )
    assert joined.height == ref.height > 0
    worst = joined.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
    assert worst <= 1e-9, f"max per_seq_gc diff vs FastQC = {worst}"


def test_basic_stats_match_fastqc():
    ref = dict(
        zip(
            *_golden()
            .filter(pl.col("module") == "basic_stats")
            .select("metric", "value")
            .to_dict(as_series=False)
            .values()
        )
    )
    got = dict(
        zip(
            *_ours()
            .filter(pl.col("module") == "basic_stats")
            .select("metric", "value")
            .to_dict(as_series=False)
            .values()
        )
    )
    assert got["n_seq"] == ref["n_seq"]  # exact
    assert abs(got["gc_pct"] - ref["gc_pct"]) <= 1.0  # FastQC rounds %GC to int
