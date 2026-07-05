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
# A file with a known mix of duplication levels (1x/2x/3x/5x reads).
DUP_GOLDEN = "tests/data/io/fastq/golden/dup_mix.nogroup.fastqc_data.txt"
DUP_FASTQ = "tests/data/io/fastq/dup_mix.fastq"


def _golden(path=GOLDEN) -> pl.DataFrame:
    from benchmarks.fastqc.parity import parse_fastqc_data

    return parse_fastqc_data(path)


def _ours(path=FASTQ) -> pl.DataFrame:
    return (
        pb.fastqc(path)
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
    # FastQC prints %GC as an integer via truncation of (G+C)*100/(A+T+G+C);
    # our full-precision value must floor to exactly that integer.
    import math

    assert math.floor(got["gc_pct"]) == ref["gc_pct"]


PER_TILE_GOLDEN = "tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt"
PER_TILE_FASTQ = "tests/data/io/fastq/per_tile_mix.fastq"


def test_per_tile_quality_matches_fastqc_exactly():
    ref = _golden(PER_TILE_GOLDEN).filter(pl.col("module") == "per_tile_quality")
    got = _ours(PER_TILE_FASTQ).filter(pl.col("module") == "per_tile_quality")
    joined = got.join(
        ref,
        on=["module", "label", "position", "metric"],
        how="inner",
        suffix="_ref",
        nulls_equal=True,
    )
    assert joined.height == ref.height > 0
    worst = joined.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
    assert worst <= 1e-6, f"max per_tile deviation diff vs FastQC = {worst}"


KMER_GOLDEN = "tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt"
KMER_FASTQ = "tests/data/io/fastq/kmer_mix.fastq"


def test_kmer_content_matches_fastqc_exactly():
    # FastQC's Kmer module samples every 50th read in file order, so exact parity
    # requires a single-partition scan (batches arriving in file order). A
    # multi-partition scan samples each partition independently and diverges.
    key = "datafusion.execution.target_partitions"
    original = pb.get_option(key)
    pb.set_option(key, "1")
    try:
        ref = _golden(KMER_GOLDEN).filter(pl.col("module") == "kmer_content")
        got = _ours(KMER_FASTQ).filter(
            (pl.col("module") == "kmer_content") & (pl.col("metric") != "status")
        )
    finally:
        pb.set_option(key, original if original is not None else "1")
    # Reported-set membership: our enriched k-mers must equal FastQC's.
    ref_kmers = set(ref.select("label").to_series().to_list())
    got_kmers = set(got.select("label").to_series().to_list())
    assert got_kmers == ref_kmers and len(ref_kmers) > 0, (
        f"kmer set mismatch: only-ours={got_kmers - ref_kmers}, "
        f"only-fastqc={ref_kmers - got_kmers}"
    )
    joined = got.join(ref, on=["module", "label", "metric"], how="inner", suffix="_ref")
    # Exact on count & max_position; tight tolerance on obs/exp (FastQC float32).
    for metric, tol in [("count", 0.0), ("max_position", 0.0), ("obs_exp_max", 1e-2)]:
        sub = joined.filter(pl.col("metric") == metric)
        worst = sub.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
        assert worst <= tol, f"kmer {metric} diff vs FastQC = {worst}"


def test_dup_levels_match_fastqc_exactly():
    # On a file with real duplicates, every bin percentage must match FastQC.
    ref = _golden(DUP_GOLDEN).filter(pl.col("module") == "dup_levels")
    got = _ours(DUP_FASTQ).filter(
        (pl.col("module") == "dup_levels") & (pl.col("metric") == "pct")
    )
    joined = got.join(
        ref,
        on=["module", "label", "position", "metric"],
        how="inner",
        suffix="_ref",
        nulls_equal=True,
    )
    assert joined.height == ref.height == 16
    worst = joined.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
    assert worst <= 1e-9, f"max dup_levels diff vs FastQC = {worst}"
