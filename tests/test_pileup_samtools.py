"""Tests comparing polars-bio depth output against samtools depth golden standard.

Uses a 10k-read BAM extracted from NA12878 WES chr1.
Golden standard: samtools depth -q 0 -Q 0 (covered positions only, 1-based).
"""

import polars as pl
import pytest

import polars_bio as pb

BAM_PATH = "tests/data/io/bam/NA12878_10k.bam"
SAMTOOLS_DEPTH_PATH = "tests/data/io/bam/NA12878_10k_samtools_depth.tsv.gz"


@pytest.fixture(scope="module")
def samtools_golden():
    """Load samtools depth output as Polars DataFrame (1-based, covered positions only)."""
    return pl.read_csv(
        SAMTOOLS_DEPTH_PATH,
        separator="\t",
        has_header=False,
        new_columns=["contig", "pos", "coverage"],
        schema_overrides={"contig": pl.Utf8, "pos": pl.Int32, "coverage": pl.Int16},
    ).sort(["contig", "pos"])


def _run_per_base(partitions: int) -> pl.DataFrame:
    """Run per-base depth with given partition count, return sorted non-zero rows."""
    pb.set_option("datafusion.execution.target_partitions", str(partitions))
    try:
        df = (
            pb.depth(BAM_PATH, per_base=True, use_zero_based=False)
            .filter(pl.col("coverage") > 0)
            .collect()
        )
    finally:
        pb.set_option("datafusion.execution.target_partitions", "1")
    return df.sort(["contig", "pos"])


def _expand_blocks(blocks: pl.DataFrame) -> pl.DataFrame:
    """Expand RLE blocks into per-position rows (1-based), keep only covered positions."""
    return (
        blocks.filter(pl.col("coverage") > 0)
        .with_columns(
            pl.int_ranges(pl.col("pos_start"), pl.col("pos_end") + 1).alias("pos")
        )
        .explode("pos")
        .select(["contig", pl.col("pos").cast(pl.Int32), "coverage"])
        .sort(["contig", "pos"])
    )


def _run_blocks(partitions: int) -> pl.DataFrame:
    """Run block-mode depth with given partition count, return expanded sorted rows."""
    pb.set_option("datafusion.execution.target_partitions", str(partitions))
    try:
        blocks = pb.depth(BAM_PATH, use_zero_based=False).collect()
    finally:
        pb.set_option("datafusion.execution.target_partitions", "1")
    return _expand_blocks(blocks)


# ── Per-base vs samtools ───────────────────────────────────────────────


@pytest.mark.parametrize("partitions", [1, 2, 4, 8])
def test_per_base_vs_samtools(samtools_golden, partitions):
    """Per-base depth output must exactly match samtools depth."""
    result = _run_per_base(partitions)

    assert result.height == samtools_golden.height, (
        f"Row count mismatch (partitions={partitions}): "
        f"got {result.height}, expected {samtools_golden.height}"
    )

    assert result.equals(
        samtools_golden
    ), f"Coverage mismatch (partitions={partitions})"


# ── Block mode vs samtools ─────────────────────────────────────────────


@pytest.mark.parametrize("partitions", [1, 2, 4, 8])
def test_blocks_vs_samtools(samtools_golden, partitions):
    """Expanded block-mode depth must exactly match samtools depth."""
    result = _run_blocks(partitions)

    assert result.height == samtools_golden.height, (
        f"Row count mismatch (partitions={partitions}): "
        f"got {result.height}, expected {samtools_golden.height}"
    )

    assert result.equals(
        samtools_golden
    ), f"Coverage mismatch (partitions={partitions})"


# ── Cross-check: block mode vs per-base ───────────────────────────────


@pytest.mark.parametrize("partitions", [1, 2, 4, 8])
def test_blocks_vs_per_base(partitions):
    """Expanded blocks and per-base must produce identical non-zero coverage."""
    blocks_expanded = _run_blocks(partitions)
    per_base = _run_per_base(partitions)

    assert blocks_expanded.height == per_base.height
    assert blocks_expanded.equals(per_base)
