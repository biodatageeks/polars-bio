import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system

BAM_PATH = "tests/data/io/bam/test.bam"
SAM_PATH = "tests/data/io/sam/test.sam"
CRAM_PATH = "tests/data/io/cram/test.cram"

EXPECTED_COLUMNS = {"contig", "pos_start", "pos_end", "coverage"}
EXPECTED_PER_BASE_COLUMNS = {"contig", "pos", "coverage"}


def test_depth_bam():
    result = pb.depth(BAM_PATH)
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert set(df.columns) == EXPECTED_COLUMNS
    assert df.height > 0


def test_depth_sam():
    result = pb.depth(SAM_PATH)
    df = result.collect()
    assert set(df.columns) == EXPECTED_COLUMNS
    assert df.height > 0


def test_depth_cram():
    result = pb.depth(CRAM_PATH)
    df = result.collect()
    assert set(df.columns) == EXPECTED_COLUMNS
    assert df.height > 0


def test_depth_with_mapq_filter():
    all_df = pb.depth(BAM_PATH).collect()
    filtered = pb.depth(BAM_PATH, min_mapping_quality=20).collect()
    assert filtered.height <= all_df.height


def test_depth_output_polars_dataframe():
    df = pb.depth(BAM_PATH, output_type="polars.DataFrame")
    assert isinstance(df, pl.DataFrame)
    assert set(df.columns) == EXPECTED_COLUMNS


def test_depth_output_pandas_dataframe():
    pd = pytest.importorskip("pandas")
    pdf = pb.depth(BAM_PATH, output_type="pandas.DataFrame")
    assert isinstance(pdf, pd.DataFrame)
    assert set(pdf.columns) == EXPECTED_COLUMNS


def test_depth_sql():
    df = pb.sql(f"SELECT * FROM depth('{BAM_PATH}')").collect()
    assert "coverage" in df.columns
    assert df.height > 0


def test_depth_dense_mode_disable():
    df = pb.depth(BAM_PATH, dense_mode="disable").collect()
    assert set(df.columns) == EXPECTED_COLUMNS
    assert df.height > 0


def test_depth_binary_cigar_false():
    df = pb.depth(BAM_PATH, binary_cigar=False).collect()
    assert set(df.columns) == EXPECTED_COLUMNS
    assert df.height > 0


def test_depth_default_coordinate_system():
    """Default should be 1-based (False)."""
    lf = pb.depth(BAM_PATH)
    assert get_coordinate_system(lf) is False


def test_depth_zero_based():
    """Explicit zero_based=True."""
    lf = pb.depth(BAM_PATH, use_zero_based=True)
    assert get_coordinate_system(lf) is True
    df = lf.collect()
    assert df.height > 0


def test_depth_one_based():
    """Explicit use_zero_based=False."""
    lf = pb.depth(BAM_PATH, use_zero_based=False)
    assert get_coordinate_system(lf) is False


# ── Coordinate values (issue #427) ─────────────────────────────────────
#
# These assert actual coordinates, not just the schema metadata flag.
# use_zero_based=True used to emit 0-based *closed* blocks, so every block
# covered one base fewer than documented; the metadata-only tests above
# passed throughout.


@pytest.mark.parametrize("path", [BAM_PATH, SAM_PATH, CRAM_PATH])
def test_depth_zero_based_blocks_are_half_open(path):
    """0-based output is half-open: pos_end is exclusive, so it equals the
    1-based closed end while pos_start is one lower."""
    one_based = pb.depth(path, use_zero_based=False).collect()
    zero_based = pb.depth(path, use_zero_based=True).collect()

    assert zero_based.height == one_based.height
    assert zero_based.height > 0

    assert (zero_based["pos_start"] == one_based["pos_start"] - 1).all()
    assert (zero_based["pos_end"] == one_based["pos_end"]).all()
    assert (zero_based["coverage"] == one_based["coverage"]).all()


@pytest.mark.parametrize("path", [BAM_PATH, SAM_PATH, CRAM_PATH])
def test_depth_coordinate_systems_cover_the_same_bases(path):
    """Switching coordinate system must not change how many bases a block
    covers: 1-based closed spans end - start + 1, 0-based half-open spans
    end - start."""
    one_based = pb.depth(path, use_zero_based=False).collect()
    zero_based = pb.depth(path, use_zero_based=True).collect()

    closed_widths = one_based["pos_end"] - one_based["pos_start"] + 1
    half_open_widths = zero_based["pos_end"] - zero_based["pos_start"]

    assert (closed_widths == half_open_widths).all()
    assert closed_widths.sum() == half_open_widths.sum()
    assert (half_open_widths > 0).all(), "half-open blocks must be non-empty"


def test_depth_zero_based_blocks_chain_without_gaps_or_overlap():
    """The defining property of half-open intervals: where two blocks abut,
    one block's end *is* the next block's start. Under the old closed output
    abutting blocks were off by one and could not be chained."""
    df = (
        pb.depth(CRAM_PATH, use_zero_based=True).collect().sort(["contig", "pos_start"])
    )

    starts = df["pos_start"].to_list()
    ends = df["pos_end"].to_list()
    contigs = df["contig"].to_list()

    assert all(e > s for s, e in zip(starts, ends)), "every block must be non-empty"

    abutting = 0
    for i in range(len(df) - 1):
        if contigs[i] != contigs[i + 1]:
            continue
        assert ends[i] <= starts[i + 1], (
            f"blocks overlap: [{starts[i]}, {ends[i]}) then "
            f"[{starts[i + 1]}, {ends[i + 1]})"
        )
        if ends[i] == starts[i + 1]:
            abutting += 1

    # This fixture is one continuous pileup, so consecutive blocks abut.
    # With closed ends, ends[i] would be starts[i + 1] - 1 and this is 0.
    assert abutting > 0, "no block chained into the next — ends are not exclusive"


def test_depth_zero_based_first_block_can_start_at_zero():
    """A read at the first base of a contig yields pos_start == 0 in 0-based
    output, and its end is still exclusive."""
    df = pb.depth(CRAM_PATH, use_zero_based=True).collect().sort("pos_start")

    first = df.row(0, named=True)
    assert first["pos_start"] == 0
    assert first["pos_end"] > first["pos_start"]


def test_depth_is_truly_lazy():
    """depth() returns LazyFrame with correct schema without executing pileup."""
    lf = pb.depth(BAM_PATH)
    assert isinstance(lf, pl.LazyFrame)
    assert set(lf.collect_schema().names()) == EXPECTED_COLUMNS


def test_depth_projection_pushdown():
    """Selecting a subset of columns works."""
    result = pb.depth(BAM_PATH).select(["contig", "coverage"]).collect()
    assert set(result.columns) == {"contig", "coverage"}
    assert result.height > 0


def test_depth_predicate_filter():
    """Client-side predicate filtering works."""
    result = pb.depth(BAM_PATH).filter(pl.col("coverage") > 0).collect()
    assert all(v > 0 for v in result["coverage"].to_list())


def test_depth_limit():
    """Limit pushdown works."""
    result = pb.depth(BAM_PATH).limit(5).collect()
    assert result.height <= 5


def test_depth_per_base():
    """Per-base mode emits one row per genomic position."""
    result = pb.depth(BAM_PATH, per_base=True)
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert set(df.columns) == EXPECTED_PER_BASE_COLUMNS
    assert df.height > 0


def test_depth_per_base_more_rows_than_blocks():
    """Per-base output should have >= as many rows as block output."""
    blocks = pb.depth(BAM_PATH).collect()
    per_base = pb.depth(BAM_PATH, per_base=True).collect()
    assert per_base.height >= blocks.height


def test_pileup_predicate_lazy_equals_eager():
    """Pileup predicate pushdown must match client-side filtering."""
    import polars as pl

    import polars_bio as pb

    lf = pb.depth(BAM_PATH).filter(pl.col("coverage") >= 1)
    eager = pb.depth(BAM_PATH).collect().filter(pl.col("coverage") >= 1)
    lazy_df = lf.collect()
    assert lazy_df.sort(by=lazy_df.columns).equals(eager.sort(by=eager.columns))
