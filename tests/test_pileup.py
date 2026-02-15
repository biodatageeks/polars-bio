import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system

BAM_PATH = "tests/data/io/bam/test.bam"
SAM_PATH = "tests/data/io/sam/test.sam"
CRAM_PATH = "tests/data/io/cram/test.cram"

EXPECTED_COLUMNS = {"contig", "pos_start", "pos_end", "coverage"}


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
