import pandas as pd
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_bio as pb

try:
    import pyranges1 as pr
except ImportError:
    pr = None


def _with_metadata(df: pl.DataFrame, zero_based: bool = True) -> pl.DataFrame:
    df.config_meta.set(coordinate_system_zero_based=zero_based)
    return df


def _left_df() -> pl.DataFrame:
    return _with_metadata(
        pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr1", "chr2"],
                "start": [100, 100, 1000, 50],
                "end": [200, 200, 1100, 60],
                "name": ["dup", "dup", "miss", "other"],
            }
        )
    )


def _right_df() -> pl.DataFrame:
    return _with_metadata(
        pl.DataFrame(
            {
                "chrom": ["chr1", "chr1", "chr2"],
                "start": [90, 120, 55],
                "end": [150, 180, 56],
                "score": [1, 2, 3],
            }
        )
    )


def _left_pyranges_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Chromosome": ["chr1", "chr1", "chr1", "chr2"],
            "Start": [100, 100, 1000, 50],
            "End": [200, 200, 1100, 60],
            "name": ["dup", "dup", "miss", "other"],
        }
    )


def _right_pyranges_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Chromosome": ["chr1", "chr1", "chr2"],
            "Start": [90, 120, 55],
            "End": [150, 180, 56],
            "score": [1, 2, 3],
        }
    )


def _sort_left_output(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(["Chromosome", "Start", "End", "name"])


def _pyranges_overlap_expected(multiple: bool) -> pl.DataFrame:
    if pr is None:
        pytest.skip("pyranges1 is not installed")

    result = pr.PyRanges(_left_pyranges_df()).overlap(
        pr.PyRanges(_right_pyranges_df()),
        multiple=multiple,
    )
    return _sort_left_output(
        pl.from_pandas(pd.DataFrame(result).reset_index(drop=True))
    )


def _polars_overlap_left_result(distinct_output: bool) -> pl.DataFrame:
    left = _with_metadata(pl.from_pandas(_left_pyranges_df()))
    right = _with_metadata(pl.from_pandas(_right_pyranges_df()))
    result = pb.overlap(
        left,
        right,
        cols1=["Chromosome", "Start", "End"],
        cols2=["Chromosome", "Start", "End"],
        overlap_output="left",
        distinct_output=distinct_output,
        output_type="polars.DataFrame",
    )
    return _sort_left_output(result)


def _expected_left_output() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chrom": ["chr1", "chr1", "chr2"],
            "start": [100, 100, 50],
            "end": [200, 200, 60],
            "name": ["dup", "dup", "other"],
        }
    ).sort(["chrom", "start", "end", "name"])


def _expected_left_multiplicity_output() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chrom": ["chr1", "chr1", "chr1", "chr1", "chr2"],
            "start": [100, 100, 100, 100, 50],
            "end": [200, 200, 200, 200, 60],
            "name": ["dup", "dup", "dup", "dup", "other"],
        }
    ).sort(["chrom", "start", "end", "name"])


def test_overlap_left_output_preserves_overlap_multiplicity_by_default():
    result = pb.overlap(
        _left_df(),
        _right_df(),
        overlap_output="left",
        output_type="polars.DataFrame",
    ).sort(["chrom", "start", "end", "name"])

    assert_frame_equal(result, _expected_left_multiplicity_output())
    assert result.columns == ["chrom", "start", "end", "name"]
    assert result.config_meta.get_metadata()["coordinate_system_zero_based"] is True


def test_overlap_left_output_matches_pyranges1_multiple_true():
    assert_frame_equal(
        _polars_overlap_left_result(distinct_output=False),
        _pyranges_overlap_expected(multiple=True),
    )


def test_overlap_left_distinct_output_preserves_left_schema_and_duplicate_rows():
    result = pb.overlap(
        _left_df(),
        _right_df(),
        overlap_output="left",
        distinct_output=True,
        output_type="polars.DataFrame",
    ).sort(["chrom", "start", "end", "name"])

    assert_frame_equal(result, _expected_left_output())
    assert result.columns == ["chrom", "start", "end", "name"]
    assert result.config_meta.get_metadata()["coordinate_system_zero_based"] is True


def test_overlap_left_distinct_output_matches_pyranges1_multiple_false():
    assert_frame_equal(
        _polars_overlap_left_result(distinct_output=True),
        _pyranges_overlap_expected(multiple=False),
    )


def test_overlap_left_output_lazyframe_namespace():
    left = _left_df().lazy()
    left.config_meta.set(coordinate_system_zero_based=True)
    right = _right_df().lazy()
    right.config_meta.set(coordinate_system_zero_based=True)

    result = (
        left.pb.overlap(right, overlap_output="left", distinct_output=True)
        .collect()
        .sort(["chrom", "start", "end", "name"])
    )

    assert_frame_equal(result, _expected_left_output())


def test_overlap_join_output_remains_default():
    result = pb.overlap(
        _left_df(),
        _right_df(),
        output_type="polars.DataFrame",
    )

    assert "chrom_1" in result.columns
    assert "chrom_2" in result.columns
    assert "score_2" in result.columns
    assert "name_1" in result.columns


def test_overlap_output_rejects_unknown_mode():
    with pytest.raises(ValueError, match="overlap_output"):
        pb.overlap(
            _left_df(),
            _right_df(),
            overlap_output="semi",
            output_type="polars.DataFrame",
        )
