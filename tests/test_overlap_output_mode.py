import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_bio as pb


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


def _expected_left_output() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "chrom": ["chr1", "chr1", "chr2"],
            "start": [100, 100, 50],
            "end": [200, 200, 60],
            "name": ["dup", "dup", "other"],
        }
    ).sort(["chrom", "start", "end", "name"])


def test_overlap_left_output_preserves_left_schema_and_duplicate_rows():
    result = pb.overlap(
        _left_df(),
        _right_df(),
        overlap_output="left",
        output_type="polars.DataFrame",
    ).sort(["chrom", "start", "end", "name"])

    assert_frame_equal(result, _expected_left_output())
    assert result.columns == ["chrom", "start", "end", "name"]
    assert result.config_meta.get_metadata()["coordinate_system_zero_based"] is True


def test_overlap_left_output_lazyframe_namespace():
    left = _left_df().lazy()
    left.config_meta.set(coordinate_system_zero_based=True)
    right = _right_df().lazy()
    right.config_meta.set(coordinate_system_zero_based=True)

    result = (
        left.pb.overlap(right, overlap_output="left")
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
