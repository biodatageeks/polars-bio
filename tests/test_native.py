import bioframe as bf
import pandas as pd
import polars as pl
from _expected import (
    BIO_DF_PATH1,
    BIO_DF_PATH2,
    BIO_PD_DF1,
    BIO_PD_DF2,
    DF_COUNT_OVERLAPS_PATH1,
    DF_COUNT_OVERLAPS_PATH2,
    DF_MERGE_PATH,
    DF_NEAREST_PATH1,
    DF_NEAREST_PATH2,
    DF_OVER_PATH1,
    DF_OVER_PATH2,
    PD_DF_COUNT_OVERLAPS,
    PD_DF_MERGE,
    PD_DF_NEAREST,
    PD_DF_OVERLAP,
)

import polars_bio as pb


def _load_csv_with_metadata(path: str, zero_based: bool = False) -> pl.LazyFrame:
    """Load CSV file as LazyFrame with coordinate system metadata."""
    lf = pl.scan_csv(path)
    lf.config_meta.set(coordinate_system_zero_based=zero_based)
    return lf


class TestOverlapNative:
    # Load data with 1-based metadata (zero_based=False)
    _df1 = _load_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
    result_csv = pb.overlap(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_overlap_count(self):
        assert len(self.result_csv) == 16

    def test_overlap_schema_rows(self):
        result_csv = self.result_csv.sort_values(
            by=list(self.result_csv.columns)
        ).reset_index(drop=True)
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result_csv, expected)


class TestNearestNative:
    _df1 = _load_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False)
    result = pb.nearest(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_nearest_count(self):
        print(self.result)
        assert len(self.result) == len(PD_DF_NEAREST)

    def test_nearest_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_NEAREST
        pd.testing.assert_frame_equal(result, expected)


class TestNearestK2Native:
    """Test nearest with k=2 (multiple nearest neighbors per query interval)."""

    _df1 = _load_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False)
    result = pb.nearest(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        k=2,
        output_type="pandas.DataFrame",
    )

    def test_nearest_k2_more_rows(self):
        """k=2 should return at least as many rows as k=1."""
        assert len(self.result) >= len(PD_DF_NEAREST)

    def test_nearest_k2_has_distance(self):
        """Distance column should still be present."""
        assert "distance" in self.result.columns

    def test_nearest_k2_columns(self):
        """Should have standard suffixed columns plus distance."""
        expected_cols = {
            "contig_1",
            "pos_start_1",
            "pos_end_1",
            "contig_2",
            "pos_start_2",
            "pos_end_2",
            "distance",
        }
        assert set(self.result.columns) == expected_cols


class TestNearestNoOverlapNative:
    """Test nearest with overlap=False (exclude overlapping intervals)."""

    _df1 = _load_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False)
    result = pb.nearest(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap=False,
        output_type="pandas.DataFrame",
    )

    def test_nearest_no_overlap_all_positive_distance(self):
        """With overlap=False, all distances should be > 0."""
        # Filter out rows where no match was found (NaN distance)
        valid = self.result.dropna(subset=["distance"])
        assert len(valid) > 0, "Should have at least some results"
        assert all(
            d > 0 for d in valid["distance"]
        ), f"All distances should be > 0, got: {valid['distance'].tolist()}"

    def test_nearest_no_overlap_has_results(self):
        """Should still return results for intervals that have non-overlapping neighbors."""
        assert len(self.result) > 0


class TestNearestNoDistanceNative:
    """Test nearest with distance=False (omit distance column)."""

    _df1 = _load_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False)
    result = pb.nearest(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        distance=False,
        output_type="pandas.DataFrame",
    )

    def test_nearest_no_distance_column(self):
        """Distance column should not be present."""
        assert "distance" not in self.result.columns

    def test_nearest_no_distance_count(self):
        """Row count should be same as default nearest (k=1, overlap=True)."""
        assert len(self.result) == len(PD_DF_NEAREST)

    def test_nearest_no_distance_columns(self):
        """Should only have suffixed interval columns, no distance."""
        expected_cols = {
            "contig_1",
            "pos_start_1",
            "pos_end_1",
            "contig_2",
            "pos_start_2",
            "pos_end_2",
        }
        assert set(self.result.columns) == expected_cols


class TestCountOverlapsNative:
    _df1 = _load_csv_with_metadata(DF_COUNT_OVERLAPS_PATH1, zero_based=False)
    _df2 = _load_csv_with_metadata(DF_COUNT_OVERLAPS_PATH2, zero_based=False)
    result = pb.count_overlaps(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        naive_query=True,
    )

    def test_count_overlaps_count(self):
        print(self.result)
        assert len(self.result) == len(PD_DF_COUNT_OVERLAPS)

    def test_count_overlaps_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_COUNT_OVERLAPS
        pd.testing.assert_frame_equal(result, expected)


class TestMergeNative:
    _df = _load_csv_with_metadata(DF_MERGE_PATH, zero_based=True)
    result = pb.merge(
        _df,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_merge_count(self):
        print(self.result)
        assert len(self.result) == len(PD_DF_MERGE)

    def test_merge_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_MERGE
        pd.testing.assert_frame_equal(result, expected)


class TestCoverageNative:
    _df1 = pl.scan_parquet(BIO_DF_PATH1)
    _df1.config_meta.set(coordinate_system_zero_based=True)
    _df2 = pl.scan_parquet(BIO_DF_PATH2)
    _df2.config_meta.set(coordinate_system_zero_based=True)
    result = pb.coverage(
        _df1,
        _df2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_bio = bf.coverage(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
    )

    def test_coverage_count(self):
        print(self.result)
        assert len(self.result) == len(self.result_bio)

    def test_coverage_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = self.result_bio.astype({"coverage": "int64"})
        pd.testing.assert_frame_equal(result, expected)
