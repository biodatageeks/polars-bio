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
