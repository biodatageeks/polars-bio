import pandas as pd
from _expected import (
    PD_COUNT_OVERLAPS_DF1,
    PD_COUNT_OVERLAPS_DF2,
    PD_DF_COUNT_OVERLAPS,
    PD_DF_MERGE,
    PD_DF_NEAREST,
    PD_DF_OVERLAP,
    PD_MERGE_DF,
    PD_NEAREST_DF1,
    PD_NEAREST_DF2,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
)

import polars_bio as pb
from polars_bio.polars_bio import FilterOp


class TestOverlapPandas:
    result = pb.overlap(
        PD_OVERLAP_DF1,
        PD_OVERLAP_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        overlap_filter=FilterOp.Weak,
    )

    def test_overlap_count(self):
        assert len(self.result) == len(PD_DF_OVERLAP)

    def test_overlap_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result, expected)


class TestNearestPandas:
    result = pb.nearest(
        PD_NEAREST_DF1,
        PD_NEAREST_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        overlap_filter=FilterOp.Weak,
    )

    def test_nearest_count(self):
        assert len(self.result) == len(PD_DF_NEAREST)

    def test_nearest_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_NEAREST
        pd.testing.assert_frame_equal(result, expected)


class TestCountOverlapsPandas:
    result_optim = pb.count_overlaps(
        PD_COUNT_OVERLAPS_DF1,
        PD_COUNT_OVERLAPS_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        overlap_filter=FilterOp.Weak,
        naive_query=False,
    )

    result_naive = pb.count_overlaps(
        PD_COUNT_OVERLAPS_DF1,
        PD_COUNT_OVERLAPS_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        overlap_filter=FilterOp.Weak,
        naive_query=True,
    )

    def test_count_overlaps_count(self):
        assert len(self.result_optim) == len(PD_DF_COUNT_OVERLAPS)
        assert len(self.result_naive) == len(PD_DF_COUNT_OVERLAPS)

    def test_count_overlaps_schema_rows(self):
        result = self.result_optim.sort_values(
            by=list(self.result_optim.columns)
        ).reset_index(drop=True)
        result_naive = self.result_naive.sort_values(
            by=list(self.result_naive.columns)
        ).reset_index(drop=True)
        expected: pd.DataFrame = PD_DF_COUNT_OVERLAPS
        print(expected.dtypes)
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_naive, expected)


class TestMergePandas:
    result = pb.merge(
        PD_MERGE_DF,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_merge_count(self):
        assert len(self.result) == len(PD_DF_MERGE)

    def test_merge_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_MERGE
        pd.testing.assert_frame_equal(result, expected)
