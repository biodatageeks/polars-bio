from _expected import (
    PL_COUNT_OVERLAPS_DF1,
    PL_COUNT_OVERLAPS_DF2,
    PL_DF1,
    PL_DF2,
    PL_DF_COUNT_OVERLAPS,
    PL_DF_MERGE,
    PL_DF_NEAREST,
    PL_DF_OVERLAP,
    PL_MERGE_DF,
    PL_NEAREST_DF1,
    PL_NEAREST_DF2,
)

import polars_bio as pb
from polars_bio.polars_bio import FilterOp


class TestOverlapPolars:
    result_frame = pb.overlap(
        PL_DF1,
        PL_DF2,
        output_type="polars.DataFrame",
        overlap_filter=FilterOp.Weak,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
    )
    result_lazy = pb.overlap(
        PL_DF1,
        PL_DF2,
        output_type="polars.LazyFrame",
        overlap_filter=FilterOp.Weak,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
    ).collect()
    expected = PL_DF_OVERLAP

    def test_overlap_count(self):
        assert len(self.result_frame) == len(PL_DF_OVERLAP)
        assert len(self.result_lazy) == len(PL_DF_OVERLAP)

    def test_overlap_schema_rows(self):
        result = self.result_frame.sort(by=self.result_frame.columns)
        assert self.expected.equals(result)

    def test_overlap_schema_rows_lazy(self):
        result = self.result_lazy.sort(by=self.result_lazy.columns)
        assert self.expected.equals(result)


class TestNearestPolars:
    result_frame = pb.nearest(
        PL_NEAREST_DF1,
        PL_NEAREST_DF2,
        output_type="polars.DataFrame",
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
    )
    result_lazy = pb.nearest(
        PL_NEAREST_DF1,
        PL_NEAREST_DF2,
        output_type="polars.LazyFrame",
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
    ).collect()
    expected = PL_DF_NEAREST

    def test_nearest_count(self):
        assert len(self.result_frame) == len(PL_DF_NEAREST)
        assert len(self.result_lazy) == len(PL_DF_NEAREST)

    def test_nearest_schema_rows(self):
        result = self.result_frame.sort(by=self.result_frame.columns)
        assert self.expected.equals(result)

    def test_nearest_schema_rows_lazy(self):
        result = self.result_lazy.sort(by=self.result_lazy.columns)
        assert self.expected.equals(result)


class TestCountOverlapsPolars:
    result_frame = pb.count_overlaps(
        PL_COUNT_OVERLAPS_DF1,
        PL_COUNT_OVERLAPS_DF2,
        output_type="polars.DataFrame",
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Weak,
        naive_query=False,
    )
    result_lazy = pb.count_overlaps(
        PL_COUNT_OVERLAPS_DF1,
        PL_COUNT_OVERLAPS_DF2,
        output_type="polars.LazyFrame",
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Weak,
        naive_query=False,
    ).collect()
    expected = PL_DF_COUNT_OVERLAPS

    def test_count_overlaps_count(self):
        assert len(self.result_frame) == len(PL_DF_COUNT_OVERLAPS)
        assert len(self.result_lazy) == len(PL_DF_COUNT_OVERLAPS)

    def test_count_overlaps_schema_rows(self):
        result = self.result_frame.sort(by=self.result_frame.columns)
        assert self.expected.equals(result)

    def test_count_overlaps_schema_rows_lazy(self):
        result = self.result_lazy.sort(by=self.result_lazy.columns)
        assert self.expected.equals(result)


class TestMergePolars:
    result_frame = pb.merge(
        PL_MERGE_DF,
        output_type="polars.DataFrame",
        cols=("contig", "pos_start", "pos_end"),
    )
    result_lazy = pb.merge(
        PL_MERGE_DF,
        output_type="polars.LazyFrame",
        cols=("contig", "pos_start", "pos_end"),
    ).collect()
    expected = PL_DF_MERGE

    def test_merge_count(self):
        assert len(self.result_frame) == len(PL_DF_MERGE)
        assert len(self.result_lazy) == len(PL_DF_MERGE)

    def test_merge_schema_rows(self):
        result = self.result_frame.sort(by=self.result_frame.columns)
        assert self.expected.equals(result)

    def test_merge_schema_rows_lazy(self):
        result = self.result_lazy.sort(by=self.result_lazy.columns)
        assert self.expected.equals(result)
