from _expected import (
    PL_DF1,
    PL_DF2,
    PL_DF_NEAREST,
    PL_DF_OVERLAP,
    PL_NEAREST_DF1,
    PL_NEAREST_DF2,
)

import polars_bio as pb


class TestOverlapPolars:
    result_frame = pb.overlap(PL_DF1, PL_DF2, output_type="polars.DataFrame")
    result_lazy = pb.overlap(PL_DF1, PL_DF2, output_type="polars.LazyFrame").collect()
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
        PL_NEAREST_DF1, PL_NEAREST_DF2, output_type="polars.DataFrame"
    )
    result_lazy = pb.nearest(
        PL_NEAREST_DF1, PL_NEAREST_DF2, output_type="polars.LazyFrame"
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