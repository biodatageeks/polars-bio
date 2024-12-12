from _expected import PL_DF1, PL_DF2, PL_DF_OVERLAP

import polars_bio as pb


class TestOverlapPolars:
    result_frame = pb.overlap(PL_DF1, PL_DF2, output_type="polars.DataFrame")
    result_lazy = pb.overlap(PL_DF1, PL_DF2, output_type="polars.LazyFrame").collect()
    expected = PL_DF_OVERLAP

    def test_overlap_count(self):
        assert len(self.result_frame) == 16
        assert len(self.result_lazy) == 16

    def test_overlap_schema_rows(self):
        result = self.result_frame.sort(by=self.result_frame.columns)
        assert self.expected.equals(result)

    def test_overlap_schema_rows_lazy(self):
        result = self.result_lazy.sort(by=self.result_lazy.columns)
        assert self.expected.equals(result)
