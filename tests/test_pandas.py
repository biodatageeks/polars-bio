import pandas as pd
from _expected import (
    PD_DF_NEAREST,
    PD_DF_OVERLAP,
    PD_NEAREST_DF1,
    PD_NEAREST_DF2,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
)

import polars_bio as pb


class TestOverlapPandas:
    result = pb.overlap(PD_OVERLAP_DF1, PD_OVERLAP_DF2, output_type="pandas.DataFrame")

    def test_overlap_count(self):
        assert len(self.result) == len(PD_DF_OVERLAP)

    def test_overlap_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result, expected)


class TestNearestPandas:
    result = pb.nearest(PD_NEAREST_DF1, PD_NEAREST_DF2, output_type="pandas.DataFrame")

    def test_nearest_count(self):
        assert len(self.result) == len(PD_DF_NEAREST)

    def test_nearest_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_NEAREST
        pd.testing.assert_frame_equal(result, expected)
