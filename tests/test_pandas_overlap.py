import pandas as pd
from _expected import PD_DF1, PD_DF2, PD_DF_OVERLAP

import polars_bio as pb


class TestOverlapPandas:
    result = pb.overlap(PD_DF1, PD_DF2, output_type="pandas.DataFrame")

    def test_overlap_count(self):
        assert len(self.result) == 16

    def test_overlap_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result, expected)
