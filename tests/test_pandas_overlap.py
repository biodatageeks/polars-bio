import pandas as pd

import polars_bio as pb
from _expected import DF_OVERLAP, DF1, DF2






class TestOverlapPandas:
    def test_overlap_count(self):
        assert len(pb.overlap(DF1, DF2, output_type="pandas.DataFrame")) == 16

    def test_overlap_schema_rows(self):
        result = pb.overlap(DF1, DF2, output_type="pandas.DataFrame")
        result = result.sort_values(by=list(result.columns)).reset_index(drop=True)
        expected = DF_OVERLAP
        pd.testing.assert_frame_equal(result, expected)




