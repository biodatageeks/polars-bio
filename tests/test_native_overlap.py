import pandas as pd
from _expected import DF_PATH1, DF_PATH2, PD_DF_OVERLAP

import polars_bio as pb


class TestOverlapNative:
    result_csv = pb.overlap(DF_PATH1, DF_PATH2, output_type="pandas.DataFrame")

    def test_overlap_count(self):
        assert len(self.result_csv) == 16

    def test_overlap_schema_rows(self):
        result_csv = self.result_csv.sort_values(
            by=list(self.result_csv.columns)
        ).reset_index(drop=True)
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result_csv, expected)
