import pandas as pd
from _expected import (
    DF_NEAREST_PATH1,
    DF_NEAREST_PATH2,
    DF_OVER_PATH1,
    DF_OVER_PATH2,
    PD_DF_NEAREST,
    PD_DF_OVERLAP,
)

import polars_bio as pb


class TestOverlapNative:
    result_csv = pb.overlap(
        DF_OVER_PATH1, DF_OVER_PATH2, output_type="pandas.DataFrame"
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
    result = pb.nearest(
        DF_NEAREST_PATH1, DF_NEAREST_PATH2, output_type="pandas.DataFrame"
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