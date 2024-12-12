import bioframe as bf
import pandas as pd
from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb
from polars_bio import OverlapFilter


class TestOverlapBioframe:
    result = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        output_type="pandas.DataFrame",
        overlap_filter=OverlapFilter.Strict,
    )
    result_bio = bf.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
        how="inner",
    )

    def test_overlap_count(self):
        assert len(self.result) == 54246
        assert len(self.result) == len(self.result_bio)

    def test_overlap_schema_rows(self):
        expected = self.result_bio.sort_values(
            by=list(self.result.columns)
        ).reset_index(drop=True)
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        pd.testing.assert_frame_equal(result, expected)
