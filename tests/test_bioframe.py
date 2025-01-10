import bioframe as bf
import pandas as pd
from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb
from polars_bio.polars_bio import FilterOp


class TestBioframe:
    result_overlap = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        overlap_filter=FilterOp.Strict,
    )
    result_overlap_lf = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="polars.LazyFrame",
        suffixes=("_1", "_3"),
        overlap_filter=FilterOp.Strict,
    )

    result_bio_overlap = bf.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        how="inner",
    )

    resust_nearest = pb.nearest(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Strict,
        output_type="pandas.DataFrame",
    )
    result_bio_nearest = bf.closest(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
    )

    def test_overlap_count(self):
        assert len(self.result_overlap) == len(self.result_bio_overlap)
        assert len(self.result_overlap_lf.collect()) == len(self.result_bio_overlap)

    def test_overlap_schema_rows(self):
        expected = self.result_bio_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        result = self.result_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
