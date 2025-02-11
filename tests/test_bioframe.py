import bioframe as bf
import pandas as pd
from _expected import BIO_PD_DF1, BIO_PD_DF2
from numpy import int32

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

    result_nearest = pb.nearest(
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

    result_count_overlaps = pb.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Strict,
        output_type="pandas.DataFrame",
    )

    result_count_overlaps_naive = pb.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Strict,
        output_type="pandas.DataFrame",
        naive_query=True,
    )

    result_bio_count_overlaps = bf.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("", "_"),
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

    def test_nearest_count(self):
        assert len(self.result_nearest) == len(self.result_bio_nearest)

    def test_nearest_schema_rows(self):
        # since the find nearest is imprecisely defined (i.e. it can return any overlapping interval) in the case of multiple hits with the same distance
        # we will only compare pos_start_1, pos_end_1 and the distance
        expected = (
            self.result_bio_nearest.sort_values(
                by=list(["contig_1", "pos_start_1", "pos_end_1", "distance"])
            )
            .reset_index(drop=True)
            .astype({"pos_start_2": int32, "pos_end_2": int32, "distance": int})
        )
        expected = expected.drop(columns=["pos_start_2", "pos_end_2"])
        result = self.result_nearest.sort_values(
            by=list(["contig_1", "pos_start_1", "pos_end_1", "distance"])
        ).reset_index(drop=True)
        result = result.drop(columns=["pos_start_2", "pos_end_2"])
        pd.testing.assert_frame_equal(result, expected)

    def test_overlaps_count(self):
        assert len(self.result_count_overlaps) == len(self.result_bio_count_overlaps)
        # assert len(self.result_count_overlaps_naive) == len(self.result_bio_count_overlaps)

    def test_overlaps_schema_rows(self):
        expected = (
            self.result_bio_count_overlaps.sort_values(
                by=list(self.result_count_overlaps.columns)
            )
            .reset_index(drop=True)
            .astype({"count": int})
        )
        result = self.result_count_overlaps.sort_values(
            by=list(self.result_count_overlaps.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
