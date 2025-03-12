import bioframe as bf
import pandas as pd
from _expected import BIO_DF_PATH1, BIO_DF_PATH2, BIO_PD_DF1, BIO_PD_DF2
from numpy import int32

import polars_bio as pb
from polars_bio.polars_bio import FilterOp

pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "true", False)


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
    result_overlap_lf = (
        pb.overlap(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="polars.LazyFrame",
            suffixes=("_1", "_3"),
            overlap_filter=FilterOp.Strict,
        )
        .collect()
        .to_pandas()
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
        naive_query=False,
    )

    result_count_overlaps_naive = pb.count_overlaps(
        BIO_DF_PATH1,
        BIO_DF_PATH2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        overlap_filter=FilterOp.Strict,
        naive_query=True,
    )

    result_bio_count_overlaps = bf.count_overlaps(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("", "_"),
    )

    result_merge = pb.merge(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )
    result_merge_lf = pb.merge(
        BIO_PD_DF1,
        cols=("contig", "pos_start", "pos_end"),
        output_type="polars.LazyFrame",
    )
    result_bio_merge = bf.merge(
        BIO_PD_DF1, cols=("contig", "pos_start", "pos_end"), min_dist=None
    ).astype(
        {"pos_start": "int32", "pos_end": "int32"}
    )  # bioframe changes input types

    def test_overlap_count(self):
        assert len(self.result_overlap) == len(self.result_bio_overlap)
        assert len(self.result_overlap_lf) == len(self.result_bio_overlap)

    def test_overlap_schema_rows(self):
        expected = self.result_bio_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        result = self.result_overlap.sort_values(
            by=list(self.result_overlap.columns)
        ).reset_index(drop=True)
        result_lf = self.result_overlap_lf.sort_values(
            by=list(self.result_overlap_lf.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_lf, expected)

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
        assert len(self.result_count_overlaps_naive.collect()) == len(
            self.result_bio_count_overlaps
        )

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
        result_naive = (
            self.result_count_overlaps_naive.collect()
            .to_pandas()
            .sort_values(
                by=list(self.result_count_overlaps_naive.collect_schema().names())
            )
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_naive, expected, check_dtype=True)

    def test_merge_count(self):
        assert len(self.result_merge) == len(self.result_bio_merge)
        assert len(self.result_merge_lf.collect()) == len(self.result_bio_merge)

    def test_merge_schema_rows(self):
        expected = self.result_bio_merge.sort_values(
            by=list(self.result_merge.columns)
        ).reset_index(drop=True)
        result = self.result_merge.sort_values(
            by=list(self.result_merge.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)

    def test_coverage_count(self):
        result = pb.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            overlap_filter=FilterOp.Strict,
        )
        result_bio = bf.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            suffixes=("_1", "_2"),
        )
        assert len(result) == len(result_bio)

    def test_coverage_schema_rows(self):
        result = pb.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            output_type="pandas.DataFrame",
            overlap_filter=FilterOp.Strict,
        )
        result_bio = bf.coverage(
            BIO_PD_DF1,
            BIO_PD_DF2,
            cols1=("contig", "pos_start", "pos_end"),
            cols2=("contig", "pos_start", "pos_end"),
            suffixes=("_1", "_2"),
        )
        expected = (
            result_bio.sort_values(by=list(result.columns))
            .reset_index(drop=True)
            .astype({"coverage": "int64"})
        )
        result = result.sort_values(by=list(result.columns)).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, expected)
