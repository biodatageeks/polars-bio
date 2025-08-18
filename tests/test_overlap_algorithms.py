import logging

import bioframe as bf
import pandas as pd
from _expected import BIO_PD_DF1, BIO_PD_DF2

import polars_bio as pb

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

pb.ctx.set_option("datafusion.execution.parquet.schema_force_view_types", "true", False)


class TestOverlapAlgorithms:
    result_bio_overlap = bf.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        how="inner",
    )

    result_overlap_coitrees = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="Coitrees",
    )

    result_overlap_lapper = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="Lapper",
    )

    result_overlap_it = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="IntervalTree",
    )

    result_overlap_ait = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="ArrayIntervalTree",
    )

    result_overlap_coitrees_log = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="Coitrees",
    )

    result_overlap_lapper_log = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="Lapper",
    )

    result_overlap_it_log = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="IntervalTree",
    )

    result_overlap_ait_log = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="ArrayIntervalTree",
    )

    result_overlap_superintervals = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="SuperIntervals",
    )

    result_overlap_superintervals_log = pb.overlap(
        BIO_PD_DF1,
        BIO_PD_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_3"),
        use_zero_based=True,
        algorithm="SuperIntervals",
    )

    expected = result_bio_overlap.sort_values(
        by=list(result_bio_overlap.columns)
    ).reset_index(drop=True)

    def test_overlap_count_coitrees(self):
        assert len(self.result_overlap_coitrees) == len(self.result_bio_overlap)

    def test_overlap_count_lapper(self):
        assert len(self.result_overlap_lapper) == len(self.result_bio_overlap)

    def test_overlap_count_its(self):
        assert len(self.result_overlap_it) == len(self.result_bio_overlap)

    def test_overlap_count_ait(self):
        assert len(self.result_overlap_ait) == len(self.result_bio_overlap)

    def test_overlap_count_superintervals(self):
        assert len(self.result_overlap_superintervals) == len(self.result_bio_overlap)

    def test_overlap_schema_rows_coitrees(self):
        result = self.result_overlap_coitrees.sort_values(
            by=list(self.result_overlap_coitrees.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, self.expected)

    def test_overlap_schema_rows_lapper(self):
        result_lapper = self.result_overlap_lapper.sort_values(
            by=list(self.result_overlap_lapper.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_lapper, self.expected)

    def test_overlap_schema_rows_it(self):
        result_it = self.result_overlap_it.sort_values(
            by=list(self.result_overlap_it.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_it, self.expected)

    def test_overlap_schema_rows_ait(self):
        result_ait = self.result_overlap_ait.sort_values(
            by=list(self.result_overlap_ait.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_ait, self.expected)

    def test_overlap_schema_rows_superintervals(self):
        result_superintervals = self.result_overlap_superintervals.sort_values(
            by=list(self.result_overlap_superintervals.columns)
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result_superintervals, self.expected)

    def test_overlap_schema_rows_it_log(self, caplog):
        caplog.set_level("INFO")
        self.result_overlap_it_log.count().collect()
        assert (
            "Optimizing into IntervalJoinExec using IntervalTree algorithm"
            in caplog.text
        )

    def test_overlap_schema_rows_ait_log(self, caplog):
        caplog.set_level("INFO")
        self.result_overlap_ait_log.count().collect()
        assert (
            "Optimizing into IntervalJoinExec using ArrayIntervalTree algorithm"
            in caplog.text
        )

    def test_overlap_schema_rows_coitrees_log(self, caplog):
        caplog.set_level("INFO")
        self.result_overlap_coitrees_log.count().collect()
        assert (
            "Optimizing into IntervalJoinExec using Coitrees algorithm" in caplog.text
        )

    def test_overlap_schema_rows_lapper_log(self, caplog):
        caplog.set_level("INFO")
        self.result_overlap_lapper_log.count().collect()
        assert "Optimizing into IntervalJoinExec using Lapper algorithm" in caplog.text

    def test_overlap_schema_rows_superintervals_log(self, caplog):
        caplog.set_level("INFO")
        self.result_overlap_superintervals_log.count().collect()
        assert (
            "Optimizing into IntervalJoinExec using SuperIntervals algorithm"
            in caplog.text
        )
