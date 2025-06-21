from pathlib import Path

import bioframe as bf
import polars as pl
from _expected import (
    DF_COUNT_OVERLAPS_PATH1,
    DF_COUNT_OVERLAPS_PATH2,
    DF_NEAREST_PATH1,
    DF_NEAREST_PATH2,
    DF_OVER_PATH1,
    DF_OVER_PATH2,
    PD_COVERAGE_DF1,
    PD_COVERAGE_DF2,
    PL_DF_COUNT_OVERLAPS,
    PL_DF_NEAREST,
    PL_DF_OVERLAP,
)

import polars_bio as pb
from polars_bio import FilterOp

columns = ["contig", "pos_start", "pos_end"]


class TestStreaming:
    result_overlap_stream = pb.overlap(
        DF_OVER_PATH1,
        DF_OVER_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        streaming=True,
        use_zero_based=False,
    )

    result_nearest_stream = pb.nearest(
        DF_NEAREST_PATH1,
        DF_NEAREST_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        streaming=True,
        use_zero_based=False,
    )

    result_count_overlaps_stream = pb.count_overlaps(
        DF_COUNT_OVERLAPS_PATH1,
        DF_COUNT_OVERLAPS_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        streaming=True,
        use_zero_based=False,
    )

    result_coverage_stream = pb.coverage(
        DF_COUNT_OVERLAPS_PATH1,
        DF_COUNT_OVERLAPS_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        streaming=True,
        use_zero_based=False,
    )

    result_coverage_bio = bf.coverage(
        PD_COVERAGE_DF1,
        PD_COVERAGE_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        suffixes=("_1", "_2"),
    )

    def test_overlap_plan(self):
        plan = str(self.result_overlap_stream.explain(streaming=True))
        assert "streaming" in plan.lower()

    def test_nearest_plan(self):
        plan = str(self.result_nearest_stream.explain(streaming=True))
        assert "streaming" in plan.lower()

    def test_count_overlaps_plan(self):
        plan = str(self.result_count_overlaps_stream.explain(streaming=True))
        assert "streaming" in plan.lower()

    def test_coverage_plan(self):
        plan = str(self.result_coverage_stream.explain(streaming=True))
        assert "streaming" in plan.lower()

    def test_overlap_execute(self):
        file = "test_overlap.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_overlap_stream
        assert len(result.collect(streaming=True)) == len(PL_DF_OVERLAP)
        result.sink_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_OVERLAP)
        file_path.unlink(missing_ok=True)

    def test_nearest_execute(self):
        file = "test_nearest.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_nearest_stream
        assert len(result.collect(streaming=True)) == len(PL_DF_NEAREST)
        result.sink_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_NEAREST)
        file_path.unlink(missing_ok=True)

    def test_count_overlaps_execute(self):
        file = "test_count_over.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_count_overlaps_stream
        assert len(result.collect(streaming=True)) == len(PL_DF_COUNT_OVERLAPS)
        result.sink_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_COUNT_OVERLAPS)
        file_path.unlink(missing_ok=True)

    def test_coverage_execute(self):
        file = "test_cov.csv"
        file_path = Path(file)
        file_path.unlink(missing_ok=True)
        result = self.result_coverage_stream
        assert len(result.collect(streaming=True)) == len(self.result_coverage_bio)
        result.sink_csv(file)
        expected = pl.read_csv(file).to_pandas()
        expected.equals(self.result_coverage_bio)
        file_path.unlink(missing_ok=True)
