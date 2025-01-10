from pathlib import Path

import polars as pl
from _expected import DF_OVER_PATH1, DF_OVER_PATH2, PL_DF_OVERLAP

import polars_bio as pb
from polars_bio import FilterOp

columns = ["contig", "pos_start", "pos_end"]


class TestStreaming:
    result_stream = pb.overlap(
        DF_OVER_PATH1,
        DF_OVER_PATH2,
        cols1=columns,
        cols2=columns,
        output_type="polars.LazyFrame",
        streaming=True,
        overlap_filter=FilterOp.Weak,
    )

    def test_plan(self):
        plan = str(self.result_stream.explain(streaming=True))
        assert "streaming" in plan.lower()

    def test_execute(self):
        file = "test.csv"
        file_path = Path("example.txt")
        file_path.unlink(missing_ok=True)
        result = self.result_stream
        assert len(result.collect(streaming=True)) == len(PL_DF_OVERLAP)
        result.sink_csv(file)
        expected = pl.read_csv(file)
        expected.equals(PL_DF_OVERLAP)
        file_path.unlink(missing_ok=True)
