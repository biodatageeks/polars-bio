import pandas as pd
import pyarrow as pa
import pytest
from _expected import (
    PD_COUNT_OVERLAPS_DF1,
    PD_COUNT_OVERLAPS_DF2,
    PD_DF_COUNT_OVERLAPS,
    PD_DF_MERGE,
    PD_DF_NEAREST,
    PD_DF_OVERLAP,
    PD_MERGE_DF,
    PD_NEAREST_DF1,
    PD_NEAREST_DF2,
    PD_OVERLAP_DF1,
    PD_OVERLAP_DF2,
)

import polars_bio as pb
from polars_bio.range_op_io import _df_to_reader

# Set coordinate system metadata on pandas DataFrames
# 1-based for overlap, nearest, count_overlaps (zero_based=False)
PD_OVERLAP_DF1.attrs["coordinate_system_zero_based"] = False
PD_OVERLAP_DF2.attrs["coordinate_system_zero_based"] = False
PD_NEAREST_DF1.attrs["coordinate_system_zero_based"] = False
PD_NEAREST_DF2.attrs["coordinate_system_zero_based"] = False
PD_COUNT_OVERLAPS_DF1.attrs["coordinate_system_zero_based"] = False
PD_COUNT_OVERLAPS_DF2.attrs["coordinate_system_zero_based"] = False
# 0-based for merge (zero_based=True)
PD_MERGE_DF.attrs["coordinate_system_zero_based"] = True


class TestOverlapPandas:
    result = pb.overlap(
        PD_OVERLAP_DF1,
        PD_OVERLAP_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_overlap_count(self):
        assert len(self.result) == len(PD_DF_OVERLAP)

    def test_overlap_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_OVERLAP
        pd.testing.assert_frame_equal(result, expected)


class TestNearestPandas:
    result = pb.nearest(
        PD_NEAREST_DF1,
        PD_NEAREST_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_nearest_count(self):
        assert len(self.result) == len(PD_DF_NEAREST)

    def test_nearest_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_NEAREST
        pd.testing.assert_frame_equal(result, expected)


class TestCountOverlapsPandas:
    result_optim = pb.count_overlaps(
        PD_COUNT_OVERLAPS_DF1,
        PD_COUNT_OVERLAPS_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        naive_query=False,
    )

    result_naive = pb.count_overlaps(
        PD_COUNT_OVERLAPS_DF1,
        PD_COUNT_OVERLAPS_DF2,
        cols1=("contig", "pos_start", "pos_end"),
        cols2=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
        naive_query=True,
    )

    def test_count_overlaps_count(self):
        assert len(self.result_optim) == len(PD_DF_COUNT_OVERLAPS)
        assert len(self.result_naive) == len(PD_DF_COUNT_OVERLAPS)

    def test_count_overlaps_schema_rows(self):
        result = self.result_optim.sort_values(
            by=list(self.result_optim.columns)
        ).reset_index(drop=True)
        result_naive = self.result_naive.sort_values(
            by=list(self.result_naive.columns)
        ).reset_index(drop=True)
        expected: pd.DataFrame = PD_DF_COUNT_OVERLAPS
        print(expected.dtypes)
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(result_naive, expected)


class TestMergePandas:
    result = pb.merge(
        PD_MERGE_DF,
        cols=("contig", "pos_start", "pos_end"),
        output_type="pandas.DataFrame",
    )

    def test_merge_count(self):
        assert len(self.result) == len(PD_DF_MERGE)

    def test_merge_schema_rows(self):
        result = self.result.sort_values(by=list(self.result.columns)).reset_index(
            drop=True
        )
        expected = PD_DF_MERGE
        pd.testing.assert_frame_equal(result, expected)


def test_pandas_dataframe_to_reader_uses_arrow_c_stream():
    if not hasattr(pd.DataFrame, "__arrow_c_stream__"):
        pytest.skip("pandas >= 3.0.0 is required for Arrow PyCapsule stream export")

    class TrackingDataFrame(pd.DataFrame):
        arrow_stream_called = False

        @property
        def _constructor(self):
            return TrackingDataFrame

        def __arrow_c_stream__(self, requested_schema=None, **kwargs):
            TrackingDataFrame.arrow_stream_called = True
            return pd.DataFrame.__arrow_c_stream__(
                self, requested_schema=requested_schema, **kwargs
            )

    for contig_values in (
        ["chr1", "chr2"],
        pd.Series(["chr1", "chr2"], dtype="string"),
    ):
        TrackingDataFrame.arrow_stream_called = False
        df = TrackingDataFrame(
            {
                "contig": contig_values,
                "pos_start": [1, 10],
                "pos_end": [5, 20],
            }
        )

        reader = _df_to_reader(df, "contig")
        batches = list(reader)

        assert TrackingDataFrame.arrow_stream_called
        assert len(batches) == 1
        assert pa.types.is_large_string(batches[0].schema.field("contig").type)
