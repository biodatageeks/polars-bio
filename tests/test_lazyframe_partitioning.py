from __future__ import annotations

import warnings
from contextlib import contextmanager

import polars as pl
import pytest
from _expected import (
    DF_COUNT_OVERLAPS_PATH1,
    DF_COUNT_OVERLAPS_PATH2,
    DF_COVERAGE_PATH1,
    DF_COVERAGE_PATH2,
    DF_MERGE_PATH,
    DF_NEAREST_PATH1,
    DF_NEAREST_PATH2,
    DF_OVER_PATH1,
    DF_OVER_PATH2,
)

import polars_bio as pb
from polars_bio.polars_bio import py_debug_arrow_stream_partition_count
from polars_bio.range_op_io import _prepare_lazy_stream_input

COLUMNS = ["contig", "pos_start", "pos_end"]
TARGET_PARTITIONS_KEY = "datafusion.execution.target_partitions"
BATCH_SIZE_KEY = "datafusion.execution.batch_size"


def _read_csv_with_metadata(path: str, zero_based: bool) -> pl.DataFrame:
    df = pl.read_csv(path)
    df.config_meta.set(coordinate_system_zero_based=zero_based)
    return df


def _scan_csv_with_metadata(path: str, zero_based: bool) -> pl.LazyFrame:
    lf = pl.scan_csv(path)
    lf.config_meta.set(coordinate_system_zero_based=zero_based)
    return lf


@contextmanager
def _target_partitions(partitions: int):
    with _session_option(TARGET_PARTITIONS_KEY, str(partitions)):
        yield


@contextmanager
def _session_option(key: str, value: str):
    original = pb.get_option(key)
    pb.set_option(key, value)
    try:
        yield
    finally:
        pb.set_option(key, original or "1")


def _sorted(df: pl.DataFrame) -> pl.DataFrame:
    if not df.columns:
        return df
    return df.sort(df.columns)


def _assert_same_rows(expected: pl.DataFrame, actual: pl.DataFrame) -> None:
    pl.testing.assert_frame_equal(_sorted(expected), _sorted(actual))


def _build_overlap_case():
    df1 = _read_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
    expected = pb.overlap(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.overlap(
            _scan_csv_with_metadata(DF_OVER_PATH1, zero_based=False),
            _scan_csv_with_metadata(DF_OVER_PATH2, zero_based=False),
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_mixed_lazy_dataframe_overlap_case(left_lazy: bool):
    df1 = _read_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
    expected = pb.overlap(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        left = (
            _scan_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
            if left_lazy
            else df1
        )
        right = (
            df2
            if left_lazy
            else _scan_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
        )
        return pb.overlap(
            left,
            right,
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_mixed_lazy_path_overlap_case(left_lazy: bool):
    df1 = _read_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
    expected = pb.overlap(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        left = (
            _scan_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
            if left_lazy
            else DF_OVER_PATH1
        )
        right = (
            DF_OVER_PATH2
            if left_lazy
            else _scan_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
        )
        return pb.overlap(
            left,
            right,
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_nearest_case():
    df1 = _read_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False)
    expected = pb.nearest(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.nearest(
            _scan_csv_with_metadata(DF_NEAREST_PATH1, zero_based=False),
            _scan_csv_with_metadata(DF_NEAREST_PATH2, zero_based=False),
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_count_overlaps_case():
    df1 = _read_csv_with_metadata(DF_COUNT_OVERLAPS_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_COUNT_OVERLAPS_PATH2, zero_based=False)
    expected = pb.count_overlaps(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.count_overlaps(
            _scan_csv_with_metadata(DF_COUNT_OVERLAPS_PATH1, zero_based=False),
            _scan_csv_with_metadata(DF_COUNT_OVERLAPS_PATH2, zero_based=False),
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_coverage_case():
    df1 = _read_csv_with_metadata(DF_COVERAGE_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_COVERAGE_PATH2, zero_based=False)
    expected = pb.coverage(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.coverage(
            _scan_csv_with_metadata(DF_COVERAGE_PATH1, zero_based=False),
            _scan_csv_with_metadata(DF_COVERAGE_PATH2, zero_based=False),
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_merge_case():
    df = _read_csv_with_metadata(DF_MERGE_PATH, zero_based=True)
    expected = pb.merge(
        df,
        cols=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.merge(
            _scan_csv_with_metadata(DF_MERGE_PATH, zero_based=True),
            cols=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_cluster_case():
    df = _read_csv_with_metadata(DF_MERGE_PATH, zero_based=True)
    expected = pb.cluster(
        df,
        cols=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.cluster(
            _scan_csv_with_metadata(DF_MERGE_PATH, zero_based=True),
            cols=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_complement_case():
    df = _read_csv_with_metadata(DF_MERGE_PATH, zero_based=True)
    view_df = (
        df.group_by("contig")
        .agg(
            pl.col("pos_start").min().alias("start"),
            pl.col("pos_end").max().alias("end"),
        )
        .rename({"contig": "chrom"})
    )
    view_df.config_meta.set(coordinate_system_zero_based=True)

    expected = pb.complement(
        df,
        view_df=view_df,
        cols=COLUMNS,
        view_cols=["chrom", "start", "end"],
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.complement(
            _scan_csv_with_metadata(DF_MERGE_PATH, zero_based=True),
            view_df=view_df,
            cols=COLUMNS,
            view_cols=["chrom", "start", "end"],
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _build_subtract_case():
    df1 = _read_csv_with_metadata(DF_OVER_PATH1, zero_based=False)
    df2 = _read_csv_with_metadata(DF_OVER_PATH2, zero_based=False)
    expected = pb.subtract(
        df1,
        df2,
        cols1=COLUMNS,
        cols2=COLUMNS,
        output_type="polars.DataFrame",
    )

    def factory() -> pl.LazyFrame:
        return pb.subtract(
            _scan_csv_with_metadata(DF_OVER_PATH1, zero_based=False),
            _scan_csv_with_metadata(DF_OVER_PATH2, zero_based=False),
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        )

    return expected, factory


def _lazy_input_partition_count(path: str, zero_based: bool) -> int:
    lazyframe = _scan_csv_with_metadata(path, zero_based=zero_based)
    schema, stream_factory = _prepare_lazy_stream_input(
        lazyframe, COLUMNS[0], batch_size=2
    )
    return py_debug_arrow_stream_partition_count(pb.ctx, stream_factory(), schema)


@pytest.mark.parametrize(
    ("path", "zero_based"),
    [
        (DF_OVER_PATH1, False),
        (DF_MERGE_PATH, True),
    ],
    ids=["binary-input", "unary-input"],
)
@pytest.mark.parametrize("target_partitions", [1, 4])
def test_lazy_arrow_stream_partition_count_matches_target_partitions(
    path: str,
    zero_based: bool,
    target_partitions: int,
):
    with _target_partitions(target_partitions):
        partition_count = _lazy_input_partition_count(path, zero_based=zero_based)

    assert partition_count == target_partitions


@pytest.mark.parametrize(
    "builder",
    [
        _build_overlap_case,
        _build_nearest_case,
        _build_count_overlaps_case,
        _build_coverage_case,
        _build_merge_case,
        _build_cluster_case,
        _build_complement_case,
        _build_subtract_case,
    ],
    ids=[
        "overlap",
        "nearest",
        "count_overlaps",
        "coverage",
        "merge",
        "cluster",
        "complement",
        "subtract",
    ],
)
def test_lazy_range_operations_preserve_results_across_partitions(builder):
    expected, lazy_factory = builder()

    with _target_partitions(1):
        result_single = lazy_factory().collect(engine="streaming")

    with _target_partitions(4):
        result_partitioned = lazy_factory().collect(engine="streaming")

    _assert_same_rows(expected, result_single)
    _assert_same_rows(expected, result_partitioned)
    _assert_same_rows(result_single, result_partitioned)


@pytest.mark.parametrize(
    "left_lazy", [True, False], ids=["lazy+dataframe", "dataframe+lazy"]
)
def test_partitioned_lazy_stream_supports_mixed_lazy_and_dataframe_inputs(left_lazy):
    expected, lazy_factory = _build_mixed_lazy_dataframe_overlap_case(left_lazy)

    with _target_partitions(1):
        result_single = lazy_factory().collect(engine="streaming")

    with _target_partitions(4):
        result_partitioned = lazy_factory().collect(engine="streaming")

    _assert_same_rows(expected, result_single)
    _assert_same_rows(expected, result_partitioned)
    _assert_same_rows(result_single, result_partitioned)


@pytest.mark.parametrize("left_lazy", [True, False], ids=["lazy+path", "path+lazy"])
def test_partitioned_lazy_stream_supports_mixed_lazy_and_path_inputs(left_lazy):
    expected, lazy_factory = _build_mixed_lazy_path_overlap_case(left_lazy)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Coordinate system metadata is missing.*",
            category=UserWarning,
        )
        with (
            _session_option("datafusion.bio.coordinate_system_check", "false"),
            _session_option("datafusion.bio.coordinate_system_zero_based", "false"),
            _target_partitions(1),
        ):
            result_single = lazy_factory().collect(engine="streaming")

        with (
            _session_option("datafusion.bio.coordinate_system_check", "false"),
            _session_option("datafusion.bio.coordinate_system_zero_based", "false"),
            _target_partitions(4),
        ):
            result_partitioned = lazy_factory().collect(engine="streaming")

    _assert_same_rows(expected, result_single)
    _assert_same_rows(expected, result_partitioned)
    _assert_same_rows(result_single, result_partitioned)


@pytest.mark.parametrize(
    "builder", [_build_overlap_case, _build_merge_case], ids=["overlap", "merge"]
)
def test_partitioned_lazy_stream_supports_prefix_consumption(builder):
    _, lazy_factory = builder()

    with _target_partitions(4), _session_option(BATCH_SIZE_KEY, "1"):
        result = lazy_factory().head(1).collect(engine="streaming")

    assert len(result) == 1
