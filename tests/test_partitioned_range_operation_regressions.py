from __future__ import annotations

import warnings
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb

COLUMNS = ["contig", "pos_start", "pos_end"]
VIEW_COLUMNS = ["chrom", "start", "end"]
TARGET_PARTITIONS_KEY = "datafusion.execution.target_partitions"
BATCH_SIZE_KEY = "datafusion.execution.batch_size"
OPTION_DEFAULTS = {
    TARGET_PARTITIONS_KEY: "1",
    BATCH_SIZE_KEY: "8192",
    "datafusion.bio.coordinate_system_check": "false",
    "datafusion.bio.coordinate_system_zero_based": "false",
}

EXPECTED_MERGE = pl.DataFrame(
    {
        "contig": ["chr1"],
        "pos_start": [0],
        "pos_end": [30],
        "n_intervals": [3],
    }
)

EXPECTED_COMPLEMENT = pl.DataFrame(
    {
        "contig": ["chr1"],
        "pos_start": [30],
        "pos_end": [40],
    }
)

EXPECTED_SUBTRACT = pl.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1"],
        "pos_start": [0, 10, 25],
        "pos_end": [5, 20, 30],
    }
)

EXPECTED_CLUSTER = pl.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1"],
        "pos_start": [0, 8, 20],
        "pos_end": [10, 25, 30],
        "cluster": [0, 0, 0],
        "cluster_start": [0, 0, 0],
        "cluster_end": [30, 30, 30],
    }
)


@dataclass(frozen=True)
class PartitionedParquetCase:
    left_path: str
    right_path: str
    left_df: pl.DataFrame
    right_df: pl.DataFrame
    left_lazy: pl.LazyFrame
    right_lazy: pl.LazyFrame
    view_df: pl.DataFrame


def _with_zero_based_metadata(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    df.config_meta.set(coordinate_system_zero_based=True)
    return df


def _sorted(df: pl.DataFrame) -> pl.DataFrame:
    if not df.columns:
        return df
    return df.sort(df.columns)


def _assert_same_rows(expected: pl.DataFrame, actual: pl.DataFrame) -> None:
    pl.testing.assert_frame_equal(_sorted(expected), _sorted(actual))


@contextmanager
def _session_option(key: str, value: str):
    original = pb.get_option(key)
    pb.set_option(key, value)
    try:
        yield
    finally:
        restored = original if original is not None else OPTION_DEFAULTS.get(key)
        if restored is None:
            raise AssertionError(f"No restore value configured for option: {key}")
        pb.set_option(key, restored)


@contextmanager
def _partitioned_range_options(
    target_partitions: int,
    *,
    batch_size: int | None = None,
):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Coordinate system metadata is missing.*",
            category=UserWarning,
        )
        with ExitStack() as stack:
            stack.enter_context(
                _session_option(TARGET_PARTITIONS_KEY, str(target_partitions))
            )
            stack.enter_context(
                _session_option("datafusion.bio.coordinate_system_check", "false")
            )
            stack.enter_context(
                _session_option("datafusion.bio.coordinate_system_zero_based", "true")
            )
            if batch_size is not None:
                stack.enter_context(_session_option(BATCH_SIZE_KEY, str(batch_size)))
            yield


def _write_partitioned_parquet_case(tmp_path: Path) -> PartitionedParquetCase:
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_dir.mkdir()
    right_dir.mkdir()

    left_partitions = [
        pl.DataFrame(
            {
                "contig": ["chr1", "chr1"],
                "pos_start": [0, 20],
                "pos_end": [10, 30],
            }
        ),
        pl.DataFrame(
            {
                "contig": ["chr1"],
                "pos_start": [8],
                "pos_end": [25],
            }
        ),
    ]
    right_partitions = [
        pl.DataFrame(
            {
                "contig": ["chr1"],
                "pos_start": [5],
                "pos_end": [10],
            }
        ),
        pl.DataFrame(
            {
                "contig": ["chr1"],
                "pos_start": [20],
                "pos_end": [25],
            }
        ),
    ]

    for index, frame in enumerate(left_partitions):
        frame.write_parquet(left_dir / f"part-{index:02d}.parquet")
    for index, frame in enumerate(right_partitions):
        frame.write_parquet(right_dir / f"part-{index:02d}.parquet")

    left_glob = str(left_dir / "*.parquet")
    right_glob = str(right_dir / "*.parquet")

    left_df = _with_zero_based_metadata(pl.concat(left_partitions, rechunk=True))
    right_df = _with_zero_based_metadata(pl.concat(right_partitions, rechunk=True))
    left_lazy = _with_zero_based_metadata(pl.scan_parquet(left_glob))
    right_lazy = _with_zero_based_metadata(pl.scan_parquet(right_glob))
    view_df = _with_zero_based_metadata(
        pl.DataFrame(
            {
                "chrom": ["chr1"],
                "start": [0],
                "end": [40],
            }
        )
    )

    return PartitionedParquetCase(
        left_path=left_glob,
        right_path=right_glob,
        left_df=left_df,
        right_df=right_df,
        left_lazy=left_lazy,
        right_lazy=right_lazy,
        view_df=view_df,
    )


def _run_merge(case: PartitionedParquetCase, input_mode: str) -> pl.DataFrame:
    if input_mode == "dataframe":
        return pb.merge(case.left_df, cols=COLUMNS, output_type="polars.DataFrame")
    if input_mode == "path":
        return pb.merge(case.left_path, cols=COLUMNS, output_type="polars.DataFrame")
    if input_mode == "lazy":
        return pb.merge(
            case.left_lazy,
            cols=COLUMNS,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")
    raise ValueError(f"Unsupported input mode: {input_mode}")


def _run_complement(case: PartitionedParquetCase, input_mode: str) -> pl.DataFrame:
    if input_mode == "dataframe":
        return pb.complement(
            case.left_df,
            view_df=case.view_df,
            cols=COLUMNS,
            view_cols=VIEW_COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "path":
        return pb.complement(
            case.left_path,
            view_df=case.view_df,
            cols=COLUMNS,
            view_cols=VIEW_COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "lazy":
        return pb.complement(
            case.left_lazy,
            view_df=case.view_df,
            cols=COLUMNS,
            view_cols=VIEW_COLUMNS,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")
    raise ValueError(f"Unsupported input mode: {input_mode}")


def _run_subtract(case: PartitionedParquetCase, input_mode: str) -> pl.DataFrame:
    if input_mode == "dataframe":
        return pb.subtract(
            case.left_df,
            case.right_df,
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "path":
        return pb.subtract(
            case.left_path,
            case.right_path,
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "lazy":
        return pb.subtract(
            case.left_lazy,
            case.right_lazy,
            cols1=COLUMNS,
            cols2=COLUMNS,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")
    raise ValueError(f"Unsupported input mode: {input_mode}")


def _run_cluster(case: PartitionedParquetCase, input_mode: str) -> pl.DataFrame:
    if input_mode == "dataframe":
        return pb.cluster(
            case.left_df,
            cols=COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "path":
        return pb.cluster(
            case.left_path,
            cols=COLUMNS,
            output_type="polars.DataFrame",
        )
    if input_mode == "lazy":
        return pb.cluster(
            case.left_lazy,
            cols=COLUMNS,
            output_type="polars.LazyFrame",
        ).collect(engine="streaming")
    raise ValueError(f"Unsupported input mode: {input_mode}")


@pytest.mark.parametrize(
    ("name", "runner", "expected"),
    [
        ("merge", _run_merge, EXPECTED_MERGE),
        ("complement", _run_complement, EXPECTED_COMPLEMENT),
        ("subtract", _run_subtract, EXPECTED_SUBTRACT),
        ("cluster", _run_cluster, EXPECTED_CLUSTER),
    ],
    ids=["merge", "complement", "subtract", "cluster"],
)
def test_partitioned_regression_case_matches_expected_rows_for_single_partition_dataframes(
    tmp_path: Path,
    name: str,
    runner,
    expected: pl.DataFrame,
):
    case = _write_partitioned_parquet_case(tmp_path)

    with _partitioned_range_options(1):
        result = runner(case, "dataframe")

    assert result.height == expected.height, (
        f"{name} single-partition dataframe control produced the wrong row count: "
        f"expected {expected.height}, got {result.height}"
    )
    _assert_same_rows(expected, result)


@pytest.mark.parametrize(
    ("name", "runner", "expected"),
    [
        ("merge", _run_merge, EXPECTED_MERGE),
        ("complement", _run_complement, EXPECTED_COMPLEMENT),
        ("subtract", _run_subtract, EXPECTED_SUBTRACT),
        ("cluster", _run_cluster, EXPECTED_CLUSTER),
    ],
    ids=["merge", "complement", "subtract", "cluster"],
)
@pytest.mark.parametrize("input_mode", ["path", "lazy"], ids=["path", "lazyframe"])
@pytest.mark.parametrize("target_partitions", [2, 4], ids=["tp2", "tp4"])
def test_partitioned_multifile_range_operations_preserve_global_semantics(
    tmp_path: Path,
    name: str,
    runner,
    expected: pl.DataFrame,
    input_mode: str,
    target_partitions: int,
):
    case = _write_partitioned_parquet_case(tmp_path)
    batch_size = 1 if name == "cluster" or input_mode == "lazy" else None

    with _partitioned_range_options(target_partitions, batch_size=batch_size):
        result = runner(case, input_mode)

    assert result.height == expected.height, (
        f"{name} row count changed when target_partitions={target_partitions} "
        f"for {input_mode}: expected {expected.height}, got {result.height}"
    )
    _assert_same_rows(expected, result)
