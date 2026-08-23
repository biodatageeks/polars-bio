from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_bio as pb
from polars_bio import get_metadata
from polars_bio.context import ctx
from polars_bio.polars_bio import (
    BigBedReadOptions,
    BigWigReadOptions,
    InputFormat,
    ReadOptions,
    py_read_table,
    py_register_table,
)

DATA_DIR = Path(__file__).parent / "data" / "io" / "bbi"
BIGWIG = str(DATA_DIR / "signal.bw")
LARGE_BIGWIG = str(DATA_DIR / "large_signal.bw")
BIGBED = str(DATA_DIR / "annotations.bb")
TARGET_PARTITIONS = "datafusion.execution.target_partitions"


@contextmanager
def _target_partitions(partitions: int):
    original = pb.get_option(TARGET_PARTITIONS)
    pb.set_option(TARGET_PARTITIONS, str(partitions))
    try:
        yield
    finally:
        pb.set_option(TARGET_PARTITIONS, original)


def _find_exec(plan, node_name: str):
    if plan.display().lstrip().startswith(f"{node_name}:"):
        return plan
    for child in plan.children():
        result = _find_exec(child, node_name)
        if result is not None:
            return result
    return None


def _bbi_execution_plan(format_name: str, partitions: int):
    if format_name == "bigwig":
        path = LARGE_BIGWIG
        input_format = InputFormat.BigWig
        options = ReadOptions(bigwig_read_options=BigWigReadOptions(zero_based=True))
    else:
        path = BIGBED
        input_format = InputFormat.BigBed
        options = ReadOptions(
            bigbed_read_options=BigBedReadOptions(zero_based=True, schema="auto")
        )

    table = py_register_table(
        ctx,
        path,
        f"parallel_{format_name}_{partitions}",
        input_format,
        options,
    )
    return py_read_table(ctx, table.name).execution_plan()


@pytest.fixture(scope="module")
def serial_bbi_frames():
    with _target_partitions(1):
        return {
            "bigwig": pb.read_bigwig(LARGE_BIGWIG, use_zero_based=True).sort(
                ["chrom", "start", "end"]
            ),
            "bigbed": pb.read_bigbed(BIGBED, use_zero_based=True).sort(
                ["chrom", "start", "end"]
            ),
        }


def test_bigwig_bigbed_public_api_exports():
    assert pb.read_bigwig is pb.data_input.read_bigwig
    assert pb.scan_bigwig is pb.data_input.scan_bigwig
    assert pb.read_bigbed is pb.data_input.read_bigbed
    assert pb.scan_bigbed is pb.data_input.scan_bigbed
    assert pb.register_bigwig is pb.data_processing.register_bigwig
    assert pb.register_bigbed is pb.data_processing.register_bigbed
    assert InputFormat.BigWig is not None
    assert InputFormat.BigBed is not None


def test_bigwig_bigbed_read_options_are_carried_by_read_options():
    bigwig = BigWigReadOptions(zero_based=False)
    bigbed = BigBedReadOptions(zero_based=True, schema="rest")
    options = ReadOptions(bigwig_read_options=bigwig, bigbed_read_options=bigbed)

    assert options.bigwig_read_options.zero_based is False
    assert options.bigbed_read_options.zero_based is True
    assert options.bigbed_read_options.schema == "rest"


def test_bigbed_rejects_unknown_schema_mode_before_io():
    with pytest.raises(ValueError, match="schema"):
        pb.scan_bigbed("missing.bb", schema="wide")


def test_read_bigwig_fixture():
    df = pb.read_bigwig(BIGWIG, use_zero_based=True).sort(["chrom", "start"])

    assert df.schema == {
        "chrom": pl.String,
        "start": pl.UInt32,
        "end": pl.UInt32,
        "value": pl.Float32,
    }
    assert df.select(["chrom", "start", "end"]).rows() == [
        ("chr1", 0, 10),
        ("chr1", 20, 30),
        ("chr2", 5, 12),
    ]
    assert df["value"].to_list() == [1.5, 2.5, 3.5]
    assert get_metadata(df)["format"] == "bigwig"


def test_scan_bigwig_projection_and_coordinate_conversion():
    df = (
        pb.scan_bigwig(BIGWIG, use_zero_based=False)
        .select(["chrom", "start"])
        .sort(["chrom", "start"])
        .collect()
    )

    assert df.columns == ["chrom", "start"]
    assert df.rows() == [("chr1", 1), ("chr1", 21), ("chr2", 6)]


def test_read_bigbed_autosql_fixture():
    df = pb.read_bigbed(BIGBED, use_zero_based=True).sort(["chrom", "start"])

    assert df.select(["chrom", "start", "end", "name", "score"]).rows() == [
        ("chr1", 0, 10, "gene1", 42),
        ("chr1", 20, 30, "gene2", 84),
        ("chr2", 5, 12, "gene3", 126),
    ]
    assert get_metadata(df)["format"] == "bigbed"


def test_scan_bigbed_rest_schema_and_filter_path():
    df = (
        pb.scan_bigbed(BIGBED, schema="rest", use_zero_based=True)
        .filter(pl.col("chrom") == "chr2")
        .select(["chrom", "start", "end", "rest"])
        .collect()
    )

    assert df.rows() == [("chr2", 5, 12, "gene3\t126")]


def test_bbi_predicate_pushdown_matches_client_side_filtering():
    predicate = (pl.col("chrom") == "chr2") & (pl.col("start") < 10)

    pushed = (
        pb.scan_bigwig(BIGWIG, predicate_pushdown=True, use_zero_based=True)
        .filter(predicate)
        .sort(["chrom", "start"])
        .collect()
    )
    client_side = (
        pb.scan_bigwig(BIGWIG, predicate_pushdown=False, use_zero_based=True)
        .filter(predicate)
        .sort(["chrom", "start"])
        .collect()
    )
    assert pushed.equals(client_side)

    pushed = (
        pb.scan_bigbed(BIGBED, predicate_pushdown=True, use_zero_based=True)
        .filter(predicate)
        .sort(["chrom", "start"])
        .collect()
    )
    client_side = (
        pb.scan_bigbed(BIGBED, predicate_pushdown=False, use_zero_based=True)
        .filter(predicate)
        .sort(["chrom", "start"])
        .collect()
    )
    assert pushed.equals(client_side)


def test_scan_bigbed_projection_and_coordinate_conversion():
    df = (
        pb.scan_bigbed(BIGBED, use_zero_based=False)
        .select(["chrom", "start"])
        .sort(["chrom", "start"])
        .collect()
    )

    assert df.columns == ["chrom", "start"]
    assert df.rows() == [("chr1", 1), ("chr1", 21), ("chr2", 6)]


@pytest.mark.parametrize("partitions", range(1, 9), ids=lambda value: f"t{value}")
@pytest.mark.parametrize(
    ("format_name", "node_name"),
    [("bigwig", "BigWigExec"), ("bigbed", "BigBedExec")],
)
def test_bbi_parallel_full_scan_matches_serial_without_duplicate_or_missing_rows(
    serial_bbi_frames, format_name: str, node_name: str, partitions: int
):
    with _target_partitions(partitions):
        plan = _bbi_execution_plan(format_name, partitions)
        if format_name == "bigwig":
            actual = pb.read_bigwig(LARGE_BIGWIG, use_zero_based=True)
            expected_partitions = partitions
        else:
            actual = pb.read_bigbed(BIGBED, use_zero_based=True)
            expected_partitions = min(partitions, 2)

    execution = _find_exec(plan, node_name)
    assert execution is not None, f"{node_name} node not found in execution plan"
    assert execution.partition_count == expected_partitions
    actual = actual.sort(["chrom", "start", "end"])
    assert_frame_equal(actual, serial_bbi_frames[format_name])


def test_register_bigwig_sql_path():
    pb.register_bigwig(BIGWIG, "test_bigwig_reg", use_zero_based=True)
    df = pb.sql(
        "SELECT chrom, start, `end`, value FROM test_bigwig_reg ORDER BY chrom, start"
    ).collect()

    assert df.select(["chrom", "start", "end"]).rows() == [
        ("chr1", 0, 10),
        ("chr1", 20, 30),
        ("chr2", 5, 12),
    ]
    assert df["value"].to_list() == [1.5, 2.5, 3.5]


def test_register_bigbed_sql_path():
    pb.register_bigbed(BIGBED, "test_bigbed_reg", use_zero_based=True)
    df = pb.sql(
        "SELECT chrom, start, `end`, name, score FROM test_bigbed_reg "
        "ORDER BY chrom, start"
    ).collect()

    assert df.select(["chrom", "start", "end", "name", "score"]).rows() == [
        ("chr1", 0, 10, "gene1", 42),
        ("chr1", 20, 30, "gene2", 84),
        ("chr2", 5, 12, "gene3", 126),
    ]
