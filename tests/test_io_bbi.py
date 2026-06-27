from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb
from polars_bio import get_metadata
from polars_bio.polars_bio import (
    BigBedReadOptions,
    BigWigReadOptions,
    InputFormat,
    ReadOptions,
)

DATA_DIR = Path(__file__).parent / "data" / "io" / "bbi"
BIGWIG = str(DATA_DIR / "signal.bw")
BIGBED = str(DATA_DIR / "annotations.bb")


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
