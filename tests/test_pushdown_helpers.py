import polars as pl

from polars_bio.pushdown import extract_source_columns


def test_plain_columns():
    cols, complete = extract_source_columns([pl.col("chrom"), pl.col("start")])
    assert cols == ["chrom", "start"]
    assert complete is True


def test_string_names():
    cols, complete = extract_source_columns(["chrom", "start"])
    assert cols == ["chrom", "start"]
    assert complete is True


def test_aliased_projection_uses_source_name():
    cols, complete = extract_source_columns([pl.col("gene_id").alias("g")])
    assert cols == ["gene_id"]
    assert complete is True


def test_computed_projection_uses_root_name():
    cols, complete = extract_source_columns([(pl.col("start") + 1).alias("s1")])
    assert cols == ["start"]
    assert complete is True


def test_single_expr_not_in_list():
    cols, complete = extract_source_columns(pl.col("chrom"))
    assert cols == ["chrom"]
    assert complete is True


def test_none_returns_empty_complete():
    cols, complete = extract_source_columns(None)
    assert cols == []
    assert complete is True
