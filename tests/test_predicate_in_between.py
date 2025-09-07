import os

import polars as pl
import pytest

import polars_bio as pb
from polars_bio.io import _build_sql_where_from_predicate_safe


def test_sql_translation_in_and_between():
    # IN + equality
    in_pred = pl.col("chrom").is_in(["chr1", "chrY"]) & (pl.col("type") == "gene")
    where = _build_sql_where_from_predicate_safe(in_pred)
    assert '"chrom"' in where
    assert "IN (" in where
    assert "'chr1'" in where and "'chrY'" in where
    assert '"type" = ' in where

    # BETWEEN collapsing from >= and <=
    between_pred = (pl.col("start") >= 1000) & (pl.col("start") <= 2000)
    where2 = _build_sql_where_from_predicate_safe(between_pred)
    assert ("BETWEEN" in where2) or ('"start" >=' in where2 and '"start" <=' in where2)


@pytest.mark.skipif(
    not os.path.exists("tests/data/io/gff/gencode.v38.annotation.gff3.bgz"),
    reason="GFF sample file not available in this environment",
)
def test_in_between_pushdown_correctness():
    gff_path = "tests/data/io/gff/gencode.v38.annotation.gff3.bgz"

    # IN predicate correctness: pushdown vs no pushdown
    pred_in = pl.col("chrom").is_in(["chr1", "chrY"]) & (pl.col("type") == "gene")
    cols = ["chrom", "start", "end", "type"]

    df_push = (
        pb.scan_gff(gff_path, projection_pushdown=True, predicate_pushdown=True)
        .filter(pred_in)
        .select(cols)
        .collect()
    )
    df_no = (
        pb.scan_gff(gff_path, projection_pushdown=False, predicate_pushdown=False)
        .filter(pred_in)
        .select(cols)
        .collect()
    )
    assert df_push.shape == df_no.shape
    assert df_push.columns == df_no.columns

    # BETWEEN predicate correctness
    pred_between = (pl.col("start") >= 1000) & (pl.col("start") <= 200000)
    df_push2 = (
        pb.scan_gff(gff_path, projection_pushdown=True, predicate_pushdown=True)
        .filter(pred_between)
        .select(cols)
        .collect()
    )
    df_no2 = (
        pb.scan_gff(gff_path, projection_pushdown=False, predicate_pushdown=False)
        .filter(pred_between)
        .select(cols)
        .collect()
    )
    assert df_push2.shape == df_no2.shape
    assert df_push2.columns == df_no2.columns
