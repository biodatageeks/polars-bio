import json

import polars as pl
import pytest

from polars_bio.predicate_translator import UnsupportedPredicate, _emit_sql

GFF_STR = {"chrom", "source", "type", "strand"}
GFF_U32 = {"start", "end", "phase"}
GFF_F32 = {"score"}


def _ast(expr: pl.Expr) -> dict:
    return json.loads(expr.meta.serialize(format="json"))


def emit(expr: pl.Expr) -> str:
    return _emit_sql(_ast(expr), GFF_STR, GFF_U32, GFF_F32)


def test_string_eq():
    assert emit(pl.col("type") == "exon") == "(\"type\" = 'exon')"


def test_string_escaping():
    assert emit(pl.col("type") == "a'b") == "(\"type\" = 'a''b')"


def test_int_comparison_uses_dyn_literal():
    assert emit(pl.col("start") >= 10) == '("start" >= 10)'


def test_float_comparison():
    assert emit(pl.col("score") > 1.5) == '("score" > 1.5)'


def test_boolean_literal():
    assert emit(pl.col("strand") == True) == '("strand" = TRUE)'  # noqa: E712


def test_and_or_nesting():
    e = (pl.col("type") == "exon") & (pl.col("start") >= 10)
    assert emit(e) == '(("type" = \'exon\') AND ("start" >= 10))'
    e2 = (pl.col("type") == "exon") | (pl.col("type") == "gene")
    assert emit(e2) == "((\"type\" = 'exon') OR (\"type\" = 'gene'))"


def test_is_in_strings_decoded_from_arrow_blob():
    assert (
        emit(pl.col("chrom").is_in(["chr1", "chr2"]))
        == "(\"chrom\" IN ('chr1', 'chr2'))"
    )


def test_is_in_ints():
    assert emit(pl.col("start").is_in([1, 2, 3])) == '("start" IN (1, 2, 3))'


def test_not_and_isnull():
    assert emit(~(pl.col("type") == "exon")) == "(NOT (\"type\" = 'exon'))"
    assert emit(pl.col("type").is_null()) == '("type" IS NULL)'
    assert emit(pl.col("type").is_not_null()) == '("type" IS NOT NULL)'


def test_str_contains_is_unsupported():
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type").str.contains("exon"))


def test_string_column_ordering_is_unsupported():
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type") < "exon")


def test_unknown_node_raises():
    with pytest.raises(UnsupportedPredicate):
        _emit_sql({"NoSuchNode": 1}, GFF_STR, GFF_U32, GFF_F32)
