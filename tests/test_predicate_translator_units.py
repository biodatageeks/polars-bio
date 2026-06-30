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


def test_string_column_ordering_on_right_operand_is_unsupported():
    # Regression: the string-column ordering guard must apply regardless of
    # which side the column lands on (pl.lit("exon") > pl.col("type")).
    with pytest.raises(UnsupportedPredicate):
        emit(pl.lit("exon") > pl.col("type"))


def test_is_in_with_null_is_unsupported():
    # Regression (Codex P2): SQL `IN (NULL)` does not match Polars' null-aware
    # is_in semantics, so a list containing None must stay client-side.
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type").is_in(["chr1", None]))
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type").is_in([None]))


def test_is_in_empty_list_is_false():
    # Empty is_in is all-False in Polars, which SQL FALSE matches faithfully.
    assert emit(pl.col("type").is_in([])) == "FALSE"


def test_not_with_empty_inputs_raises():
    with pytest.raises(UnsupportedPredicate):
        _emit_sql(
            {"Function": {"function": {"Boolean": "Not"}, "input": []}},
            GFF_STR,
            GFF_U32,
            GFF_F32,
        )


def test_flatten_and_handles_depth_beyond_recursion_limit():
    # _flatten_and must be iterative: a synthetic AST nested far deeper than
    # Python's recursion limit must flatten without RecursionError. Build the
    # dict directly (constructing it is not recursive) to isolate _flatten_and
    # from json.loads' own depth limit.
    from polars_bio.predicate_translator import _flatten_and

    leaf = {"Column": "start"}
    node = leaf
    depth = 5000
    for _ in range(depth):
        node = {"BinaryExpr": {"left": node, "op": "And", "right": leaf}}

    conjuncts = _flatten_and(node)
    assert len(conjuncts) == depth + 1
    assert all(c == leaf for c in conjuncts)


def test_emit_sql_deep_or_raises_unsupported_not_recursionerror():
    # Regression (Codex P2): a very deep OR tree must degrade to UnsupportedPredicate
    # (-> client-side fallback), never leak a RecursionError that crashes collect().
    from polars_bio.predicate_translator import _emit_sql

    leaf = {
        "BinaryExpr": {
            "left": {"Column": "start"},
            "op": "Eq",
            "right": {"Literal": {"Dyn": {"Int": 0}}},
        }
    }
    node = leaf
    for _ in range(1500):
        node = {"BinaryExpr": {"left": node, "op": "Or", "right": leaf}}
    with pytest.raises(UnsupportedPredicate):
        _emit_sql(node, GFF_STR, GFF_U32, GFF_F32)


def test_plan_deep_or_degrades_gracefully():
    # End-to-end: a deep OR predicate must not raise RecursionError out of the
    # planner; it either returns (pushdown skipped) or raises the wrapped
    # PredicateTranslationError, both of which the caller handles as a fallback.
    import functools

    expr = functools.reduce(
        lambda a, b: a | b, [pl.col("start") == i for i in range(1500)]
    )
    try:
        p = plan_predicate_pushdown(
            expr, string_cols=GFF_STR, uint32_cols=GFF_U32, float32_cols=GFF_F32
        )
    except PredicateTranslationError:
        return
    assert p.fully_translated is False


def test_deeply_nested_and_degrades_gracefully():
    # An end-to-end chain deep enough that json.loads itself may exceed the
    # interpreter's recursion limit must never leak a RecursionError: the
    # planner converts any serialize/decode failure into a safe client-side
    # fallback (PredicateTranslationError), and a translatable depth is fully
    # pushed. Either outcome is correct; a crash is not.
    expr = pl.col("start") >= 0
    for i in range(2000):
        expr = expr & (pl.col("start") >= i)
    try:
        p = plan_predicate_pushdown(
            expr, string_cols=GFF_STR, uint32_cols=GFF_U32, float32_cols=GFF_F32
        )
    except PredicateTranslationError:
        return  # serialize/json.loads hit the limit -> safe fallback path
    assert p.fully_translated is True


def test_unknown_node_raises():
    with pytest.raises(UnsupportedPredicate):
        _emit_sql({"NoSuchNode": 1}, GFF_STR, GFF_U32, GFF_F32)


from polars_bio.predicate_translator import (
    PredicateTranslationError,
    PushdownPlan,
    plan_predicate_pushdown,
)


def plan(expr):
    return plan_predicate_pushdown(
        expr, string_cols=GFF_STR, uint32_cols=GFF_U32, float32_cols=GFF_F32
    )


def test_plan_full_translation_certified():
    p = plan((pl.col("type") == "transcript") & (pl.col("start") >= 10))
    assert p.fully_translated is True
    assert p.pushdown_sql == '("type" = \'transcript\') AND ("start" >= 10)'


def test_plan_partial_pushes_translatable_conjunct_only():
    # str.contains is unsupported -> only the type== conjunct is pushed,
    # and fully_translated must be False so callers reapply the full predicate.
    p = plan(
        (pl.col("type") == "transcript")
        & pl.col("gene_biotype").str.contains("pseudogene")
    )
    assert p.pushdown_sql == "(\"type\" = 'transcript')"
    assert p.fully_translated is False


def test_plan_nothing_translatable():
    p = plan(pl.col("gene_biotype").str.contains("pseudogene"))
    assert p.pushdown_sql is None
    assert p.fully_translated is False


def test_plan_or_is_atomic_not_split():
    # An OR with one unsupported side must push NOTHING (can't split an OR).
    p = plan(
        (pl.col("type") == "exon") | pl.col("gene_biotype").str.contains("pseudogene")
    )
    assert p.pushdown_sql is None
    assert p.fully_translated is False
