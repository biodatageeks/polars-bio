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


import logging

from polars_bio.pushdown import apply_predicate_pushdown, apply_projection_pushdown

LOG = logging.getLogger("test")
GFF = {"string_cols": {"type"}, "uint32_cols": {"start"}, "float32_cols": {"score"}}


class _StubDF:
    """Records calls; configurable to raise on binding."""

    def __init__(self, raise_on=None):
        self.raise_on = raise_on or set()
        self.filtered = None
        self.selected = None

    def parse_sql_expr(self, s):
        if "parse" in self.raise_on:
            raise ValueError("boom parse")
        return ("expr", s)

    def filter(self, native):
        if "filter" in self.raise_on:
            raise ValueError("boom filter")
        self.filtered = native
        return self

    def select(self, *exprs):
        if "select" in self.raise_on:
            raise ValueError("boom select")
        self.selected = exprs
        return self


def test_predicate_fully_translated_skips_client_filter():
    df = _StubDF()
    out, needs_client = apply_predicate_pushdown(
        df, pl.col("type") == "exon", GFF, log=LOG
    )
    assert needs_client is False
    assert df.filtered is not None


def test_predicate_partial_forces_client_filter():
    df = _StubDF()
    pred = (pl.col("type") == "exon") & pl.col("type").str.contains("ex")
    out, needs_client = apply_predicate_pushdown(df, pred, GFF, log=LOG)
    assert needs_client is True  # reapply the full predicate
    assert df.filtered is not None  # the translatable conjunct was still pushed


def test_predicate_bind_failure_forces_client_filter():
    df = _StubDF(raise_on={"parse"})
    out, needs_client = apply_predicate_pushdown(
        df, pl.col("type") == "exon", GFF, log=LOG
    )
    assert needs_client is True
    assert df.filtered is None  # nothing pushed


def test_predicate_none_is_noop():
    df = _StubDF()
    out, needs_client = apply_predicate_pushdown(df, None, GFF, log=LOG)
    assert needs_client is False
    assert df.filtered is None


def test_projection_complete_skips_client_select():
    df = _StubDF()
    out, needs_select = apply_projection_pushdown(
        df, [pl.col("chrom"), pl.col("start")], log=LOG
    )
    assert needs_select is False
    assert df.selected is not None


def test_projection_bind_failure_forces_client_select():
    df = _StubDF(raise_on={"parse"})
    out, needs_select = apply_projection_pushdown(df, [pl.col("chrom")], log=LOG)
    assert needs_select is True


def test_projection_none_is_noop():
    df = _StubDF()
    out, needs_select = apply_projection_pushdown(df, None, log=LOG)
    assert needs_select is False
