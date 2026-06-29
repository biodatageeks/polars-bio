"""
Polars to DataFusion predicate translator for bio-format table providers.

This module converts Polars expressions to DataFusion expressions for predicate pushdown optimization.
Uses the DataFusion Python DataFrame API instead of SQL string construction for better type safety.

Supports BAM/SAM/CRAM, VCF, and GFF formats. Each format defines its own known column types;
unknown columns (BAM tags, VCF INFO/FORMAT fields, GFF attribute fields) are handled permissively,
allowing all operators and letting DataFusion type-check at execution time.
"""

from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Per-format column type definitions
# ---------------------------------------------------------------------------

# GFF
GFF_STRING_COLUMNS = {"chrom", "source", "type", "strand"}
GFF_UINT32_COLUMNS = {"start", "end", "phase"}
GFF_FLOAT32_COLUMNS = {"score"}
GFF_STATIC_COLUMNS = (
    GFF_STRING_COLUMNS | GFF_UINT32_COLUMNS | GFF_FLOAT32_COLUMNS | {"attributes"}
)

# GTF (identical schema to GFF)
GTF_STRING_COLUMNS = GFF_STRING_COLUMNS
GTF_UINT32_COLUMNS = GFF_UINT32_COLUMNS
GTF_FLOAT32_COLUMNS = GFF_FLOAT32_COLUMNS

# BAM / SAM / CRAM
BAM_STRING_COLUMNS = {
    "name",
    "chrom",
    "cigar",
    "mate_chrom",
    "sequence",
    "quality_scores",
}
BAM_UINT32_COLUMNS = {"start", "end", "flags", "mapping_quality", "mate_start"}
BAM_INT32_COLUMNS = {"template_length"}

# VCF
VCF_STRING_COLUMNS = {"chrom", "ref", "alt"}
VCF_UINT32_COLUMNS = {"start"}

# Pairs (Hi-C)
PAIRS_STRING_COLUMNS = {"readID", "chr1", "chr2", "strand1", "strand2"}
PAIRS_UINT32_COLUMNS = {"pos1", "pos2"}
PAIRS_FLOAT32_COLUMNS: set = set()

# BigWig
BIGWIG_STRING_COLUMNS = {"chrom"}
BIGWIG_UINT32_COLUMNS = {"start", "end"}
BIGWIG_FLOAT32_COLUMNS = {"value"}

# BigBed
# Only the universally-present columns are typed statically. BigBed autoSQL
# schemas are dynamic — fields beyond the BED3 core (score, strand, thickStart,
# blockCount, …) vary per file and may be redefined by a file's autoSQL, so they
# are intentionally left out of these sets. Such columns fall through to the
# permissive path (all operators allowed, DataFusion type-checks at execution),
# rather than risking incorrect static coercion.
BIGBED_STRING_COLUMNS = {"chrom", "name", "rest"}
BIGBED_UINT32_COLUMNS = {"start", "end"}
BIGBED_FLOAT32_COLUMNS: set = set()


class PredicateTranslationError(Exception):
    """Raised when a Polars predicate cannot be translated to DataFusion expression."""

    pass


# ---------------------------------------------------------------------------
# Structured AST emitter (issue #396): walks expr.meta.serialize(format="json")
# node-by-node. Unsupported nodes raise loudly instead of silently vanishing.
# ---------------------------------------------------------------------------
import json as _json

_COMPARISON_SQL = {
    "Eq": "=",
    "NotEq": "!=",
    "Lt": "<",
    "LtEq": "<=",
    "Gt": ">",
    "GtEq": ">=",
}
_STRING_ALLOWED_OPS = {"Eq", "NotEq"}


class UnsupportedPredicate(Exception):
    """A node that cannot be faithfully translated to SQL for pushdown."""


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sql_string(s) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _sql_scalar(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if value is None:
        return "NULL"
    return _sql_string(value)


def _column_name(node: dict) -> str:
    if isinstance(node, dict) and "Column" in node:
        return str(node["Column"])
    raise UnsupportedPredicate(f"expected a column, got: {node!r}")


def _emit_literal(body: dict) -> str:
    wrapper = body.get("Scalar")
    if wrapper is None:
        wrapper = body.get("Dyn")
    if not isinstance(wrapper, dict) or len(wrapper) != 1:
        raise UnsupportedPredicate(f"unsupported literal: {body!r}")
    kind, value = next(iter(wrapper.items()))
    if kind == "String":
        return _sql_string(value)
    if kind == "Boolean":
        return "TRUE" if value else "FALSE"
    if kind == "Int":
        return str(int(value))
    if kind == "Float":
        return repr(float(value))
    raise UnsupportedPredicate(f"unsupported literal kind: {kind}")


def _decode_is_in_list(list_node: dict) -> list:
    try:
        blob = list_node["Literal"]["Scalar"]["List"]
    except (KeyError, TypeError):
        raise UnsupportedPredicate("is_in value is not a list literal")
    import pyarrow as pa

    raw = bytes(b & 0xFF for b in blob)
    try:
        table = pa.ipc.open_stream(raw).read_all()
    except Exception as exc:  # decode failure -> safe client-side fallback
        raise UnsupportedPredicate(f"could not decode is_in list: {exc}")
    return table.column(0).to_pylist()


def _emit_is_in(inputs: list) -> str:
    if len(inputs) != 2:
        raise UnsupportedPredicate("is_in expects exactly two inputs")
    col_sql = _quote_ident(_column_name(inputs[0]))
    values = _decode_is_in_list(inputs[1])
    if not values:
        # Polars is_in([]) is uniformly False; SQL FALSE matches it faithfully.
        return "FALSE"
    if any(v is None for v in values):
        # SQL `IN (..., NULL)` yields UNKNOWN (not FALSE) for non-matching rows,
        # which diverges from Polars' null-aware is_in. Keep it client-side.
        raise UnsupportedPredicate("is_in list contains NULL; pushdown unsafe")
    items = ", ".join(_sql_scalar(v) for v in values)
    return f"({col_sql} IN ({items}))"


def _emit_function(body: dict, string_cols, uint32_cols, float32_cols) -> str:
    fn = body.get("function")
    inputs = body.get("input", [])
    if isinstance(fn, dict) and "Boolean" in fn:
        boolean = fn["Boolean"]
        if isinstance(boolean, dict) and "IsIn" in boolean:
            return _emit_is_in(inputs)
        if boolean in ("Not", "IsNull", "IsNotNull") and not inputs:
            raise UnsupportedPredicate(f"{boolean} with no input")
        if boolean == "Not":
            inner = _emit_sql(inputs[0], string_cols, uint32_cols, float32_cols)
            return f"(NOT {inner})"
        if boolean == "IsNull":
            return f"({_quote_ident(_column_name(inputs[0]))} IS NULL)"
        if boolean == "IsNotNull":
            return f"({_quote_ident(_column_name(inputs[0]))} IS NOT NULL)"
    raise UnsupportedPredicate(f"unsupported function: {fn!r}")


def _validate_comparison(left: dict, op: str, string_cols) -> None:
    if (
        string_cols
        and isinstance(left, dict)
        and left.get("Column") in string_cols
        and op not in _STRING_ALLOWED_OPS
    ):
        raise UnsupportedPredicate(
            f"ordering op {op} not allowed on string column {left.get('Column')!r}"
        )


def _emit_sql(node: dict, string_cols, uint32_cols, float32_cols) -> str:
    if not isinstance(node, dict) or len(node) != 1:
        raise UnsupportedPredicate(f"unexpected node: {node!r}")
    key, body = next(iter(node.items()))
    if key == "Column":
        return _quote_ident(body)
    if key == "Literal":
        return _emit_literal(body)
    if key == "BinaryExpr":
        op = body["op"]
        if op in ("And", "Or"):
            left = _emit_sql(body["left"], string_cols, uint32_cols, float32_cols)
            right = _emit_sql(body["right"], string_cols, uint32_cols, float32_cols)
            joiner = "AND" if op == "And" else "OR"
            return f"({left} {joiner} {right})"
        if op in _COMPARISON_SQL:
            # Guard both operands: a string column may sit on either side
            # (e.g. pl.lit("a") > pl.col("chrom")).
            _validate_comparison(body["left"], op, string_cols)
            _validate_comparison(body["right"], op, string_cols)
            left = _emit_sql(body["left"], string_cols, uint32_cols, float32_cols)
            right = _emit_sql(body["right"], string_cols, uint32_cols, float32_cols)
            return f"({left} {_COMPARISON_SQL[op]} {right})"
        raise UnsupportedPredicate(f"unsupported binary op: {op}")
    if key == "Function":
        return _emit_function(body, string_cols, uint32_cols, float32_cols)
    raise UnsupportedPredicate(f"unsupported node type: {key}")


@dataclass
class PushdownPlan:
    pushdown_sql: Optional[str]
    fully_translated: bool


def _is_and(node) -> bool:
    return (
        isinstance(node, dict)
        and "BinaryExpr" in node
        and node["BinaryExpr"].get("op") == "And"
    )


def _flatten_and(node: dict) -> list:
    # Iterative (not recursive): deep `&` chains can exceed Python's recursion
    # limit. Left-to-right order is preserved via the explicit stack.
    conjuncts = []
    stack = [node]
    while stack:
        current = stack.pop()
        if _is_and(current):
            body = current["BinaryExpr"]
            stack.append(body["right"])
            stack.append(body["left"])
        else:
            conjuncts.append(current)
    return conjuncts


def plan_predicate_pushdown(
    predicate,
    *,
    string_cols=None,
    uint32_cols=None,
    float32_cols=None,
) -> PushdownPlan:
    try:
        ast = _json.loads(predicate.meta.serialize(format="json"))
    except Exception as exc:  # serialization itself failed -> caller falls back
        raise PredicateTranslationError(
            f"could not serialize predicate: {exc}"
        ) from exc

    conjuncts = _flatten_and(ast)
    translated = []
    all_ok = True
    for conjunct in conjuncts:
        try:
            translated.append(
                _emit_sql(conjunct, string_cols, uint32_cols, float32_cols)
            )
        except UnsupportedPredicate:
            all_ok = False

    pushdown_sql = " AND ".join(translated) if translated else None
    return PushdownPlan(pushdown_sql=pushdown_sql, fully_translated=all_ok)
