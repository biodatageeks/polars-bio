"""Safely translate supported GFF Polars predicates to DataFusion SQL."""

from __future__ import annotations

import json

import polars as pl

from .predicate_translator import (
    GFF_FLOAT32_COLUMNS,
    GFF_STRING_COLUMNS,
    GFF_UINT32_COLUMNS,
    plan_predicate_pushdown,
)


class SqlPredicateBuildError(Exception):
    """A predicate cannot be translated without changing its meaning."""


_COMPARISON_OPS = {"Eq", "NotEq", "Lt", "LtEq", "Gt", "GtEq"}
_MAX_VALIDATION_DEPTH = 200


def _is_boolean_expression(node: dict, depth: int = 0) -> bool:
    """Conservatively recognize boolean AST nodes, including logical operators."""
    if depth > _MAX_VALIDATION_DEPTH or not isinstance(node, dict) or len(node) != 1:
        return False

    key, body = next(iter(node.items()))
    if key == "Literal":
        wrapper = body.get("Scalar") or body.get("Dyn")
        return isinstance(wrapper, dict) and "Boolean" in wrapper
    if key == "BinaryExpr":
        op = body.get("op")
        if op in _COMPARISON_OPS:
            return True
        if op in ("And", "Or"):
            return _is_boolean_expression(
                body.get("left"), depth + 1
            ) and _is_boolean_expression(body.get("right"), depth + 1)
        return False
    if key == "Function":
        function = body.get("function")
        inputs = body.get("input", [])
        if not isinstance(function, dict) or "Boolean" not in function:
            return False
        boolean_fn = function["Boolean"]
        if boolean_fn in ("IsNull", "IsNotNull"):
            return len(inputs) == 1
        if boolean_fn == "Not":
            return len(inputs) == 1 and _is_boolean_expression(inputs[0], depth + 1)
        if isinstance(boolean_fn, dict) and "IsIn" in boolean_fn:
            return len(inputs) == 2
    return False


def _validate_logical_operators(node: dict, depth: int = 0) -> None:
    """Reject ambiguous bitwise nodes before the shared emitter maps them to SQL."""
    if depth > _MAX_VALIDATION_DEPTH:
        raise SqlPredicateBuildError("predicate nesting too deep")
    if not isinstance(node, dict) or len(node) != 1:
        raise SqlPredicateBuildError("malformed predicate AST")

    key, body = next(iter(node.items()))
    if key == "BinaryExpr":
        op = body.get("op")
        if op in ("And", "Or") and not _is_boolean_expression(node):
            raise SqlPredicateBuildError(
                f"{op} may be integer bitwise operation; refusing SQL translation"
            )
        _validate_logical_operators(body.get("left"), depth + 1)
        _validate_logical_operators(body.get("right"), depth + 1)
    elif key == "Function":
        function = body.get("function")
        inputs = body.get("input", [])
        if (
            isinstance(function, dict)
            and function.get("Boolean") == "Not"
            and not _is_boolean_expression(node)
        ):
            raise SqlPredicateBuildError(
                "Boolean Not may be integer bitwise inversion; refusing SQL translation"
            )
        for child in inputs:
            _validate_logical_operators(child, depth + 1)


def polars_predicate_to_sql(predicate: pl.Expr) -> str:
    """Return SQL only when the complete predicate has a faithful translation.

    Polars uses the same serialized ``And``/``Or``/``Not`` nodes for boolean
    logic and integer bitwise operations. Validate those nodes first, then use
    the shared structured translator rather than parsing ``str(predicate)``.
    """
    if not isinstance(predicate, pl.Expr):
        raise SqlPredicateBuildError("predicate must be a Polars expression")
    try:
        ast = json.loads(predicate.meta.serialize(format="json"))
        _validate_logical_operators(ast)
        plan = plan_predicate_pushdown(
            predicate,
            string_cols=GFF_STRING_COLUMNS,
            uint32_cols=GFF_UINT32_COLUMNS,
            float32_cols=GFF_FLOAT32_COLUMNS,
        )
    except SqlPredicateBuildError:
        raise
    except Exception as exc:
        raise SqlPredicateBuildError(f"could not translate predicate: {exc}") from exc

    if not plan.fully_translated or not plan.pushdown_sql:
        raise SqlPredicateBuildError(
            "predicate is not fully supported for SQL pushdown"
        )
    return plan.pushdown_sql
