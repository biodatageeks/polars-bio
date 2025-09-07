"""
Polars predicate -> SQL WHERE builder for DataFusion.

Supports GFF pushdown operators:
- Strings (chrom, source, type, strand, attribute fields): =, !=, IN, NOT IN
- UInt32 (start, end, phase): =, !=, <, <=, >, >=, BETWEEN
- Float32 (score): same as numeric
- AND combinations
- IS NULL / IS NOT NULL
"""

from __future__ import annotations

import re
from typing import Any, List, Tuple

import polars as pl

GFF_STRING_COLUMNS = {"chrom", "source", "type", "strand"}
GFF_UINT32_COLUMNS = {"start", "end", "phase"}
GFF_FLOAT32_COLUMNS = {"score"}
GFF_STATIC_COLUMNS = (
    GFF_STRING_COLUMNS | GFF_UINT32_COLUMNS | GFF_FLOAT32_COLUMNS | {"attributes"}
)


class SqlPredicateBuildError(Exception):
    pass


def polars_predicate_to_sql(predicate: pl.Expr) -> str:
    expr_str = str(predicate)

    # Binary comparisons
    if _is_binary_expr(expr_str):
        return _translate_binary_expr(expr_str)

    # AND combinations
    if _is_and_expr(expr_str):
        return _translate_and_expr(expr_str)

    # IN / NOT IN
    if _is_in_expr(expr_str):
        return _translate_in_expr(expr_str)
    if _is_not_in_expr(expr_str):
        return _translate_not_in_expr(expr_str)

    # BETWEEN via range combination
    if _is_between_expr(expr_str):
        return _translate_between_expr(expr_str)

    # IS NULL / IS NOT NULL
    if _is_not_null_expr(expr_str):
        return _translate_not_null_expr(expr_str)
    if _is_null_expr(expr_str):
        return _translate_null_expr(expr_str)

    raise SqlPredicateBuildError(f"Unsupported predicate: {expr_str}")


def _is_binary_expr(expr_str: str) -> bool:
    return any(op in expr_str for op in [" == ", " != ", " <= ", " >= ", " < ", " > "])


def _translate_binary_expr(expr_str: str) -> str:
    patterns: List[Tuple[str, str]] = [
        (r"(.+?)\s==\s(.+)", "="),
        (r"(.+?)\s!=\s(.+)", "!="),
        (r"(.+?)\s<=\s(.+)", "<="),
        (r"(.+?)\s>=\s(.+)", ">="),
        (r"(.+?)\s<\s(.+)", "<"),
        (r"(.+?)\s>\s(.+)", ">"),
    ]
    for pattern, op in patterns:
        m = re.search(pattern, expr_str)
        if m:
            left = m.group(1).strip()
            right = m.group(2).strip()
            col = _extract_column_name(left)
            lit = _extract_sql_literal(right)
            _validate_column_operator(col, op)
            return f'"{col}" {op} {lit}'
    raise SqlPredicateBuildError(f"Cannot parse binary expr: {expr_str}")


def _is_and_expr(expr_str: str) -> bool:
    return " & " in expr_str


def _translate_and_expr(expr_str: str) -> str:
    parts = _split_on(expr_str, " & ")
    if len(parts) != 2:
        raise SqlPredicateBuildError(f"Cannot parse AND expression: {expr_str}")
    left = polars_predicate_to_sql(_mock_expr(parts[0]))
    right = polars_predicate_to_sql(_mock_expr(parts[1]))
    return f"({left}) AND ({right})"


def _is_in_expr(expr_str: str) -> bool:
    return ".is_in([" in expr_str


def _translate_in_expr(expr_str: str) -> str:
    m = re.search(r"(.+?)\.is_in\(\[(.+?)\]\)", expr_str)
    if not m:
        raise SqlPredicateBuildError(f"Cannot parse IN expr: {expr_str}")
    col_part = m.group(1).strip()
    vals_part = m.group(2).strip()
    col = _extract_column_name(col_part)
    _validate_column_operator(col, "IN")
    vals = [_extract_sql_literal(v.strip()) for v in vals_part.split(",") if v.strip()]
    return f'"{col}" IN ({", ".join(vals)})'


def _is_not_in_expr(expr_str: str) -> bool:
    s = expr_str.replace(" ", "")
    return (
        (s.startswith("~(") and ".is_in([" in s and s.endswith(")"))
        or ".is_in([" in s
        and ").not()" in s
    )


def _translate_not_in_expr(expr_str: str) -> str:
    s = expr_str.strip()
    inner = s
    if s.startswith("~(") and s.endswith(")"):
        inner = s[2:-1]
    in_sql = _translate_in_expr(inner)
    # turn 'col IN (...)' into 'col NOT IN (...)'
    return in_sql.replace(" IN ", " NOT IN ")


def _is_between_expr(expr_str: str) -> bool:
    return (
        (" & " in expr_str)
        and any(op in expr_str for op in [" >= ", " > "])
        and any(op in expr_str for op in [" <= ", " < "])
    )


def _translate_between_expr(expr_str: str) -> str:
    parts = _split_on(expr_str, " & ")
    if len(parts) != 2:
        raise SqlPredicateBuildError(f"Cannot parse BETWEEN: {expr_str}")
    l_col, l_op, l_val = _parse_comparison(parts[0])
    r_col, r_op, r_val = _parse_comparison(parts[1])
    if l_col != r_col:
        raise SqlPredicateBuildError("BETWEEN parts refer to different columns")
    col = l_col
    _validate_column_operator(col, "BETWEEN")
    # Determine bounds regardless of ordering
    lower = l_val if l_op in (">", ">=") else r_val
    upper = r_val if r_op in ("<", "<=") else l_val
    return (
        f'"{col}" BETWEEN {_to_sql_number(col, lower)} AND {_to_sql_number(col, upper)}'
    )


def _is_not_null_expr(expr_str: str) -> bool:
    return ".is_not_null()" in expr_str


def _translate_not_null_expr(expr_str: str) -> str:
    col = _extract_column_name(expr_str.split(".is_not_null()", 1)[0])
    return f'"{col}" IS NOT NULL'


def _is_null_expr(expr_str: str) -> bool:
    return ".is_null()" in expr_str


def _translate_null_expr(expr_str: str) -> str:
    col = _extract_column_name(expr_str.split(".is_null()", 1)[0])
    return f'"{col}" IS NULL'


# Helpers


def _extract_column_name(col_expr: str) -> str:
    s = col_expr.strip().strip("()").strip()
    for pat in [r'col\("([^"]+)"\)', r"col\('([^']+)'\)"]:
        m = re.search(pat, s)
        if m:
            return m.group(1)
    # Sometimes string form may already be bare column
    return s


def _extract_sql_literal(literal_expr: str) -> str:
    s = literal_expr.strip()
    # Strip parentheses
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    # Strip Polars debug prefixes like 'dyn int:'
    if ":" in s:
        head, tail = s.split(":", 1)
        head_l = head.strip().lower()
        if head_l.startswith("dyn ") or head_l in {
            "int",
            "float",
            "string",
            "lit",
            "literal",
        }:
            s = tail.strip()
    # String literal
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return _quote_string(s[1:-1])
    # Boolean
    if s.lower() in ("true", "false"):
        return s.upper()
    # Numeric (leave unquoted)
    try:
        float(s)  # validate numeric
        return s
    except Exception:
        pass
    # Fallback: quote as string
    return _quote_string(s)


def _to_sql_number(column: str, value: Any) -> str:
    # value may already be numeric; ensure output is numeric literal (no quotes)
    if isinstance(value, (int, float)):
        return str(value)
    try:
        return str(int(value)) if "." not in str(value) else str(float(value))
    except Exception:
        return str(value)


def _quote_string(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"


def _validate_column_operator(column: str, operator: str) -> None:
    if column in GFF_STRING_COLUMNS or column not in GFF_STATIC_COLUMNS:
        if operator not in ("=", "!=", "IN", "NOT IN"):
            raise SqlPredicateBuildError(
                f"Column '{column}' (String) unsupported op '{operator}'"
            )
    elif column in GFF_UINT32_COLUMNS or column in GFF_FLOAT32_COLUMNS:
        if operator not in ("=", "!=", "<", "<=", ">", ">=", "BETWEEN"):
            raise SqlPredicateBuildError(
                f"Column '{column}' (Numeric) unsupported op '{operator}'"
            )


def _parse_comparison(comp_str: str) -> tuple[str, str, Any]:
    s = comp_str.strip().strip("()")
    for op in [" >= ", " <= ", " > ", " < ", " == ", " != "]:
        if op in s:
            left, right = s.split(op, 1)
            col = _extract_column_name(left)
            lit = _extract_sql_literal(right)
            return col, op.strip(), lit
    raise SqlPredicateBuildError(f"Cannot parse comparison: {comp_str}")


def _split_on(expr: str, sep: str) -> List[str]:
    parts: List[str] = []
    cur = ""
    depth = 0
    i = 0
    while i < len(expr):
        if expr[i] == "(":
            depth += 1
        elif expr[i] == ")":
            depth -= 1
        if depth == 0 and expr[i : i + len(sep)] == sep:
            parts.append(cur)
            cur = ""
            i += len(sep)
            continue
        cur += expr[i]
        i += 1
    parts.append(cur)
    return [p.strip().strip("()") for p in parts if p.strip()]


def _mock_expr(s: str) -> pl.Expr:
    class E:
        def __init__(self, expr_str: str) -> None:
            self._s = expr_str

        def __str__(self) -> str:
            return self._s

    return E(s)
