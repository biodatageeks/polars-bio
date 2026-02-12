"""
Polars to DataFusion predicate translator for bio-format table providers.

This module converts Polars expressions to DataFusion expressions for predicate pushdown optimization.
Uses the DataFusion Python DataFrame API instead of SQL string construction for better type safety.

Supports BAM/SAM/CRAM, VCF, and GFF formats. Each format defines its own known column types;
unknown columns (BAM tags, VCF INFO/FORMAT fields, GFF attribute fields) are handled permissively,
allowing all operators and letting DataFusion type-check at execution time.
"""

import re
from typing import Any, List, Optional, Set, Union

import polars as pl
from datafusion import col
from datafusion import functions as F
from datafusion import lit

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

# ---------------------------------------------------------------------------
# Module-level context for the current translation (set by translate_predicate)
# ---------------------------------------------------------------------------
_ACTIVE_STRING_COLS: Optional[Set[str]] = None
_ACTIVE_UINT32_COLS: Optional[Set[str]] = None
_ACTIVE_FLOAT32_COLS: Optional[Set[str]] = None


class PredicateTranslationError(Exception):
    """Raised when a Polars predicate cannot be translated to DataFusion expression."""

    pass


def translate_predicate(
    predicate: pl.Expr,
    string_cols: Optional[Set[str]] = None,
    uint32_cols: Optional[Set[str]] = None,
    float32_cols: Optional[Set[str]] = None,
):
    """
    Convert a Polars predicate to a DataFusion expression, with format-aware validation.

    When *no* column sets are provided the translator is **permissive**: every
    operator is accepted for every column and DataFusion's type system will
    catch mismatches at execution time.

    Args:
        predicate: Polars expression representing filter conditions.
        string_cols: Column names known to be strings (equality/IN only).
        uint32_cols: Column names known to be UInt32 (all comparison ops).
        float32_cols: Column names known to be Float32 (all comparison ops).

    Returns:
        DataFusion ``Expr`` suitable for ``DataFrame.filter()``.

    Raises:
        PredicateTranslationError: If the predicate cannot be translated.
    """
    global _ACTIVE_STRING_COLS, _ACTIVE_UINT32_COLS, _ACTIVE_FLOAT32_COLS
    _ACTIVE_STRING_COLS = string_cols
    _ACTIVE_UINT32_COLS = uint32_cols
    _ACTIVE_FLOAT32_COLS = float32_cols
    try:
        return _translate_polars_expr(predicate)
    except Exception as e:
        raise PredicateTranslationError(
            f"Cannot translate predicate to DataFusion: {e}"
        ) from e
    finally:
        _ACTIVE_STRING_COLS = None
        _ACTIVE_UINT32_COLS = None
        _ACTIVE_FLOAT32_COLS = None


def translate_polars_predicate_to_datafusion(predicate: pl.Expr):
    """
    Convert Polars predicate expressions to DataFusion expressions (GFF defaults).

    Thin wrapper around :func:`translate_predicate` that uses GFF column types
    for backward compatibility.

    Args:
        predicate: Polars expression representing filter conditions

    Returns:
        DataFusion Expr object that can be used with DataFrame.filter()

    Raises:
        PredicateTranslationError: If predicate cannot be translated
    """
    return translate_predicate(
        predicate,
        string_cols=GFF_STRING_COLUMNS,
        uint32_cols=GFF_UINT32_COLUMNS,
        float32_cols=GFF_FLOAT32_COLUMNS,
    )


def _strip_polars_wrapping(s: str) -> str:
    """Strip outermost ``[…]`` wrapper that Polars adds around expression reprs.

    Examples::

        [(col("chrom")) == ("chr1")]  →  (col("chrom")) == ("chr1")
        [(…) & (…)]                   →  (…) & (…)

    Only strips when the outermost ``[`` and ``]`` are a matching pair that
    wraps the entire string (i.e. nothing follows the closing ``]``).
    """
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return s


def _translate_polars_expr(expr: pl.Expr):
    """Recursively translate Polars expression to DataFusion expression."""

    expr_str = _strip_polars_wrapping(str(expr))

    # NOTE: check order matters!  AND / BETWEEN must come before binary,
    # because compound expressions like (col == "x") & (col > 1) also
    # match the binary detector (it sees "==" in the string).

    # Handle logical AND operations (must be before binary!)
    if _is_and_expr(expr_str):
        return _translate_and_expr(expr_str)

    # Handle NOT IN operations (negated IN) — before plain IN
    if _is_not_in_expr(expr_str):
        return _translate_not_in_expr(expr_str)

    # Handle IN operations
    if _is_in_expr(expr_str):
        return _translate_in_expr(expr_str)

    # Handle IS NOT NULL — before IS NULL
    if _is_not_null_expr(expr_str):
        return _translate_not_null_expr(expr_str)

    # Handle IS NULL
    if _is_null_expr(expr_str):
        return _translate_null_expr(expr_str)

    # Handle binary operations (col op literal) — simple, single comparison
    if _is_binary_expr(expr_str):
        return _translate_binary_expr(expr_str)

    raise PredicateTranslationError(f"Unsupported expression type: {expr_str}")


def _is_binary_expr(expr_str: str) -> bool:
    """Check if expression is a binary operation (col op literal)."""
    binary_patterns = [r"\s==\s", r"\s!=\s", r"\s<\s", r"\s<=\s", r"\s>\s", r"\s>=\s"]
    return any(re.search(pattern, expr_str) for pattern in binary_patterns)


def _translate_binary_expr(expr_str: str):
    """Translate binary expressions like col == value, col > value, etc."""

    # Parse binary operations with regex to handle complex expressions
    binary_ops = [
        (r"(.+?)\s==\s(.+)", lambda l, r: col(l) == lit(r)),
        (r"(.+?)\s!=\s(.+)", lambda l, r: col(l) != lit(r)),
        (r"(.+?)\s<=\s(.+)", lambda l, r: col(l) <= lit(r)),
        (r"(.+?)\s>=\s(.+)", lambda l, r: col(l) >= lit(r)),
        (r"(.+?)\s<\s(.+)", lambda l, r: col(l) < lit(r)),
        (r"(.+?)\s>\s(.+)", lambda l, r: col(l) > lit(r)),
    ]

    for pattern, op_func in binary_ops:
        match = re.search(pattern, expr_str)
        if match:
            left_part = match.group(1).strip()
            right_part = match.group(2).strip()

            # Extract column name and literal value
            column = _extract_column_name(left_part)
            value = _extract_literal_value(right_part)

            # Validate column and operator combination
            op_symbol = pattern.split(r"\s")[1].replace("\\", "")
            _validate_column_operator(column, op_symbol)

            return op_func(column, value)

    raise PredicateTranslationError(f"Cannot parse binary expression: {expr_str}")


def _is_and_expr(expr_str: str) -> bool:
    """Check if expression is an AND operation."""
    return " & " in expr_str or ".and(" in expr_str


def _translate_and_expr(expr_str: str):
    """Translate AND expressions (supports 2+ conjuncts via left-fold)."""

    if " & " in expr_str:
        parts = _split_on_main_operator(expr_str, " & ")
        if len(parts) >= 2:
            # Left-fold: ((a & b) & c) & d …
            result = _translate_polars_expr(
                _create_mock_expr(_strip_polars_wrapping(parts[0].strip().strip("()")))
            )
            for part in parts[1:]:
                right_expr = _translate_polars_expr(
                    _create_mock_expr(_strip_polars_wrapping(part.strip().strip("()")))
                )
                result = result & right_expr
            return result

    raise PredicateTranslationError(f"Cannot parse AND expression: {expr_str}")


def _is_in_expr(expr_str: str) -> bool:
    """Check if expression is an IN operation."""
    return ".is_in(" in expr_str


def _translate_in_expr(expr_str: str):
    """Translate IN expressions like col.is_in([val1, val2])."""

    # Parse col("column").is_in([values]) pattern
    # Polars repr uses double brackets: is_in([["v1", "v2"]]) — strip inner []
    match = re.search(r"(.+?)\.is_in\(\[(.+?)\]\)", expr_str)
    if match:
        col_part = match.group(1).strip()
        values_part = match.group(2).strip().strip("[]")

        column = _extract_column_name(col_part)
        values = _parse_list_values(values_part)

        # Validate column supports IN operation
        _validate_column_operator(column, "IN")

        # Convert values to DataFusion literals
        df_values = [lit(value) for value in values]

    return F.in_list(col(column), df_values)


def _is_not_in_expr(expr_str: str) -> bool:
    """Check if expression is a NOT IN operation (negated is_in)."""
    # Common patterns from Polars repr:
    # 1) ~(col("x").is_in([..]))
    # 2) col("x").is_in([..]).not()
    s = expr_str.replace(" ", "")
    return (
        (s.startswith("~(") and ".is_in([" in s and s.endswith(")"))
        or ".is_in([" in s
        and ").not()" in s
    )


def _translate_not_in_expr(expr_str: str):
    """Translate NOT IN expressions as negated in_list."""
    s = expr_str.strip()
    # Normalize to extract inner is_in([...]) part
    inner = s
    if s.startswith("~(") and s.endswith(")"):
        inner = s[2:-1]
    # Reuse IN translator on inner and negate
    in_expr = _translate_in_expr(inner)
    return ~in_expr

    raise PredicateTranslationError(f"Cannot parse IN expression: {expr_str}")


def _is_between_expr(expr_str: str) -> bool:
    """Check if expression represents a BETWEEN operation."""
    # Look for patterns like (col >= val1) & (col <= val2)
    return (" >= " in expr_str and " <= " in expr_str and " & " in expr_str) or (
        " > " in expr_str and " < " in expr_str and " & " in expr_str
    )


def _translate_between_expr(expr_str: str):
    """Translate BETWEEN expressions from range conditions."""

    # Parse (col >= min_val) & (col <= max_val) pattern
    if " & " in expr_str:
        parts = _split_on_main_operator(expr_str, " & ")
        if len(parts) == 2:
            left_part = parts[0].strip().strip("()")
            right_part = parts[1].strip().strip("()")

            # Extract column and values from both parts
            left_col, left_op, left_val = _parse_comparison(left_part)
            right_col, right_op, right_val = _parse_comparison(right_part)

            # Verify same column in both parts
            if left_col == right_col:
                column = left_col

                # Determine BETWEEN bounds
                if left_op in [">", ">="] and right_op in ["<", "<="]:
                    min_val = left_val
                    max_val = right_val
                elif left_op in ["<", "<="] and right_op in [">", ">="]:
                    min_val = right_val
                    max_val = left_val
                else:
                    raise PredicateTranslationError("Invalid BETWEEN pattern")

                # Validate column supports BETWEEN
                _validate_column_operator(column, "BETWEEN")

                return col(column).between(lit(min_val), lit(max_val))

    raise PredicateTranslationError(f"Cannot parse BETWEEN expression: {expr_str}")


def _is_not_null_expr(expr_str: str) -> bool:
    """Check if expression is IS NOT NULL."""
    return ".is_not_null()" in expr_str


def _translate_not_null_expr(expr_str: str):
    """Translate IS NOT NULL expressions."""
    col_part = expr_str.split(".is_not_null()")[0]
    column = _extract_column_name(col_part)
    return col(column).is_not_null()


def _is_null_expr(expr_str: str) -> bool:
    """Check if expression is IS NULL."""
    return ".is_null()" in expr_str


def _translate_null_expr(expr_str: str):
    """Translate IS NULL expressions."""
    col_part = expr_str.split(".is_null()")[0]
    column = _extract_column_name(col_part)
    return col(column).is_null()


# Helper functions


def _extract_column_name(col_expr: str) -> str:
    """Extract column name from col() expression."""
    col_expr = col_expr.strip()

    # Handle col("name") or col('name')
    patterns = [r'col\("([^"]+)"\)', r"col\('([^']+)'\)"]

    for pattern in patterns:
        match = re.search(pattern, col_expr)
        if match:
            return match.group(1)

    # Handle parentheses around the whole expression
    col_expr = col_expr.strip("()")
    for pattern in patterns:
        match = re.search(pattern, col_expr)
        if match:
            return match.group(1)

    raise PredicateTranslationError(f"Cannot extract column name from: {col_expr}")


def _extract_literal_value(literal_expr: str) -> Any:
    """Extract literal value from expression.

    Handles Polars repr formats such as::

        ("chr1")        → chr1
        (dyn int: 1000) → 1000
        (dyn float: 1.5)→ 1.5
        "chr1"          → chr1
        1000            → 1000
    """
    literal_expr = literal_expr.strip()

    # Unwrap Polars-style parenthesised literal: ("value") or (dyn int: 123)
    if literal_expr.startswith("(") and literal_expr.endswith(")"):
        literal_expr = literal_expr[1:-1].strip()

    # Strip Polars "dyn int:" / "dyn float:" type prefixes
    for prefix in ("dyn int: ", "dyn float: "):
        if literal_expr.startswith(prefix):
            literal_expr = literal_expr[len(prefix) :]
            break

    # Handle string literals
    if (literal_expr.startswith('"') and literal_expr.endswith('"')) or (
        literal_expr.startswith("'") and literal_expr.endswith("'")
    ):
        return literal_expr[1:-1]

    # Handle numeric literals
    try:
        if "." in literal_expr:
            return float(literal_expr)
        else:
            return int(literal_expr)
    except ValueError:
        pass

    # Handle boolean literals
    if literal_expr.lower() == "true":
        return True
    elif literal_expr.lower() == "false":
        return False

    return literal_expr


def _validate_column_operator(column: str, operator: str) -> None:
    """Validate that column supports the given operator.

    Uses the module-level context (_ACTIVE_*_COLS) set by translate_predicate().
    When no context is set (all None), validation is **permissive** — all
    operators are allowed and DataFusion will type-check at execution.
    """
    string_cols = _ACTIVE_STRING_COLS
    uint32_cols = _ACTIVE_UINT32_COLS
    float32_cols = _ACTIVE_FLOAT32_COLS

    # Permissive mode: no column type info available — allow everything
    if string_cols is None and uint32_cols is None and float32_cols is None:
        return

    known_cols = (
        (string_cols or set()) | (uint32_cols or set()) | (float32_cols or set())
    )

    # String columns: =, !=, IN, NOT IN
    if column in (string_cols or set()):
        if operator not in ["==", "!=", "IN", "NOT IN"]:
            raise PredicateTranslationError(
                f"Column '{column}' (String) does not support operator '{operator}'. "
                f"Supported: ==, !=, IN, NOT IN"
            )

    # Numeric columns: =, !=, <, <=, >, >=, BETWEEN
    elif column in (uint32_cols or set()) or column in (float32_cols or set()):
        if operator not in ["==", "!=", "<", "<=", ">", ">=", "BETWEEN"]:
            raise PredicateTranslationError(
                f"Column '{column}' (Numeric) does not support operator '{operator}'. "
                f"Supported: ==, !=, <, <=, >, >=, BETWEEN"
            )

    # Unknown column (BAM tags, VCF INFO/FORMAT, GFF attributes): permissive
    elif column not in known_cols:
        pass  # Allow all operators; DataFusion will type-check at execution


def _parse_list_values(values_str: str) -> List[Any]:
    """Parse list of values from string."""
    if not values_str.strip():
        return []

    items = [item.strip() for item in values_str.split(",")]
    return [_extract_literal_value(item) for item in items if item.strip()]


def _split_on_main_operator(expr_str: str, operator: str) -> List[str]:
    """Split expression on main operator, respecting parentheses.

    Parentheses ``(`` / ``)`` are tracked for depth but **preserved** in the
    output parts so that downstream parsers receive the original text.
    """
    parts = []
    current = ""
    paren_depth = 0
    i = 0

    while i < len(expr_str):
        ch = expr_str[i]
        if ch == "(":
            paren_depth += 1
            current += ch
        elif ch == ")":
            paren_depth -= 1
            current += ch
        elif paren_depth == 0 and expr_str[i : i + len(operator)] == operator:
            parts.append(current)
            current = ""
            i += len(operator) - 1
        else:
            current += ch
        i += 1

    parts.append(current)
    return parts


def _parse_comparison(comp_str: str) -> tuple:
    """Parse comparison string into (column, operator, value)."""
    comp_str = comp_str.strip("()")

    for op in [" >= ", " <= ", " > ", " < ", " == ", " != "]:
        if op in comp_str:
            parts = comp_str.split(op, 1)
            if len(parts) == 2:
                col_part = parts[0].strip()
                val_part = parts[1].strip()
                column = _extract_column_name(col_part)
                value = _extract_literal_value(val_part)
                return column, op.strip(), value

    raise PredicateTranslationError(f"Cannot parse comparison: {comp_str}")


def _create_mock_expr(expr_str: str) -> pl.Expr:
    """Create a mock Polars expression from string for recursive parsing."""

    class MockExpr:
        def __init__(self, expr_str):
            self.expr_str = expr_str

        def __str__(self):
            return self.expr_str

    return MockExpr(expr_str.strip())


def is_predicate_pushdown_supported(predicate: pl.Expr) -> bool:
    """
    Check if a Polars predicate can be pushed down to DataFusion.

    Args:
        predicate: Polars expression to check

    Returns:
        True if predicate can be translated and pushed down
    """
    try:
        translate_polars_predicate_to_datafusion(predicate)
        return True
    except PredicateTranslationError:
        return False


def get_supported_predicates_info() -> str:
    """Return information about supported predicate types."""
    return """
Supported Predicate Pushdown Operations (all formats):

| Format    | Column                                        | Data Type | Supported Operators          |
|-----------|-----------------------------------------------|-----------|------------------------------|
| GFF       | chrom, source, type, strand                   | String    | =, !=, IN, NOT IN           |
| GFF       | start, end, phase                             | UInt32    | =, !=, <, <=, >, >=, BETWEEN|
| GFF       | score                                         | Float32   | =, !=, <, <=, >, >=, BETWEEN|
| GFF       | Attribute fields                              | String    | =, !=, IN, NOT IN           |
| BAM/CRAM  | name, chrom, cigar, mate_chrom, ...           | String    | =, !=, IN, NOT IN           |
| BAM/CRAM  | start, end, flags, mapping_quality, ...       | UInt32    | =, !=, <, <=, >, >=, BETWEEN|
| VCF       | chrom, ref, alt                               | String    | =, !=, IN, NOT IN           |
| VCF       | start                                         | UInt32    | =, !=, <, <=, >, >=, BETWEEN|
| All       | Unknown/dynamic columns                       | Any       | All (DataFusion type-checks) |
| All       | Complex                                       | -         | AND combinations             |

Examples:
- pl.col("chrom") == "chr1"
- pl.col("start") > 1000
- pl.col("chrom").is_in(["chr1", "chr2"])
- (pl.col("chrom") == "chr1") & (pl.col("start") > 1000)
- (pl.col("start") >= 1000) & (pl.col("start") <= 2000)  # BETWEEN
"""


def datafusion_expr_to_sql(expr) -> str:
    """Convert a DataFusion Expr to a SQL WHERE clause string.

    The DataFusion Python ``Expr`` has a predictable ``str()`` format::

        Expr(chrom = Utf8View("1") AND start > Int64(10000))

    This function extracts the inner text and converts DataFusion type
    wrappers to SQL literals.

    Args:
        expr: DataFusion ``Expr`` object (from :func:`translate_predicate`).

    Returns:
        SQL WHERE clause string (without the ``WHERE`` keyword).

    Raises:
        PredicateTranslationError: If the expression cannot be converted.
    """
    s = str(expr)

    # Unwrap Expr(…)
    if s.startswith("Expr(") and s.endswith(")"):
        s = s[5:-1]

    # Utf8View("value") → 'value'   (quote single-quotes inside)
    s = re.sub(
        r'Utf8View\("([^"]*)"\)',
        lambda m: "'" + m.group(1).replace("'", "''") + "'",
        s,
    )

    # Int64(N) → N
    s = re.sub(r"Int64\((-?\d+)\)", r"\1", s)

    # Float64(N) → N
    s = re.sub(r"Float64\((-?[\d.]+(?:e[+-]?\d+)?)\)", r"\1", s)

    # UInt32(N) → N
    s = re.sub(r"UInt32\((\d+)\)", r"\1", s)

    # Boolean(true/false) → TRUE/FALSE
    s = re.sub(r"Boolean\(true\)", "TRUE", s)
    s = re.sub(r"Boolean\(false\)", "FALSE", s)

    # IN ([val, val, …]) → IN (val, val, …)  — remove square brackets
    s = re.sub(r"IN \(\[", "IN (", s)
    s = re.sub(r"\]\)", ")", s)

    # Quote bare column names: word tokens before operators
    # This is already handled because DataFusion Expr uses bare names
    # and SQL engines accept unquoted identifiers for simple names.

    if not s.strip():
        raise PredicateTranslationError("Empty expression after conversion")

    return s
