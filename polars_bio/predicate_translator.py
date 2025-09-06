"""
Polars to DataFusion predicate translator for GFF table provider.

This module converts Polars expressions to DataFusion expressions for predicate pushdown optimization.
Uses the DataFusion Python DataFrame API instead of SQL string construction for better type safety.

Supports the following operators based on GFF table provider capabilities:

| Column                      | Data Type | Supported Operators          | Example                         |
|-----------------------------|-----------|------------------------------|---------------------------------|
| chrom, source, type, strand | String    | =, !=, IN, NOT IN            | chrom = 'chr1'                  |
| start, end                  | UInt32    | =, !=, <, <=, >, >=, BETWEEN | start > 1000                    |
| score                       | Float32   | =, !=, <, <=, >, >=, BETWEEN | score BETWEEN 50.0 AND 100.0   |
| Attribute fields            | String    | =, !=, IN, NOT IN            | "ID" = 'gene1'                  |
| Complex                     | -         | AND combinations             | chrom = 'chr1' AND start > 1000 |
"""

import re
from typing import Any, List, Optional, Union

import polars as pl
from datafusion import col
from datafusion import functions as F
from datafusion import lit

# GFF schema column types for validation
GFF_STRING_COLUMNS = {"chrom", "source", "type", "strand"}
GFF_UINT32_COLUMNS = {"start", "end", "phase"}
GFF_FLOAT32_COLUMNS = {"score"}
GFF_STATIC_COLUMNS = (
    GFF_STRING_COLUMNS | GFF_UINT32_COLUMNS | GFF_FLOAT32_COLUMNS | {"attributes"}
)


class PredicateTranslationError(Exception):
    """Raised when a Polars predicate cannot be translated to DataFusion expression."""

    pass


def translate_polars_predicate_to_datafusion(predicate: pl.Expr):
    """
    Convert Polars predicate expressions to DataFusion expressions.

    Args:
        predicate: Polars expression representing filter conditions

    Returns:
        DataFusion Expr object that can be used with DataFrame.filter()

    Raises:
        PredicateTranslationError: If predicate cannot be translated

    Examples:
        >>> df_expr = translate_polars_predicate_to_datafusion(pl.col("chrom") == "chr1")
        >>> datafusion_df.filter(df_expr)

        >>> df_expr = translate_polars_predicate_to_datafusion(
        ...     (pl.col("chrom") == "chr1") & (pl.col("start") > 100000)
        ... )
        >>> datafusion_df.filter(df_expr)
    """
    try:
        return _translate_polars_expr(predicate)
    except Exception as e:
        raise PredicateTranslationError(
            f"Cannot translate predicate to DataFusion: {e}"
        ) from e


def _translate_polars_expr(expr: pl.Expr):
    """Recursively translate Polars expression to DataFusion expression."""

    expr_str = str(expr)

    # Handle binary operations (col op literal)
    if _is_binary_expr(expr_str):
        return _translate_binary_expr(expr_str)

    # Handle logical AND operations
    if _is_and_expr(expr_str):
        return _translate_and_expr(expr_str)

    # Handle IN operations
    if _is_in_expr(expr_str):
        return _translate_in_expr(expr_str)

    # Handle NOT IN operations (negated IN)
    if _is_not_in_expr(expr_str):
        return _translate_not_in_expr(expr_str)

    # Handle BETWEEN operations (range checks)
    if _is_between_expr(expr_str):
        return _translate_between_expr(expr_str)

    # Handle IS NOT NULL
    if _is_not_null_expr(expr_str):
        return _translate_not_null_expr(expr_str)

    # Handle IS NULL
    if _is_null_expr(expr_str):
        return _translate_null_expr(expr_str)

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
    """Translate AND expressions."""

    # Handle & operator by finding the main & split point
    if " & " in expr_str:
        parts = _split_on_main_operator(expr_str, " & ")
        if len(parts) == 2:
            left_part = parts[0].strip().strip("()")
            right_part = parts[1].strip().strip("()")

            # Recursively translate both parts
            left_expr = _translate_polars_expr(_create_mock_expr(left_part))
            right_expr = _translate_polars_expr(_create_mock_expr(right_part))

            return left_expr & right_expr

    raise PredicateTranslationError(f"Cannot parse AND expression: {expr_str}")


def _is_in_expr(expr_str: str) -> bool:
    """Check if expression is an IN operation."""
    return ".is_in(" in expr_str


def _translate_in_expr(expr_str: str):
    """Translate IN expressions like col.is_in([val1, val2])."""

    # Parse col("column").is_in([values]) pattern
    match = re.search(r"(.+?)\.is_in\(\[(.+?)\]\)", expr_str)
    if match:
        col_part = match.group(1).strip()
        values_part = match.group(2).strip()

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
    """Extract literal value from expression."""
    literal_expr = literal_expr.strip()

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
    """Validate that column supports the given operator."""

    # String columns: =, !=, IN, NOT IN
    if (
        column in GFF_STRING_COLUMNS or column not in GFF_STATIC_COLUMNS
    ):  # Attribute fields
        if operator not in ["==", "!=", "IN", "NOT IN"]:
            raise PredicateTranslationError(
                f"Column '{column}' (String) does not support operator '{operator}'. "
                f"Supported: ==, !=, IN, NOT IN"
            )

    # Numeric columns: =, !=, <, <=, >, >=, BETWEEN
    elif column in GFF_UINT32_COLUMNS or column in GFF_FLOAT32_COLUMNS:
        if operator not in ["==", "!=", "<", "<=", ">", ">=", "BETWEEN"]:
            raise PredicateTranslationError(
                f"Column '{column}' (Numeric) does not support operator '{operator}'. "
                f"Supported: ==, !=, <, <=, >, >=, BETWEEN"
            )


def _parse_list_values(values_str: str) -> List[Any]:
    """Parse list of values from string."""
    if not values_str.strip():
        return []

    items = [item.strip() for item in values_str.split(",")]
    return [_extract_literal_value(item) for item in items if item.strip()]


def _split_on_main_operator(expr_str: str, operator: str) -> List[str]:
    """Split expression on main operator, respecting parentheses."""
    parts = []
    current = ""
    paren_depth = 0
    i = 0

    while i < len(expr_str):
        if expr_str[i] == "(":
            paren_depth += 1
        elif expr_str[i] == ")":
            paren_depth -= 1
        elif paren_depth == 0 and expr_str[i : i + len(operator)] == operator:
            parts.append(current)
            current = ""
            i += len(operator) - 1
        else:
            current += expr_str[i]
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
Supported GFF Predicate Pushdown Operations:

| Column                      | Data Type | Supported Operators          | Example                         |
|-----------------------------|-----------|------------------------------|---------------------------------|
| chrom, source, type, strand | String    | =, !=, IN, NOT IN            | chrom = 'chr1'                  |
| start, end                  | UInt32    | =, !=, <, <=, >, >=, BETWEEN | start > 1000                    |
| score                       | Float32   | =, !=, <, <=, >, >=, BETWEEN | score BETWEEN 50.0 AND 100.0   |
| Attribute fields            | String    | =, !=, IN, NOT IN            | "ID" = 'gene1'                  |
| Complex                     | -         | AND combinations             | chrom = 'chr1' AND start > 1000 |

Examples:
- pl.col("chrom") == "chr1"
- pl.col("start") > 1000
- pl.col("chrom").is_in(["chr1", "chr2"])
- (pl.col("chrom") == "chr1") & (pl.col("start") > 1000)
- (pl.col("start") >= 1000) & (pl.col("start") <= 2000)  # BETWEEN
"""
