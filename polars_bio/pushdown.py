"""Shared, audited pushdown helpers.

Correctness contract: the client-side full predicate filter and full projection
select are the source of truth. These helpers push down only what can be applied
faithfully, and always report whether the caller must reapply client-side.
"""

from typing import Any, List, Optional, Tuple

from .predicate_translator import PredicateTranslationError, plan_predicate_pushdown


def _root_names(item: Any) -> Optional[List[str]]:
    if isinstance(item, str):
        return [item]
    meta = getattr(item, "meta", None)
    if meta is None or not hasattr(meta, "root_names"):
        return None
    try:
        return list(meta.root_names())
    except Exception:
        return None


def extract_source_columns(with_columns) -> Tuple[List[str], bool]:
    """Return (source_columns, complete).

    source_columns: de-duplicated source column names the scan must request.
    complete: False if any element's names could not be recovered (forcing a
    client-side reapply); True otherwise.
    """
    if with_columns is None:
        return [], True

    if isinstance(with_columns, (list, tuple)):
        items = list(with_columns)
    else:
        items = [with_columns]

    cols: List[str] = []
    complete = True
    for item in items:
        names = _root_names(item)
        if names is None:
            complete = False
            continue
        for name in names:
            if name not in cols:
                cols.append(name)
    return cols, complete


def apply_predicate_pushdown(
    query_df, predicate, col_types, *, log
) -> Tuple[Any, bool]:
    """Push down what is faithfully translatable. Returns (query_df, needs_client_filter)."""
    if predicate is None:
        return query_df, False
    try:
        plan = plan_predicate_pushdown(
            predicate,
            string_cols=col_types.get("string_cols"),
            uint32_cols=col_types.get("uint32_cols"),
            float32_cols=col_types.get("float32_cols"),
        )
    except PredicateTranslationError as exc:
        log.warning("predicate pushdown skipped (translation): %s", exc)
        return query_df, True
    if plan.pushdown_sql is not None:
        try:
            native = query_df.parse_sql_expr(plan.pushdown_sql)
            query_df = query_df.filter(native)
        except Exception as exc:  # parse_sql_expr / filter binding failure
            log.warning("predicate pushdown skipped (bind): %s", exc)
            return query_df, True
    return query_df, not plan.fully_translated


def apply_projection_pushdown(query_df, with_columns, *, log) -> Tuple[Any, bool]:
    """Push down the source-column projection. Returns (query_df, needs_client_select)."""
    if with_columns is None:
        return query_df, False
    cols, complete = extract_source_columns(with_columns)
    if not cols:
        return query_df, True
    try:
        select_exprs = [query_df.parse_sql_expr(f'"{c}"') for c in cols]
        query_df = query_df.select(*select_exprs)
    except Exception as exc:  # parse_sql_expr / select binding failure
        log.warning("projection pushdown skipped (bind): %s", exc)
        return query_df, True
    return query_df, not complete
