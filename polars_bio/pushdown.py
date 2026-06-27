"""Shared, audited pushdown helpers.

Correctness contract: the client-side full predicate filter and full projection
select are the source of truth. These helpers push down only what can be applied
faithfully, and always report whether the caller must reapply client-side.
"""

from typing import Any, List, Optional, Tuple


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
