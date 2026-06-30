# Robust Predicate Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make predicate and projection pushdown pure optimizations so a translation bug can cost performance but never correctness — fixing the silently-dropped-filter class of bugs in issue #396.

**Architecture:** Replace the regex-on-`str(expr)` translator with a structured emitter that walks `expr.meta.serialize(format="json")` node-by-node (unsupported nodes raise, never vanish), split top-level `AND` conjuncts so a partial predicate still pushes its translatable subset, and route every scan path through two shared helpers whose contract is "the client-side full filter/select is the source of truth." Lock the eager-vs-lazy invariant with a hypothesis property test.

**Tech Stack:** Python, Polars 1.40.1, DataFusion (via the `polars_bio` Rust extension + pip `datafusion`), pyarrow, pytest, hypothesis.

## Global Constraints

- **Polars AST shapes are exact (Polars 1.40.1), keyed literally by the emitter:**
  - `{"Column": "<name>"}`
  - `{"Literal": {"Scalar": {"String": <s>}}}`, `{"Literal": {"Scalar": {"Boolean": <bool>}}}`
  - `{"Literal": {"Dyn": {"Int": <i>}}}`, `{"Literal": {"Dyn": {"Float": <f>}}}` — integers/floats use **`Dyn`**, not `Scalar`.
  - `{"BinaryExpr": {"left": ..., "op": <Op>, "right": ...}}`, `Op ∈ {Eq, NotEq, Lt, LtEq, Gt, GtEq, And, Or}`
  - `{"Function": {"input": [col, listlit], "function": {"Boolean": {"IsIn": {"nulls_equal": <bool>}}}}}` — the list is an **Arrow IPC byte blob** at `listlit["Literal"]["Scalar"]["List"]`.
  - `{"Function": {"input": [expr], "function": {"Boolean": "Not"}}}`
  - `{"Function": {"input": [col], "function": {"Boolean": "IsNull" | "IsNotNull"}}}`
  - `{"Function": {"input": [col, lit], "function": {"StringExpr": {...}}}}` → unsupported (client-side).
- **Correctness invariant:** pushdown may only ever remove rows the client-side filter would also remove. A pushed predicate must keep a *superset* of rows. Only top-level `AND` conjuncts are splittable; `OR` is atomic.
- **SQL → `parse_sql_expr` round-trip is REQUIRED** and must be retained: pip `datafusion` `Expr` is incompatible with the `polars_bio` Rust extension (separate PyO3 compilation units); `parse_sql_expr` rebuilds a binding-compatible `Expr` from the same unit. The translator changes how SQL is *built*, not how it is bound.
- **No silent swallowing:** translation failures are typed (`PredicateTranslationError` / `UnsupportedPredicate`) and caught specifically; the only broad `except` allowed is around `parse_sql_expr`/`filter`/`select` *binding*, and it must always force the client-side reapply.
- **Format column-type sets** live in `predicate_translator.py` (`GFF_*`, `BAM_*`, `VCF_*`, `PAIRS_*`) and are mapped per format by `_FORMAT_COLUMN_TYPES` in `polars_bio/io.py`. Do not duplicate them.
- Build after Rust-affecting changes: none here (Python-only). Run tests with `python -m pytest`.

---

## File Structure

- `polars_bio/predicate_translator.py` — **rewrite the translation internals.** Add `UnsupportedPredicate`, the structured emitter (`_emit_sql` + node helpers), `PushdownPlan`, and `plan_predicate_pushdown`. Keep the old `translate_predicate` / `datafusion_expr_to_sql` / regex helpers temporarily until Task 10 deletes them.
- `polars_bio/pushdown.py` — **new module.** The shared helpers `apply_predicate_pushdown`, `apply_projection_pushdown`, and `extract_source_columns`. This is the single audited home of the fallback semantics.
- `polars_bio/io.py` — wire the two call sites (`_overlap_source`, the GFF/GTF `select()` wrapper) through the helpers; replace `_extract_column_names_from_expr` usage.
- `polars_bio/utils.py` — wire the overlap/range `register_io_source` path.
- `polars_bio/pileup_op.py` — wire the pileup path.
- `tests/test_predicate_translator_units.py` — **new.** Exhaustive per-node emitter + `plan_predicate_pushdown` tests.
- `tests/test_pushdown_helpers.py` — **new.** Helper fallback tests with raising stubs.
- `tests/test_pushdown_equivalence.py` — **new.** Hypothesis property test + #396 regression.
- `pyproject.toml` — add `hypothesis` to the test dependency group.

---

## Task 1: Structured AST emitter (`_emit_sql`) + `UnsupportedPredicate`

**Files:**
- Modify: `polars_bio/predicate_translator.py` (add new code near the top, below the column-type constants; do not touch the old regex functions yet)
- Test: `tests/test_predicate_translator_units.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces:
  - `class UnsupportedPredicate(Exception)`
  - `_emit_sql(node: dict, string_cols, uint32_cols, float32_cols) -> str` — raises `UnsupportedPredicate` for any node it cannot faithfully translate.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_predicate_translator_units.py`:

```python
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
    assert emit(pl.col("start") >= 10) == "(\"start\" >= 10)"


def test_float_comparison():
    assert emit(pl.col("score") > 1.5) == "(\"score\" > 1.5)"


def test_boolean_literal():
    assert emit(pl.col("strand") == True) == "(\"strand\" = TRUE)"  # noqa: E712


def test_and_or_nesting():
    e = (pl.col("type") == "exon") & (pl.col("start") >= 10)
    assert emit(e) == "((\"type\" = 'exon') AND (\"start\" >= 10))"
    e2 = (pl.col("type") == "exon") | (pl.col("type") == "gene")
    assert emit(e2) == "((\"type\" = 'exon') OR (\"type\" = 'gene'))"


def test_is_in_strings_decoded_from_arrow_blob():
    assert emit(pl.col("chrom").is_in(["chr1", "chr2"])) == "(\"chrom\" IN ('chr1', 'chr2'))"


def test_is_in_ints():
    assert emit(pl.col("start").is_in([1, 2, 3])) == "(\"start\" IN (1, 2, 3))"


def test_not_and_isnull():
    assert emit(~(pl.col("type") == "exon")) == "(NOT (\"type\" = 'exon'))"
    assert emit(pl.col("type").is_null()) == "(\"type\" IS NULL)"
    assert emit(pl.col("type").is_not_null()) == "(\"type\" IS NOT NULL)"


def test_str_contains_is_unsupported():
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type").str.contains("exon"))


def test_string_column_ordering_is_unsupported():
    with pytest.raises(UnsupportedPredicate):
        emit(pl.col("type") < "exon")


def test_unknown_node_raises():
    with pytest.raises(UnsupportedPredicate):
        _emit_sql({"NoSuchNode": 1}, GFF_STR, GFF_U32, GFF_F32)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_predicate_translator_units.py -v`
Expected: FAIL with `ImportError: cannot import name 'UnsupportedPredicate'` / `_emit_sql`.

- [ ] **Step 3: Implement the emitter**

Add to `polars_bio/predicate_translator.py` (below the `_FORMAT`-style constants, above the old `translate_predicate`):

```python
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
    except Exception as exc:  # decode failure → safe client-side fallback
        raise UnsupportedPredicate(f"could not decode is_in list: {exc}")
    return table.column(0).to_pylist()


def _emit_is_in(inputs: list) -> str:
    if len(inputs) != 2:
        raise UnsupportedPredicate("is_in expects exactly two inputs")
    col_sql = _quote_ident(_column_name(inputs[0]))
    values = _decode_is_in_list(inputs[1])
    if not values:
        return "FALSE"
    items = ", ".join(_sql_scalar(v) for v in values)
    return f"({col_sql} IN ({items}))"


def _emit_function(body: dict, string_cols, uint32_cols, float32_cols) -> str:
    fn = body.get("function")
    inputs = body.get("input", [])
    if isinstance(fn, dict) and "Boolean" in fn:
        boolean = fn["Boolean"]
        if isinstance(boolean, dict) and "IsIn" in boolean:
            return _emit_is_in(inputs)
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
            _validate_comparison(body["left"], op, string_cols)
            left = _emit_sql(body["left"], string_cols, uint32_cols, float32_cols)
            right = _emit_sql(body["right"], string_cols, uint32_cols, float32_cols)
            return f"({left} {_COMPARISON_SQL[op]} {right})"
        raise UnsupportedPredicate(f"unsupported binary op: {op}")
    if key == "Function":
        return _emit_function(body, string_cols, uint32_cols, float32_cols)
    raise UnsupportedPredicate(f"unsupported node type: {key}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_predicate_translator_units.py -v`
Expected: PASS (all 12 tests).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/predicate_translator.py tests/test_predicate_translator_units.py
git commit -m "feat: structured AST emitter for predicate pushdown (issue #396)"
```

---

## Task 2: `plan_predicate_pushdown` + `PushdownPlan` (conjunction split + certification)

**Files:**
- Modify: `polars_bio/predicate_translator.py` (add below `_emit_sql`)
- Test: `tests/test_predicate_translator_units.py` (append)

**Interfaces:**
- Consumes: `_emit_sql`, `UnsupportedPredicate` (Task 1); `PredicateTranslationError` (already defined in this module).
- Produces:
  - `@dataclass class PushdownPlan: pushdown_sql: Optional[str]; fully_translated: bool`
  - `plan_predicate_pushdown(predicate: pl.Expr, *, string_cols=None, uint32_cols=None, float32_cols=None) -> PushdownPlan`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_predicate_translator_units.py`:

```python
from polars_bio.predicate_translator import PushdownPlan, plan_predicate_pushdown


def plan(expr):
    return plan_predicate_pushdown(
        expr, string_cols=GFF_STR, uint32_cols=GFF_U32, float32_cols=GFF_F32
    )


def test_plan_full_translation_certified():
    p = plan((pl.col("type") == "transcript") & (pl.col("start") >= 10))
    assert p.fully_translated is True
    assert p.pushdown_sql == "(\"type\" = 'transcript') AND (\"start\" >= 10)"


def test_plan_partial_pushes_translatable_conjunct_only():
    # str.contains is unsupported → only the type== conjunct is pushed,
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
        (pl.col("type") == "exon")
        | pl.col("gene_biotype").str.contains("pseudogene")
    )
    assert p.pushdown_sql is None
    assert p.fully_translated is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_predicate_translator_units.py -k plan -v`
Expected: FAIL with `ImportError: cannot import name 'PushdownPlan'`.

- [ ] **Step 3: Implement**

Add to `polars_bio/predicate_translator.py` (below `_emit_sql`). Ensure `from dataclasses import dataclass` and `from typing import Optional` are imported at the top (add if missing):

```python
@dataclass
class PushdownPlan:
    pushdown_sql: Optional[str]
    fully_translated: bool


def _flatten_and(node: dict) -> list:
    if (
        isinstance(node, dict)
        and "BinaryExpr" in node
        and node["BinaryExpr"].get("op") == "And"
    ):
        body = node["BinaryExpr"]
        return _flatten_and(body["left"]) + _flatten_and(body["right"])
    return [node]


def plan_predicate_pushdown(
    predicate,
    *,
    string_cols=None,
    uint32_cols=None,
    float32_cols=None,
) -> PushdownPlan:
    try:
        ast = _json.loads(predicate.meta.serialize(format="json"))
    except Exception as exc:  # serialization itself failed → caller falls back
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_predicate_translator_units.py -v`
Expected: PASS (all tests, including the new `plan` ones).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/predicate_translator.py tests/test_predicate_translator_units.py
git commit -m "feat: conjunction-splitting plan_predicate_pushdown with faithful certification"
```

---

## Task 3: `extract_source_columns` (structured projection column extraction)

**Files:**
- Create: `polars_bio/pushdown.py`
- Test: `tests/test_pushdown_helpers.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `extract_source_columns(with_columns) -> tuple[list[str], bool]` — returns the **source** column names to request and a `complete` flag (`False` if any element's names could not be recovered). No column is ever silently dropped.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pushdown_helpers.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_pushdown_helpers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polars_bio.pushdown'`.

- [ ] **Step 3: Implement**

Create `polars_bio/pushdown.py`:

```python
"""Shared, audited pushdown helpers.

Correctness contract: the client-side full predicate filter and full projection
select are the source of truth. These helpers push down only what can be applied
faithfully, and always report whether the caller must reapply client-side.
"""

from typing import Any, List, Optional, Tuple

import polars as pl


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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pushdown_helpers.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/pushdown.py tests/test_pushdown_helpers.py
git commit -m "feat: structured source-column extraction for projection pushdown"
```

---

## Task 4: Shared helpers `apply_predicate_pushdown` / `apply_projection_pushdown`

**Files:**
- Modify: `polars_bio/pushdown.py`
- Test: `tests/test_pushdown_helpers.py` (append)

**Interfaces:**
- Consumes: `plan_predicate_pushdown`, `PredicateTranslationError` (Task 2); `extract_source_columns` (Task 3).
- Produces:
  - `apply_predicate_pushdown(query_df, predicate, col_types: dict, *, log) -> tuple[Any, bool]` returning `(query_df, needs_client_filter)`. `col_types` keys: `string_cols`, `uint32_cols`, `float32_cols`.
  - `apply_projection_pushdown(query_df, with_columns, *, log) -> tuple[Any, bool]` returning `(query_df, needs_client_select)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pushdown_helpers.py`:

```python
import logging

import pytest

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_pushdown_helpers.py -k "predicate or projection" -v`
Expected: FAIL with `ImportError: cannot import name 'apply_predicate_pushdown'`.

- [ ] **Step 3: Implement**

Append to `polars_bio/pushdown.py` (add the imports at the top of the file):

```python
from .predicate_translator import (
    PredicateTranslationError,
    plan_predicate_pushdown,
)


def apply_predicate_pushdown(query_df, predicate, col_types, *, log) -> Tuple[Any, bool]:
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pushdown_helpers.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/pushdown.py tests/test_pushdown_helpers.py
git commit -m "feat: centralized safe predicate/projection pushdown helpers"
```

---

## Task 5: Wire `_overlap_source` (io.py call site 1) through the helpers

**Files:**
- Modify: `polars_bio/io.py` (the `_overlap_source` body — predicate block ~2976-2999, projection block ~3001-3013, and the stream-loop reapply ~3025-3030)
- Test: `tests/test_io_gtf.py` (append a #396-shaped regression)

**Interfaces:**
- Consumes: `apply_predicate_pushdown`, `apply_projection_pushdown` (Task 4); `_FORMAT_COLUMN_TYPES`, `_extract_column_names_from_expr` (existing in io.py).
- Produces: unchanged public behavior; `_overlap_source` now routes pushdown through the helpers.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_io_gtf.py`:

```python
def test_gtf_str_contains_lazy_equals_eager(tmp_path):
    """Regression for #396: a str.contains filter must not be silently dropped."""
    import polars as pl

    import polars_bio as pb

    gtf = tmp_path / "mini.gtf"
    gtf.write_text(
        '1\thavana\ttranscript\t3073253\t3074322\t.\t+\t.\t'
        'gene_id "ENSMUSG00000102693"; transcript_id "ENSMUST00000193812"; '
        'gene_biotype "TEC";\n'
    )
    attr = ["gene_id", "gene_biotype", "transcript_id"]
    annot = pb.scan_gtf(str(gtf), attr_fields=attr)
    pseudo = annot.filter(pl.col("type") == "transcript").filter(
        pl.col("gene_biotype").str.contains("pseudogene")
    )
    lazy = pseudo.select("transcript_id").collect()
    eager = pseudo.collect().select("transcript_id")
    assert lazy.equals(eager)
    assert lazy.height == 0  # no pseudogenes in the fixture
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_io_gtf.py::test_gtf_str_contains_lazy_equals_eager -v`
Expected: FAIL — lazy returns 1 row (filter dropped) while eager returns 0, so `lazy.equals(eager)` is False. (This is the #396 bug reproduced against the current code path.)

- [ ] **Step 3: Replace the inline pushdown blocks**

In `polars_bio/io.py`, add near the other local imports inside `_overlap_source` (top of the function body):

```python
        from .pushdown import apply_predicate_pushdown, apply_projection_pushdown
```

Replace the predicate block (currently the `datafusion_predicate_applied = False` ... `except Exception as e:` warning, ~lines 2976-2999) with:

```python
        # 2. Predicate pushdown (optimization; client-side filter is source of truth)
        _fmt_key = str(input_format).rsplit(".", 1)[-1]
        _scols, _ucols, _fcols = _FORMAT_COLUMN_TYPES.get(_fmt_key, (None, None, None))
        needs_client_filter = predicate is not None
        if predicate_pushdown and predicate is not None:
            query_df, needs_client_filter = apply_predicate_pushdown(
                query_df,
                predicate,
                {"string_cols": _scols, "uint32_cols": _ucols, "float32_cols": _fcols},
                log=logger,
            )
```

Replace the projection block (currently `datafusion_projection_applied = False` ... `except Exception as e:` debug, ~lines 3001-3013) with:

```python
        # 3. Projection pushdown (optimization; client-side select is source of truth)
        needs_client_select = with_columns is not None
        if projection_pushdown and with_columns is not None:
            query_df, needs_client_select = apply_projection_pushdown(
                query_df, with_columns, log=logger
            )
```

Replace the stream-loop reapply (currently the two `if ... not datafusion_*_applied` blocks, ~lines 3025-3030) with:

```python
            if predicate is not None and needs_client_filter:
                out = out.filter(predicate)
            if with_columns is not None and needs_client_select:
                out = out.select(with_columns)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_io_gtf.py tests/test_io_gff.py tests/test_predicate_pushdown.py tests/test_projection_pushdown.py -v`
Expected: PASS (the new regression plus existing GFF/GTF/pushdown tests).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/io.py tests/test_io_gtf.py
git commit -m "fix: route _overlap_source pushdown through safe helpers (issue #396)"
```

---

## Task 6: Wire the GFF/GTF `select()` wrapper (io.py call site 2)

**Files:**
- Modify: `polars_bio/io.py` (the `select()` method block ~3541-3593: the `translate_predicate`/`datafusion_expr_to_sql` try/except and the `datafusion_columns`/reapply logic)
- Test: `tests/test_filter_select_attributes_bug_fix.py` (append)

**Interfaces:**
- Consumes: `apply_predicate_pushdown` (Task 4). This path drives projection manually (it must request `scan_columns` = projection + predicate roots for the one-shot SQL), so it uses the predicate helper plus its existing explicit select.
- Produces: unchanged public behavior; deferred predicate routed through the helper.

- [ ] **Step 1: Write the failing/period test**

Append to `tests/test_filter_select_attributes_bug_fix.py`:

```python
def test_select_wrapper_str_contains_not_dropped(tmp_path):
    import polars as pl

    import polars_bio as pb

    gtf = tmp_path / "mini2.gtf"
    gtf.write_text(
        '1\thavana\ttranscript\t1\t100\t.\t+\t.\t'
        'gene_id "G1"; transcript_id "T1"; gene_biotype "TEC";\n'
    )
    annot = pb.scan_gtf(str(gtf), attr_fields=["gene_id", "gene_biotype", "transcript_id"])
    q = annot.filter(pl.col("gene_biotype").str.contains("pseudogene")).select(
        "transcript_id"
    )
    assert q.collect().height == 0
    assert q.collect().equals(annot.collect().filter(
        pl.col("gene_biotype").str.contains("pseudogene")
    ).select("transcript_id"))
```

- [ ] **Step 2: Run test to verify it fails (or already passes via deferred reapply)**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py::test_select_wrapper_str_contains_not_dropped -v`
Expected: This path already reapplies the deferred predicate client-side, so it may PASS. If it PASSES, keep it as a guard and still perform the refactor in Step 3 for consistency and to remove the duplicated translate block. If it FAILS, the refactor fixes it.

- [ ] **Step 3: Replace the inline translate block**

In `polars_bio/io.py` `select()` method, add the import near the top of the method body:

```python
            from .pushdown import apply_predicate_pushdown
```

Replace the deferred-predicate try/except (`datafusion_predicate_applied = False` ... the `except Exception as e:` warning, ~lines 3541-3567) with:

```python
            datafusion_predicate_applied = False
            if self._predicate_pushdown and self._deferred_predicate is not None:
                _fmt_key = str(input_fmt).rsplit(".", 1)[-1]
                _scols, _ucols, _fcols = _FORMAT_COLUMN_TYPES.get(
                    _fmt_key, (None, None, None)
                )
                query_df, _needs_client = apply_predicate_pushdown(
                    query_df,
                    self._deferred_predicate,
                    {"string_cols": _scols, "uint32_cols": _ucols, "float32_cols": _fcols},
                    log=logger,
                )
                datafusion_predicate_applied = not _needs_client
```

The existing downstream logic (`datafusion_columns = columns if datafusion_predicate_applied else scan_columns`, the explicit `select`, and the `if ... and not datafusion_predicate_applied: new_lf = new_lf.filter(...)` reapply) is preserved unchanged — it already keys off `datafusion_predicate_applied`, which now means "fully translated, no reapply needed."

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py tests/test_io_gff.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add polars_bio/io.py tests/test_filter_select_attributes_bug_fix.py
git commit -m "refactor: route GFF/GTF select() deferred predicate through safe helper"
```

---

## Task 7: Wire the overlap/range `register_io_source` path (utils.py)

**Files:**
- Modify: `polars_bio/utils.py` (the predicate translate block and the two stream-loop reapply sites ~lines 91-107 and the streaming loop below)
- Test: `tests/test_predicate_pushdown.py` (append a utils-path equivalence guard)

**Interfaces:**
- Consumes: `apply_predicate_pushdown` (Task 4).
- Produces: unchanged behavior; utils path routed through the helper.

- [ ] **Step 1: Read the current block**

Run: `grep -n "translate_predicate\|datafusion_predicate_applied\|not datafusion_predicate_applied" polars_bio/utils.py`
Expected: shows the inline translate block and the two `if predicate is not None and not datafusion_predicate_applied:` reapply sites.

- [ ] **Step 2: Establish the guard suite (existing tests exercise this path)**

The `utils.py` `_lazy_scan` callback backs `pb.overlap` / `pb.nearest` / `pb.count_overlaps` / `pb.coverage` / `pb.merge` / `pb.cluster` / `pb.complement` / `pb.subtract` (see `polars_bio/__init__.py`). These are already covered by `tests/test_overlap_algorithms.py`, `tests/test_partitioned_range_operation_regressions.py`, and `tests/test_polars_bio_projection_validation.py`. Capture the baseline before refactoring:

Run: `python -m pytest tests/test_overlap_algorithms.py tests/test_partitioned_range_operation_regressions.py tests/test_polars_bio_projection_validation.py -q`
Expected: PASS (record the count; the refactor must keep all green).

- [ ] **Step 3: Refactor utils.py**

In `polars_bio/utils.py`, locate the predicate translate try/except inside the `register_io_source` callback (the block that imports `translate_polars_predicate_to_datafusion` and does `query_df = query_df.filter(datafusion_predicate)`). The overlap/range result has arbitrary user columns (not a fixed bio format), so pass **permissive** column-type sets (`None`) — matching the prior GFF-defaults wrapper's permissive treatment of unknown columns and letting DataFusion type-check at execution.

`utils.py`'s `query_df` is a `datafusion.DataFrame` which also exposes `parse_sql_expr` (verified), so the shared helper binds correctly here. Add `from .pushdown import apply_predicate_pushdown` and `import logging; logger = logging.getLogger(__name__)` near the top of `utils.py` if not already present, then replace the try/except with:

```python
            needs_client_filter = predicate is not None
            if predicate is not None:
                query_df, needs_client_filter = apply_predicate_pushdown(
                    query_df,
                    predicate,
                    {"string_cols": None, "uint32_cols": None, "float32_cols": None},
                    log=logger,
                )
```

Then change both stream-loop reapply sites from:

```python
            if predicate is not None and not datafusion_predicate_applied:
                df = df.filter(predicate)
```
to:
```python
            if predicate is not None and needs_client_filter:
                df = df.filter(predicate)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_overlap_algorithms.py tests/test_partitioned_range_operation_regressions.py tests/test_polars_bio_projection_validation.py -q`
Expected: PASS (same count as the Step 2 baseline).

- [ ] **Step 5: Commit**

```bash
git add polars_bio/utils.py
git commit -m "refactor: route overlap/range predicate pushdown through safe helper"
```

---

## Task 8: Wire the pileup path (pileup_op.py)

**Files:**
- Modify: `polars_bio/pileup_op.py` (predicate try/except ~lines 174-192 and the stream-loop reapply)
- Test: `tests/test_pileup.py` (append a guard)

**Interfaces:**
- Consumes: `apply_predicate_pushdown` (Task 4).
- Produces: unchanged behavior; pileup path routed through the helper.

- [ ] **Step 1: Write the guard test**

Append to `tests/test_pileup.py`. The module already defines `BAM_PATH = "tests/data/io/bam/test.bam"` at the top and uses `pb.depth(...)`; reuse it:

```python
def test_pileup_predicate_lazy_equals_eager():
    """Pileup predicate pushdown must match client-side filtering."""
    import polars as pl

    import polars_bio as pb

    lf = pb.depth(BAM_PATH).filter(pl.col("coverage") >= 1)
    eager = pb.depth(BAM_PATH).collect().filter(pl.col("coverage") >= 1)
    lazy_df = lf.collect()
    assert lazy_df.sort(by=lazy_df.columns).equals(eager.sort(by=eager.columns))
```

- [ ] **Step 2: Run test to verify it passes or fails**

Run: `python -m pytest tests/test_pileup.py::test_pileup_predicate_lazy_equals_eager -v`
Expected: PASS or FAIL depending on translation; either way Step 3 makes the path consistent.

- [ ] **Step 3: Refactor pileup_op.py**

In `polars_bio/pileup_op.py`, add `from .pushdown import apply_predicate_pushdown` near the callback's local imports. Replace the predicate try/except (`predicate_pushed_down = False` ... `except Exception as e: logger.debug(...)`, ~lines 174-192) with:

```python
            needs_client_filter = predicate is not None
            if predicate is not None:
                query_df, needs_client_filter = apply_predicate_pushdown(
                    query_df,
                    predicate,
                    {
                        "string_cols": {"contig"},
                        "uint32_cols": {"pos", "pos_start", "pos_end", "coverage"},
                        "float32_cols": None,
                    },
                    log=logger,
                )
```

Then in the stream loop, ensure the client-side reapply uses `needs_client_filter`:

```python
                if predicate is not None and needs_client_filter:
                    df = df.filter(predicate)
```

(If the pileup loop did not previously reapply client-side at all, this adds the correctness safety net it was missing.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pileup.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add polars_bio/pileup_op.py tests/test_pileup.py
git commit -m "refactor: route pileup predicate pushdown through safe helper"
```

---

## Task 9: Hypothesis property test (eager == lazy) + #396 fixture regression

**Files:**
- Modify: `pyproject.toml` (add `hypothesis` to the test dependency group)
- Create: `tests/test_pushdown_equivalence.py`

**Interfaces:**
- Consumes: the full wired pipeline (Tasks 5-8).
- Produces: the headline invariant test.

- [ ] **Step 1: Add the dev dependency**

In `pyproject.toml`, find the test dependencies list (the block containing `"pytest>=8.3.3"`, `"pytest-cov>=6.0.0"`) and add:

```toml
  "hypothesis>=6.100.0",
```

Then install it:

Run: `python -m pip install "hypothesis>=6.100.0"`
Expected: hypothesis installed.

- [ ] **Step 2: Write the property test (it should pass once the pipeline is wired)**

Create `tests/test_pushdown_equivalence.py`:

```python
import polars as pl
from hypothesis import given, settings
from hypothesis import strategies as st

import polars_bio as pb

GTF_FIXTURE = (
    '#!genome-build GRCm38.p6\n'
    '1\thavana\tgene\t3073253\t3074322\t.\t+\t.\t'
    'gene_id "G1"; gene_biotype "TEC";\n'
    '1\thavana\ttranscript\t3073253\t3074322\t.\t+\t.\t'
    'gene_id "G1"; transcript_id "T1"; gene_biotype "TEC";\n'
    '1\tensembl\ttranscript\t3102016\t3102125\t.\t+\t.\t'
    'gene_id "G2"; transcript_id "T2"; gene_biotype "snRNA";\n'
)
ATTRS = ["gene_id", "gene_biotype", "transcript_id"]


def _predicate(kind, value):
    if kind == "type_eq":
        return pl.col("type") == value
    if kind == "type_in":
        return pl.col("type").is_in(["transcript", "exon"])
    if kind == "start_ge":
        return pl.col("start") >= value
    if kind == "biotype_contains":
        return pl.col("gene_biotype").str.contains(value)
    if kind == "combined":
        return (pl.col("type") == "transcript") & pl.col("gene_biotype").str.contains(
            value
        )
    raise AssertionError(kind)


@settings(max_examples=75, deadline=None)
@given(
    kind=st.sampled_from(
        ["type_eq", "type_in", "start_ge", "biotype_contains", "combined"]
    ),
    value=st.sampled_from(["transcript", "gene", "pseudogene", "snRNA", "TEC", 3100000]),
    cols=st.sampled_from(
        [["transcript_id"], ["type", "gene_biotype"], ["chrom", "start", "transcript_id"]]
    ),
)
def test_lazy_equals_eager(tmp_path_factory, kind, value, cols):
    gtf = tmp_path_factory.mktemp("eq") / "f.gtf"
    gtf.write_text(GTF_FIXTURE)
    pred = _predicate(kind, value)
    scan = pb.scan_gtf(str(gtf), attr_fields=ATTRS)

    lazy = scan.filter(pred).select(cols).collect().sort(by=cols)
    eager = scan.collect().filter(pred).select(cols).sort(by=cols)
    assert lazy.equals(eager), f"kind={kind} value={value} cols={cols}"
```

- [ ] **Step 3: Run the property test**

Run: `python -m pytest tests/test_pushdown_equivalence.py -v`
Expected: PASS for every generated example. A failure prints the exact `(kind, value, cols)` that diverges — that is a real correctness bug to fix before proceeding.

- [ ] **Step 4: Run the full pushdown-related suite**

Run: `python -m pytest tests/test_pushdown_equivalence.py tests/test_predicate_translator_units.py tests/test_pushdown_helpers.py tests/test_io_gtf.py tests/test_io_gff.py tests/test_gff_eager_vs_lazy.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml tests/test_pushdown_equivalence.py
git commit -m "test: hypothesis eager-vs-lazy pushdown equivalence (issue #396)"
```

---

## Task 10: Delete the dead regex translator

**Files:**
- Modify: `polars_bio/predicate_translator.py` (remove the old regex chain)
- Modify: `polars_bio/io.py` (replace `_extract_column_names_from_expr` with `extract_source_columns` where still referenced for the scan-column computation)

**Interfaces:**
- Consumes: confirmation that no caller imports the deleted names.
- Produces: a translator whose only public translation entry points are `plan_predicate_pushdown` / `_emit_sql`.

- [ ] **Step 1: Confirm no remaining callers of the old API**

Run: `grep -rn "translate_predicate\|datafusion_expr_to_sql\|translate_polars_predicate_to_datafusion\|is_predicate_pushdown_supported" polars_bio/ tests/`
Expected: only references inside `predicate_translator.py` itself (and possibly `sql_predicate_builder.py` / `_build_sql_where_from_predicate_safe`, which are used only by write/selection utils — leave those untouched). If any production scan path still imports `translate_predicate`, stop and wire it through the helper first.

- [ ] **Step 2: Delete the regex functions**

In `polars_bio/predicate_translator.py`, delete: `_strip_polars_wrapping`, `_translate_polars_expr`, `_is_binary_expr`, `_translate_binary_expr`, `_is_and_expr`, `_translate_and_expr`, `_is_in_expr`, `_translate_in_expr`, `_is_not_in_expr`, `_translate_not_in_expr`, `_is_between_expr`, `_translate_between_expr`, `_is_not_null_expr`, `_translate_not_null_expr`, `_is_null_expr`, `_translate_null_expr`, `_extract_column_name`, `_extract_literal_value`, `_validate_column_operator`, `_parse_list_values`, `_split_on_main_operator`, `_parse_comparison`, `_create_mock_expr`, `datafusion_expr_to_sql`, and the `translate_predicate` / `translate_polars_predicate_to_datafusion` wrappers. Keep `PredicateTranslationError`, the column-type constants, `UnsupportedPredicate`, the emitter, `PushdownPlan`, and `plan_predicate_pushdown`. Remove the now-unused `import re` and the `_ACTIVE_*` module globals.

- [ ] **Step 3: Replace `_extract_column_names_from_expr` references**

In `polars_bio/io.py`, where `_extract_column_names_from_expr(with_columns)` is still called to compute `requested_cols`/`scan_columns` (the `select()` wrapper at ~3004 and ~2886), replace with:

```python
from .pushdown import extract_source_columns
requested_cols, _ = extract_source_columns(with_columns)
```

Leave the function definition in place only if other modules still import it (check with grep); otherwise delete it too.

- [ ] **Step 4: Run the full test suite**

Run: `python -m pytest tests/test_predicate_translator_units.py tests/test_pushdown_helpers.py tests/test_pushdown_equivalence.py tests/test_io_gtf.py tests/test_io_gff.py tests/test_io_vcf.py tests/test_io_bam.py tests/test_predicate_pushdown.py tests/test_projection_pushdown.py tests/test_pileup.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add polars_bio/predicate_translator.py polars_bio/io.py
git commit -m "refactor: remove dead regex predicate translator (issue #396)"
```

---

## Self-Review Notes

- **Spec coverage:** §4.1 emitter → Task 1; §4.2 plan + certification → Task 2; §4.3 predicate helper → Task 4; §4.4 projection helper + `extract_source_columns` → Tasks 3-4; §4.5 exception narrowing → Tasks 1/4 (typed errors, single broad bind-catch); §3 invariant (both predicate & projection) → Tasks 4-8; §7 testing (per-node, helper fallback, property, #396 regression, all call sites) → Tasks 1-9; §8 rollout (safety lever effectively lands with Tasks 4-8, translator with 1-2, property test with 9, dead-code delete with 10).
- **Known behavior choices:** `is_in` pushdown preserved via pyarrow Arrow-IPC decode (Task 1); decode failure → client-side. String functions intentionally client-side. String-column ordering ops intentionally client-side.
- **Type consistency:** `apply_predicate_pushdown(query_df, predicate, col_types, *, log) -> (df, needs_client_filter)` and `apply_projection_pushdown(query_df, with_columns, *, log) -> (df, needs_client_select)` are used identically in Tasks 5-8. `plan_predicate_pushdown` keyword args (`string_cols`/`uint32_cols`/`float32_cols`) match `col_types` keys consumed by the helper.
