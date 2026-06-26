# GFF/GTF: emit nested `attributes` alongside flattened fields (sentinel design)

- **Date:** 2026-06-26
- **Status:** Approved (brainstorm) — ready for implementation plan
- **Repos touched:** `datafusion-bio-formats` (upstream, Rust) + `polars-bio` (Python/Rust glue)
- **Related:** PR #397 (`issue-396-gff-gtf-predicate-fallback`), issue #396
- **Supersedes:** the interim `NotImplementedError` guard added to PR #397

## Problem

`AnnotationLazyFrameWrapper.select()` re-registers the GFF/GTF file to materialize
attribute columns on demand. A single registration can expose **either** the raw
nested `attributes` column **or** flattened attribute fields, never both. When a
query needs both at once — the projection wants one representation and the deferred
predicate the other — the registered table is missing a column and the query fails.

Two reachable failures (both confirmed):

1. `scan_gff().filter(<predicate on raw attributes>).select("ID")`
   — table registered with `attr_fields=["ID"]`, raw `attributes` absent → fallback
   select on `attributes` errors.
2. `scan_gff(attr_fields=["ID"]).filter(pl.col("ID")...).select("attributes")`
   — table registered with `attr_fields=None`, flattened `ID` absent → fallback
   select/filter on `ID` errors.

A projection-only variant `scan_gff().select(["attributes","ID"])` is the same
root cause and already crashed before PR #397.

PR #397 currently converts these into a clear `NotImplementedError`. This spec
replaces that with a real fix that returns correct results.

## Root cause (upstream reader)

In `datafusion-bio-formats` the GFF/GTF readers have two mutually exclusive modes
driven by `attr_fields` (see `table_provider.rs::determine_schema_on_demand` and
`physical_exec.rs::get_local_gff` / object-store variant):

- **Mode 1** (`attr_fields=None`): schema = 8 static columns + nested
  `attributes` `List<Struct{tag: Utf8, value: Utf8}>`. Execution fills a "nested
  builder".
- **Mode 2** (`attr_fields=Some([...])`): schema = 8 static columns + one **Utf8**
  column per requested field. Execution fills "flattened builders".

A binary `unnest_enable` flag selects exactly one builder set at batch assembly.
Requesting `attr_fields=["ID","attributes"]` today produces flattened `ID` plus an
**all-null Utf8** `attributes` column, because the executor looks up a nonexistent
attribute key literally named `"attributes"`.

## Design

### Mechanism: `"attributes"` as a sentinel in Mode 2

When `attr_fields` (Mode 2) contains the literal name `"attributes"`, the reader
emits the **nested `List<Struct>`** column for it (Mode-1 representation), in the
positional slot matching its index in `attr_fields`, alongside the other flattened
fields. The remaining names continue to flatten as today.

This is purely additive: `attr_fields=None` and `attr_fields` without
`"attributes"` are unchanged.

### Upstream change — `datafusion-bio-formats` (mirror in `bio-format-gff` and `bio-format-gtf`)

**Branch base:** the rev currently pinned by polars-bio (`94d4d68…`), to isolate
this feature from unrelated `master` changes.

1. **Schema** (`table_provider.rs::determine_schema_on_demand`): in the Mode 2 loop
   over `attr_fields`, if the name == `"attributes"`, push the nested
   `List<Struct{tag,value}>` field (identical to the Mode 1 attributes field)
   instead of a `Utf8` field. Field order follows `attr_fields` order.

2. **Execution** (`physical_exec.rs`, both the local and the object-store streaming
   functions):
   - Partition `attr_fields` into `real_keys` (names ≠ `"attributes"`) and a flag
     `include_nested = "attributes" ∈ attr_fields`.
   - Keep flattened builders for `real_keys`. When `include_nested`, also populate
     the nested builder per record (the same path Mode 1 uses).
   - At batch assembly, build the output column vector by iterating `attr_fields`
     in order: for `"attributes"` take the nested array, otherwise the matching
     flattened array; prepend the static columns. Respect `projection` so
     unrequested columns are still skipped.
   - The `needs_attributes` short-circuit (skip parsing when only static columns
     are requested) must remain true when `include_nested` or `real_keys` is
     non-empty.

3. Leave the existing either/or fast paths intact for the two unchanged modes.

> Note: a latent `object_storage_options.unwrap()` in `get_local_gff` panics when
> `None` is passed directly. The high-level polars-bio API never passes `None`, so
> hardening it is **out of scope** here (noted for a future change).

### polars-bio change

**`polars_bio/io.py` — `AnnotationLazyFrameWrapper.select()`**: replace the
`NotImplementedError` guard with a combined `_attr` request:

```python
needs_raw_attributes = "attributes" in scan_columns   # columns ∪ predicate roots
if needs_raw_attributes and attr_cols:
    _attr = attr_cols + ["attributes"]   # flattened fields + nested sentinel
elif needs_raw_attributes:
    _attr = None                          # pure nested (unchanged)
elif attr_cols:
    _attr = attr_cols                     # flattened only (unchanged)
else:
    _attr = []
```

`attr_cols` already excludes `"attributes"` (it is in `STATIC`), so no duplication.
No other code in `select()` changes: once the registered table contains static +
flattened + nested `attributes`, the existing predicate-pushdown/fallback and the
final `.select(columns)` resolve every column by name and succeed.

**`Cargo.toml`**: during development, point the ten `datafusion-bio-format-*`
dependencies at the local clone path (`/Users/mwiewior/research/git/datafusion-bio-formats`)
via `path = ...` to iterate without pushing. For the final commit, push the
upstream feature branch and pin all ten deps to its commit SHA. Rebuild with
`maturin develop --release`.

## Testing (TDD)

Tests are written before implementation in each repo and must fail first.

### Upstream (`datafusion-bio-formats`) — Rust unit/integration tests
- Schema: `attr_fields=["ID","attributes"]` yields columns `[...static, ID: Utf8,
  attributes: List<Struct{tag,value}>]` in that order.
- Execution: a small GFF and GTF fixture returns correct flattened `ID` **and**
  fully-populated nested `attributes` for every row, including a row missing the
  flattened key (→ null) while its nested `attributes` is still complete.
- Unchanged-mode regression: `attr_fields=None` and `attr_fields=["ID"]` outputs
  are byte-for-byte identical to the pinned-rev behavior.

### polars-bio — pytest (extend `tests/test_filter_select_attributes_bug_fix.py`, `tests/test_io_gtf.py`)
- **Rewrite** the two interim tests that assert `NotImplementedError` to assert
  correct results instead.
- Finding 1: `scan_gff().filter(pl.col("attributes").list.len() > N).select("ID")`
  equals the `.collect()`-first oracle and has the expected rows.
- Finding 2: `scan_gff(attr_fields=["ID"]).filter(pl.col("ID").str.contains(...))
  .select("attributes")` returns the correct nested attributes for filtered rows.
- Projection-only: `scan_gff().select(["attributes","ID"])` returns both columns
  with correct values.
- Issue #396 eager-vs-lazy equivalence: `lf.select(x).collect()` equals
  `lf.collect().select(x)` for the mixed cases.
- GTF equivalents of the above.
- Oracle for parsed-field correctness: a derived field extracted from the nested
  `attributes` via `list.eval(... struct.field ...)` equals the native flattened
  column (already verified true for GFF and GTF during brainstorming).

### Verification commands
- Upstream: `cargo test -p datafusion-bio-format-gff -p datafusion-bio-format-gtf`
- polars-bio: `maturin develop --release` then
  `python -m pytest tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py tests/test_gff_eager_vs_lazy.py tests/test_optimization_bug_fix.py`

## Risks & mitigations

- **Two-repo coordination / pin ordering.** Mitigate with a local `path` override
  for development; flip to a pushed commit SHA only for the final commit.
- **Object-store streaming path divergence.** The remote variant must receive the
  same dual-builder change as the local one; covered by an explicit checklist item
  and (where feasible) a test.
- **Column-order / projection mapping bugs.** Assemble output strictly in
  `attr_fields` order and honor `projection`; covered by schema-order tests.
- **Rebuild cost.** `maturin develop --release` against a changed Rust dep is slow;
  expected, not avoidable.

## Out of scope
- Hardening the `object_storage_options.unwrap()` panic.
- Changing the default (`attr_fields=None`) or pure-flattened behavior.
- DataFusion-level predicate pushdown for nested `attributes` (`List<Struct>`)
  predicates — these continue to evaluate client-side.
