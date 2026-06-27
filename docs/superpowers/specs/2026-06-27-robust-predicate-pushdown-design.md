# Robust Predicate Pushdown — Design Spec

- **Date:** 2026-06-27
- **Issue:** [#396 — Incorrect results from scan_gtf(), dropping a filter()](https://github.com/biodatageeks/polars-bio/issues/396)
- **Triggering review:** [tgbrooks comment 4564747142](https://github.com/biodatageeks/polars-bio/issues/396#issuecomment-4564747142)
- **Status:** Proposed (awaiting review)

## 1. Problem

`scan_gtf()` (and the other scan paths) can return **silently wrong results**: a
`filter()` is dropped during predicate pushdown and never reapplied, so a query
returns rows that the predicate should have excluded — with no error, only a log
warning at best.

Two root causes, both flagged by the reviewer:

1. **The translator parses a stringified expression with regex.** `predicate_translator.py`
   runs `re.search(pattern, str(expr))` (lines ~193, 210, 263, 386) and a final
   regex SQL-cleanup pass (lines ~637–658). Any AST node whose textual form no
   pattern matches simply *vanishes* from the generated SQL — a silent partial
   translation that can both drop conditions and (worse) emit a predicate that
   removes rows the real predicate keeps.

2. **Correctness depends on translation success.** The client-side safety filter is
   applied *conditionally* — only `if not datafusion_predicate_applied`
   (`io.py:3026`, `utils.py:92`, the second `io.py` path ~3563, `pileup_op.py`).
   So a translation that *looks* successful but is subtly wrong produces wrong
   results with **no net**. The `except Exception` around pushdown is also a
   catch-all that swallows genuine bugs (AttributeError, etc.) into "drop the
   filter".

The recent `feat/gff-gtf-attributes-sentinel` commits fixed specific GFF/GTF
symptoms but left both root causes in place.

## 2. Goals / Non-goals

**Goals**

- A translation bug can cost *performance* but **never correctness**. Pushdown
  becomes a pure optimization.
- Replace regex-on-stringified-`Expr` with a structured walk over the Polars
  expression AST. Unsupported nodes fail *loudly and locally*, never silently.
- Push down the translatable subset of a predicate even when the whole thing
  isn't translatable (partial pushdown), without sacrificing correctness.
- Centralize the copy-pasted fallback logic into one audited helper used by all
  scan paths, covering **both** predicate and projection pushdown.
- Bring projection pushdown under the same correctness invariant: a botched
  projection must never silently change the output. (Projection keeps its existing
  structured column-name extraction — it does **not** use the AST translator; see
  §4.4.)
- Lock the eager-vs-lazy equivalence invariant — for predicate **and** projection
  — with a property-based test that mirrors the reviewer's fuzzer.
- **Extensive unit-test coverage** as a first-class deliverable, not an
  afterthought: every AST node type, every fallback branch, and every call site
  is covered (see §7).

**Non-goals**

- The **AST-to-SQL translator (§4.1) is not applied to projection.** Projection
  only needs column *names*, which Polars already exposes structurally
  (`meta.output_name()` / `meta.root_names()`); there is no boolean expression to
  translate. Projection is in scope only for the correctness lever (§3) and the
  centralized helper (§4.4).
- No new operators or expanded predicate coverage beyond what is needed to keep
  parity with today's supported set. Coverage can grow later against the new
  structured translator.
- Rust-side changes are out of scope; this is a Python-layer correctness fix.

## 3. Core invariant

> **The client-side filter on the full original predicate is the source of truth.
> Pushdown may only ever remove rows the client-side filter would also remove.**

Formally: let `P` be the user predicate and `P'` the predicate we push to
DataFusion. We guarantee `P ⟹ P'` (the pushed predicate keeps a *superset* of
rows). Reapplying `P` client-side then yields exactly `P`'s rows regardless of how
incomplete or buggy the translation of `P'` was.

`P ⟹ P'` is guaranteed structurally by only ever pushing a **subset of the
top-level AND-conjuncts** of `P`, each of which must be **faithfully** translated:

- **AND is splittable.** `a AND b AND c`, pushing only `a AND c`, keeps a superset
  of rows. Safe.
- **OR is atomic.** Pushing one side of `a OR b` would wrongly drop rows. An OR is
  translated whole or not pushed at all.
- **Faithful** means the translator emits SQL for a node *only* when it is certain
  the SQL is semantically equivalent; any doubt → mark the node unsupported.

When *every* conjunct of `P` translates faithfully, the translator certifies
`fully_translated = True`, and the client-side reapply is skipped as an
optimization. Otherwise the full `P` is reapplied client-side.

**The same principle governs projection.** The requested column set `C` is the
source of truth. Pushdown may select a *superset* of `C` from the source; the
client-side `.select(with_columns)` then yields exactly `C`. The client reapply is
skipped only when the extraction is certified complete (every requested column was
recovered structurally with no swallowed error). A botched projection therefore
costs at most an extra select, never a wrong/missing column.

## 4. Architecture

Four components.

### 4.1 Structured AST translator (`predicate_translator.py`, rewritten)

Input: a Polars `pl.Expr` plus the format's column-type sets (unchanged signature
intent). Output: a `PushdownPlan` (below).

Mechanism: `json.loads(expr.meta.serialize(format="json"))` → recursive emitter
over the AST. Confirmed node shapes in Polars 1.40.1:

```
BinaryExpr { left, op, right }      op ∈ {Eq, NotEq, Lt, LtEq, Gt, GtEq, And, Or}
Column "<name>"
Literal { Scalar { String|Int|Float|Boolean: <value> } }
Function { input: [...], function: { StringExpr: { Contains|StartsWith|EndsWith } } }
Function { input: [...], function: { Boolean: { IsIn ... } } }
```

Each node type has an explicit handler that emits a SQL fragment:

- `Column` → quoted identifier (`"name"`), validated against the active column-type
  sets where types matter.
- `Literal.Scalar.*` → correctly formatted SQL literal (string quoting/escaping,
  numeric, boolean) — replacing the regex cleanup pass entirely.
- `BinaryExpr` comparison ops → `<left> <op> <right>`, with column-type checks
  (e.g. string columns: equality/IN only, matching today's rules).
- `BinaryExpr` And/Or → combine fragments; And participates in conjunct splitting,
  Or is atomic.
- `Function.Boolean.IsIn` → `<col> IN (...)`.
- String functions (`Contains`, `StartsWith`, `EndsWith`) → **treated as
  unsupported for pushdown by default** (the original failure was exactly
  `str.contains` being unpushable in the bio-format provider). They are not
  emitted; they fall to client-side filtering. This is correct under the invariant
  and removes the temptation to emit a wrong `LIKE`/regex.

Any unrecognized node type, or a recognized node in an unsupported position, raises
`UnsupportedPredicate(node)` — caught locally during conjunct splitting, never
allowed to silently disappear.

The old `_translate_polars_expr` regex chain and `datafusion_expr_to_sql` regex
cleanup are deleted.

### 4.2 Conjunction decomposition + certification

```python
@dataclass
class PushdownPlan:
    pushdown_sql: Optional[str]   # SQL for the faithfully-translatable AND-conjunct
                                  # subset, or None if nothing is pushable
    fully_translated: bool        # True iff the ENTIRE predicate translated faithfully

def plan_predicate_pushdown(
    predicate: pl.Expr,
    *, string_cols, uint32_cols, float32_cols,
) -> PushdownPlan
```

Algorithm:
1. Flatten the top-level `AND` chain into conjuncts `[c1, c2, ...]`.
2. Translate each `ci` independently; collect the ones that translate faithfully.
3. `pushdown_sql` = the translatable conjuncts re-joined with `AND` (or `None`).
4. `fully_translated` = every conjunct translated faithfully.

A single non-AND predicate is the degenerate one-conjunct case.

### 4.3 Centralized safe-pushdown helper (new, shared)

One function replaces the four copy-pasted blocks (`io.py` ×2, `utils.py`,
`pileup_op.py`):

```python
def apply_predicate_pushdown(query_df, predicate, col_types, *, log) -> tuple[Any, bool]:
    """Returns (query_df_with_pushdown, needs_client_filter)."""
    if predicate is None:
        return query_df, False
    try:
        plan = plan_predicate_pushdown(predicate, **col_types)
    except PredicateTranslationError as e:
        log.warning("predicate pushdown skipped (translation): %s", e)
        return query_df, True
    if plan.pushdown_sql is not None:
        try:
            native = query_df.parse_sql_expr(plan.pushdown_sql)
            query_df = query_df.filter(native)
        except Exception as e:               # parse_sql_expr / filter binding failure
            log.warning("predicate pushdown skipped (bind): %s", e)
            return query_df, True            # nothing safely pushed → full client filter
    return query_df, not plan.fully_translated
```

Callers then do, in the stream loop:

```python
if predicate is not None and needs_client_filter:
    out = out.filter(predicate)
```

Note the SQL → `parse_sql_expr` round-trip is **retained** — it is required by the
documented PyO3 binding mismatch (pip `datafusion` Expr is incompatible with the
`polars_bio` Rust extension; `parse_sql_expr` rebuilds a binding-compatible Expr
from the same compilation unit). The structured translator changes how the SQL is
*built*, not how it is bound.

### 4.4 Centralized safe-projection helper (new, shared)

A sibling helper applies the same source-of-truth discipline to projection. It does
**not** use the AST translator — it relies on Polars' structured column-name API.

```python
def apply_projection_pushdown(query_df, with_columns, *, log) -> tuple[Any, bool]:
    """Returns (query_df_with_projection, needs_client_select)."""
    if with_columns is None:
        return query_df, False
    cols, complete = extract_source_columns(with_columns)   # structured, no silent skips
    if not cols:
        return query_df, True                               # couldn't determine → client select
    try:
        query_df = query_df.select(*[query_df.parse_sql_expr(f'"{c}"') for c in cols])
    except Exception as e:                                   # parse_sql_expr / select binding failure
        log.warning("projection pushdown skipped (bind): %s", e)
        return query_df, True
    return query_df, not complete
```

`extract_source_columns` replaces `_extract_column_names_from_expr`. It uses
`expr.meta.root_names()` to determine which **source** columns must be requested
(so a computed/aliased projection like `(col("c")+1).alias("y")` correctly requests
`c`, not `y`), and returns a `complete` flag that is `False` if *any* element's
names could not be recovered. The current `except Exception: pass` that silently
drops a column is removed — an unrecoverable element flips `complete=False` so the
full client-side `.select(with_columns)` runs.

Callers then do, in the stream loop:

```python
if with_columns is not None and needs_client_select:
    out = out.select(with_columns)
```

### 4.5 Exception narrowing

- Translation errors are typed (`PredicateTranslationError` / `UnsupportedPredicate`)
  and caught specifically.
- The only remaining broad catches are around `parse_sql_expr`/`filter` and
  `parse_sql_expr`/`select` *binding*, which are genuine "couldn't push, fall back
  safely" boundaries and **always** return `needs_client_filter=True` /
  `needs_client_select=True` respectively.
- Genuine programming errors in the translator surface as `PredicateTranslationError`
  with the original cause chained (`raise ... from e`) and are logged — never
  silently swallowed into a dropped filter.
- Projection's previous `except Exception: pass` (silently dropping a column from
  the requested set) is **removed**; an unrecoverable element flips the
  `complete` flag instead, forcing a correct client-side select.

## 5. Data flow

```
pl.Expr predicate
   │  expr.meta.serialize(format="json")
   ▼
JSON AST ──(recursive emitter, per-node)──► faithful SQL fragments
   │                                          (unsupported node → UnsupportedPredicate)
   ▼
plan_predicate_pushdown: split top-level ANDs, keep faithful conjuncts
   │
   ├─ PushdownPlan.pushdown_sql ──► query_df.parse_sql_expr() ──► query_df.filter()   [optimization]
   └─ PushdownPlan.fully_translated ──► needs_client_filter
                                          │
   stream batches ──► if needs_client_filter: out.filter(P)   [correctness, source of truth]

with_columns C
   │  extract_source_columns (meta.root_names(), structured, no silent skips)
   ▼
source column set ──► query_df.parse_sql_expr('"col"') ──► query_df.select()        [optimization]
   │  complete? ──► needs_client_select
   ▼
   stream batches ──► if needs_client_select: out.select(C)   [correctness, source of truth]
```

## 6. Error handling

**Predicate**

| Situation | Behavior |
|---|---|
| Node type unknown / unsupported position | `UnsupportedPredicate`, conjunct not pushed, full `P` reapplied client-side |
| No conjunct translatable | `pushdown_sql=None`, full scan + full client filter (logged at warning) |
| Some conjuncts translatable | push subset, reapply full `P` client-side |
| All conjuncts translatable | push all, skip client reapply |
| `parse_sql_expr`/`filter` raises | skip pushdown entirely, `needs_client_filter=True` (logged) |
| Translator internal bug | `PredicateTranslationError` (chained cause), safe fallback, logged — not silent |

**Projection**

| Situation | Behavior |
|---|---|
| All source columns recovered | push select, skip client reapply (`complete=True`) |
| Some element's names unrecoverable | `complete=False`, push what's known, reapply full `C` client-side |
| No columns determinable | `needs_client_select=True`, no pushdown, client select runs |
| `parse_sql_expr`/`select` raises | skip pushdown entirely, `needs_client_select=True` (logged) |

In every row-returning path, the result is exactly `P`'s rows over exactly `C`'s columns.

## 7. Testing

**Extensive unit-test coverage is a first-class deliverable of this work, not an
afterthought.** The fragility that caused #396 went undetected because the
translator had no per-node tests; the rewrite must not repeat that. Every AST node
type, every fallback branch, the certification flags, and every call site are
covered. Concretely:

1. **Property-based equivalence (the headline test).** Mirror the reviewer's
   `hypothesis.stateful` fuzzer: generate predicates *and* column projections over
   GTF/GFF/VCF/BAM columns (including `str.contains`, `is_in`, AND/OR nesting,
   negation, mixed attribute + parsed fields, aliased/computed projections) and
   assert, for every generated `(P, C)`:
   - `scan(...).filter(P).select(C).collect() == scan(...).collect().filter(P).select(C)`.
   This is the invariant from §3 expressed as a test and would have caught #396.
2. **Regression test for #396** — the exact GTF snippet from the issue; the
   `type=='transcript' & gene_biotype.str.contains('pseudogene')` query must return
   0 rows in both lazy and eager forms.
3. **Translator unit tests — exhaustive, per node type.** For *each* AST node the
   emitter claims to support (Column, every BinaryExpr comparison op, And/Or,
   Literal of each scalar type, IsIn): assert the exact SQL emitted, including
   literal quoting/escaping and identifier quoting. For *each* unsupported node
   (string functions, arithmetic, unknown types): assert `UnsupportedPredicate` is
   raised — never silent emission. Plus: conjunct splitting keeps the correct
   subset; `fully_translated` is `True` only when every conjunct translated;
   OR-atomicity (an OR with one unsupported side pushes nothing); literal-escaping
   edge cases (quotes, unicode, empty string, negative/float/bool literals).
4. **Projection extraction unit tests.** `extract_source_columns` returns the right
   *source* names for plain, aliased, and computed projections (via `root_names`);
   `complete=False` when an element's names can't be recovered; no column is ever
   silently dropped.
5. **Helper-level fallback tests.** Drive `apply_predicate_pushdown` /
   `apply_projection_pushdown` directly with a stub `query_df` whose
   `parse_sql_expr`/`filter`/`select` raise, and assert they return
   `needs_client_filter=True` / `needs_client_select=True` and log — never propagate
   or silently succeed.
6. **All four call sites** exercised (io streaming, io second path, overlap/range
   via `utils.py`, pileup) to confirm they route through the shared helpers and
   reapply correctly for both predicate and projection.

## 8. Rollout

- Land the **safety lever first** (§4.3 + §4.4 helpers + unconditional reapply
  semantics + exception narrowing, with their fallback unit tests) so the
  wrong-results class is closed immediately for both predicate and projection,
  even before the translator rewrite. This is the cheap high-value piece.
- Land the **structured translator** (§4.1/§4.2) second, swapping the regex
  internals behind the unchanged `plan_predicate_pushdown` surface, with its
  exhaustive per-node unit tests.
- Land the **property-based equivalence test** (§7.1) to lock the invariant.
- Delete dead regex code (`_translate_polars_expr` chain, `datafusion_expr_to_sql`
  cleanup) once the structured translator is in.

## 9. Risks & mitigations

- **`meta.serialize` format drift across Polars versions.** Mitigation: handlers
  key off node-type names with an explicit "unknown → unsupported" default, so a
  new/renamed node degrades to client-side filtering, not wrong results. Pin the
  observed schema in tests.
- **Double-filter cost when fully translated.** Mitigation: `fully_translated`
  skips the reapply; partial cases filter on already-reduced data.
- **Behavioral change: more full scans surface as warnings.** This is intended —
  correctness over silent speed. The property test guards equivalence.

## 10. Out of scope / follow-ups

- Expanding pushdown coverage (e.g. safe `str.contains` → provider-native regex)
  once the structured translator makes it safe to add.
- Rust-side predicate support in the bio-format providers.
