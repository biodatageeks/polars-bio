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
  scan paths.
- Lock the eager-vs-lazy equivalence invariant with a property-based test that
  mirrors the reviewer's fuzzer.

**Non-goals**

- Projection pushdown is *not* rewritten here. Over-selecting columns and
  reapplying the client-side projection is already correctness-safe; only its
  exception narrowing is folded into the shared helper opportunistically.
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

### 4.4 Exception narrowing

- Translation errors are typed (`PredicateTranslationError` / `UnsupportedPredicate`)
  and caught specifically.
- The only remaining broad catch is around `parse_sql_expr`/`filter` binding, which
  is a genuine "couldn't push, fall back safely" boundary and **always** returns
  `needs_client_filter=True`.
- Genuine programming errors in the translator surface as `PredicateTranslationError`
  with the original cause chained (`raise ... from e`) and are logged — never
  silently swallowed into a dropped filter.

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
```

## 6. Error handling

| Situation | Behavior |
|---|---|
| Node type unknown / unsupported position | `UnsupportedPredicate`, conjunct not pushed, full `P` reapplied client-side |
| No conjunct translatable | `pushdown_sql=None`, full scan + full client filter (logged at warning) |
| Some conjuncts translatable | push subset, reapply full `P` client-side |
| All conjuncts translatable | push all, skip client reapply |
| `parse_sql_expr`/`filter` raises | skip pushdown entirely, `needs_client_filter=True` (logged) |
| Translator internal bug | `PredicateTranslationError` (chained cause), safe fallback, logged — not silent |

In every row-returning path, the result is exactly `P`'s rows.

## 7. Testing

1. **Property-based equivalence (the headline test).** Mirror the reviewer's
   `hypothesis.stateful` fuzzer: generate predicates over GTF/GFF/VCF/BAM columns
   (including `str.contains`, `is_in`, AND/OR nesting, mixed attribute + parsed
   fields) and assert
   `scan(...).filter(P).collect()  ==  scan(...).collect().filter(P)`
   for every generated `P`. This is the invariant from §3 expressed as a test and
   would have caught #396.
2. **Regression test for #396** — the exact GTF snippet from the issue; the
   `type=='transcript' & gene_biotype.str.contains('pseudogene')` query must return
   0 rows in both lazy and eager forms.
3. **Translator unit tests** — per node type: faithful SQL for supported nodes;
   `UnsupportedPredicate` for unsupported nodes; conjunct splitting keeps the right
   subset; `fully_translated` flag correctness; OR-atomicity (an OR with one
   unsupported side pushes nothing).
4. **All four call sites** exercised (io streaming, io second path, overlap/range
   via `utils.py`, pileup) to confirm they route through the shared helper and
   reapply correctly.

## 8. Rollout

- Land the **safety lever first** (§4.3 helper + unconditional reapply semantics +
  exception narrowing) so the wrong-results class is closed immediately, even
  before the translator rewrite. This is the cheap high-value piece.
- Land the **structured translator** (§4.1/§4.2) second, swapping the regex
  internals behind the unchanged `plan_predicate_pushdown` surface.
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
- Projection-pushdown rewrite.
- Rust-side predicate support in the bio-format providers.
