---
name: gff-gtf-pushdown-regression
description: >-
  Validate polars_bio GFF/GTF predicate and projection pushdown for correctness
  and performance against real GENCODE data, using oxbow as an independent
  reference reader. Use this whenever you touch the pushdown path — the
  predicate translator (predicate_translator.py), the shared helpers
  (pushdown.py), the scan call sites (io.py _overlap_source / the GFF/GTF
  select() wrapper, utils.py, pileup_op.py), scan_gtf/scan_gff, or anything
  about predicate/projection pushdown, filter pushdown, attribute fields, or
  the issue #396 / PR #407 work — and any time you want to confirm a filtered
  or projected scan returns identical results with pushdown on vs off, or to
  benchmark how much pushdown actually speeds a query up. Reach for it before
  claiming a pushdown change is correct.
---

# GFF/GTF pushdown regression testing

## Why this exists

polars_bio pushes Polars `filter()`/`select()` predicates down into the
DataFusion GFF/GTF scan as an **optimization**. The correctness contract
(issue #396 / PR #407) is:

> The client-side full filter/select is the **source of truth**. Pushdown may
> only remove rows the client filter would also remove. A translation bug can
> cost performance, never correctness — and must never crash `collect()`.

The classic failure (#396) was a predicate silently *dropped* during
translation, so a filtered scan returned **too many rows** with no error. Unit
tests alone miss this because the bug can be wrong identically on a naive
reference. So this harness checks three implementations against each other on
a real 11M-row GENCODE annotation:

- **A** — `scan_gtf/gff` with pushdown **ON**
- **B** — `scan_gtf/gff` with pushdown **OFF** (client-side filter/select only)
- **C** — **oxbow** (a separate Rust/Arrow GFF/GTF reader): read everything,
  filter in Polars — an *independent* ground truth

A case passes only when **A == C and B == C** as row sets (row order is also
checked). Using oxbow as a third party is the point: it catches a bug that is
wrong in the same way on both polars_bio paths.

## When to run it

Run the harness after any change to:

- `polars_bio/predicate_translator.py` (the AST emitter / `plan_predicate_pushdown`)
- `polars_bio/pushdown.py` (`apply_predicate_pushdown` / `apply_projection_pushdown`)
- the scan call sites: `io.py` `_overlap_source`, the GFF/GTF `select()` wrapper
  (`AnnotationLazyFrameWrapper`), `utils.py`, `pileup_op.py`
- `scan_gtf` / `scan_gff` / `register_gtf` / `register_gff` or their options

Treat a green run (`ALL PASS`) as the bar for claiming a pushdown change is
correct. The fast Python unit suite
(`tests/test_predicate_translator_units.py`, `tests/test_pushdown_helpers.py`)
is the inner loop; this harness is the real-data gate.

## How to run

```bash
cd <repo root>
source .venv/bin/activate          # or however the project venv is activated
unset CONDA_PREFIX                  # maturin/uv dislike both VIRTUAL_ENV + CONDA_PREFIX set
export TQDM_DISABLE=1               # silence polars_bio's per-row progress bar
python .claude/skills/gff-gtf-pushdown-regression/scripts/bench_pushdown.py gtf gff3
```

- First run downloads GENCODE v50 GTF (~124 MB) and GFF3 (~160 MB) into
  `~/.cache/polars-bio-gencode` (override with `POLARS_BIO_GENCODE_DIR`); later
  runs reuse them. Needs network on the first run and `oxbow` installed
  (`pip install oxbow`).
- Args select formats: `gtf`, `gff3`, or both. Default is `gtf`. Use `gtf gff3`
  for a full check — they have slightly different feature representations and
  each has caught format-specific bugs.
- If you changed Rust, rebuild first: `maturin develop --release`. Pure-Python
  pushdown changes need no rebuild.
- **Exit code is non-zero if any case fails**, so it works as a CI/pre-merge
  gate, not just an interactive report.

## Reading the output

```
[PASS] gtf chrom == chr7    rows=  570,522 A=C:True B=C:True order:True | ON 6.5s OFF 26.0s (4.0x)
--- gtf: 18/18 cases PASS ---
==== ALL PASS ====
```

- `A=C` / `B=C` — pushdown-on and pushdown-off both match oxbow. Both must be
  `True`.
- `order:True` — results also match in row order, not just as a set.
- `ON` / `OFF (Nx)` — wall time with pushdown on vs off, and the speedup. A
  **selective** predicate should be visibly faster on (e.g. `chrom==chr1 &
  type==gene` ≈ 4x); a predicate matching ~all rows trends toward 1x. If a
  selective predicate shows ~1x, pushdown probably is **not** firing — a
  performance regression worth investigating even when correctness passes.

## A FAIL means

The changed pushdown either dropped/added rows or mangled a projection. The
`A.h/B.h/C.h` line shows which path diverged. Almost always the fix is: make
the translator **decline** to push that predicate (raise `UnsupportedPredicate`
so it falls back to the client-side filter) rather than push a subtly-wrong
SQL. Add a focused unit test in `tests/test_predicate_translator_units.py`
reproducing the case, then re-run this harness.

## Extending the case list

`cases()` in the script is the coverage. Each entry is
`(name, predicate, projection, attr_fields)`. When you add a translator branch
or fix a bug, add a case here. Keep the existing edge cases — they encode hard-
won failure modes and each maps to a real regression:

- `str.contains`, string-column ordering (`chrom < "chr2"`) — must stay
  **client-side** (the exact #396 failure); pushing them is wrong.
- `is_in([..., None])` and `is_in(nulls_equal=True)` — SQL `IN` null semantics
  differ from Polars; must stay client-side.
- `score > nan` / non-finite floats — must not crash; fall back.
- deeply nested `&`/`|` chains — must degrade gracefully, never `RecursionError`.
- `gene_name.contains(...) -> gene_id` — the #396 attribute filter+select path.
- aliased / computed projections (`chrom.alias("c")`, `(start+1).alias("s1")`,
  aliased attribute `gene_id.alias("g")`) — projection pushdown must apply the
  alias/computation, not silently return the raw source column.
