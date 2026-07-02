# Allocator impact assessment for polars-bio 0.32.0 (issue #402)

**Date:** 2026-07-01
**Issue:** [#402 — Check mimalloc allocator impact](https://github.com/biodatageeks/polars-bio/issues/402)
**Type:** Measurement experiment (measure-only — no adoption decision made here)

## Goal

Quantify the runtime impact of swapping polars-bio's global memory allocator, using
the single-threaded blog benchmark config as the assessment vehicle and stock
**polars-bio 0.32.0** (default system allocator) as the baseline.

**Deliverable:** a summary table of per-operation timings for
`default` vs `mimalloc` vs `jemalloc`, plus a short writeup. The adopt/reject
decision for #402 is explicitly left to the maintainer.

## Scope decisions (agreed)

| Decision | Choice |
|---|---|
| Deliverable | Measure only (produce numbers + table; no merge) |
| Environment | This macOS machine, driven end-to-end |
| Allocators | Three-way: default (system) vs mimalloc vs jemalloc (`tikv-jemallocator`) |
| Benchmark scope | `polars_bio` only (pyranges1/bioframe stripped), all 8 operations |
| Build flags | `RUSTFLAGS="-C target-cpu=native"` for **all** variants |
| Branch | Throwaway branch/worktree, **not merged** |

## Baseline: current state

polars-bio 0.32.0 sets **no** `#[global_allocator]` and has no mimalloc/jemalloc in
`Cargo.toml` — it uses the platform default (macOS libmalloc). Version is pinned in
three places (Cargo.toml, pyproject.toml, `polars_bio/__init__.py`), all at `0.32.0`.

## 1. Building the three variants

The global allocator is compiled into the extension, so each allocator is a separate
wheel. Use **one branch with Cargo features** so the default build stays
byte-identical to stock 0.32.0.

```toml
# Cargo.toml
[dependencies]
mimalloc = { version = "0.1", optional = true }
tikv-jemallocator = { version = "0.6", optional = true }

[features]
mimalloc = ["dep:mimalloc"]
jemalloc = ["dep:tikv-jemallocator"]
```

```rust
// src/lib.rs
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

Default (no feature) = system allocator. `mimalloc` and `jemalloc` are mutually
exclusive at build time (only one is enabled per wheel).

Build three wheels against the bench's poetry venv (Python 3.12.8):

```bash
RUSTFLAGS="-C target-cpu=native" maturin build --release
RUSTFLAGS="-C target-cpu=native" maturin build --release --features mimalloc
RUSTFLAGS="-C target-cpu=native" maturin build --release --features jemalloc
```

Each wheel is `pip install --force-reinstall`'d into the bench venv immediately before
its run. Only the compiled `.so` differs, so swapping variants is a reinstall.

## 2. Benchmark config & harness

Derive a stripped config from the blog file:

- **Source:** `conf/blog/benchmark_single_thread-2tools-benchmark-2026.yaml`
- **Derived:** `conf/blog/benchmark_alloc-polars_bio-only.yaml`
- Identical to the source (8 ops: overlap, nearest, count_overlaps, coverage, merge,
  cluster, complement, subtract; same test-cases; `num_repeats: 3`; `parallel: false`)
  but `tools: [polars_bio]` only.

Run with `POLARS_MAX_THREADS=1` and `BENCH_DATA_ROOT` pointing at the existing
`data/` dir (databio present, 3.7G). The harness exports per-op/per-test-case mean
timings to CSV under `results/<timestamp>/`.

## 3. Run protocol (isolation + noise control)

- Each allocator runs in its **own Python process** (fresh `run-benchmarks.py`
  invocation) → no allocator cross-contamination.
- **Interleave** in ordered passes: one pass = run the config once for each of
  `[default, mimalloc, jemalloc]` in that order; repeat the whole ordered pass `R`
  times. Default **`R = 1`** (within-variant repeats come from the config's
  `num_repeats: 3`), which matches the runtime estimate below. Raise `R` to 2–3 only
  if the deltas look noisy; runtime scales linearly with `R`. This ordering (not
  all-default-then-all-mimalloc) averages out thermal drift.
- macOS hygiene: AC power, `caffeinate` during the run, heavy apps quit.
- **jemalloc override check:** `tikv-jemallocator` on Apple Silicon can need the
  `unprefixed_malloc_on_supported_platforms` feature to actually take over allocation.
  Verify the override is in effect before trusting jemalloc numbers (e.g. confirm the
  jemalloc symbols are linked / a quick allocation-stats probe).

## 4. Analysis & deliverable

A small script joins the three CSVs on `(operation, test-case)` and computes
`% delta vs default` and `speedup`.

- **Summary table:** rows = 8 ops × test-cases; columns = default ms / mimalloc ms
  (Δ%) / jemalloc ms (Δ%).
- **Writeup:** which ops benefit and by how much, the macOS caveat, and a
  recommendation on whether a Linux confirmation run is warranted before acting on
  #402.

## Caveats / risks

- **macOS is directional, not publication-grade.** macOS libmalloc is slow relative to
  Linux glibc malloc, so mimalloc/jemalloc wins here **overstate** what a Linux release
  build would show. The writeup must say this explicitly.
- Thermal throttling on a laptop adds variance; interleaving + `num_repeats: 3`
  mitigate but do not eliminate it.
- jemalloc build on Apple Silicon may need extra features (see §3).

## Out of scope

- Merging the allocator change into polars-bio.
- Multi-threaded / parallel benchmarks.
- Reference tools (pyranges1/bioframe) — stripped from the config.
- A Linux confirmation run (may be *recommended* by the writeup, but not executed here).

## Runtime estimate

~3 release rebuilds (few min each) + 3× the single-threaded config. Single-thread on
the larger databio cases (7-8 / 8-7) is slow → budget **~45–90 min** total.
