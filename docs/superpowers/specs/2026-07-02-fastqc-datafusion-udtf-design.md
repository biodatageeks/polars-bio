# `fastqc` — Streaming FastQC on the DataFusion Engine

**Date:** 2026-07-02
**Status:** Design — approved for planning
**Author:** brainstormed with Claude

## Summary

Add a native, streaming, Arrow-first FastQC implementation to polars-bio, exposed
as a single DataFusion table function `fastqc(path, modules?)`. It ports the QC
module algorithms from [RastQC](https://github.com/Huang-lab/RastQC) (MIT) onto
polars-bio's DataFusion engine, consuming the **existing FASTQ `TableProvider`**
stream rather than re-reading files. Results surface as a uniform *tidy* Arrow
`RecordBatch` stream (SQL-native) and, in Python, as a `FastQCResult` object with
typed per-module LazyFrames.

Correctness is validated against **FastQC** (s-andrews, the canonical reference);
performance is benchmarked against **RastQC** (the Rust speed baseline). Both a
parity harness and a benchmark harness are first-class deliverables, built in
Phase 1 and extended with every subsequent phase until all 15 modules are covered.

## Goals

- One streaming pass over the reads, bounded memory, engine-auto-partitioned.
- Arrow-native output, directly consumable as polars-bio LazyFrames and via SQL.
- Reuse the datafusion-bio-formats FASTQ `TableProvider` as the input stream.
- Numerical parity with FastQC; wall-clock parity-or-better vs RastQC.
- Per-module comparability: every module is independently parity-checked and
  benchmarked.

## Non-Goals (YAGNI)

- No HTML / ZIP / SVG report generation. polars-bio returns DataFrames;
  visualization is the caller's concern.
- No POD5 / FAST5 / colorspace input.
- BAM/CRAM QC input is deferred (the providers already expose `quality_scores`,
  so it is a later, mechanical add — not part of Phases 1–5).

## Architecture

`fastqc(path, modules?)` is a DataFusion **table function** (UDTF) registered in
`context.rs`, mirroring how `depth` is wired today
(`DepthTableProvider` in `src/pileup.rs`, `datafusion-bio-function-pileup`).

- **Input:** the existing datafusion-bio-formats FASTQ `TableProvider` produces
  `RecordBatch`es with `sequence` and `quality` columns already decoded to Arrow.
  The QC operator sits on top of that stream — no second reader.
- **Operator:** a two-phase aggregate `ExecutionPlan`:
  - **accumulate** (per partition) ↔ RastQC `process_sequence`
  - **merge** (across partitions) ↔ RastQC `merge_from`
  - **finalize** (emit tidy rows) ↔ RastQC `calculate_results`
- **Parallelism:** partitions come from the engine's automatic FASTQ partitioning;
  each partition owns independent, bounded accumulator state; a final merge folds
  them. Deterministic result regardless of partition count.
- **Module gating:** the `modules` argument selects which accumulators are
  constructed. Unselected modules are **never allocated and never updated** — the
  selector is the performance lever, not a post-filter.

### Code layout

- New upstream crate `datafusion-bio-function-fastqc` (parallel to the pileup
  function crate) holding the module accumulators and the `ExecutionPlan`.
- `src/fastqc.rs` — PyO3 binding (`py_fastqc(...)`), following `src/pileup.rs`.
- `context.rs` — register the `fastqc` UDTF (`SELECT * FROM fastqc('f.fastq')`).
- `polars_bio/fastqc_op.py` — `FastQCOperations` + `FastQCResult`, exposed as
  `pb.fastqc`, following `polars_bio/pileup_op.py`.

## Output Contract — Tidy Schema

A single fixed schema, independent of which `modules` run (so the UDTF's planned
schema is stable):

| column      | type        | meaning                                                        |
|-------------|-------------|----------------------------------------------------------------|
| `module`    | Utf8        | e.g. `basic_stats`, `per_base_quality`                         |
| `label`     | Utf8 (null) | secondary key — tile, adapter name, k-mer, overrep sequence    |
| `position`  | Int32 (null)| read offset / bin, when positional                             |
| `metric`    | Utf8        | `mean`,`median`,`q1`,`q3`,`p10`,`p90`,`A`,`C`,`G`,`T`,`pct`,`count`,`status`,… |
| `value`     | Float64 (null) | numeric value                                               |
| `value_str` | Utf8 (null) | string-valued results (status, overrep source, flagged seq)    |

This encodes every module shape: per-base-quality boxplots → 6 rows/position;
per-base content → 4 rows/position (A/C/G/T); histograms → one row/bin;
overrepresented sequences → `label`+`value`(+`value_str` source); module
PASS/WARN/FAIL → one `metric='status'` row per module in `value_str`.

## API Surface

### SQL

```sql
SELECT * FROM fastqc('reads_R1.fastq.gz');                 -- all modules
SELECT * FROM fastqc('reads_R1.fastq.gz', ['per_base_quality','per_seq_gc']);
SELECT module, value_str AS status
FROM   fastqc('reads_R1.fastq.gz') WHERE metric = 'status';
```

### Python

```python
qc = pb.fastqc("reads_R1.fastq.gz", modules=None)   # None → all; or a list
qc.per_base_quality.collect()   # position, mean, median, q1, q3, p10, p90
qc.basic_stats.collect()        # metric, value
qc.summary().collect()          # module, status (PASS/WARN/FAIL)
qc.tidy                         # raw tidy LazyFrame == SELECT * FROM fastqc(...)
```

- `pb.fastqc(path, modules=None, group=True) -> FastQCResult`.
- Each typed attribute is an Arrow-backed LazyFrame pivoted from the single run.
- Accessing a **non-computed** module (not in `modules`) **raises** a clear error.
- `group` toggles FastQC's >50 bp position binning (`group=False` ==
  `fastqc --nogroup`, exact per-position output).

## Module Inventory & Phasing (all 15)

Grouped by accumulator shape (the real driver of implementation effort):

- **Phase 1 — vertical slice + harness.** One module per shape class, to prove the
  whole pipeline and stand up both harnesses:
  - Basic Statistics (scalar)
  - Per-Base Sequence Quality (positional)
  - Per-Sequence GC Content (histogram)
  - Sequence Duplication Levels (hash/dictionary, partition-merge stress)
- **Phase 2 — remaining scalar/histogram:** Per-Sequence Quality Scores,
  Sequence Length Distribution, Per-Base N Content.
- **Phase 3 — remaining positional:** Per-Base Sequence Content,
  Per-Tile Sequence Quality.
- **Phase 4 — remaining hash/dictionary:** Overrepresented Sequences,
  Adapter Content, K-mer Content.
- **Phase 5 — long-read extras (`--long-read` parity):** Read Length N50,
  Quality-Stratified Length, Homopolymer Content.

Each phase adds its modules to the accumulator set, the pivot wrapper, the parity
matrix, and the benchmark matrix.

## Parity + Benchmark Harness

Built in Phase 1; every later phase adds its modules to both matrices.

### Correctness — three-way, FastQC is ground truth

- **FastQC (s-andrews, Java)** is the correctness oracle. `fastqc --extract`
  emits `fastqc_data.txt` (canonical `>>Module … >>END_MODULE` sections) +
  `summary.txt`. An adapter parses these into the tidy schema.
- **RastQC** provides a second correctness vote via `--multiqc-json` /
  `--summary`.
- The harness joins polars-bio `fastqc()` tidy output against the reference on
  `(module, label, position, metric)` and asserts equality within per-metric
  tolerance: **exact** for integer counts (N-content, GC histogram, duplication
  bins), tight float tolerance for means/quantiles. Report per module: rows
  compared / exact / within-tol / mismatch. Target: zero mismatches on shared
  modules.
- **Binning:** run references with `--nogroup` for exact per-position parity
  (`group=False`); separately validate our default binned output matches FastQC's
  default position binning.
- Match reference defaults (k-mer size 7, adapter/contaminant lists) so parity is
  meaningful.

### Performance — RastQC is the baseline

- **RastQC `--time`** yields a per-step timing breakdown from a single all-modules
  run → compared against our per-module *marginal* cost via the selector
  (`pb.fastqc(file, modules=[m])`).
- **Isolated fair run:** a generated RastQC limits file (`<module> ignore 1`)
  disables all but the target module; time the full invocation vs
  `fastqc(file,[m])`.
- **Controls:** match parallelism (RastQC `-t N` / `--no-parallel` ↔ our
  target-partitions); report both **1-thread** (algorithmic efficiency) and
  **all-core** (scaling). Subtract a **scan-only baseline** (FASTQ provider with no
  QC) so a win/loss is attributed to the QC math vs the I/O path.
- **Metrics per module:** wall time, throughput (reads/s, MB/s), peak RSS.
- FastQC (Java) is timed for context only, not as the speed target.

## Testing

- Golden-file parity per module on small committed fixture FASTQs (paired against
  checked-in FastQC reference output so CI needs no Java at test time; the live
  three-way harness is a separate, opt-in benchmark job).
- Partition-merge correctness: identical result at 1 vs N partitions.
- `modules` gating: unselected modules absent from tidy output; accessing them in
  Python raises.
- Streaming/out-of-core sanity on a large synthetic FASTQ (bounded RSS).
- `group=True` vs `group=False` position handling.

## Open Questions / Deferred

- BAM/CRAM QC input (providers already expose `quality_scores`).
- Typed per-module UDTFs (`qc_per_base_quality(...)` with native columns) as a
  later SQL convenience over the same run.
- MultiQC JSON *export* from our tidy output (we already ingest it for parity;
  emitting it is a small follow-on).
- Multi-file / batch QC ergonomics (FastQC/MultiQC run over many files).
