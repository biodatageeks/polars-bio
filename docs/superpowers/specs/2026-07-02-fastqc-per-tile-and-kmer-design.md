# FastQC modules: per_tile_quality (11) and kmer_content (12)

**Date:** 2026-07-02
**Branch:** feat/fastqc-phase1
**Status:** Design — awaiting review

## Context

The `feat/fastqc-phase1` branch has exposed 10 streaming FastQC modules in Python,
each matching FastQC 0.12.1 output (exact where FastQC's math is reproducible,
arithmetically-validated where FastQC applies cosmetic transforms). The QC
algorithms live in the upstream crate
`datafusion-bio-functions/datafusion/bio-function-fastqc` (local path dep); each
module implements the `QcModule` trait (`update`/`merge`/`finalize`) and emits
rows in a fixed tidy schema. polars-bio is thin glue (`src/fastqc.rs` UDTF +
`polars_bio/fastqc_op.py` per-module views).

This change adds the final two Phase-1 modules: **per_tile_quality** and
**kmer_content**. Both are more involved than modules 1–10:

- **per_tile_quality** needs the read *header* (to parse the flowcell tile),
  which `QcModule::update(seq, qual)` does not currently receive.
- **kmer_content** is disabled by default in FastQC 0.12.1, so there is no
  committed golden section to test against without regenerating one.

Both FastQC binary (v0.12.1) and its `Configuration/limits.txt` are available
locally, so golden reference data can be generated at implementation time.

## Module 11: per_tile_quality

### The trait-signature change

FastQC parses the tile from the read identifier (e.g.
`@SRR9130495.1 D00236:723:HG32CBCX2:1:1108:1330:1935/1` → tile `1108`). The
polars-bio FASTQ provider splits the header into `name` + `description` columns,
but `QcModule::update(&mut self, seq, qual)` receives neither.

**Decision:** extend the trait to
`fn update(&mut self, name: &[u8], seq: &[u8], qual: &[u8])`, threading the read
name (reconstructed full header = `name` + `" "` + `description` where a
description exists) from `ModuleSet::update_batch` and adding `name`/`description`
to the `fastqc.rs` scan projection. All 10 existing modules gain a `_name`
parameter (trivial, unused).

**Alternatives considered:**
- A separate `update_named` trait method with a default no-op — less invasive but
  splits the hot loop and lets a module silently miss names. Rejected.
- Thread-local carrying the current read name — hidden state, rejected.

The explicit parameter keeps the streaming/merge contract intact and is
consistent across all modules.

### Algorithm (FastQC parity, merge-safe)

- **Tile extraction:** reconstruct the full id, `split(':')`, apply FastQC's exact
  `getTile` branching (verify field-count cutoffs against FastQC 0.12.1 source:
  the observed data uses the "≥7 fields → field index 4" branch). Reads with no
  parseable tile are excluded (FastQC behaviour).
- **State:** per `(tile, position)` accumulate `sum_quality` and `count`. Both
  additive → parallel-merge-safe. Quality is decoded phred+33.
- **Finalize:** for each position, the reference is the **unweighted mean of the
  per-tile means** (FastQC averages tile means, *not* the per-read mean). Emit
  `deviation = tile_mean − reference` as `metric="mean"`, `label=<tile>`,
  `position=pos+1`. Status (verified from source + `limits.txt`): let
  `maxDeviation` = max **absolute** deviation over all (tile, position); FAIL if
  `maxDeviation > 10` (`tile error`), else WARN if `maxDeviation > 5`
  (`tile warn`), else PASS.

### Test gate (exact parity, TDD)

The committed `example.nogroup.fastqc_data.txt` per-tile section is all `0.0`
because `example.fastq` is single-tile (all 200 reads → tile 1108) — a trivial
gate a stub would pass. Therefore:

1. Synthesize `tests/data/io/fastq/per_tile_mix.fastq` — reads across ≥3 tiles,
   with one tile's quality deliberately depressed so deviations are non-zero.
2. Generate `tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt`
   via the local FastQC 0.12.1 (`--nogroup`).
3. Write the failing parity test first (mirror
   `test_dup_levels_match_fastqc_exactly`), watch it fail, implement until
   `worst ≤ 1e-9`.

## Module 12: kmer_content

### Enabling FastQC's Kmer Content

`Configuration/limits.txt` line 5 is `kmer ignore 1` (module disabled by
default). FastQC accepts `--limits <file>`; a patched copy with `kmer ignore 0`
(k defaults to 7, overridable via `-k`) makes FastQC emit a `>>Kmer Content`
section. Status thresholds live in the same file: `kmer warn 2`, `kmer error 5`
(on the `−log10` binomial p-value of the most significant k-mer).

### Algorithm (FastQC `KmerContent.java`, k=7) — verified from source

FastQC's real algorithm (confirmed against FastQC `KmerContent.java`) is **not**
a naive all-reads count. To achieve exact golden parity we replicate it faithfully
(decision: "replicate sampling → exact golden"):

- **2% read sampling:** `++skipCount; if (skipCount % 50 != 0) return;` — only every
  50th read (1-based positions 50, 100, 150, …) is processed, in file order.
- **500 bp truncation:** reads longer than 500 bp are truncated to the first 500 bp.
- **State:** per k-mer, a total count and a per-position count vector; plus the
  total observed k-mers per position. K-mers containing `N` are added to the total
  count but **skipped** from the per-k-mer table (FastQC: `addKmerCount(...)` then
  `if (kmer.indexOf("N") >= 0) continue;`).
- **Finalize (per k-mer):**
  - `expectedProportion = count / totalKmerCount`
  - per position group g: `predicted = expectedProportion * totalGroupCount`;
    `obsExp[g] = totalGroupHits / predicted`
  - `Obs/Exp Max` = max over g; its position is the max group.
  - p-value: `BinomialDistribution((int)totalGroupCount, expectedProportion)`;
    if `totalGroupHits > predicted`,
    `p = (1 − cdf((int)totalGroupHits)) * 4^k`, else `p = 1`.
  - Report k-mers with `p < 0.01 && obsExpMax > 5`, **sorted descending by
    `maxObsExp`, top 20**. Status: `−log10(p_of_top_kmer)` vs `kmer warn 2` /
    `kmer error 5`.

**Merge-safety caveat (documented):** the every-50th sampling is file-order
dependent, so exact parity holds only when the FASTQ scan is **single-partition**
(plain `.fastq`/`.gz`, which stream in file order; `FastqcExec` accumulates one
`ModuleSet` per input partition then merges, so a multi-partition/BGZF input would
sample differently). The golden fixture is therefore a plain `.fastq`. The
per-module `skip_count` lives in the accumulator and is additive-only through
`merge` for the single-partition case.

**Binomial CDF:** port a small exact binomial-CDF helper in Rust (no external
crate) to reproduce Apache-commons `BinomialDistribution.cumulativeProbability`.

### Test gate (exact parity, TDD)

1. Synthesize `tests/data/io/fastq/kmer_mix.fastq` — **≥1000 reads** (so the 2%
   sample is non-trivial) with a fixed 7-mer injected at a constant position in
   the sampled reads, enough to clear `p<0.01 && obs/exp>5`. Plain `.fastq`
   (single partition).
2. Generate `tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt`
   via `fastqc --nogroup --limits patched_limits.txt` (kmer enabled, k=7).
3. Failing test first: **exact** (`≤1e-9`) match on `Count`, `Obs/Exp Max`, and
   max-position for every k-mer FastQC reports (inner-join on the k-mer
   sequence); p-value within a small tolerance (float32 vs f64); module **status**
   parity; and reported-set membership (our top-20 set == FastQC's).

## Registry checklist (both modules)

| Location | Change | File |
| --- | --- | --- |
| Rust upstream | `pub mod`, `use`, `ALL_MODULES` (bump `[&str; 10]` → `12`), `make_module` arm | `bio-function-fastqc/src/lib.rs` |
| Rust upstream | `QcModule::update` signature + all 10 modules + `update_batch` | `lib.rs`, each module `*.rs` |
| polars-bio Rust | scan projection add `name`/`description` | `src/fastqc.rs` |
| Python | `ALL_MODULES`, new properties, `fastqc()` docstring | `polars_bio/fastqc_op.py` |
| Tests | `EXPECTED_MODULES` | `tests/test_fastqc.py` |
| Harness | `_FASTQC_MODULE` map, `_emit` branches, `TOLERANCES` (kmer p-value) | `benchmarks/fastqc/parity.py` |

The fixed tidy schema (`tidy_schema()`) is unchanged.

## Order of work

1. Rust trait change (`update` gains the header) + adapt all 10 modules. Land
   with per_tile.
2. per_tile_quality: fixture → golden → failing parity test → implement → green.
3. kmer_content: patched-limits golden → failing parity test → port FastQC
   k-mer math → green.
4. Python properties, registries, docstrings, harness for both modules.

Both modules follow strict TDD: golden/failing test first, minimal implementation
to green.
