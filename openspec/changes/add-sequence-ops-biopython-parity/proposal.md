# Change: Add Biopython-parity sequence operations

GitHub Issue: https://github.com/biodatageeks/polars-bio/issues/512

Follow-up to `add-sequence-operations` (#508, #510), which shipped the
seqkit-style kernels in 0.38.0.

## Why

Many potential users still write `for rec in SeqIO.parse(...)` loops with
Biopython for everyday sequence work. Two things keep them there:

- **Translation differs from Biopython.** `seq.translate` has no `cds=True`
  validation and no `stop_symbol`, and it returns `X` where Biopython returns
  `B`, `Z` or `J`. A user comparing outputs during a migration sees
  mismatches.
- **Common operations are missing.** ORF finding, IUPAC motif search,
  transcription, windowed GC and GC skew, molecular weight, sequence hashing
  for deduplication, expected errors and quality trimming have no polars-bio
  equivalent. The fallback is `map_elements` with Biopython, which holds the
  GIL, breaks streaming and is orders of magnitude slower.

## What Changes

Every operation follows the pattern of `add-sequence-operations`: a pure
`&[u8]` kernel in `datafusion-bio-function-seq`, a `seq_*` SQL function, and a
`pl.col(...).seq.*` expression backed by the Polars plugin. The work is split
into three phases, each releasable on its own.

**Phase 1: translation parity and small operations**
- `seq.translate` gains three keyword options. Defaults keep today's output.
  - `cds=True`: Biopython's complete-CDS validation. An invalid sequence
    raises `ComputeError`.
  - `stop_symbol="*"`: any string, including the empty string.
  - `ambiguous="x"|"biopython"`: `"biopython"` returns `B`, `Z` and `J` where
    Biopython does.
- `seq.cds_status(frame=1, table=1)`: null for a sequence that
  `translate(cds=True)` accepts, otherwise the reason. It lets users filter or
  report invalid rows before translating.
- `seq.transcribe()` and `seq.back_transcribe()`.
- `seq.expected_errors(offset=33)`, `seq.min_quality(offset=33)` and
  `seq.max_quality(offset=33)`.

**Phase 2: ORFs and motif search**
- `seq.orfs(...)` returns `List[Struct]` with one entry per open reading
  frame. Coordinates follow polars-bio's coordinate-system setting.
- `pb.orfs(source, ...)` is the frame-level form: one row per ORF, tagged
  with its coordinate system, so the range operations accept it directly.
  It exists because an expression cannot tag a frame: a hand-exploded
  `seq.orfs()` column is rejected by `pb.overlap` next to a tagged input.
- `seq.find_motif(pattern, both_strands=False, overlapping=True)` returns
  `List[Int64]` positions. The pattern may contain IUPAC ambiguity codes.

**Phase 3: windows, weight, hash and trimming**
- `seq.gc_content_windows(window, step)` and `seq.gc_skew(window, step)`
  return `List[Float64]`.
- `seq.molecular_weight(...)` matches `Bio.SeqUtils.molecular_weight`.
- `seq.hash(canonical=False)` returns a stable 64-bit hash, for
  `seqkit rmdup -s`-style deduplication with `unique(subset=...)`.
- `seq.trim_quality(quality, leading=, trailing=, sliding_window=)` returns
  the trimmed sequence and quality together as a struct (Trimmomatic-style).

**Across phases**
- The matching `seq_*` SQL functions.
- A "Coming from Biopython" docs page mapping Biopython calls to polars-bio.
- The golden corpus in `datafusion-bio-function-seq` is extended with
  Biopython 1.88, vsearch, Trimmomatic, seqkit and an ORF finder.
- Comparative benchmarks against Biopython loops in polars-bio-bench, once the
  harness from `add-sequence-operations` phase 6 exists.

**Not included**
- `Binary` / `BinaryView` input (item 5 of the issue). The issue makes it
  conditional on a reader emitting sequences as bytes, and none does: FASTA
  and FASTQ emit `Utf8`/`LargeUtf8`, and the only `Binary` columns are the
  BAM/CRAM binary CIGAR.
- GenBank/EMBL feature tables, BLAST/DIAMOND/HMMER tabular readers, batch
  pairwise alignment, PWM motif scanning and `ProtParam` protein features.
  The issue lists these as possible separate issues.
- Pushdown of the new plugin predicates into DataFusion.

## Impact

- Affected specs: `sequence-expressions` (ADDED requirements only).
  - The capability is introduced by `add-sequence-operations`, which is not
    archived yet. Archive that change first, then this one.
  - No `packaging` delta. The existing wheel-size budget and Polars
    compatibility gate already cover the new plugin functions.
- Affected code:
  - **datafusion-bio-functions:** `datafusion-bio-function-seq` (new kernels,
    start/stop codon sets and weight tables generated from Biopython, new
    return types in the UDF layer, golden corpus).
  - **polars-bio Rust:** `Cargo.toml` (functions pin, `polars` crate
    `dtype-struct` feature), `src/seq_plugin.rs`. `src/context.rs` needs no
    change: `register_seq_functions` registers the new functions.
  - **polars-bio Python:** `polars_bio/seq.py`, and `polars_bio/__init__.py`
    for the `pb.orfs` export.
  - **Tests and docs:** `tests/test_seq_expressions.py`,
    `docs/features/sequences.md`, `docs/api/sequences.md`, a new Biopython
    migration page, `CHANGELOG.md`.
  - **polars-bio-bench:** Biopython-loop baselines in the `seqops` suite. No
    polars-bio behaviour changes, so no spec delta.
- Relation to `add-sequence-operations`:
  - The Polars-version CI job (`seq_plugin_polars_matrix` in
    `publish_to_pypi.yml`) already runs `tests/test_seq_expressions.py` on
    Polars 1.40.0, newest 1.x, 2.0.0 and newest 2.1.x. Tests added to that
    file are covered without further CI work. Phase 2 adds the first list and
    struct plugin outputs, so it starts with a spike on that matrix.
  - The benchmark tasks here reuse the environment and harness from tasks
    6.1–6.2 there, which are still open.
- Not breaking: everything is additive. Existing `seq.translate` calls and
  `seq_translate` SQL calls return the same results as before.
- Evidence: Biopython 1.88 was probed on 2026-11-23 for every rule this change
  takes from it. [oracles.md](oracles.md) records the probes.
