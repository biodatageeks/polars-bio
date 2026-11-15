# Change: Add sequence operations (seqkit/seqfu-inspired)

## Why

polars-bio can read and write FASTA/FASTQ and run FastQC, but it has no
per-sequence operations. Users who reach for [seqkit](https://github.com/shenwei356/seqkit)
or [seqfu](https://github.com/telatin/seqfu2) have to leave the frame, or write
slow Python UDFs, for the operations that plain Polars expressions cannot
express well: reverse complement, translation, GC content and quality
averages. They also have no polars-bio equivalent for `seqkit stats -a` or
`seqkit subseq --bed`.

Most other seqkit commands (`grep`, `seq -m/-M`, `head`, `rmdup`, `sort`,
`sample`, `fx2tab`, `split`, `pair`, ...) are already one Polars or SQL
expression over `scan_fasta`/`scan_fastq`. Those need documentation, not new
code.

## What Changes

- **Sequence expressions:** a `.seq` expression namespace backed by a **native
  Polars expression plugin** compiled into `polars_bio.abi3.so`
  (pyo3-polars, option "B" of the design spike):
  `pl.col("sequence").seq.revcomp()`, `.complement()`,
  `.gc_content(denominator=...)`, `.translate(frame=..., table=..., to_stop=...)`
  and `pl.col("quality_scores").seq.mean_quality(offset=..., method=...)`.
  They work on any Polars frame, stream in Polars' streaming engine and never
  take the GIL.
- **SQL functions:** the same kernels are registered as DataFusion scalar UDFs
  `seq_revcomp`, `seq_complement`, `seq_gc_content`, `seq_translate` and
  `seq_mean_quality`. The `seq_` prefix is needed because DataFusion already
  has a built-in `translate`.
- **`pb.seq_stats()`:** one streaming pass over FASTA/FASTQ paths or a frame.
  It produces the `seqkit stats -a` metrics (num_seqs, sum/min/avg/max length,
  length quartiles, N50/L50, gaps, Q20/Q30, average quality, GC), with parity
  tests against seqkit.
- **`pb.subseq()` (final phase):** extracts regions from a FASTA indexed with
  `.fai` (plus `.gzi` for BGZF) using a region frame. It honours the frame's
  coordinate-system metadata, applies strand-aware reverse complement, and
  only reads the blocks that overlap the requested regions. It depends on a
  new indexed region reader in `datafusion-bio-formats`.
- **Docs:** a "Coming from seqkit / seqfu" page that maps commands to
  polars-bio one-liners.
- **Comparative benchmarks** in
  [polars-bio-bench](https://github.com/biodatageeks/polars-bio-bench):
  - a sequence-operations suite comparing polars-bio with seqkit, seqfu,
    seqtk, samtools faidx, bedtools getfasta and Python libraries (Biopython,
    pyfaidx, pyfastx);
  - end-to-end file-to-file runs on shared datasets, reporting wall time,
    total CPU and peak RSS at 1/2/4/8 threads;
  - each tool's output checked against an oracle before its timing is
    reported.
- **Packaging prerequisites:**
  - Rust toolchain 1.94 → 1.95. pyo3-polars' `derive` feature pulls
    polars-io → `sysinfo 0.39`, which requires 1.95.
  - New dependencies `pyo3-polars 0.28` and `polars 0.55`
    (`default-features = false`).
  - A wheel-size budget check against PyPI's per-file upload limit. The plugin
    adds about 4.1 MB per wheel, and the 0.37.0 manylinux x86_64 wheel is
    already 94.6 MB.

Not included: trimming, k-mer counting, motif `locate`, `subseq` over remote
(object-store) FASTA, and translating plugin predicates into DataFusion SQL for
pushdown. Plugin expressions serialize as named nodes, so that pushdown can be
added later without an API change.

## Impact

- Affected specs:
  - `sequence-expressions` (new)
  - `sequence-stats` (new)
  - `sequence-extraction` (new)
  - `packaging` (ADDED: wheel size budget, plugin/Polars compatibility gate)
- Affected code:
  - **datafusion-bio-functions:** new crate `datafusion-bio-function-seq`
    (byte-level kernels, DataFusion UDFs, subseq execution).
  - **datafusion-bio-formats:** `bio-format-fasta` indexed region reader
    (phase 5 only).
  - **polars-bio Rust:** `Cargo.toml`, `rust-toolchain.toml`,
    `src/seq_plugin.rs` (new), `src/lib.rs`, `src/context.rs`.
  - **polars-bio Python:** `polars_bio/seq.py` (new), `polars_bio/__init__.py`.
  - **CI and packaging:** CI workflows, bioconda recipe.
  - **Docs:** docs pages.
  - **polars-bio-bench:** new `seqops` operations, tool adapters, a conda
    environment for the command-line tools, configs and results. This changes
    no polars-bio behaviour, so it has no spec delta.
- Spike evidence (2026-11-12/14): a plugin built into a polars-bio wheel from
  `origin/master` 2d80677 (DataFusion 55.2, pyo3 0.29.3) passed 63/63 checks on
  Polars 1.40.1, 1.41.2, 1.42.1, 1.43.2, 1.44.2, 2.0.0 and 2.1.0. It used
  polars-bio's mimalloc global allocator; PolarsAllocator is optional and was
  not used. Sizes, macOS arm64, local build: wheel 90.6 → 94.7 MB, `.so`
  250.1 → 265.5 MB.
- Not breaking: everything is additive. The only user-visible packaging change
  is a slightly larger wheel.
