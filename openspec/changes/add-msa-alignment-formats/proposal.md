# Change: Add A2M, A3M and Stockholm multiple-sequence-alignment read support

## Why

Protein/RNA multiple-sequence-alignment (MSA) formats are the input of every modern
structure-prediction and profile-HMM pipeline (hh-suite, HMMER, AlphaFold/ColabFold MSAs,
Pfam/Rfam), yet polars-bio cannot read any of them. A2M and A3M are FASTA-shaped but ragged
(insert-state gaps may be omitted, so rows differ in length); Stockholm is a line-oriented
block format with four annotation kinds and many alignments per file. Both
[polars-bio#459](https://github.com/biodatageeks/polars-bio/issues/459) and
[datafusion-bio-formats#245](https://github.com/biodatageeks/datafusion-bio-formats/issues/245)
ask for `.a2m`, `.a3m` and `.sto` readers through the same lazy DataFusion/Polars pipeline
as the existing formats.

## What Changes

- Add explicit `scan_a2m`/`read_a2m`, `scan_a3m`/`read_a3m`, `scan_sto`/`read_sto` APIs
  (per-format functions, matching house style; **no** `scan_alignment(format=...)` dispatcher).
- Add `register_a2m`, `register_a3m`, `register_sto` SQL table-registration APIs.
- Add `describe_sto(path)` exposing alignment-level `#=GF` and `#=GC` annotations, mirroring
  `describe_vcf`/`describe_bam`/`describe_pgen`.
- Implement one new provider crate, `datafusion-bio-format-msa`, in the companion
  `biodatageeks/datafusion-bio-formats` repository: a shared FASTA-like path for A2M and A3M
  (delegating record reading to `noodles-fasta`) and a from-scratch Stockholm parser — no
  usable Rust crate exists for Stockholm.
- Expose A2M/A3M with the **same schema as FASTA** (`name`, `description`, `sequence`), with
  `sequence` as verbatim bytes: no case folding, no `.`/`-` rewriting, ragged rows allowed.
- Expose Stockholm as one row per sequence per alignment: `alignment_id`, `name`, `sequence`,
  plus `gs` and `gr` annotation bags as `List<Struct<tag, value>>` (the GFF `attributes`
  type), with an optional `gs_fields=[...]` keyword that flattens named `#=GS` features into
  top-level columns (the GFF `attr_fields` pattern).
- Support interleaved (block-wrapped) Stockholm and multi-alignment files; partition
  multi-alignment files on `//` boundaries across `target_partitions`.
- Reuse the shared object-store and `gz`/`bgz` compression plumbing from
  `datafusion-bio-format-core`.
- Wire projection and predicate pushdown through `_FORMAT_COLUMN_TYPES` so filters on the
  string columns are pushed down rather than silently no-op'd.
- Add `pyhmmer` and `biopython` as **test-only** optional dependencies (parity oracles), the
  same way `pysam` and `pyBigWig` are used today.

Out of scope for this change (documented as follow-ups, not silently dropped): writing any of
the three formats; A3M→A2M insert expansion (`expand_inserts`); a `skip_pseudo_sequences`
flag for `ss_pred`/`ss_conf`/`ss_dssp` rows; `#=GC` as per-column columns; any
alignment-aware operation (consensus, profile, column statistics).

## Impact

- Affected specs: `msa-io` (new capability)
- Planning ownership: this polars-bio OpenSpec change is the original cross-repository
  feature plan. Following the `add-cool-mcool-support` precedent — and the companion repo's
  own `openspec/AGENTS.md`, which requires a proposal for a new capability — the provider
  crate additionally carries `add-msa-format-provider` in `datafusion-bio-formats`, recording
  its own schema, parsing, partitioning and parity criteria so they can be reviewed and
  archived there.
- Affected companion checkout (`biodatageeks/datafusion-bio-formats`):
  - new crate `datafusion/bio-format-msa`
  - workspace `Cargo.toml` member list
- Affected polars-bio code:
  - `Cargo.toml`, `Cargo.lock` — **all 14 existing `datafusion-bio-format-*` pins move to
    the same new rev together**; a partial pin is not an option
  - `src/option.rs` — `InputFormat::{A2m, A3m, Sto}`, `MsaReadOptions`, `ReadOptions` field
  - `src/scan.rs` — three `register_table` arms
  - `src/lib.rs` — class registration, `py_describe_sto`
  - `polars_bio/io.py`, `polars_bio/sql.py`, `polars_bio/__init__.py`
  - `polars_bio/metadata_extractors.py` (only if `describe_sto` reuses the schema-metadata
    channel; see design)
  - `pyproject.toml` — test-only oracle dependencies
  - `docs/features/reading.md`, API docs, `CHANGELOG.md`
  - `tests/test_msa_io.py`, `tests/data/msa/`
- Public API: additive only; no existing signature changes.
