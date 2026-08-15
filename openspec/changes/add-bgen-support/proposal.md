# Change: Add BGEN genotype input

## Why

BGEN is the distribution format for large imputed cohorts, including UK
Biobank. polars-bio can already read every other common variant format, so a
user holding a `.bgen` file has to convert it before any polars-bio query, which
costs a full rewrite of the cohort.

## What Changes

- Add dedicated `read_bgen`, `scan_bgen`, `describe_bgen`, and `register_bgen`
  APIs backed by the upstream `datafusion-bio-format-bgen` provider.
- Emit one row per BGEN variant, keeping encoded alleles ordered in `alleles`
  without reference/alternate semantics, because BGEN does not define them.
- Expose `genotype_output="probability"` as the default, preserving every
  format-defined probability state in `genotypes.GP` alongside the declared
  `genotypes.PLOIDY`.
- Expose `genotype_output="dosage"`, emitting `genotypes.DS` as the expected
  copy count of `alleles[1]`, and reject multiallelic variants in that mode.
- Expose `probability_layout="fixed"`, storing each sample's states as a
  fixed-width list so the per-sample offsets are not emitted, for files whose
  variants all store the same number of states. `"nested"` remains the default
  because BGEN does not require a uniform width.
- Discover a neighbouring `.bgi` index and push `chrom`, `id`, `rsid`, `start`,
  and `end` predicates into the scan; accept an explicit `bgi_path`.
- Resolve sample identifiers from the embedded sample block, an explicit
  `sample_path`, or generated names, and allow `samples=[...]` to select and
  reorder emitted samples.
- Report BGEN layout, index provenance, emitted sample order, and genotype
  representation through `get_metadata`.
- Return registration errors instead of panicking, so an absent sample name
  raises `ValueError`.

## Impact

- Affected specs: `bgen`
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`,
  `polars_bio/metadata_extractors.py`, package exports, `src/option.rs`,
  `src/scan.rs`, `src/lib.rs`, BGEN tests, and reading documentation
- BGEN is input-only; no writer is added.
