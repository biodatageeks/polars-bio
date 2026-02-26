# Change: update VEP annotation compatibility for bio-functions `ab88d47`

## Why
`polars-bio` pins `datafusion-bio-functions` to specific git revisions. Moving to revision `ab88d47c8f4e4b7e72da48797ac038c6c0439597` requires compatibility verification for the Python annotation entrypoints.

## What Changes
- Update `datafusion-bio-functions` git revision pins in Rust dependencies.
- Keep public Python annotation API stable:
  - `pb.annotations.annotate_variants`
  - `pb.annotations.create_vep_cache`
- Harden `annotate_variants` lookup argument construction:
  - normalize and validate `match_mode`
  - sanitize and normalize optional `columns`
  - preserve explicit default-column behavior when lookup arguments must be fully specified.
- Add annotation-focused tests for:
  - supported `match_mode` values
  - native/parquet/fjall cache workflows
  - validation failures
  - chr-prefixed VCF row preservation through annotation output.

## Impact
- Affected specs: `annotations` (new capability spec delta).
- Affected code:
  - `Cargo.toml`
  - `Cargo.lock`
  - `rust-toolchain.toml`
  - `src/lib.rs`
  - `polars_bio/vep_op.py`
  - `polars_bio/annotations.py`
  - `tests/test_annotations.py`
