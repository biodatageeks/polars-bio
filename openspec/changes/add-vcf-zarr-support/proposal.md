# Change: Add VCF Zarr read/scan support

## Why

VCF Zarr provides chunked, columnar storage for large VCF datasets. polars-bio should be able to scan VCF Zarr stores through the same lazy DataFusion and Polars pipeline used for existing genomic formats, while preserving projection and predicate pruning.

## What Changes

- Add explicit `scan_vcf_zarr` and `read_vcf_zarr` APIs.
- Implement a VCF Zarr DataFusion table provider in the local `datafusion-bio-formats` checkout.
- Use `zarrs` for local filesystem Zarr access.
- Expose the same logical schema as existing `scan_vcf`.
- Preserve supported VCF Zarr primitive types for INFO and non-GT FORMAT fields instead of stringifying them.
- Default VCF Zarr `GT` to raw typed allele calls, with an explicit string-encoding option for compatibility.
- Support read/scan only in the first version.
- Support local filesystem stores only in the first version.
- Support VCF Zarr spec 0.4 / Zarr v2 stores readable by `zarrs`.
- Use VCF Zarr `region_index` for genomic chunk pruning when present.
- Fall back to lightweight position-array chunk filtering when `region_index` is absent.
- Support `info_fields`, `format_fields`, `samples`, `projection_pushdown`, `predicate_pushdown`, and `use_zero_based`.

## Impact

- Affected specs: `vcf-zarr`
- Affected upstream checkout:
  - `/Users/mwiewior/CLionProjects/datafusion-bio-formats`
- Affected polars-bio code:
  - `Cargo.toml`, `Cargo.lock`
  - `src/option.rs`
  - `src/scan.rs`
  - `src/lib.rs`
  - `polars_bio/io.py`
  - `polars_bio/predicate_translator.py`
  - metadata extraction helpers as needed
  - docs and tests
- Development workflow:
  - Use branch `add-vcf-zarr-support` in both repositories.
  - Temporarily use a local path dependency from polars-bio to `/Users/mwiewior/CLionProjects/datafusion-bio-formats`.
  - Replace the local path dependency with a publishable git revision before final review.
