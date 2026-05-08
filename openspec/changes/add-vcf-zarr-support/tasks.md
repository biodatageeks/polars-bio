## 1. Upstream Provider

- [ ] 1.1 Create or extend the VCF Zarr crate/module in `/Users/mwiewior/CLionProjects/datafusion-bio-formats`.
- [ ] 1.2 Add `zarrs` filesystem dependencies and local-store opening logic.
- [ ] 1.3 Validate root metadata, including `vcf_zarr_version = "0.4"`.
- [ ] 1.4 Discover mandatory and optional VCF Zarr arrays.
- [ ] 1.5 Build the Arrow schema for polars-bio logical VCF columns.
- [ ] 1.6 Implement logical column to raw array dependency planning.
- [ ] 1.7 Implement projection pruning before array reads.
- [ ] 1.8 Implement sample subset resolution from `sample_id`.
- [ ] 1.9 Implement region-index based chunk pruning.
- [ ] 1.10 Implement fallback chunk pruning from `variant_contig` and `variant_position`.
- [ ] 1.11 Implement Arrow `RecordBatch` construction for core, INFO, FORMAT, and multisample `genotypes` output.
- [ ] 1.12 Implement residual filter handling and limit handling.
- [ ] 1.13 Add Rust tests and fixtures for schema, projection, predicates, region pruning, sample pruning, and unsupported stores.

## 2. polars-bio Integration

- [ ] 2.1 Add local path dependency to the checked-out `datafusion-bio-formats` branch for development.
- [ ] 2.2 Add `InputFormat::VcfZarr` and VCF Zarr read options in PyO3.
- [ ] 2.3 Register `VcfZarrTableProvider` in `src/scan.rs`.
- [ ] 2.4 Add explicit `scan_vcf_zarr` and `read_vcf_zarr` Python APIs.
- [ ] 2.5 Reuse `_lazy_scan` for projection and predicate pushdown.
- [ ] 2.6 Add predicate type metadata for VCF Zarr logical columns.
- [ ] 2.7 Preserve coordinate-system and source metadata.
- [ ] 2.8 Add Python tests for API behavior, VCF parity, projection pushdown, predicate pushdown, sample pruning, and metadata.
- [ ] 2.9 Add user documentation and API examples.

## 3. Finalization

- [ ] 3.1 Replace the local path dependency with a publishable git revision or agreed dependency reference.
- [ ] 3.2 Run upstream Rust tests for the affected `datafusion-bio-formats` crate(s).
- [ ] 3.3 Run polars-bio Rust checks/tests.
- [ ] 3.4 Run targeted polars-bio VCF Zarr and existing VCF pytest tests.
- [ ] 3.5 Run `openspec validate add-vcf-zarr-support --strict`.
- [ ] 3.6 Update this task list to checked state after implementation is complete.

