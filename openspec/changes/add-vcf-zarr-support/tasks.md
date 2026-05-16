## 1. Upstream Provider

- [x] 1.1 Create or extend the VCF Zarr crate/module in `/Users/mwiewior/CLionProjects/datafusion-bio-formats`.
- [x] 1.2 Add `zarrs` filesystem dependencies and local-store opening logic.
- [x] 1.3 Validate root metadata, including `vcf_zarr_version = "0.4"`.
- [x] 1.4 Discover mandatory and optional VCF Zarr arrays.
- [x] 1.5 Build the Arrow schema for polars-bio logical VCF columns.
- [x] 1.6 Implement logical column to raw array dependency planning.
- [x] 1.7 Implement projection pruning before array reads.
- [x] 1.8 Implement sample subset resolution from `sample_id`.
- [x] 1.9 Implement region-index based chunk pruning.
- [x] 1.10 Implement fallback chunk pruning from `variant_contig` and `variant_position`.
- [x] 1.11 Implement Arrow `RecordBatch` construction for core, INFO, FORMAT, and multisample `genotypes` output.
- [x] 1.12 Implement residual filter handling and limit handling.
- [x] 1.13 Add Rust tests and fixtures for schema, projection, predicates, region pruning, sample pruning, and unsupported stores.

## 2. polars-bio Integration

- [x] 2.1 Add local path dependency to the checked-out `datafusion-bio-formats` branch for development.
- [x] 2.2 Add `InputFormat::VcfZarr` and VCF Zarr read options in PyO3.
- [x] 2.3 Register `VcfZarrTableProvider` in `src/scan.rs`.
- [x] 2.4 Add explicit `scan_vcf_zarr` and `read_vcf_zarr` Python APIs.
- [x] 2.5 Reuse `_lazy_scan` for projection and predicate pushdown.
- [x] 2.6 Add predicate type metadata for VCF Zarr logical columns.
- [x] 2.7 Preserve coordinate-system and source metadata.
- [x] 2.8 Add Python tests for API behavior, projection pushdown, predicate pushdown, sample pruning, and metadata.
- [x] 2.9 Extend `describe_vcf` output to include FORMAT rows with a `field_type` discriminator.
- [x] 2.10 Add `describe_vcf_zarr` Rust/PyO3 and Python API support.
- [x] 2.11 Add `register_vcf_zarr` Python SQL registration API support.
- [x] 2.12 Add Python tests for `describe_vcf`, `describe_vcf_zarr`, and `register_vcf_zarr`.
- [x] 2.13 Add user documentation and API examples.

## 3. Typed VCZ Values and Genotype Encoding

- [x] 3.1 Add upstream VCF Zarr read options for `genotype_encoding_raw`, defaulting to raw typed `GT`.
- [x] 3.2 Infer Arrow schema types for projected INFO and non-GT FORMAT fields from supported VCF Zarr array dtypes and dimensions.
- [x] 3.3 Build typed Arrow arrays for scalar, list-valued, and nested selected-sample INFO/FORMAT values without stringifying numeric or boolean data.
- [x] 3.4 Implement mutually exclusive `GT` output modes: raw typed allele calls by default, existing VCF-style strings when `genotype_encoding_raw=False`.
- [x] 3.5 Expose `genotype_encoding_raw: bool = True` in `scan_vcf_zarr` and `read_vcf_zarr`, and pass it through to the upstream provider.
- [x] 3.6 Add upstream Rust coverage for typed INFO, typed FORMAT, raw `GT` default, string `GT` compatibility mode, sample subsets, and projection pruning.
- [x] 3.7 Add polars-bio Python coverage for default raw `GT`, requested string `GT`, no duplicate raw/string `GT` exposure, typed INFO/FORMAT dtypes, and representative analytical queries.

## 4. Finalization

- [x] 4.1 Replace the local path dependency with a publishable git revision or agreed dependency reference.
- [x] 4.2 Run upstream Rust tests for the affected `datafusion-bio-formats` crate(s).
- [x] 4.3 Run polars-bio Rust checks/tests.
- [x] 4.4 Run targeted polars-bio VCF Zarr pytest tests.
- [x] 4.5 Run `openspec validate add-vcf-zarr-support --strict`.
- [x] 4.6 Update this task list to checked state after implementation is complete.
