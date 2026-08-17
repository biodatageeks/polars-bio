# Change: Add PGEN genotype input

## Why

PLINK 2 filesets are the working format for genome-wide association analysis
and the storage format of large biobank genotype releases. polars-bio reads
every other common variant format, so a user holding a `.pgen` must convert the
cohort before any polars-bio query, which costs a full rewrite.

## What Changes

- Add dedicated `read_pgen`, `scan_pgen`, `describe_pgen`, and `register_pgen`
  APIs backed by the upstream `datafusion-bio-format-pgen` provider.
- Emit one row per PVAR variant, with `chrom`, `start`, `end`, `id`, `ref`, and
  a list-typed `alt`, alongside a `genotypes` struct.
- Select genotype children by name through `genotype_fields`, from `GT`,
  `PHASED`, `DS`, `DS_STORED`, and `HDS`, in the requested order.
- Default `genotype_fields` to `("GT",)`, narrowing the provider default of
  every available child, so the default read is not the expensive one.
- Discover the `.pvar` (then `.pvar.zst`) and `.psam` companions from the
  `.pgen` basename, and accept explicit `pvar_path`, `psam_path`, and
  `pgi_path`.
- Build selectable sample names from PSAM identifiers under `psam_id_mode`,
  one of `iid`, `fid_iid`, or `fid_iid_sid`, and allow `samples=[...]` to
  select and reorder emitted samples under `missing_sample_policy`.
- Expose `max_range_gap`, `max_range_bytes`, and `batch_soft_byte_limit`, so a
  caller can trade wasted bytes for fewer object-storage requests. The
  provider's `max_range_gap` default of 0 never bridges a gap.
- Report storage mode, index provenance, specification baseline, emitted sample
  order, and full PSAM identities through `get_metadata`.
- Return registration errors instead of panicking, so an absent sample name
  raises rather than aborting the interpreter.

## Impact

- Affected specs: `pgen`
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`,
  `polars_bio/metadata_extractors.py`, package exports, `src/option.rs`,
  `src/scan.rs`, `src/lib.rs`, PGEN tests, and reading documentation
