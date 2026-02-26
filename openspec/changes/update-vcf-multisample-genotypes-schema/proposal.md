# Change: Update VCF multisample FORMAT output to nested `genotypes`

## Why
`polars-bio` is being updated to the upstream `datafusion-bio-formats` VCF changes from PR #82, which redesign multisample FORMAT output. Keeping the old tests/docs would misrepresent runtime behavior and hide a user-visible breaking change.

## What Changes
- Bump `datafusion-bio-format-*` git dependencies to commit `f3fd02e25d4ee5b9bf2ca0a575ee991c1fadccb1` (PR #82).
- Keep single-sample VCF FORMAT fields as top-level columns.
- Switch multisample VCF FORMAT output to one nested `genotypes` column.
- Keep `info_fields=None` behavior aligned with upstream default (all INFO fields).
- Update VCF metadata extraction to prefer schema-level sample metadata (`bio.vcf.samples`).
- Update VCF tests/docs to reflect nested multisample schema.
- Add compressed multisample test fixture `tests/data/io/vcf/multisample.vcf.gz`.

## Impact
- Affected specs: `vcf` (new capability spec).
- Affected code:
  - `Cargo.toml`, `Cargo.lock`
  - `polars_bio/io.py`
  - `polars_bio/metadata_extractors.py`
  - `tests/test_vcf_format_columns.py`
  - `tests/test_vcf_write.py`
  - `tests/data/io/vcf/multisample.vcf.gz`
- **Breaking change**: multisample code selecting columns like `NA12878_GT` must migrate to nested `genotypes` access.
