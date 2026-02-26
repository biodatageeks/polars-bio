## ADDED Requirements

### Requirement: Multisample FORMAT Output Uses Nested Genotypes
When reading a multisample VCF, the system SHALL expose FORMAT values in a nested `genotypes` column rather than flattening them into `{sample}_{format}` columns.

#### Scenario: Multisample scan returns nested data
- **WHEN** a user calls `scan_vcf` or `read_vcf` on a VCF with more than one sample and requests FORMAT fields
- **THEN** the result includes a `genotypes` column with per-sample FORMAT values
- **AND** flattened sample FORMAT columns (for example `NA12878_GT`) are not produced.

### Requirement: Single-Sample FORMAT Output Remains Top-Level
Single-sample VCF FORMAT fields SHALL remain available as top-level columns.

#### Scenario: Single-sample read keeps direct columns
- **WHEN** a user reads a single-sample VCF requesting FORMAT fields `GT` and `DP`
- **THEN** the output includes top-level `GT` and `DP` columns.

### Requirement: Default INFO Projection Includes All Header INFO Fields
VCF read APIs SHALL treat `info_fields=None` as "include all INFO fields from the header".

#### Scenario: Default INFO fields are present
- **WHEN** a user reads a VCF without passing `info_fields`
- **THEN** INFO fields defined in the VCF header are available in the output schema.

### Requirement: VCF Metadata Extraction Supports Nested Multisample Schema
VCF metadata extraction SHALL populate sample names from schema-level metadata when present.

#### Scenario: Sample names from schema metadata
- **WHEN** the Arrow schema contains `bio.vcf.samples`
- **THEN** extracted metadata includes those sample names even when FORMAT values are nested under `genotypes`.
