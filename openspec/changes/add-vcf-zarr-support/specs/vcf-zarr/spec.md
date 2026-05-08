## ADDED Requirements

### Requirement: Explicit VCF Zarr Read APIs
The system SHALL provide explicit `scan_vcf_zarr` and `read_vcf_zarr` APIs for reading local VCF Zarr stores.

#### Scenario: Scan VCF Zarr lazily
- **WHEN** a user calls `scan_vcf_zarr` with a supported local VCF Zarr path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no records are materialized until the lazy query is collected.

#### Scenario: Read VCF Zarr eagerly
- **WHEN** a user calls `read_vcf_zarr` with a supported local VCF Zarr path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: VCF Zarr Version Compatibility
The system SHALL support VCF Zarr stores with root attribute `vcf_zarr_version` equal to `"0.4"` and SHALL reject unsupported or missing versions with a clear error.

#### Scenario: Supported version
- **WHEN** a local store has `vcf_zarr_version = "0.4"`
- **THEN** the reader opens the store if all required arrays are valid.

#### Scenario: Unsupported version
- **WHEN** a local store has a missing or unsupported `vcf_zarr_version`
- **THEN** the reader fails before scanning records
- **AND** the error identifies the unsupported or missing version metadata.

### Requirement: Logical VCF Schema
The system SHALL expose VCF Zarr data using the same logical VCF schema conventions as existing VCF scans.

#### Scenario: Core columns are projected
- **WHEN** a user scans a supported VCF Zarr store
- **THEN** core fields are exposed as logical columns such as `chrom`, `start`, `end`, `id`, `ref`, `alt`, `qual`, and `filter`.

#### Scenario: INFO fields are projected
- **WHEN** a user requests INFO fields through `info_fields` or lazy projection
- **THEN** matching `variant_<ID>` arrays are exposed as logical columns named by their INFO field IDs.

#### Scenario: FORMAT fields are projected
- **WHEN** a user requests FORMAT fields through `format_fields` or lazy projection
- **THEN** matching `call_<ID>` arrays are exposed according to existing single-sample and multisample VCF output conventions.

### Requirement: Projection Pruning
The system SHALL avoid reading raw VCF Zarr arrays that are not required by the projected logical columns or pushed predicates.

#### Scenario: Core-only projection
- **WHEN** a lazy query selects only `chrom` and `start`
- **THEN** genotype, INFO, quality, ID, allele, and filter arrays are not read.

#### Scenario: FORMAT projection
- **WHEN** a lazy query selects one requested FORMAT field
- **THEN** unrelated `call_<ID>` arrays are not read.

### Requirement: Genomic Predicate Chunk Pruning
The system SHALL use genomic predicates on `chrom`, `start`, and `end` to prune VCF Zarr variant chunks before reading projected data arrays.

#### Scenario: Region index exists
- **WHEN** a supported store contains `region_index`
- **AND** a query filters by genomic region
- **THEN** the provider uses `region_index` to select candidate variant chunks.

#### Scenario: Region index is absent
- **WHEN** a supported store does not contain `region_index`
- **AND** a query filters by genomic region
- **THEN** the provider uses lightweight position arrays to identify candidate chunks
- **AND** reads heavier projected arrays only for candidate chunks.

### Requirement: Sample Subset Selection
The system SHALL allow callers to provide an optional `samples` list that limits multisample FORMAT output to selected sample names.

#### Scenario: Selected samples preserve order
- **WHEN** a user scans a VCF Zarr store with `samples=["S2", "S1"]`
- **THEN** emitted multisample FORMAT output contains only those samples
- **AND** sample order follows the requested order.

#### Scenario: Missing samples are skipped
- **WHEN** a user requests a missing sample and an existing sample
- **THEN** the output includes the existing sample
- **AND** the missing sample does not cause the scan to fail.

### Requirement: Local Filesystem Scope
The initial implementation SHALL support local filesystem VCF Zarr stores and SHALL fail clearly for unsupported storage schemes.

#### Scenario: Local path
- **WHEN** a user provides a local filesystem path
- **THEN** the reader opens the store through the local filesystem Zarr backend.

#### Scenario: Unsupported remote path
- **WHEN** a user provides an unsupported remote path
- **THEN** the reader fails with an error that remote VCF Zarr storage is not supported in this version.

