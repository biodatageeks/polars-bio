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
- **THEN** matching `variant_<ID>` arrays are exposed as logical columns named by their INFO field IDs
- **AND** supported VCF Zarr primitive types are preserved as typed Arrow columns rather than VCF text strings.

#### Scenario: FORMAT fields are projected
- **WHEN** a user requests FORMAT fields through `format_fields` or lazy projection
- **THEN** matching `call_<ID>` arrays are exposed according to existing single-sample and multisample VCF output conventions
- **AND** supported non-GT primitive types are preserved as typed Arrow values inside FORMAT output rather than VCF text strings.

### Requirement: Typed VCF Zarr INFO and FORMAT Columns
The system SHALL preserve supported VCF Zarr primitive types for projected INFO and non-GT FORMAT fields.

#### Scenario: Numeric INFO remains numeric
- **WHEN** a supported VCF Zarr store contains a numeric `variant_<ID>` array
- **AND** a user requests that INFO field
- **THEN** the logical INFO column uses a numeric Arrow type
- **AND** numeric predicates and aggregations do not require casting from string values.

#### Scenario: Boolean INFO remains boolean
- **WHEN** a supported VCF Zarr store contains a boolean `variant_<ID>` array
- **AND** a user requests that INFO field
- **THEN** the logical INFO column uses a boolean Arrow type
- **AND** values are not converted to `"true"` or `"false"` strings.

#### Scenario: Array-valued INFO preserves list shape
- **WHEN** a supported VCF Zarr INFO array has an additional value dimension
- **AND** a user requests that INFO field
- **THEN** the logical INFO column preserves the value dimension as an Arrow list
- **AND** values are not comma-joined into a single string.

#### Scenario: Non-GT FORMAT remains typed
- **WHEN** a supported VCF Zarr store contains a numeric, boolean, or string `call_<ID>` FORMAT array other than `call_genotype`
- **AND** a user requests that FORMAT field
- **THEN** the `genotypes.<ID>` field preserves the source primitive type inside the existing sample-oriented FORMAT output
- **AND** values are not converted to `List<Utf8>` unless the source array is string-typed.

#### Scenario: Array-valued FORMAT preserves list shape
- **WHEN** a supported VCF Zarr FORMAT array has sample and value dimensions
- **AND** a user requests that FORMAT field
- **THEN** the `genotypes.<ID>` field preserves both the selected-sample dimension and value dimension using nested Arrow lists
- **AND** values are not comma-joined into strings.

### Requirement: VCF Zarr Genotype Encoding Option
The system SHALL default VCF Zarr `GT` output to a raw typed encoding and SHALL allow callers to request the existing string encoding instead.

#### Scenario: Raw GT is the default
- **WHEN** a user scans or reads VCF Zarr with `format_fields` containing `GT`
- **AND** the user does not override genotype encoding
- **THEN** `genotypes.GT` is emitted as raw typed allele calls preserving the selected-sample and ploidy dimensions
- **AND** no string-formatted `GT` field is emitted for the same scan.

#### Scenario: String GT can be requested
- **WHEN** a user scans or reads VCF Zarr with `format_fields` containing `GT`
- **AND** the user requests string genotype encoding
- **THEN** `genotypes.GT` uses the VCF-style string representation
- **AND** no raw-typed `GT` field is emitted for the same scan.

#### Scenario: Raw GT sidecar metadata is typed
- **WHEN** raw genotype encoding is used
- **AND** the VCF Zarr store contains phasing or genotype mask arrays
- **THEN** the reader exposes the necessary phasing or mask metadata as typed genotype fields
- **AND** those metadata fields are not duplicated as VCF-style strings.

#### Scenario: Sample subset applies to both encodings
- **WHEN** a user provides `samples`
- **AND** the scan emits raw or string `GT`
- **THEN** genotype values are limited to the selected samples
- **AND** selected-sample order follows the requested order.

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

### Requirement: VCF And VCF Zarr Describe And Register APIs
The system SHALL provide explicit `describe_vcf_zarr` and `register_vcf_zarr` APIs that follow the existing VCF API conventions for local VCF Zarr stores, and SHALL include FORMAT fields in VCF describe output.

#### Scenario: Describe VCF fields
- **WHEN** a user calls `describe_vcf` with a supported VCF path
- **THEN** the system returns a Polars `DataFrame` with `name`, `field_type`, `data_type`, and `description` columns
- **AND** each row describes one discovered INFO field or exposed FORMAT column
- **AND** nested multisample FORMAT data is described as the exposed `genotypes` column
- **AND** single-sample FORMAT rows use the exposed column name, including collision names such as `fmt_DP`
- **AND** `field_type` is `INFO` for INFO rows and `FORMAT` for FORMAT rows.

#### Scenario: Describe VCF Zarr fields
- **WHEN** a user calls `describe_vcf_zarr` with a supported local VCF Zarr path
- **THEN** the system returns a Polars `DataFrame` with `name`, `field_type`, `data_type`, and `description` columns
- **AND** each row describes one discovered INFO field or exposed FORMAT column
- **AND** nested FORMAT data is described as the exposed `genotypes` column
- **AND** `field_type` is `INFO` for INFO rows and `FORMAT` for FORMAT rows.

#### Scenario: Register VCF Zarr for SQL
- **WHEN** a user calls `register_vcf_zarr` with a supported local VCF Zarr path and table name
- **THEN** the system registers the VCF Zarr store as a DataFusion table
- **AND** SQL queries can read core, INFO, and requested FORMAT columns through the logical VCF schema.
