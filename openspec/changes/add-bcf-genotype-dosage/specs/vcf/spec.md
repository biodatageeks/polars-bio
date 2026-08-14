## ADDED Requirements

### Requirement: Explicit BCF Genotype Dosage Output

The system SHALL preserve string GT output by default and SHALL allow callers
to request nullable signed 8-bit biallelic dosage from dedicated BCF input
methods.

#### Scenario: Lazy dosage scan
- **WHEN** a user calls `scan_bcf` with `format_fields=["GT"]` and
  `genotype_output="dosage"`
- **THEN** for multisample input, `genotypes.GT` contains the count of allele
  index 1 for each selected sample
- **AND** for single-sample input, the top-level `GT` column contains that
  sample's count, preserving the existing FORMAT layout
- **AND** the scan remains lazy until collection.

#### Scenario: Missing and phased genotype
- **WHEN** a selected GT contains a missing allele
- **THEN** its dosage is null
- **AND** phase separators do not affect called-allele counts.

#### Scenario: Compatibility default
- **WHEN** genotype output is omitted from a BCF call or set to `"string"`
- **THEN** existing VCF-compatible GT strings and schema are preserved.

#### Scenario: Unsupported dosage request
- **WHEN** BCF dosage is requested for multiple selected FORMAT fields,
  multiallelic records, or unsupported ploidy
- **THEN** the operation fails with a clear error instead of silently changing
  genotype meaning.

### Requirement: Dedicated VCF and BCF Public APIs

The system SHALL expose text VCF through `read_vcf`, `scan_vcf`, `describe_vcf`,
and `register_vcf`; expose BCF through `read_bcf`, `scan_bcf`, `describe_bcf`,
and `register_bcf`; and keep format-specific genotype controls off APIs where
they have no effect.

#### Scenario: Text VCF signatures
- **WHEN** a caller inspects or invokes `read_vcf` or `scan_vcf`
- **THEN** neither `genotype_output` nor `genotype_encoding_raw` is exposed
- **AND** a BCF path is rejected with guidance to use the BCF methods.

#### Scenario: BCF signatures
- **WHEN** a caller inspects or invokes `read_bcf` or `scan_bcf`
- **THEN** `genotype_output` is exposed with `"string"` as its default
- **AND** `genotype_encoding_raw` is not exposed
- **AND** a non-BCF path is rejected with a clear format error.

#### Scenario: Format-specific schema description
- **WHEN** a caller describes equivalent VCF and BCF inputs with
  `describe_vcf` and `describe_bcf`, respectively
- **THEN** their INFO and FORMAT schema descriptions match
- **AND** each method rejects the other physical format with guidance to use
  the corresponding method.

#### Scenario: Format-specific SQL registration
- **WHEN** a caller registers equivalent VCF and BCF inputs with `register_vcf`
  and `register_bcf`, respectively
- **THEN** equivalent SQL projections return matching rows and schemas
- **AND** `register_bcf` can expose nullable `Int8` dosage under the same
  constraints as `read_bcf` and `scan_bcf`
- **AND** each method rejects the other physical format with guidance to use
  the corresponding method.

#### Scenario: Signed BCF URL routing
- **WHEN** a BCF URL contains query parameters or a fragment
- **THEN** the URL path is still classified as BCF by reads and range operations
- **AND** the complete URL remains available to the object-store reader
- **AND** literal `?` or `#` characters in local BCF filenames are preserved.

#### Scenario: VCF Zarr raw encoding remains supported
- **WHEN** a caller uses `read_vcf_zarr` or `scan_vcf_zarr`
- **THEN** `genotype_encoding_raw` remains available because it controls the
  VCF Zarr representation.

### Requirement: Comparable One-Thread BCF Benchmark

The system SHALL compare typed BCF dosage with a pinned snputils baseline using
equivalent output and controlled one-thread optimized processes.

#### Scenario: Performance acceptance
- **WHEN** both implementations run at least three interleaved times on the same
  cohort, samples, and normalized dosage cells
- **THEN** every output row and cell matches
- **AND** the polars-bio release/native median wall time is lower than snputils
- **AND** median peak RSS is reported for both.

### Requirement: VCF-Compatible BCF Input

The system SHALL read BCF input through dedicated BCF eager and lazy APIs while
retaining the shared SQL and schema-description integration, and SHALL preserve
the equivalent VCF result contract when string genotype output is used.

#### Scenario: Converted fixture parity
- **WHEN** equivalent valid VCF and BCF fixtures are read through their
  respective eager or lazy methods
- **THEN** their sorted rows, column order, and data types match exactly
- **AND** their INFO and FORMAT schema descriptions match.

#### Scenario: CSI-backed BCF scan
- **WHEN** a coordinate-sorted BCF has a neighboring `.bcf.csi`
- **THEN** genomic predicates use CSI-backed range planning
- **AND** a full scan exposes up to the configured target input partitions
- **AND** changing the partition count does not change the result rows.

#### Scenario: Unindexed BCF fallback
- **WHEN** no CSI is available for a BCF input
- **THEN** the scan remains correct with one sequential input partition.
