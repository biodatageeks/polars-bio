## ADDED Requirements

### Requirement: Explicit BCF Genotype Dosage Output

The system SHALL preserve string GT output by default and SHALL allow callers
to request nullable signed 8-bit biallelic dosage from BCF input.

#### Scenario: Lazy dosage scan
- **WHEN** a user scans BCF with `format_fields=["GT"]` and
  `genotype_output="dosage"`
- **THEN** `genotypes.GT` contains the count of allele index 1 for each selected
  sample
- **AND** the scan remains lazy until collection.

#### Scenario: Missing and phased genotype
- **WHEN** a selected GT contains a missing allele
- **THEN** its dosage is null
- **AND** phase separators do not affect called-allele counts.

#### Scenario: Compatibility default
- **WHEN** genotype output is omitted or set to `"string"`
- **THEN** existing VCF-compatible GT strings and schema are preserved.

#### Scenario: Unsupported dosage request
- **WHEN** dosage is requested for text VCF, multiple selected FORMAT fields,
  multiallelic records, or unsupported ploidy
- **THEN** the operation fails with a clear error instead of silently changing
  genotype meaning.

### Requirement: Comparable One-Thread BCF Benchmark

The system SHALL compare typed BCF dosage with a pinned snputils baseline using
equivalent output and controlled one-thread optimized processes.

#### Scenario: Performance acceptance
- **WHEN** both implementations run at least three interleaved times on the same
  cohort, samples, and normalized dosage cells
- **THEN** every output row and cell matches
- **AND** the polars-bio release/native median wall time is lower than snputils
- **AND** median peak RSS is reported for both.
