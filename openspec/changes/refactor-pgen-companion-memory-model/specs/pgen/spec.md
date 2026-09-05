## ADDED Requirements

### Requirement: PGEN Companion Limit Controls

The system SHALL let a caller raise or lower the PVAR and PSAM companion caps
at every PGEN entry point, and SHALL default them to provider values that open
the published PLINK 2 reference panels.

#### Scenario: Reference panel metadata opens untuned
- **WHEN** `pgsc_1000G_v1/GRCh38_1000G_ALL.pgen` is described with
  `describe_pgen` and scanned with only metadata columns, with no cap arguments
- **THEN** describing the fileset returns its schema and file properties
- **AND** the metadata scan's row count matches the PGEN header's variant count.

This scenario does not require materializing a full-panel dense genotype
matrix. Its output alone needs about 224 GiB for `ALT_COUNT`; row selection
remains the separate `add-pgen-matrix-variant-selection` change.

#### Scenario: Caps are accepted at every entry point
- **WHEN** `max_companion_bytes`, `max_decompressed_companion_bytes`, or
  `max_variants` is passed to `read_pgen`, `scan_pgen`, `read_pgen_matrix`,
  `describe_pgen`, or `register_pgen`
- **THEN** the value reaches the provider options
- **AND** an unset value keeps the provider default rather than zero.

#### Scenario: A lowered cap fails by name
- **WHEN** a cap is set below the fileset's size
- **THEN** the call raises before any genotype record is read
- **AND** the message names the argument and the configured value.

#### Scenario: Matrix positions avoid Python objects
- **WHEN** `read_pgen_matrix` reports row positions
- **THEN** they are delivered as an `int64` NumPy array filled from Rust
- **AND** no intermediate Python list of integers is built.

#### Scenario: A raised cap does not change content
- **WHEN** the same fileset is read with a cap raised above the default
- **THEN** the emitted rows and genotypes are identical to the default read.
