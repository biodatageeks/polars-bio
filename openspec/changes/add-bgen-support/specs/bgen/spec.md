## ADDED Requirements

### Requirement: BGEN Genotype Input

The system SHALL read BGEN 1.2 and 1.3 genotype files through dedicated eager,
lazy, registration, and description methods, emitting one row per BGEN variant.

#### Scenario: Lazy probability scan
- **WHEN** a user calls `scan_bgen` without a genotype output option
- **THEN** `genotypes.GP` contains every format-defined probability state for
  each selected sample
- **AND** `genotypes.PLOIDY` contains each selected sample's declared ploidy
- **AND** the scan remains lazy until collection.

#### Scenario: Dosage output
- **WHEN** a user calls `read_bgen` or `scan_bgen` with
  `genotype_output="dosage"`
- **THEN** `genotypes.DS` contains the expected copy count of `alleles[1]` for
  each selected sample
- **AND** a multiallelic variant is rejected rather than silently collapsed.

#### Scenario: Encoded alleles carry no reference semantics
- **WHEN** any BGEN variant is read
- **THEN** `alleles` preserves the encoded order
- **AND** no column presents an allele as reference or alternate.

#### Scenario: Unsupported genotype output
- **WHEN** a genotype output other than `"probability"` or `"dosage"` is given
- **THEN** the call raises `ValueError` before any file is opened.

#### Scenario: Non-BGEN path
- **WHEN** a path that does not end in `.bgen` is passed to a BGEN method
- **THEN** the call raises `ValueError` naming the expected suffix.

### Requirement: BGEN Sample Selection And Metadata

The system SHALL resolve sample identifiers from the file and SHALL report the
emitted sample order and file layout as metadata.

#### Scenario: Sample subset and order
- **WHEN** a user passes `samples=[...]`
- **THEN** only those samples are emitted, in the requested order.

#### Scenario: Absent sample
- **WHEN** a requested sample is not in the file
- **THEN** the call raises `ValueError` naming the sample.

#### Scenario: Reported metadata
- **WHEN** a user inspects a BGEN scan's metadata
- **THEN** it reports the emitted sample order, the BGEN layout, whether an
  index was used, and the genotype representation.

### Requirement: BGEN Index Pushdown And Projection

The system SHALL use a discovered or explicit `.bgi` index for genomic
predicate pushdown and SHALL avoid reading probability payloads that no
projected column needs.

#### Scenario: Metadata-only projection
- **WHEN** a scan projects only variant metadata columns
- **THEN** no probability block is read or decompressed.

#### Scenario: Partition-independent content
- **WHEN** the same file is scanned at different `target_partitions` values
- **THEN** the emitted rows and values are identical after ordering by variant
  position.

### Requirement: BGEN Description Isolation

Describing a BGEN file SHALL NOT disturb tables the caller has registered.

#### Scenario: Describe alongside a registered table
- **WHEN** a user registers a BGEN table and then describes the same file
- **THEN** the registered table keeps its own options and schema
- **AND** no table from the description remains registered.
