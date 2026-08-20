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

### Requirement: BGEN Probability Storage Layout

The system SHALL store probability states as a variable-length list per sample
by default, and SHALL offer a fixed-width layout for files whose variants all
store the same number of states.

#### Scenario: Default layout
- **WHEN** probabilities are read without selecting a layout
- **THEN** each sample's states are a variable-length list
- **AND** a file whose variants store different numbers of states is readable.

#### Scenario: Fixed layout
- **WHEN** `probability_layout="fixed"` is selected
- **THEN** the collected schema declares the number of states per sample
- **AND** the probabilities equal those the default layout returns
- **AND** every sample of a variant is present, whether or not it was called.

#### Scenario: Fixed layout on a file that mixes state counts
- **WHEN** `probability_layout="fixed"` is selected and a variant stores a
  different number of states than the declared width
- **THEN** the read fails and names the layout
- **AND** no value is padded or truncated to fit.

#### Scenario: Unknown layout
- **WHEN** a layout other than `"nested"` or `"fixed"` is given
- **THEN** the call raises `ValueError` before any file is opened.

#### Scenario: Layout and dosage output
- **WHEN** a layout is given together with `genotype_output="dosage"`
- **THEN** the dosage output is unaffected.

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

#### Scenario: Describing with an explicit index
- **WHEN** `describe_bgen` is given `bgi_path`
- **THEN** that index is opened, so the reported `index` property reflects the
  index a read of the same file would use.

#### Scenario: Describe alongside a registered table
- **WHEN** a user registers a BGEN table and then describes the same file
- **THEN** the registered table keeps its own options and schema
- **AND** no table from the description remains registered.
