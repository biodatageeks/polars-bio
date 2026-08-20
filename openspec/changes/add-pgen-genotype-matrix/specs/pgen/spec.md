## ADDED Requirements

### Requirement: Dense PGEN Genotype Matrix

The system SHALL read one genotype field of a PGEN fileset into a dense,
C-contiguous NumPy matrix with one row per PVAR variant and one column per
selected sample, decoding into that matrix directly rather than copying into it.

#### Scenario: Reading a hardcall matrix
- **WHEN** a user calls `read_pgen_matrix` with `field="ALT_COUNT"`
- **THEN** the result's `values` is a C-contiguous `int8` array shaped
  `(variants, samples)`
- **AND** `positions` labels every row and `sample_names` every column
- **AND** the values equal the `ALT_COUNT` child `read_pgen` reports.

#### Scenario: Reading a dosage matrix
- **WHEN** a user calls `read_pgen_matrix` with `field="DS"`
- **THEN** `values` is a C-contiguous `float32` array
- **AND** a fileset carrying a stored dosage track reports its fractional
  values, not the hardcall counts derived from the same records.

#### Scenario: Field without a dense form
- **WHEN** `field` is `GT`, `HDS`, `PHASED`, or `DS_STORED`
- **THEN** the call raises `ValueError` naming the supported fields and
  directing the caller to `read_pgen`
- **AND** no file is opened.

#### Scenario: Missing genotypes
- **WHEN** a genotype is missing
- **THEN** the caller's `missing` value is written at that cell
- **AND** the default is `-9` for `ALT_COUNT` and NaN for `DS`
- **AND** an integer field never widens to `float64` on the way to NumPy.

#### Scenario: A sentinel the field cannot hold
- **WHEN** `missing` is not a whole number in `[-128, 127]` and `field` is
  `ALT_COUNT`
- **THEN** the call raises `ValueError` before the file is opened, rather than
  letting the `f64`-to-`i8` cast saturate it or turn NaN into `0`, which would
  be indistinguishable from a homozygous-reference call.

#### Scenario: Sample selection
- **WHEN** `samples=[...]` is given
- **THEN** the matrix has one column per requested sample, in that order
- **AND** `sample_names` reports the same order.

#### Scenario: Row order is independent of partition count
- **WHEN** the same fileset is read at one and at several partitions
- **THEN** both matrices are identical, in PVAR order
- **AND** `positions` labels the same rows in both.

#### Scenario: Decoder threads follow the configured partitions
- **WHEN** `copy_threads` is not given
- **THEN** the number of decoding threads follows
  `datafusion.execution.target_partitions`
- **AND** a single-partition read decodes on one thread.

#### Scenario: Destination is validated before it is written
- **WHEN** the destination array is not writable, not C-contiguous, or not the
  shape the fileset reports
- **THEN** the call raises before any genotype is decoded.
