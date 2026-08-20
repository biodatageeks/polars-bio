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
- **THEN** `read_pgen_matrix` SHALL raise `ValueError` before the file is
  opened, rather than letting the `f64`-to-`i8` cast saturate it or turn NaN
  into `0`, which would be indistinguishable from a homozygous-reference call
- **AND** the same value SHALL be refused where the cast happens, so a caller
  holding the reader directly cannot route around the wrapper's check.

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
- **WHEN** a destination that is not a `numpy.ndarray`, not writable, not
  C-contiguous, not aligned, not the dtype the field implies, or not the length
  the fileset's shape reports is given to the matrix reader
- **THEN** the reader SHALL raise before any genotype is decoded
- **AND** the reader SHALL take the destination array itself, never an address,
  so no caller can direct a decode at memory of its choosing
- **AND** the type SHALL be checked by identity rather than by `isinstance`,
  because every other check is an attribute lookup an arbitrary object — or a
  subclass through `__getattr__` — can answer while supplying any address
- **AND** `read_pgen_matrix` allocates its own destination, so a caller of the
  public function cannot reach that failure.
