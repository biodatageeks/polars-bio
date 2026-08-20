# BGEN dense genotype matrix

## ADDED Requirements

### Requirement: Dense dosage matrix
The library SHALL provide `read_bgen_matrix`, returning a dense row-major
`float32` NumPy array with one row per variant and one column per selected
sample, together with the variant positions and sample names labelling its axes.

#### Scenario: Whole cohort
- **WHEN** `read_bgen_matrix` is called on a BGEN file
- **THEN** the returned array SHALL have shape `(variants, samples)` and dtype
  `float32`, and SHALL be C-contiguous

#### Scenario: Values match the DataFrame path
- **WHEN** the same file is read through `read_bgen_matrix` and through
  `read_bgen(genotype_output="dosage")`
- **THEN** the dosages SHALL be identical as `float32` bit patterns, not merely
  close

#### Scenario: Row labels follow the coordinate system
- **WHEN** the matrix is read under either coordinate system
- **THEN** the returned positions SHALL equal the `start` the scan emits for the
  same variants

#### Scenario: Missing genotypes
- **WHEN** a sample has no called genotype
- **THEN** the caller-supplied missing value SHALL be written, defaulting to
  `NaN`

#### Scenario: Destination is validated before it is written
- **WHEN** a destination that is not a `numpy.ndarray`, not writable, not
  C-contiguous, not aligned, not `float32`, or not the length the file's shape
  reports is given to the matrix reader
- **THEN** the reader SHALL raise before any dosage is decoded, and SHALL take
  the destination array itself rather than an address
- **AND** the decode SHALL hold the GIL, so no Python thread can resize or free
  the destination between the checks and the write

#### Scenario: Probabilities have no dense form
- **WHEN** a caller wants BGEN probability states as a matrix
- **THEN** `read_bgen_matrix` SHALL NOT offer an output mode to ask for them,
  because probabilities are variable width and have no single dense shape
- **AND** they SHALL be reached through `scan_bgen(genotype_output="probability")`
  instead

### Requirement: Genotype child projection
`scan_bgen`, `read_bgen`, and `register_bgen` SHALL accept `genotype_fields`,
selecting which children of the `genotypes` struct are emitted.

#### Scenario: Default is unchanged
- **WHEN** `genotype_fields` is `None`
- **THEN** every child SHALL be emitted, as before

#### Scenario: Declining PLOIDY
- **WHEN** `genotype_fields=["DS"]`
- **THEN** the struct SHALL carry only `DS`, and the dosages SHALL be unchanged

#### Scenario: Registration carries the projection
- **WHEN** a table is registered with `genotype_fields=["DS"]`
- **THEN** the registered table's `genotypes` struct SHALL carry only `DS`, so a
  SQL user is not forced to materialize `PLOIDY`

#### Scenario: An empty or misspelled selection
- **WHEN** `genotype_fields` is empty, or names a field outside `DS`, `GP`, and
  `PLOIDY`
- **THEN** the call SHALL raise `ValueError` before the file is opened
- **AND** which of those names a given output mode accepts SHALL remain the
  provider's rule, so the mode semantics have one home rather than two

#### Scenario: The value child is required
- **WHEN** a projection omits the output mode's value child
- **THEN** the call SHALL fail at plan time naming the child it requires
