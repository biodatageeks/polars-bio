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

#### Scenario: Probabilities are refused
- **WHEN** a probability output is requested from the matrix path
- **THEN** the call SHALL fail when the file is opened, because probabilities
  are variable width and have no single dense shape

### Requirement: Genotype child projection
`scan_bgen` and `read_bgen` SHALL accept `genotype_fields`, selecting which
children of the `genotypes` struct are emitted.

#### Scenario: Default is unchanged
- **WHEN** `genotype_fields` is `None`
- **THEN** every child SHALL be emitted, as before

#### Scenario: Declining PLOIDY
- **WHEN** `genotype_fields=["DS"]`
- **THEN** the struct SHALL carry only `DS`, and the dosages SHALL be unchanged

#### Scenario: The value child is required
- **WHEN** a projection omits the output mode's value child
- **THEN** the call SHALL fail at plan time naming the child it requires
