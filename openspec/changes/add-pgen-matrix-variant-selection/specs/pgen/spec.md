## ADDED Requirements

### Requirement: PGEN Matrix Variant Selection

`read_pgen_matrix` SHALL decode only the variants a caller selects by region
or by PVAR row indices, and SHALL size its output, positions, and sample names
by that selection.

#### Scenario: Region selection
- **WHEN** `region="chr22:16000000-17000000"` is given
- **THEN** the matrix holds exactly the rows whose start lies in that range, in
  PVAR order, and equals the same rows of a full read.

#### Scenario: Row-range windows
- **WHEN** a caller loops over `rows=range(a, b)` windows covering the fileset
- **THEN** concatenating the windows reproduces the full matrix.

#### Scenario: Conflicting or invalid selection
- **WHEN** both `region` and `rows` are given, or `rows` is unsorted or out of
  range
- **THEN** the call raises before any genotype record is read.
