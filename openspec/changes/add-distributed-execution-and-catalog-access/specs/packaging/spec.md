## ADDED Requirements

### Requirement: Optional catalog dependencies

Catalog client libraries SHALL be optional dependencies exposed as extras.
They SHALL NOT be runtime requirements of the base package or of the conda
recipe.

#### Scenario: Base install
- **WHEN** a user runs `pip install polars-bio`
- **THEN** no catalog client library SHALL be installed
- **AND** every feature that does not use a catalog SHALL work

#### Scenario: Extra install
- **WHEN** a user runs `pip install "polars-bio[lakekeeper]"`
- **THEN** the Lakekeeper client library SHALL be installed

### Requirement: Distributed runtime in the standard wheel

Release wheels SHALL include the distributed coordinator and the worker entry
point, so that a coordinator and its workers can be installed from the same
package at the same version. The distributed runtime SHALL be a build feature
that can be disabled.

#### Scenario: Worker from the published wheel
- **WHEN** a user installs a release wheel
- **THEN** `python -m polars_bio.worker --help` SHALL run

#### Scenario: Build without the distributed feature
- **WHEN** polars-bio is built with the distributed feature disabled
- **THEN** the package SHALL import and every single-node feature SHALL work
- **AND** `pb.distributed.connect` SHALL raise an error that says the build
  has no distributed support

### Requirement: Engine dependency lockstep

The distributed execution library SHALL be pinned to the release line that
matches the pinned DataFusion major version, and SHALL be bumped in the same
change as DataFusion.

#### Scenario: DataFusion upgrade
- **WHEN** a pull request raises the DataFusion major version
- **THEN** the same pull request SHALL move the distributed execution library
  to the release line built for that version
- **AND** the distributed parity suite SHALL pass before merge
