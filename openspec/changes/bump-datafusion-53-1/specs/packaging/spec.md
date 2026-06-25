## MODIFIED Requirements

### Requirement: Flexible Dependency Versioning

The project SHALL use permissive version specifiers for dependencies that allow minor version upgrades while preventing breaking major version changes. For Apache DataFusion Python bindings, the project SHALL use the latest published package in the selected DataFusion major version even when Rust DataFusion has a newer patch release in the same major line.

#### Scenario: Minor version compatibility
- **WHEN** a user has a newer minor version of a dependency installed (e.g., `typing-extensions==4.15.0` when polars-bio requires `>=4.14.0,<5`)
- **THEN** polars-bio installation SHALL succeed without forcing a downgrade

#### Scenario: Major version protection
- **WHEN** a dependency releases a new major version with potential breaking changes
- **THEN** the version constraint SHALL prevent automatic upgrades to that major version

#### Scenario: DataFusion 53 Python binding compatibility
- **WHEN** Rust dependencies use Apache DataFusion 53.1.0
- **THEN** Python metadata SHALL allow `datafusion>=53.0.0,<54`
- **AND** the Rust extension SHALL use the latest published compatible `datafusion-python` crate in the 53 major line

#### Scenario: PyArrow vulnerability floor
- **WHEN** polars-bio declares its Python runtime dependencies
- **THEN** Python metadata SHALL require `pyarrow>=23.0.1,<25`
- **AND** dependency resolution SHALL exclude vulnerable `pyarrow<23.0.1` releases
