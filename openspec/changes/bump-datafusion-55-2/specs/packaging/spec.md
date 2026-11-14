## MODIFIED Requirements

### Requirement: Flexible Dependency Versioning

The project SHALL use permissive version specifiers for dependencies that allow minor version upgrades while preventing breaking major version changes. For Apache DataFusion Python bindings, the project SHALL use the latest published package in the selected DataFusion major version even when Rust DataFusion has a newer patch release in the same major line.

Polars is an exception: the requirement SHALL be `>=1.40.0,<2.2` until newer
minor releases have passed compatibility testing, because the integration uses
unstable IO-plugin and expression-serialization APIs.

#### Scenario: Polars compatibility boundary
- **WHEN** a user installs polars-bio
- **THEN** Polars 1.x from 1.40.0, 2.0.x and 2.1.x SHALL be allowed, and Polars 2.2 or later SHALL be excluded until validated

#### Scenario: Minor version compatibility
- **WHEN** a user has a newer minor version of a dependency installed (e.g., `typing-extensions==4.15.0` when polars-bio requires `>=4.14.0,<5`)
- **THEN** polars-bio installation SHALL succeed without forcing a downgrade

#### Scenario: Major version protection
- **WHEN** a dependency releases a new major version with potential breaking changes
- **THEN** the version constraint SHALL prevent automatic upgrades to that major version

#### Scenario: DataFusion 55 Python binding compatibility
- **WHEN** Rust dependencies use Apache DataFusion 55.2.0
- **THEN** Python metadata SHALL allow `datafusion>=55.0.0,<56`
- **AND** the Rust extension SHALL use the latest published compatible `datafusion-python` crate in the 55 major line

#### Scenario: PyArrow vulnerability floor
- **WHEN** polars-bio declares its Python runtime dependencies
- **THEN** Python metadata SHALL require `pyarrow>=23.0.1,<25`
- **AND** dependency resolution SHALL exclude vulnerable `pyarrow<23.0.1` releases

## ADDED Requirements

### Requirement: Stable ABI Wheel Floor

Each platform SHALL publish one stable-ABI (abi3) wheel whose minimum Python version equals the `requires-python` lower bound. The floor SHALL be declared explicitly with a PyO3 `abi3-pyXY` feature rather than derived from whichever interpreter the build discovers first.

#### Scenario: Wheel installs on the oldest supported Python
- **WHEN** a wheel is built on any platform, with any interpreter on the build host
- **THEN** it SHALL be tagged `cp311-abi3`, matching `requires-python = ">=3.11"`
- **AND** it SHALL install on CPython 3.11 through 3.14

#### Scenario: No untested interpreter-specific wheels
- **WHEN** the build host also provides free-threaded CPython or PyPy interpreters
- **THEN** the release SHALL NOT include version-specific wheels for them until they are tested
