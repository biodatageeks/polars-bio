# packaging Specification

## Purpose
TBD - created by archiving change relax-dependency-constraints. Update Purpose after archive.
## Requirements
### Requirement: Flexible Dependency Versioning

The project SHALL use permissive version specifiers for dependencies that allow minor version upgrades while preventing breaking major version changes.

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

### Requirement: Consistent Version Specifier Format

The project SHALL use `>=X.Y.Z,<(X+1)` format for runtime dependencies to clearly communicate the supported version range, except for the tested-minor-version boundary for Polars described above.

#### Scenario: Version range clarity
- **WHEN** a user or tool inspects the dependency requirements
- **THEN** the minimum and maximum supported versions SHALL be explicitly stated

#### Scenario: Semantic versioning alignment
- **WHEN** dependencies follow semantic versioning
- **THEN** the version constraints SHALL allow all compatible minor and patch releases within a major version

