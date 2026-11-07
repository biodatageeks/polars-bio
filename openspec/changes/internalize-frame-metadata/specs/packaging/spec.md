## ADDED Requirements

### Requirement: Self-contained frame metadata provider

Published polars-bio distributions SHALL include their owned metadata provider
and SHALL NOT require the `polars-config-meta` distribution. Any adapted upstream
material SHALL retain its required attribution in both wheel and source packages.

#### Scenario: Clean installation without the old dependency
- **WHEN** a user installs a published wheel in an environment without `polars-config-meta`
- **THEN** polars-bio SHALL initialize frame metadata and complete a tagged reader/transform/range-operation workflow
- **AND** its package metadata SHALL not declare the old runtime dependency

#### Scenario: Packaging consistency
- **WHEN** wheel, source archive, lockfile, and maintained Bioconda recipe are inspected
- **THEN** their runtime dependency declarations SHALL consistently reflect the owned provider
- **AND** applicable upstream license notices SHALL be included in distributed artifacts
