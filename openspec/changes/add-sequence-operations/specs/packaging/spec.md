## ADDED Requirements

### Requirement: Polars Plugin Compatibility Gate

Because polars-bio ships a native Polars expression plugin, CI SHALL run the
sequence-expression test suite against the built wheel on the lowest and the
newest release of every Polars line in the supported range. Polars' supported
range SHALL NOT be widened until the plugin passes on the new release.

#### Scenario: Supported Polars versions
- **WHEN** CI runs for a pull request that touches the plugin, its Python namespace, or the Polars requirement
- **THEN** the sequence tests SHALL pass with Polars `1.40.0`, the newest `1.x`, `2.0.0` and the newest `2.1.x`

#### Scenario: Raising the Polars upper bound
- **WHEN** a pull request raises the Polars upper bound to include a new minor release
- **THEN** the plugin compatibility job SHALL include that release and pass before the change is merged

### Requirement: Wheel Size Budget

Every platform wheel built for release SHALL be smaller than the polars-bio
project's PyPI per-file upload limit, which is 100 MB unless a higher limit
has been confirmed for the project. CI SHALL fail when a wheel exceeds the
limit minus a 5 MB safety margin.

#### Scenario: Wheel size check
- **WHEN** the release workflow builds the Linux, macOS and Windows wheels
- **THEN** it SHALL report every wheel's size, and SHALL fail if any wheel is larger than the budget

#### Scenario: Symbol stripping keeps the extension loadable
- **WHEN** release builds strip symbols to meet the budget
- **THEN** `PyInit_polars_bio` and every `_polars_plugin_*` symbol SHALL remain exported, and the `seq` namespace tests SHALL pass on the stripped wheel
