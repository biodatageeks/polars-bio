## ADDED Requirements

### Requirement: PGEN Companion Cache Controls

Every PGEN entry point SHALL accept a companion cache mode and an optional
cache directory and forward them to the provider, defaulting to read-only use
of an existing sidecar.

#### Scenario: Read-write mode creates the sidecar
- **WHEN** a fileset is read with `companion_cache="read_write"`
- **THEN** a sidecar is written next to the PVAR or in `cache_dir`
- **AND** a later read of the same fileset opens without parsing the PVAR.

#### Scenario: Off mode
- **WHEN** `companion_cache="off"` is given
- **THEN** no sidecar is read or written.

#### Scenario: Invalid mode
- **WHEN** an unknown cache mode is given
- **THEN** the call raises naming the accepted values.
