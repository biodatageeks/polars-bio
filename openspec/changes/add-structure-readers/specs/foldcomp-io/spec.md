## ADDED Requirements

### Requirement: Native Foldcomp structural sources
The system SHALL read supported standalone FCZ files and local Foldcomp databases using the upstream codec through a checked native adapter. It SHALL expose the common atom and residue schemas without Python-list or intermediate PDB-text construction in the production decode path.

#### Scenario: Standalone FCZ
- **WHEN** a valid standalone FCZ file is scanned
- **THEN** reconstructed atom coordinates and available identities SHALL be exposed through the common structural schema.

### Requirement: Explicit selective database reads
Database readers SHALL distinguish lookup name, internal title, numeric entry key, and index position. Name/key selectors SHALL decode only the distinct selected payloads. An omitted selector SHALL mean all entries; an explicitly empty selector SHALL mean zero entries. Missing requested IDs SHALL error by default, and duplicate requested IDs SHALL be deduplicated.

#### Scenario: Small named subset
- **WHEN** two valid distinct entry names are requested from a larger database
- **THEN** exactly those two payloads SHALL be decoded
- **AND** metadata lookup work SHALL be accounted for separately from payload decoding.

#### Scenario: Empty IDs
- **WHEN** `ids=[]` is provided
- **THEN** the reader SHALL return a typed empty result with zero payload decodes rather than invoking upstream all-entry behavior.

#### Scenario: Missing or ambiguous name
- **WHEN** a requested name is absent or maps ambiguously to multiple database entries
- **THEN** name-based selection SHALL report a contextual error rather than silently skipping or choosing an entry.

### Requirement: Reconstructed-coordinate descriptor semantics
Foldcomp public coordinates SHALL represent the codec's reconstructed coordinates, and public angles SHALL be computed from those same coordinates with the common residue kernels. Codec torsion padding/alignment SHALL NOT appear as residue geometry. Metadata absent from the codec SHALL remain null or explicitly marked as reconstructed.

#### Scenario: Quantized angle arrays
- **WHEN** FCZ exposes a padded phi/psi/omega vector or parameters that differ from reconstructed-coordinate geometry
- **THEN** residue outputs SHALL follow the common coordinate-based definitions and terminal null rules.

#### Scenario: Missing confidence provenance
- **WHEN** a database provides a B-factor-like value without a confidence-score contract
- **THEN** the value SHALL NOT automatically be relabeled as pLDDT.

### Requirement: Validated sidecar and codec boundaries
The reader SHALL validate supported dbtype, required sidecars, integer/range arithmetic, selected payload bounds, record terminators, and lookup/key consistency. Native failures SHALL become contextual recoverable errors, and resources SHALL be released on completion, cancellation, or failure. Entry decoding SHALL respect the configured worker/size budgets.

#### Scenario: Invalid selected offset
- **WHEN** a selected entry's offset/length overflows or extends beyond the payload file
- **THEN** reading SHALL fail before passing the invalid range to the decoder.

#### Scenario: Missing lookup with numeric selection
- **WHEN** a database lacks a lookup table but has a valid index and an explicit supported numeric-key selector
- **THEN** selection SHALL use numeric keys without inventing names
- **AND** a name-based selector SHALL report the missing lookup.

### Requirement: Codec parity and work-count validation
Foldcomp integration tests SHALL compare full atom identities/counts and reconstructed arrays against a pinned official codec, and common descriptors against independent geometry oracles. Tests SHALL separately validate subset payload decode counts and original-versus-reconstructed compression error.

#### Scenario: Decoder parity
- **WHEN** the native adapter decodes a committed FCZ fixture
- **THEN** its reconstructed-coordinate results SHALL match the official raw-array oracle within the validated implementation tolerance
- **AND** it SHALL NOT use compression-loss RMSD as a tolerance for adapter errors.
