## ADDED Requirements

### Requirement: Structural collection readers
The system SHALL expose lazy and eager readers for PDB, PDBx/mmCIF Cartesian coordinate files, and explicit mixed structural collections, with matching SQL registration. It SHALL support atom and residue output levels, plain/gzip text, local paths/lists/globs, and explicit supported object-storage text paths.

#### Scenario: Mixed collection
- **WHEN** a user scans a list containing a PDB file and a gzipped mmCIF file
- **THEN** rows SHALL have a common schema and distinct source provenance
- **AND** eager, lazy, and SQL execution SHALL agree after explicit sorting by stable keys.

#### Scenario: Unsupported CIF content
- **WHEN** a requested CIF source has no supported Cartesian atom data or requires unsupported fractional-coordinate conversion
- **THEN** the reader SHALL report a contextual unsupported-data error rather than silently returning a successful empty structure.

### Requirement: Lossless structural identifiers in normalized tables
The atom schema SHALL preserve original author and label atom/residue/chain identifiers separately when available, use a string for author residue identifiers, and provide stable source/entry/atom/site ordinals independent of filtering and projection. Missing PDB label mappings SHALL remain null.

#### Scenario: Nonnumeric author identifier
- **WHEN** an mmCIF atom site has author residue ID `X1`, label residue ID `1`, and distinct author/label chain IDs
- **THEN** all identifiers SHALL be retained without numeric coercion or replacement.

#### Scenario: Reused identifiers
- **WHEN** two source files or two models reuse atom serials and residue numbers
- **THEN** their stable row keys SHALL remain distinct
- **AND** filename stems and residue numbers SHALL NOT be used as unique keys.

### Requirement: Explicit physical units and missing values
Structural Cartesian values SHALL use Float64 angstroms. Optional missing values SHALL be null, and malformed/non-finite required coordinates SHALL produce an error. Structural readers SHALL NOT attach genomic interval coordinate-system metadata or respond to genomic zero-based-coordinate settings.

#### Scenario: Missing occupancy
- **WHEN** an atom has valid XYZ but absent occupancy and B-factor
- **THEN** XYZ SHALL be preserved and the optional values SHALL be null rather than defaults.

#### Scenario: Genomic configuration independence
- **WHEN** a user changes the global genomic coordinate convention
- **THEN** reading the same structural source SHALL preserve identical XYZ and residue identifiers.

### Requirement: Atom-site and structural-boundary preservation
The atom view SHALL retain ATOM/HETATM rows, hydrogens, waters, ligands, alternate sites, source model numbers, and PDB TER boundaries. It SHALL support all/first/explicit model selection and SHALL NOT expand biological assemblies implicitly.

#### Scenario: Repeated author chain after TER
- **WHEN** a PDB file reuses chain A after a TER record
- **THEN** its segments SHALL remain distinguishable and downstream residue geometry SHALL NOT cross that boundary.

#### Scenario: Multiple models
- **WHEN** the first source model has number 5 and another has number 9
- **THEN** first-model selection SHALL choose model 5 while all-model selection SHALL retain both independently.

### Requirement: Correct lazy execution and collection resource bounds
Providers SHALL expose fixed schemas without collection-wide coordinate decoding, partition whole entries/files under the configured worker budget, and maintain independent execution state for repeated or concurrent collects. Initial parsing MAY buffer one entry/document per active worker, and this bound SHALL be documented and enforced through size controls.

#### Scenario: Two scans of one source
- **WHEN** atom and residue scans or scans with different model/altloc options are collected concurrently
- **THEN** each SHALL use its own intended options without catalog or cursor interference.

#### Scenario: Interleaved mmCIF rows
- **WHEN** atom rows for a residue are interleaved with other residues in a valid mmCIF entry
- **THEN** normalization SHALL preserve every atom and supply correctly grouped site data to residue execution.

### Requirement: Independent frozen input-output corpus
The implementation SHALL be validated against versioned small input files and expected tables from independent parser/numerical oracles and hand-audited synthetic cases. Fixture manifests SHALL include input/output hashes, generator/oracle versions, schema/policy versions, and explicit discrepancy records.

#### Scenario: Golden verification
- **WHEN** correctness tests run without external network access or oracle executables installed
- **THEN** they SHALL validate complete key/count/schema/null-mask/value equality against committed expected data.

#### Scenario: Oracle regeneration discrepancy
- **WHEN** independently generated outputs disagree beyond an established policy or tolerance
- **THEN** regeneration SHALL fail pending an explained discrepancy rather than silently updating expected values.
