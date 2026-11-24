## ADDED Requirements

### Requirement: Selectable record and feature views

The reader SHALL expose lazy, eager and SQL access to GenBank records and features using the same schema and value rules for a given set of options.

#### Scenario: Two views of one file

- **WHEN** a file contains two LOCUS records, with three features in the first and none in the second
- **THEN** the default view returns two rows, the features view returns three rows, and every feature's record_id equals the first record's name
- **AND** both views can be queried concurrently through independent scans or separately named SQL tables.

#### Scenario: Declared length without a sequence

- **WHEN** a record supplies a declared length and CONTIG assembly information without ORIGIN
- **THEN** the records view returns the declared length and a null sequence.

#### Scenario: Protein LOCUS record

- **WHEN** a LOCUS declares its length in amino acids (`aa`)
- **THEN** the records view returns its residue count and `protein` molecule type, and feature coordinates refer to positions in that protein.

### Requirement: Location and coordinate fidelity

Feature rows SHALL expose native location text, ordered parts, strand and interpretable local bounds. Numeric bounds SHALL follow the explicit coordinate setting or the session setting when omitted.

#### Scenario: Complement of a compound location

- **WHEN** a feature with location `complement(join(2..5,9..12))` is read with zero-based coordinates
- **THEN** its parts have half-open bounds (8,12) and (1,5) in that order, both strands are minus, its scalar bounds are (1,12), and the location text retains its native numbers.

#### Scenario: A circular feature crosses the origin

- **WHEN** a feature joins the last segment and the first segment of a circular record
- **THEN** both segments remain available in order and the scalar bounds are null.

#### Scenario: Sites, remote references and fuzzy ends

- **WHEN** features include `100^101`, a remote accession or `order(<1..5,9..>12)`
- **THEN** the reader preserves each expression, represents the between-site as an empty zero-based interval or null one-based bounds, leaves remote bounds null, and retains the fuzzy numeric anchors in the ordered parts.

### Requirement: Qualifier preservation and projection

The features view SHALL expose a default map of qualifier keys to ordered values and support GFF-style promotion of selected qualifier names.

#### Scenario: Repeated, absent and flag qualifiers

- **WHEN** a feature contains two notes, a valueless flag and no gene qualifier
- **THEN** the map contains the two notes in order and a flag entry with a null value, and a promoted gene column is null.

#### Scenario: Protein translation selected directly

- **WHEN** a user selects translation from the default lazy features view or declares it via qualifier_fields for SQL
- **THEN** the result contains the unwrapped translation string, and the parser can omit the full qualifier map.

### Requirement: Streaming and pushdown

The provider SHALL parse with a custom streaming Rust parser, build Arrow batches for the requested projection, preserve filter dependencies, and report input framing errors with source context.

#### Scenario: Narrow scan of a large annotated record

- **WHEN** a scan returns only start and end for features matching a supported type predicate
- **THEN** the parser emits matching rows in bounded batches and materializes zero bytes of ORIGIN sequence and translation values.

#### Scenario: Residual filter followed by a limit

- **WHEN** the first matching record is beyond the requested limit in the unfiltered input
- **THEN** the scan applies the residual predicate before limiting and returns the same rows with pushdown enabled and disabled for a single-stream read.

#### Scenario: Truncated input

- **WHEN** the last record lacks its terminator
- **THEN** a full read fails with the source path and location of the incomplete record.

### Requirement: Compression, parallel reads and object storage

The reader SHALL support plain, gzip and BGZF input and pass existing storage options through to S3, GCS and Azure. Parallel scans SHALL assign every record to exactly one partition.

#### Scenario: Record spans a split

- **WHEN** a multi-record plain or BGZF file is read with multiple partitions on seekable input and a record crosses a proposed split
- **THEN** the partitions align to full record boundaries and together produce the same records and features, including duplicates, as a serial scan.

#### Scenario: Sequential remote stream

- **WHEN** a user reads a gzip object with credentials and compression options for the selected backend
- **THEN** the reader uses one decoding stream and returns the same rows as the local uncompressed file.
