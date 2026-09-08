## ADDED Requirements

### Requirement: Explicit A2M APIs
The system SHALL provide explicit `scan_a2m` and `read_a2m` APIs for reading A2M files.

#### Scenario: Scan A2M lazily
- **WHEN** a user calls `scan_a2m` with a supported local A2M path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no records are materialized until the lazy query is collected.

#### Scenario: Read A2M eagerly
- **WHEN** a user calls `read_a2m` with a supported local A2M path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: Explicit A3M APIs
The system SHALL provide explicit `scan_a3m` and `read_a3m` APIs for reading A3M files.

#### Scenario: Scan A3M lazily
- **WHEN** a user calls `scan_a3m` with a supported local A3M path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no records are materialized until the lazy query is collected.

#### Scenario: Read A3M eagerly
- **WHEN** a user calls `read_a3m` with a supported local A3M path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: Explicit Stockholm APIs
The system SHALL provide explicit `scan_sto` and `read_sto` APIs for reading Stockholm files.

#### Scenario: Scan Stockholm lazily
- **WHEN** a user calls `scan_sto` with a supported local Stockholm path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no records are materialized until the lazy query is collected.

#### Scenario: Read Stockholm eagerly
- **WHEN** a user calls `read_sto` with a supported local Stockholm path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: SQL Table Registration
The system SHALL allow A2M, A3M and Stockholm files to be registered as DataFusion SQL tables via `register_a2m`, `register_a3m` and `register_sto`.

#### Scenario: Query registered A3M
- **WHEN** a user registers an A3M file with `register_a3m`
- **AND** runs a SQL query selecting `name` and `sequence`
- **THEN** the query returns one row per record with the requested columns.

#### Scenario: Query registered Stockholm
- **WHEN** a user registers a Stockholm file with `register_sto`
- **AND** runs `SELECT count(*)` against it
- **THEN** the query returns the total number of sequence rows across all alignments in the file
- **AND** no sequence strings are buffered to produce the count.

### Requirement: A2M and A3M Logical Schema
The system SHALL expose A2M and A3M records with the same schema as FASTA: `name` (Utf8, non-null), `description` (Utf8, nullable) and `sequence` (LargeUtf8, non-null).

#### Scenario: Header split
- **WHEN** a record header is `>sp|Q5VUD6|FA69B_HUMAN Protein FAM69B OS=Homo sapiens`
- **THEN** `name` is `sp|Q5VUD6|FA69B_HUMAN`
- **AND** `description` is `Protein FAM69B OS=Homo sapiens`.

#### Scenario: Header without description
- **WHEN** a record header contains no whitespace after the identifier
- **THEN** `description` is null.

#### Scenario: Comma in identifier is not a separator
- **WHEN** a record header is `>tr|A0A0B4J2F0|A0A0B4J2F0_HUMAN, n=1 Tax=Homo sapiens`
- **THEN** `name` is `tr|A0A0B4J2F0|A0A0B4J2F0_HUMAN,`
- **AND** the identifier is split on whitespace only, not on the comma.

### Requirement: Verbatim Sequence Passthrough
The system SHALL return the `sequence` column of A2M and A3M records as the exact concatenation of the record's sequence lines, preserving letter case, `-` and `.` characters, and MUST NOT expand, pad, or validate the alignment.

#### Scenario: Ragged A3M rows are preserved
- **WHEN** an A3M file contains records whose sequence lengths differ because insert-state gaps are omitted
- **THEN** every record is returned with its own length
- **AND** no error is raised.

#### Scenario: Lowercase insert states are preserved
- **WHEN** a record's sequence contains lowercase letters
- **THEN** the returned `sequence` contains the same lowercase letters at the same positions.

#### Scenario: Dotted A2M is preserved
- **WHEN** an A2M file uses `.` in insert columns
- **THEN** the returned `sequence` contains the `.` characters unchanged.

### Requirement: A3M Header Lines and Pseudo-Sequences
The system SHALL skip lines beginning with `#` that precede the first `>` in an A2M or A3M file, and SHALL emit reserved pseudo-sequence records (names beginning with `ss_`, `sa_` or `aa_`) as ordinary rows.

#### Scenario: A3M header marker
- **WHEN** an A3M file begins with `#A3M#` followed by further `#` lines and then `>` records
- **THEN** the `#` lines are ignored
- **AND** all `>` records are returned.

#### Scenario: Secondary-structure pseudo-sequences
- **WHEN** an A3M file contains `>ss_pred`, `>ss_conf` and `>ss_dssp` records before the query sequence
- **THEN** those three records are returned as rows with those names, in file order
- **AND** the query sequence is the fourth row.

### Requirement: Stockholm Logical Schema
The system SHALL expose Stockholm files as one row per sequence per alignment with columns `alignment_id` (Utf8, non-null), `name` (Utf8, non-null), `sequence` (LargeUtf8, non-null), `gs` (List of Struct{tag: Utf8, value: Utf8}, nullable) and `gr` (List of Struct{tag: Utf8, value: Utf8}, nullable).

#### Scenario: Alignment identifier from GF ID
- **WHEN** an alignment carries `#=GF ID 7tm_1`
- **THEN** every row of that alignment has `alignment_id` equal to `7tm_1`.

#### Scenario: Alignment identifier fallback
- **WHEN** an alignment carries no `#=GF ID` but carries `#=GF AC PF00001.27`
- **THEN** its rows have `alignment_id` equal to `PF00001.27`
- **AND** WHEN it carries neither, its rows have `alignment_id` equal to the alignment's 0-based ordinal within the file, as a string.

#### Scenario: Sequence name is verbatim
- **WHEN** a sequence line begins with `NPY1R_HUMAN/57-320`
- **THEN** `name` is `NPY1R_HUMAN/57-320` and is not split into name, start and end.

#### Scenario: Per-sequence annotations
- **WHEN** the file contains `#=GS NPY1R_HUMAN/57-320 AC P25929.1` and `#=GS NPY1R_HUMAN/57-320 DR PDB; 1abc;`
- **THEN** the `gs` value for that row is a list of two structs, `{tag: "AC", value: "P25929.1"}` then `{tag: "DR", value: "PDB; 1abc;"}`, in file order.

#### Scenario: Per-residue annotations
- **WHEN** the file contains `#=GR seqA PP` lines for `seqA`
- **THEN** the `gr` value for `seqA` contains `{tag: "PP", value: <concatenated PP string>}`
- **AND** the value length equals the length of `seqA`'s `sequence`.

#### Scenario: No annotations
- **WHEN** a sequence has no `#=GS` or `#=GR` lines
- **THEN** its `gs` and `gr` values are null.

### Requirement: Stockholm Named GS Promotion
The system SHALL accept an optional `gs_fields` list on `scan_sto`, `read_sto` and `register_sto` that promotes the named `#=GS` features to top-level nullable Utf8 columns.

#### Scenario: Promote accession
- **WHEN** a user calls `scan_sto(path, gs_fields=["AC"])`
- **THEN** the schema contains a nullable Utf8 column `AC`
- **AND** each row's `AC` is the value of that sequence's first `#=GS … AC` line, or null if absent.

#### Scenario: Keep the bag alongside promoted fields
- **WHEN** a user passes `gs_fields=["AC", "gs"]`
- **THEN** the schema contains both the promoted `AC` column and the full `gs` list column.

### Requirement: Stockholm Interleaved Blocks
The system SHALL concatenate sequence lines, `#=GR` values and `#=GC` values that appear in multiple blocks for the same name and feature, in file order, and SHALL treat `//` as the end of an alignment.

#### Scenario: Two-block alignment
- **WHEN** a Stockholm alignment is wrapped into two blocks so that each sequence name appears on two lines
- **THEN** each row's `sequence` is the concatenation of both lines
- **AND** the number of rows equals the number of distinct sequence names.

#### Scenario: Multiple alignments per file
- **WHEN** a file contains two alignments separated by `//`
- **THEN** rows from both alignments are returned
- **AND** rows are distinguishable by `alignment_id`.

#### Scenario: Missing trailing terminator
- **WHEN** the final alignment in a file is not terminated by `//` before end-of-file
- **THEN** its rows are still returned.

### Requirement: Stockholm Header Validation
The system SHALL reject as an error any Stockholm input whose first non-blank line is not `# STOCKHOLM 1.0`.

#### Scenario: Wrong header
- **WHEN** a user scans a file whose first line is `>seq1`
- **THEN** collecting the scan raises an error that names the path and states that a `# STOCKHOLM 1.0` header was expected.

### Requirement: Stockholm Alignment-Level Annotations
The system SHALL provide `describe_sto(path)` returning a Polars `DataFrame` with one row per alignment-level annotation line and columns `alignment_id` (Utf8), `kind` (Utf8, `GF` or `GC`), `feature` (Utf8), `value` (LargeUtf8), `n_sequences` (UInt32) and `alignment_length` (UInt32).

#### Scenario: Repeated GF features are preserved in order
- **WHEN** an alignment contains eight `#=GF DR` lines and ten `#=GF CC` lines
- **THEN** `describe_sto` returns eight rows with `feature` `DR` and ten rows with `feature` `CC`
- **AND** they appear in the same relative order as in the file.

#### Scenario: Column annotations
- **WHEN** an alignment contains `#=GC RF` and `#=GC seq_cons` lines
- **THEN** `describe_sto` returns rows with `kind` `GC` and `feature` `RF` and `seq_cons`
- **AND** each `value` length equals `alignment_length`.

#### Scenario: Counts per alignment
- **WHEN** an alignment has 63 sequences of 722 aligned columns
- **THEN** every row for that alignment has `n_sequences` 63 and `alignment_length` 722.

### Requirement: Compression and Object Storage
The system SHALL read A2M, A3M and Stockholm files from local paths and supported object stores, with `gz` and `bgz` compression detected from the file extension or set explicitly via `compression_type`.

#### Scenario: Gzipped Stockholm
- **WHEN** a user scans `alignment.sto.gz`
- **THEN** the rows are identical to scanning the uncompressed `alignment.sto`.

#### Scenario: Extension detection
- **WHEN** a user passes a path ending in `.stk` or `.stockholm`
- **THEN** it is read as Stockholm without an explicit format argument.

### Requirement: Projection and Predicate Pushdown
The system SHALL push down projection of any subset of the declared columns and pushdown-eligible predicates on the Utf8 columns to the DataFusion execution level, while keeping the client-side filter as the source of truth.

#### Scenario: Projection-only scan
- **WHEN** a user selects only `name` from a Stockholm scan
- **THEN** the result contains the `name` column only
- **AND** the sequence strings are not materialized.

#### Scenario: Equality predicate pushdown
- **WHEN** a user filters an A3M scan with `pl.col("name") == "sp|Q5VUD6|FA69B_HUMAN"`
- **THEN** the result contains exactly the matching records
- **AND** the result is identical with pushdown enabled and disabled.

### Requirement: Streaming and Partitioning
The system SHALL stream A2M and A3M records without buffering the whole file, and SHALL partition multi-alignment Stockholm files on `//` boundaries across DataFusion `target_partitions`, holding at most one alignment's sequences in memory per partition.

#### Scenario: Multi-alignment file is partitioned
- **WHEN** `target_partitions` is greater than 1 and a Stockholm file contains more alignments than partitions
- **THEN** the scan is executed with more than one partition
- **AND** the union of rows across partitions equals the single-partition result.

#### Scenario: Single-alignment file
- **WHEN** a Stockholm file contains exactly one alignment
- **THEN** the scan is executed with one partition
- **AND** the result is identical regardless of `target_partitions`.

### Requirement: Parity With Reference Implementations
The system SHALL be verified against independent reference implementations for every fixture: `pyhmmer` (Easel) for Stockholm structure and A2M/A3M alignment semantics, and Biopython for byte-level record parsing.

#### Scenario: Stockholm parity
- **WHEN** the Pfam PF00001 seed and the interleaved Rfam RF00001 seed are scanned
- **THEN** the row count, sequence strings and names equal those produced by `pyhmmer.easel.MSAFile(format="stockholm")`.

#### Scenario: A3M parity
- **WHEN** an A3M fixture is scanned and its reserved pseudo-sequence rows are excluded
- **THEN** the number of uppercase-or-`-` characters is identical on every row
- **AND** equals the number of match columns reported by `pyhmmer.easel.MSAFile(format="a2m")` for the same file.

#### Scenario: Byte-level parity
- **WHEN** any A2M or A3M fixture is scanned
- **THEN** `name` and `sequence` per record equal `record.id` and `str(record.seq)` from Biopython `SeqIO.parse(..., "fasta-blast")`.
