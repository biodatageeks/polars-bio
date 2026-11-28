## ADDED Requirements

### Requirement: Optional Lakekeeper connection

polars-bio SHALL provide `pb.catalog.lakekeeper(...)`, which connects to a
Lakekeeper warehouse with a caller-supplied authentication strategy. The
client library SHALL be an optional dependency.

#### Scenario: Extra not installed
- **WHEN** a user calls `pb.catalog.lakekeeper(...)` without the optional
  client library installed
- **THEN** an error SHALL name the `polars-bio[lakekeeper]` extra

#### Scenario: Import without the extra
- **WHEN** a user imports `polars_bio` without the optional client library
- **THEN** the import SHALL succeed and SHALL NOT import the client library

### Requirement: Generic table resolution

A catalog connection SHALL resolve a generic table identifier to its base
location, its format and storage credentials vended by the catalog. The
credentials SHALL be passed to the readers as per-scan credentials with a
provider that reloads the table before expiry (see `storage-credentials`).

#### Scenario: Load with vended credentials
- **WHEN** a user with the read-data permission resolves a generic table
- **THEN** the result SHALL contain the table location and credentials scoped
  to that location

#### Scenario: Credentials expire during a scan
- **WHEN** a scan of a governed file outlives the vended credentials
- **THEN** polars-bio SHALL reload the table from the catalog and continue
  with the new credentials

### Requirement: No fallback to ambient credentials

For a catalog-governed location, polars-bio SHALL use only credentials that
the catalog returned, unless the caller explicitly requested client-managed
access.

#### Scenario: Permission denied
- **WHEN** the catalog refuses to load the table or returns no credentials
- **THEN** the call SHALL raise a permission error
- **AND** no request SHALL be sent to the storage location with credentials
  from the process environment

#### Scenario: Client-managed access
- **WHEN** the caller requests client-managed access
- **THEN** polars-bio SHALL resolve the location from the catalog and read it
  with the caller's own credentials

### Requirement: File listing

A catalog connection SHALL list the files under a generic table's base
location using the vended credentials, on every supported storage backend.

#### Scenario: List a dataset
- **WHEN** a user calls `Catalog.files("namespace.table")`
- **THEN** the paths of the files under the table's base location SHALL be
  returned

### Requirement: Scan governed files

`Catalog.scan` SHALL return a LazyFrame for one file, or for a set of files
selected by a pattern, under a generic table. The reader SHALL be chosen from
an explicit `format` argument, then from the table's format when it names a
supported reader, then from the file extension.

#### Scenario: Single file
- **WHEN** a user calls `Catalog.scan("genomics.cohort", path="NA12878.bam")`
- **THEN** a LazyFrame SHALL be returned that reads that file with the BAM
  reader and the vended credentials
- **AND** predicate and projection pushdown SHALL behave as for `pb.scan_bam`

#### Scenario: Several files as one LazyFrame
- **WHEN** a user calls `Catalog.scan("genomics.cohort", glob="*.vcf.gz")`
- **THEN** one LazyFrame SHALL be returned that contains the rows of every
  matching file

#### Scenario: Schema mismatch across files
- **WHEN** the selected files do not have the same schema
- **THEN** an error SHALL name the first file and column that differ

#### Scenario: Unknown format
- **WHEN** no reader can be chosen from the argument, the table format or the
  file extension
- **THEN** an error SHALL list the supported formats
