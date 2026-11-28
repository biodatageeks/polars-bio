## ADDED Requirements

### Requirement: Register an Iceberg table from a REST catalog

polars-bio SHALL provide `pb.register_iceberg`, which registers an Iceberg
table from an Iceberg REST catalog under a session table name. The table
SHALL be readable with `pb.sql` and usable as an input to range operations.
polars-bio SHALL request vended credentials from the catalog and read the
table's data files with them.

#### Scenario: Query a registered table
- **WHEN** a user registers an Iceberg table and runs `pb.sql` against it
- **THEN** the query SHALL return the rows of the table's current snapshot
- **AND** filters SHALL be pushed down to the Iceberg scan

#### Scenario: Interval operation against an Iceberg table
- **WHEN** a registered Iceberg table with contig, start and end columns is
  passed to `pb.overlap`
- **THEN** the operation SHALL return the same result as for the same rows
  read from a Parquet file

#### Scenario: Permission denied
- **WHEN** the catalog refuses to load the table
- **THEN** registration SHALL raise a permission error
- **AND** no data file SHALL be read with ambient credentials

#### Scenario: Build without Iceberg support
- **WHEN** polars-bio was built without Iceberg support
- **THEN** `pb.register_iceberg` SHALL raise an error that says so

### Requirement: Distributed Iceberg scans

polars-bio SHALL assign the data files of a registered Iceberg table to tasks
when distributed mode is enabled, and the vended credentials SHALL reach
every task that reads them.

#### Scenario: Scan across workers
- **WHEN** a query scans a registered Iceberg table with several workers
  connected
- **THEN** each data file SHALL be read by exactly one task
- **AND** the result SHALL equal the single-node result

### Requirement: Append to an Iceberg table

polars-bio SHALL provide `pb.write_iceberg` with an append mode that writes
the rows of a query or LazyFrame to an existing Iceberg table and commits
them as one snapshot.

#### Scenario: Successful append
- **WHEN** a user appends the result of a VCF scan to an Iceberg table
- **THEN** one new snapshot SHALL be committed that contains all rows

#### Scenario: Failure before commit
- **WHEN** writing fails before the commit
- **THEN** the table's current snapshot SHALL be unchanged
- **AND** the error SHALL report any data files that were written but not
  committed

#### Scenario: Distributed append
- **WHEN** an append runs in distributed mode
- **THEN** data files SHALL be written by the tasks that produce the rows
- **AND** exactly one commit SHALL be made, by the coordinator

#### Scenario: Schema mismatch
- **WHEN** the rows to append do not match the table schema
- **THEN** the append SHALL fail before any data file is written
