## ADDED Requirements

### Requirement: Explicit BigWig APIs
The system SHALL provide explicit `scan_bigwig` and `read_bigwig` APIs for reading BigWig files.

#### Scenario: Scan BigWig lazily
- **WHEN** a user calls `scan_bigwig` with a supported local BigWig path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no interval records are materialized until the lazy query is collected.

#### Scenario: Read BigWig eagerly
- **WHEN** a user calls `read_bigwig` with a supported local BigWig path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: Explicit BigBed APIs
The system SHALL provide explicit `scan_bigbed` and `read_bigbed` APIs for reading BigBed files.

#### Scenario: Scan BigBed lazily
- **WHEN** a user calls `scan_bigbed` with a supported local BigBed path
- **THEN** the system returns a Polars `LazyFrame`
- **AND** no interval records are materialized until the lazy query is collected.

#### Scenario: Read BigBed eagerly
- **WHEN** a user calls `read_bigbed` with a supported local BigBed path
- **THEN** the system returns a Polars `DataFrame`.

### Requirement: SQL Table Registration
The system SHALL allow BigWig and BigBed files to be registered as DataFusion SQL tables.

#### Scenario: Query registered BigWig
- **WHEN** a user registers a BigWig file with `register_bigwig`
- **AND** runs a SQL query selecting BigWig columns
- **THEN** the query returns matching rows with BigWig signal values.

#### Scenario: Query registered BigBed
- **WHEN** a user registers a BigBed file with `register_bigbed`
- **AND** runs a SQL query selecting BigBed columns
- **THEN** the query returns matching annotation rows.

### Requirement: BigWig Logical Schema
The system SHALL expose BigWig records as interval rows with signal values.

#### Scenario: BigWig core columns
- **WHEN** a user scans a BigWig file
- **THEN** the schema contains `chrom`, `start`, `end`, and `value`
- **AND** `value` is a floating-point column.

### Requirement: BigBed Logical Schema
The system SHALL expose BigBed records as interval rows with annotation fields.

#### Scenario: BigBed autoSQL schema
- **WHEN** a BigBed file contains parseable autoSQL metadata
- **THEN** the schema contains `chrom`, `start`, `end`, and supported autoSQL fields.

#### Scenario: BigBed fallback schema
- **WHEN** a BigBed file has no usable autoSQL metadata
- **THEN** the schema contains `chrom`, `start`, `end`, and `rest`
- **AND** `rest` contains the raw trailing BigBed fields.

### Requirement: Coordinate System Handling
The system SHALL emit coordinate-system metadata and support the same `use_zero_based` behavior as existing interval readers.

#### Scenario: Zero-based output
- **WHEN** a user scans BigWig or BigBed with `use_zero_based=True`
- **THEN** `start` and `end` use native 0-based half-open coordinates
- **AND** the LazyFrame metadata records zero-based coordinates.

#### Scenario: One-based output
- **WHEN** a user scans BigWig or BigBed with `use_zero_based=False`
- **THEN** `start` is converted from native 0-based to 1-based
- **AND** `end` is emitted as the closed 1-based end
- **AND** the LazyFrame metadata records one-based coordinates.

### Requirement: Projection Pushdown
The system SHALL avoid building unused Arrow columns during BigWig and BigBed scans.

#### Scenario: BigWig projected columns
- **WHEN** a lazy BigWig query selects only `chrom` and `start`
- **THEN** the resulting DataFrame contains only those columns
- **AND** the provider execution plan reports the projected columns.

#### Scenario: BigBed projected autoSQL fields
- **WHEN** a lazy BigBed query selects a subset of autoSQL fields
- **THEN** unselected autoSQL fields are not parsed into Arrow arrays.

### Requirement: Genomic Predicate Pushdown
The system SHALL use BigWig and BigBed interval indexes for supported genomic filters.

#### Scenario: Chromosome filter
- **WHEN** a lazy BigWig or BigBed query filters `chrom` by equality or `IN`
- **THEN** the provider reads only matching chromosome regions when predicate pushdown is enabled.

#### Scenario: Genomic range filter
- **WHEN** a lazy BigWig or BigBed query filters by `chrom`, `start`, and `end`
- **THEN** the provider uses interval-region queries to prune scanned records
- **AND** residual filtering preserves exact query semantics.

### Requirement: Parallel Indexed Scans
The system SHALL use multiple DataFusion partitions for BigWig and BigBed scans when useful.

#### Scenario: Parallel full scan
- **WHEN** a BigWig or BigBed scan has multiple chromosomes or splittable large chromosome regions
- **AND** DataFusion `target_partitions` is greater than one
- **THEN** the provider creates multiple partitions backed by independent bigtools readers.

#### Scenario: Boundary correctness
- **WHEN** a partition boundary splits a chromosome region
- **THEN** interval records that overlap the boundary are emitted once.

### Requirement: Storage Scope Errors
The initial implementation SHALL fail clearly for unsupported storage schemes.

#### Scenario: Unsupported object storage path
- **WHEN** a user scans a BigWig or BigBed path using an unsupported storage scheme
- **THEN** the system fails before scanning
- **AND** the error says the storage scheme is not supported for BigWig or BigBed in this version.

