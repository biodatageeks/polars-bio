## ADDED Requirements

### Requirement: Default Zero-Based Coordinate System

All genomic interval operations SHALL default to 0-based, half-open `[start, end)` coordinate system to align with Python bioinformatics ecosystem conventions. Both `start` and `end` columns SHALL be in the same coordinate system.

#### Scenario: Default coordinate system for range operations
- **WHEN** a user calls `overlap()`, `nearest()`, `count_overlaps()`, `coverage()`, or `merge()` without specifying coordinate system
- **THEN** the operation SHALL use 0-based, half-open interval semantics where `start` is inclusive and `end` is exclusive

#### Scenario: Explicit one-based coordinate system
- **WHEN** a user calls a range operation with `one_based=True`
- **THEN** the operation SHALL use 1-based, closed interval semantics where both `start` and `end` are inclusive

### Requirement: I/O Coordinate Conversion

File readers SHALL output coordinates in a consistent system. By default (when `one_based=False`), all formats SHALL output 0-based half-open `[start, end)` coordinates.

#### Scenario: 1-based format reading with default coordinates
- **WHEN** a user reads a VCF or GFF file without specifying `one_based` parameter
- **THEN** the `start` column SHALL be converted from 1-based to 0-based (start = original_start - 1)
- **AND** the `end` column SHALL remain unchanged (as it becomes the exclusive end in half-open system)

#### Scenario: 0-based format reading with default coordinates
- **WHEN** a user reads a BED or BAM file without specifying `one_based` parameter
- **THEN** the coordinates SHALL remain in their native 0-based half-open format (no conversion)

#### Scenario: Any format reading with one_based=True
- **WHEN** a user reads any file with `one_based=True`
- **THEN** the coordinates SHALL be output in 1-based closed format (current behavior)

### Requirement: Coordinate System Parameter at I/O Level Only

I/O functions SHALL provide an `one_based` optional boolean parameter to control coordinate system. Range operations SHALL NOT have this parameter - they read coordinate system from DataFrame metadata.

#### Scenario: I/O parameter not specified uses session config
- **WHEN** the `one_based` parameter is not specified (None) on an I/O function
- **THEN** the coordinate system SHALL be determined by session configuration `datafusion.bio.coordinate_system_zero_based`

#### Scenario: I/O explicit parameter overrides session config
- **WHEN** the `one_based` parameter is explicitly set to `True` or `False` on an I/O function
- **THEN** that value SHALL override the session configuration for that table

#### Scenario: Range operations use DataFrame metadata
- **WHEN** a range operation is called
- **THEN** the coordinate system SHALL be read from the input DataFrame's metadata (set at I/O time)

#### Scenario: Coordinate consistency
- **WHEN** data is read with a specific coordinate system setting
- **THEN** both `start` and `end` columns SHALL be in that same coordinate system

### Requirement: Global Configuration System

The library SHALL provide a DataFusion-style session configuration system for coordinate system defaults.

#### Scenario: Set and get configuration
- **WHEN** a user calls `pb.set_option("datafusion.bio.coordinate_system_zero_based", True)`
- **THEN** subsequent operations without explicit `one_based` parameter SHALL use 0-based coordinates

#### Scenario: Default configuration value
- **WHEN** no configuration has been set
- **THEN** `datafusion.bio.coordinate_system_zero_based` SHALL default to `False` (1-based coordinates)

#### Scenario: Switch to 0-based coordinates
- **WHEN** a user sets `pb.set_option("datafusion.bio.coordinate_system_zero_based", True)`
- **THEN** all operations SHALL use 0-based coordinates (matching Python bioinformatics ecosystem)

### Requirement: Coordinate System Mismatch Validation

The library SHALL detect and prevent operations on data with mismatched coordinate systems.

#### Scenario: Mismatch between two input tables
- **WHEN** a user performs a range operation (overlap, nearest, etc.) on two DataFrames with different coordinate systems (as tracked in metadata)
- **THEN** the library SHALL raise `CoordinateSystemMismatchError` with a descriptive message

#### Scenario: Tables with same coordinate system
- **WHEN** both input DataFrames use the same coordinate system (both 0-based or both 1-based)
- **THEN** the operation SHALL proceed normally

#### Scenario: Error message clarity
- **WHEN** a `CoordinateSystemMismatchError` is raised
- **THEN** the message SHALL indicate which inputs have which coordinate system and suggest how to resolve the mismatch

### Requirement: DataFrame Metadata Tracking (all input/output types)

The library SHALL track coordinate system metadata consistently across all supported DataFrame types.

#### Scenario: Polars DataFrame/LazyFrame metadata
- **WHEN** a Polars DataFrame or LazyFrame is created via I/O functions
- **THEN** the coordinate system SHALL be stored using [polars-config-meta](https://github.com/lmmx/polars-config-meta) via `df.config_meta.set()`

#### Scenario: Pandas DataFrame metadata
- **WHEN** a Pandas DataFrame is used as input or returned as output
- **THEN** the coordinate system SHALL be stored in `df.attrs["coordinate_system_zero_based"]`

#### Scenario: Registered DataFusion table metadata
- **WHEN** a table is registered in DataFusion (Parquet, bio formats)
- **THEN** the coordinate system SHALL be stored in Arrow schema metadata with key `bio.coordinate_system_zero_based`

#### Scenario: Metadata propagation through Polars transformations
- **WHEN** a user applies Polars transformations like `filter()`, `with_columns()`, `select()`
- **THEN** the coordinate system metadata SHALL be automatically propagated to the result (via polars-config-meta)

#### Scenario: Metadata persistence to Parquet
- **WHEN** a user saves a DataFrame with `df.config_meta.write_parquet()`
- **THEN** the coordinate system metadata SHALL be stored in the Parquet file's Arrow schema metadata

#### Scenario: Output propagation from range operations
- **WHEN** a range operation completes
- **THEN** the coordinate system from validated inputs SHALL be propagated to the output DataFrame (Polars or Pandas)

#### Scenario: Metadata used by range operations
- **WHEN** a range operation is performed with any input type (Polars, Pandas, table name)
- **THEN** the operation SHALL read coordinate system from the appropriate metadata storage
