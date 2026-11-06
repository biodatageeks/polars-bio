## ADDED Requirements

### Requirement: Coordinate-aware tabular readers

The library SHALL provide eager and lazy CSV and Parquet readers that record an explicitly declared source coordinate system, normalize interval column names, and tag returned Polars frames with the output coordinate system. `scan_table` and `read_table` SHALL accept optional coordinate-system declarations without changing their untagged behavior when the declaration is omitted.

#### Scenario: Read tabular intervals with a declared source system
- **WHEN** a user reads CSV or Parquet interval data with `zero_based=True` or `zero_based=False`
- **THEN** the reader SHALL normalize the selected chromosome, start, and end columns and record the coordinate system in frame metadata

#### Scenario: Convert the output coordinate system
- **WHEN** `use_zero_based` requests a system different from the declared source system
- **THEN** the reader SHALL shift `start` by one in the required direction, leave `end` unchanged, and record the requested output system

#### Scenario: Derive an end for point data
- **WHEN** a user reads point data with `end=None`
- **THEN** the reader SHALL derive `end` as `start + 1` for a 0-based source or `start` for a 1-based source

#### Scenario: Validate interval column types
- **WHEN** a selected start or end column is not an integer type no wider than 64 bits
- **THEN** the reader SHALL raise an actionable `TypeError` before tagging the frame

#### Scenario: Prevent coordinate shift overflow
- **WHEN** converting a coordinate would exceed the source integer type's range
- **THEN** the reader SHALL raise an error instead of wrapping the coordinate

### Requirement: Safe coordinate resolution for range operations

Range operations SHALL use recorded coordinate metadata when available and SHALL reject inputs whose coordinate relationship cannot be established safely.

#### Scenario: Matching tagged inputs
- **WHEN** both inputs carry the same coordinate-system metadata
- **THEN** the range operation SHALL proceed using that system

#### Scenario: Mismatched tagged inputs
- **WHEN** both inputs are tagged with different coordinate systems
- **THEN** the range operation SHALL raise `CoordinateSystemMismatchError`

#### Scenario: A tagged input paired with an unknown input
- **WHEN** one input has coordinate metadata and the other is untagged
- **THEN** the range operation SHALL raise `MissingCoordinateSystemError` in both strict and lenient modes

#### Scenario: Both inputs are unknown
- **WHEN** neither input has coordinate metadata and strict checking is enabled
- **THEN** the range operation SHALL raise `MissingCoordinateSystemError`
- **WHEN** neither input has coordinate metadata and lenient checking is enabled
- **THEN** the operation SHALL use the configured global coordinate system and emit a warning

#### Scenario: Native VCF or BCF input
- **WHEN** a range operation reads a VCF or BCF path
- **THEN** coordinate resolution SHALL use the coordinate system applied by that native reader and its read options

#### Scenario: Unsupported input format
- **WHEN** a range operation receives an unsupported file format
- **THEN** it SHALL raise an actionable format error before coordinate-system validation
