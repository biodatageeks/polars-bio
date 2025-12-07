# Implementation Tasks

## 1. datafusion-bio-formats Changes (separate PR)

**Local repo path**: `/Users/mwiewior/CLionProjects/datafusion-bio-formats`
**PR**: https://github.com/biodatageeks/datafusion-bio-formats/pull/36

- [x] 1.1 Add `coordinate_system_zero_based: bool` parameter to `VcfTableProvider::new()` with default `true`
- [x] 1.2 Add `coordinate_system_zero_based: bool` parameter to `GffTableProvider::new()` and `BgzfParallelGffTableProvider::try_new()` with default `true`
- [x] 1.3 Add `coordinate_system_zero_based: bool` parameter to `BamTableProvider::new()` with default `true`
- [x] 1.4 Add `coordinate_system_zero_based: bool` parameter to `CramTableProvider::new()` with default `true`
- [x] 1.5 Add `coordinate_system_zero_based: bool` parameter to `BedTableProvider::new()` with default `true`
- [x] 1.6 Implement coordinate conversion at parse time:
  - Since noodles normalizes ALL formats to 1-based, the logic is the same for all:
  - When `coordinate_system_zero_based=true`: `start = noodles_position.get() - 1`
  - `end` unchanged (1-based closed end = 0-based half-open exclusive end)
  - Apply to: `start`, `mate_start` columns where applicable
- [x] 1.7 Add Arrow schema metadata to TableProvider schemas:
  - Use `Schema::new_with_metadata()` to include `bio.coordinate_system_zero_based` key
  - Value should be `"true"` or `"false"` string
- [x] 1.8 Add unit tests for coordinate conversion in each format:
  - [x] 1.8.1 VCF format tests:
    - Test `coordinate_system_zero_based=true` (default) outputs 0-based coordinates (start - 1)
    - Test `coordinate_system_zero_based=false` outputs 1-based coordinates (unchanged)
    - Verify schema metadata contains correct `bio.coordinate_system_zero_based` value
  - [x] 1.8.2 GFF/GTF format tests:
    - Test `coordinate_system_zero_based=true` (default) outputs 0-based coordinates (start - 1)
    - Test `coordinate_system_zero_based=false` outputs 1-based coordinates (unchanged)
    - Verify schema metadata contains correct `bio.coordinate_system_zero_based` value
  - [x] 1.8.3 BAM format tests:
    - Test `coordinate_system_zero_based=true` (default) outputs 0-based coordinates (start - 1, mate_start - 1)
    - Test `coordinate_system_zero_based=false` outputs 1-based coordinates (unchanged)
    - Verify schema metadata contains correct `bio.coordinate_system_zero_based` value
  - [x] 1.8.4 CRAM format tests:
    - Test `coordinate_system_zero_based=true` (default) outputs 0-based coordinates (start - 1, mate_start - 1)
    - Test `coordinate_system_zero_based=false` outputs 1-based coordinates (unchanged)
    - Verify schema metadata contains correct `bio.coordinate_system_zero_based` value
  - [x] 1.8.5 BED format tests:
    - Test `coordinate_system_zero_based=true` (default) outputs 0-based coordinates (start - 1)
    - Test `coordinate_system_zero_based=false` outputs 1-based coordinates (unchanged)
    - Verify schema metadata contains correct `bio.coordinate_system_zero_based` value
- [ ] 1.9 Tag new release of datafusion-bio-formats

## 2. polars-bio Rust Layer (src/)

- [x] 2.1 Update Cargo.toml to use new datafusion-bio-formats revision
- [x] 2.2 Add `zero_based: Option<bool>` to `VcfReadOptions`, `GffReadOptions`, `BamReadOptions`, `CramReadOptions`, `BedReadOptions`
- [x] 2.3 Update `register_table()` in `scan.rs` to pass `zero_based` parameter to all TableProviders
- [x] 2.4 Update `ReadOptions` struct in `option.rs`

## 3. Global Configuration System (polars_bio/context.py)

- [x] 3.1 ~~Create `polars_bio/config.py` with session-level configuration~~ → Refactored to use DataFusion context in `context.py`
- [x] 3.2 Implement `set_option(key, value)` and `get_option(key)` functions in `context.py`
- [x] 3.3 Add `datafusion.bio.coordinate_system_zero_based` option with default `"true"` (stored in BioSessionContext)
- [x] 3.4 Export `get_option`, `set_option`, and `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` constant in `polars_bio/__init__.py`
- [x] 3.5 Add `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` constant in `constants.py` for consistent key naming
- [x] 3.6 Move `_resolve_zero_based()` helper to `context.py` (priority: explicit param > context config > default)

## 4. DataFrame Metadata Tracking (unified across all types)

The coordinate system is set at I/O time and stored as DataFrame metadata. Range operations read this metadata instead of requiring an explicit `use_zero_based` parameter.

**Input types for range operations and their metadata storage:**

| Input Type | Metadata Storage | Notes |
|------------|------------------|-------|
| Polars LazyFrame/DataFrame | `polars-config-meta` | Set by `scan_*`/`read_*` functions |
| String (file path) | Arrow schema metadata | Set when table is registered in DataFusion |
| Pandas DataFrame | `df.attrs["coordinate_system_zero_based"]` | Must be set by user before passing to range ops |

- [ ] 4.1 Add `polars-config-meta` to dependencies in `pyproject.toml`
- [ ] 4.2 Import and initialize `polars-config-meta` in `polars_bio/__init__.py`
- [ ] 4.3 Create exception classes in `polars_bio/exceptions.py`:
  - `CoordinateSystemMismatchError` - raised when mixing 0-based and 1-based DataFrames
  - `MissingCoordinateSystemError` - raised when any input lacks coordinate system metadata
- [ ] 4.4 Create unified metadata abstraction in `polars_bio/_metadata.py`:
  - `set_coordinate_system(df, zero_based: bool)` - set metadata on DataFrame
  - `get_coordinate_system(df) -> Optional[bool]` - read metadata from DataFrame
  - `validate_coordinate_systems(df1, df2)` - raise `CoordinateSystemMismatchError` if mismatch
  - Raise `MissingCoordinateSystemError` when metadata is missing for any input type:
    - Polars LazyFrame/DataFrame without `polars-config-meta` metadata
    - Pandas DataFrame without `df.attrs["coordinate_system_zero_based"]`
    - DataFusion table (file path) without `bio.coordinate_system_zero_based` Arrow schema metadata
- [ ] 4.5 Update I/O functions (`scan_*`, `read_*`) to set coordinate system metadata on returned DataFrames

## 5. Python API Changes - Range Operations (polars_bio/range_op.py)

Remove explicit `use_zero_based` parameter from range operations. Instead, read coordinate system from DataFrame metadata set at I/O time.

- [ ] 5.1 Remove `use_zero_based` parameter from `overlap()`
- [ ] 5.2 Remove `use_zero_based` parameter from `nearest()`
- [ ] 5.3 Remove `use_zero_based` parameter from `count_overlaps()`
- [ ] 5.4 Remove `use_zero_based` parameter from `coverage()`
- [ ] 5.5 Remove `use_zero_based` parameter from `merge()`
- [ ] 5.6 Add `_get_filter_op_from_metadata(df1, df2)` helper function:
  - Read coordinate system from both DataFrames' metadata
  - Raise `MissingCoordinateSystemError` if either input lacks metadata
  - Call `validate_coordinate_systems(df1, df2)` to check for mismatch
  - Return `FilterOp.Strict` if 0-based, `FilterOp.Weak` if 1-based
- [ ] 5.7 Update all range operations to use `_get_filter_op_from_metadata()` instead of `use_zero_based` parameter
- [ ] 5.8 Update docstrings to explain metadata-based coordinate system detection

## 6. Python API Changes - I/O Layer (polars_bio/io.py, sql.py)

- [x] 6.1 Add `one_based` parameter to `scan_vcf()` / `read_vcf()` with default `None` (uses session config)
- [x] 6.2 Add `one_based` parameter to `scan_gff()` / `read_gff()` with default `None`
- [x] 6.3 Add `one_based` parameter to `scan_bam()` / `read_bam()` with default `None`
- [x] 6.4 Add `one_based` parameter to `scan_cram()` / `read_cram()` with default `None`
- [x] 6.5 Add `one_based` parameter to `scan_bed()` / `read_bed()` with default `None`
- [x] 6.6 Resolve effective `zero_based` value: explicit param > session config > default (via `_resolve_zero_based()`)
- [x] 6.7 Pass resolved `zero_based` value through to Rust layer
- [ ] 6.8 Set coordinate system metadata on returned LazyFrames/DataFrames:
  - Use `polars-config-meta` for Polars DataFrames/LazyFrames (I/O functions only return Polars types)
  - Metadata key: `coordinate_system_zero_based` (bool)
  - Note: Pandas `df.attrs` only applies to range operation outputs when `output_type="pandas"`
- [x] 6.9 Update docstrings to document new coordinate behavior and config system

## 7. Test Updates

- [x] 7.6 Update `test_io.py` - verify coordinate values from I/O (updated to use 0-based values)
- [x] 7.8 Update tests verifying coordinate values match expected 0-based output
  - Updated `test_vcf_parsing.py` with 0-based expected values
  - Updated `test_filter_select_attributes_bug_fix.py` with 0-based filter values
- [ ] 7.1 Add tests for DataFrame metadata tracking:
  - Test that `scan_*`/`read_*` functions set `coordinate_system_zero_based` metadata
  - Test that metadata is preserved through Polars transformations
  - Test that metadata is accessible via `get_coordinate_system()`
- [ ] 7.2 Add tests for coordinate system mismatch detection:
  - Test `CoordinateSystemMismatchError` is raised when mixing 0-based and 1-based DataFrames
  - Test that matching coordinate systems work correctly
- [ ] 7.3 Add tests for `MissingCoordinateSystemError`:
  - Test error raised when Polars LazyFrame/DataFrame lacks metadata
  - Test error raised when Pandas DataFrame lacks `df.attrs["coordinate_system_zero_based"]`
  - Test error raised when file path registers table without Arrow schema metadata
  - Test that error message explains how to set metadata for each input type
  - Test that all input types with proper metadata work correctly
- [ ] 7.4 Add tests for range operations without `use_zero_based` parameter:
  - Test `overlap()` reads coordinate system from metadata
  - Test `nearest()` reads coordinate system from metadata
  - Test `count_overlaps()` reads coordinate system from metadata
  - Test `coverage()` reads coordinate system from metadata
  - Test `merge()` reads coordinate system from metadata
- [ ] 7.5 Update existing range operation tests to remove `use_zero_based` parameter

## 8. Documentation

- [x] 8.2 Update docstrings in `io.py` (updated all scan/read functions with coordinate system docs)
- [ ] 8.1 Update docstrings in `range_op.py`:
  - Remove `use_zero_based` parameter documentation
  - Add explanation that coordinate system is read from DataFrame metadata
  - Document that `MissingCoordinateSystemError` is raised if metadata is missing
  - Document how to set metadata for each input type (Polars, Pandas, file paths)
- [ ] 8.3 Update tutorials to reflect 0-based default
- [ ] 8.4 Add migration guide for users upgrading from previous versions:
  - Explain removal of `use_zero_based` parameter
  - Explain new metadata-based approach
  - Show how to set metadata for each input type before passing to range ops
- [ ] 8.5 Document global configuration system with examples (affects I/O functions only)
- [ ] 8.6 Document DataFrame metadata tracking:
  - How coordinate system is set at I/O time (Polars via `scan_*`/`read_*`)
  - How range operations read from metadata
  - How mismatch detection works
  - `MissingCoordinateSystemError` - when and why it's raised
  - Example code for setting metadata on each input type:
    - Polars: `lf.config_meta.set(coordinate_system_zero_based=True)`
    - Pandas: `df.attrs["coordinate_system_zero_based"] = True`
    - File paths: Use `pb.scan_*()` to ensure Arrow metadata is set
- [ ] 8.7 Update `openspec/project.md` domain context section

## 9. Validation

- [x] 9.1 Run full test suite (223 passed, 2 skipped)
- [x] 9.3 Build and test package installation (maturin develop succeeded)
- [ ] 9.2 Verify bioframe compatibility tests pass - deferred
- [ ] 9.4 Test with real VCF/GFF/BAM files to verify coordinate values - deferred
