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
- [ ] 3.3 Add `datafusion.bio.coordinate_system_zero_based` option with default `"false"` (1-based by default)
- [x] 3.4 Export `get_option`, `set_option`, and `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` constant in `polars_bio/__init__.py`
- [x] 3.5 Add `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` constant in `constants.py` for consistent key naming
- [ ] 3.6 Update `_resolve_zero_based()` helper to use `use_zero_based` parameter with default `False`

## 4. DataFrame Metadata Tracking (unified across all types)

The coordinate system is set at I/O time and stored as DataFrame metadata. Range operations read this metadata instead of requiring an explicit `use_zero_based` parameter.

**Input types for range operations and their metadata storage:**

| Input Type | Metadata Storage | Notes |
|------------|------------------|-------|
| Polars LazyFrame/DataFrame | `polars-config-meta` | Set by `scan_*`/`read_*` functions |
| String (file path) | Arrow schema metadata | Set when table is registered in DataFusion |
| Pandas DataFrame | `df.attrs["coordinate_system_zero_based"]` | Must be set by user before passing to range ops |

- [x] 4.1 Add `polars-config-meta` to dependencies in `pyproject.toml`
- [x] 4.2 Import and initialize `polars-config-meta` in `polars_bio/__init__.py`
- [x] 4.3 Create exception classes in `polars_bio/exceptions.py`:
  - `CoordinateSystemMismatchError` - raised when mixing 0-based and 1-based DataFrames
  - `MissingCoordinateSystemError` - raised when any input lacks coordinate system metadata
- [x] 4.4 Create unified metadata abstraction in `polars_bio/_metadata.py`:
  - `set_coordinate_system(df, zero_based: bool)` - set metadata on DataFrame
  - `get_coordinate_system(df) -> Optional[bool]` - read metadata from DataFrame
  - `validate_coordinate_systems(df1, df2)` - raise `CoordinateSystemMismatchError` if mismatch
  - Raise `MissingCoordinateSystemError` when metadata is missing for any input type:
    - Polars LazyFrame/DataFrame without `polars-config-meta` metadata
    - Pandas DataFrame without `df.attrs["coordinate_system_zero_based"]`
    - DataFusion table (file path) without `bio.coordinate_system_zero_based` Arrow schema metadata
- [x] 4.5 Update I/O functions (`scan_*`, `read_*`) to set coordinate system metadata on returned DataFrames

## 5. Python API Changes - Range Operations (polars_bio/range_op.py)

Remove explicit `use_zero_based` parameter from range operations. Instead, read coordinate system from DataFrame metadata set at I/O time.

- [x] 5.1 Remove `use_zero_based` parameter from `overlap()`
- [x] 5.2 Remove `use_zero_based` parameter from `nearest()`
- [x] 5.3 Remove `use_zero_based` parameter from `count_overlaps()`
- [x] 5.4 Remove `use_zero_based` parameter from `coverage()`
- [x] 5.5 Remove `use_zero_based` parameter from `merge()`
- [x] 5.6 Add `_get_filter_op_from_metadata(df1, df2)` helper function:
  - Read coordinate system from both DataFrames' metadata
  - Raise `MissingCoordinateSystemError` if either input lacks metadata
  - Call `validate_coordinate_systems(df1, df2)` to check for mismatch
  - Return `FilterOp.Strict` if 0-based, `FilterOp.Weak` if 1-based
- [x] 5.7 Update all range operations to use `_get_filter_op_from_metadata()` instead of `use_zero_based` parameter
- [x] 5.8 Update docstrings to explain metadata-based coordinate system detection

### 5.9 Fallback mode for DataFrames without coordinate system metadata

Add `datafusion.bio.coordinate_system_check` session parameter to allow processing DataFrames without explicit coordinate system metadata by using the global `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` configuration.

- [x] 5.9.1 Add `POLARS_BIO_COORDINATE_SYSTEM_CHECK` constant to `constants.py`:
  - Key: `"datafusion.bio.coordinate_system_check"`
  - Default value: `"true"` (strict check enabled)
- [x] 5.9.2 Initialize session parameter in `context.py`:
  - Set default to `"true"` in `Context.__init__()`
- [x] 5.9.3 Update `_metadata.py` to read from session parameter:
  - Add `_get_coordinate_system_check()` helper function
  - When `"true"` (default): Raise `MissingCoordinateSystemError` if metadata is missing
  - When `"false"`: Fall back to `POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED` global config and emit a warning
- [x] 5.9.4 Update `validate_coordinate_systems()` and `validate_coordinate_system_single()`:
  - Read check setting from session config instead of function parameter
- [x] 5.9.5 Export constant from `__init__.py`:
  - Add `POLARS_BIO_COORDINATE_SYSTEM_CHECK` to exports
- [x] 5.9.6 Add unit tests:
  - Test that `coordinate_system_check="true"` (default) raises `MissingCoordinateSystemError`
  - Test that `coordinate_system_check="false"` uses global config when metadata is missing
  - Test that warning is emitted when falling back to global config
  - Test all range operations work with session parameter

## 6. Python API Changes - I/O Layer (polars_bio/io.py, sql.py)

- [ ] 6.1 Rename `one_based` to `use_zero_based` in `scan_vcf()` / `read_vcf()` with default `False` (1-based by default)
- [ ] 6.2 Rename `one_based` to `use_zero_based` in `scan_gff()` / `read_gff()` with default `False`
- [ ] 6.3 Rename `one_based` to `use_zero_based` in `scan_bam()` / `read_bam()` with default `False`
- [ ] 6.4 Rename `one_based` to `use_zero_based` in `scan_cram()` / `read_cram()` with default `False`
- [ ] 6.5 Rename `one_based` to `use_zero_based` in `scan_bed()` / `read_bed()` with default `False`
- [ ] 6.6 Update `_resolve_zero_based()` to handle `use_zero_based` with default `False`
- [x] 6.7 Pass resolved `zero_based` value through to Rust layer
- [x] 6.8 Set coordinate system metadata on returned LazyFrames/DataFrames:
  - Use `polars-config-meta` for Polars DataFrames/LazyFrames (I/O functions only return Polars types)
  - Metadata key: `coordinate_system_zero_based` (bool)
  - Note: Pandas `df.attrs` only applies to range operation outputs when `output_type="pandas"`
- [ ] 6.9 Update docstrings to document 1-based default and `use_zero_based` parameter

## 7. Test Updates

- [ ] 7.6 Update `test_io.py` - verify coordinate values from I/O (update to use 1-based values by default)
- [ ] 7.8 Update tests verifying coordinate values match expected 1-based output (default)
  - Update `test_vcf_parsing.py` with 1-based expected values
  - Update `test_filter_select_attributes_bug_fix.py` with 1-based filter values
- [ ] 7.1 Update tests for DataFrame metadata tracking:
  - Test that `scan_*`/`read_*` functions set `coordinate_system_zero_based=False` by default (1-based)
  - Test that `use_zero_based=True` sets `coordinate_system_zero_based=True`
  - Test that metadata is preserved through Polars transformations
  - Test that metadata is accessible via `get_coordinate_system()`
- [x] 7.2 Add tests for coordinate system mismatch detection:
  - Test `CoordinateSystemMismatchError` is raised when mixing 0-based and 1-based DataFrames
  - Test that matching coordinate systems work correctly
- [x] 7.3 Add tests for `MissingCoordinateSystemError`:
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

- [ ] 8.1 **Update `docs/features.md` - Coordinate systems support section**:
  - Add Mermaid diagram showing the coordinate system flow:
    - How I/O functions set metadata on DataFrames
    - How range operations read metadata
    - How session parameters control behavior
  - Document the two session parameters:
    - `datafusion.bio.coordinate_system_zero_based`: Default coordinate system (default: "false" = 1-based)
    - `datafusion.bio.coordinate_system_check`: Metadata validation behavior (default: "true" = strict)
  - Explain the metadata-based approach:
    - Polars DataFrames: `df.config_meta.set(coordinate_system_zero_based=True/False)`
    - Pandas DataFrames: `df.attrs["coordinate_system_zero_based"] = True/False`
    - File paths: Use `pb.scan_*()` / `pb.read_*()` functions (auto-sets metadata)
  - Add code examples:
    - Using polars-bio I/O functions (recommended)
    - Setting metadata manually on Polars/Pandas DataFrames
    - Using fallback mode with `datafusion.bio.coordinate_system_check = "false"`
  - Document error handling:
    - `MissingCoordinateSystemError` - when raised and how to fix
    - `CoordinateSystemMismatchError` - when raised and how to fix
  - Add migration guide for users upgrading from previous versions
- [ ] 8.2 Update docstrings in `io.py` to document `use_zero_based` parameter (default `False` = 1-based)
- [ ] 8.3 Update docstrings in `range_op.py`:
  - Remove `use_zero_based` parameter documentation
  - Add explanation that coordinate system is read from DataFrame metadata
  - Document that `MissingCoordinateSystemError` is raised if metadata is missing
  - Document how to set metadata for each input type (Polars, Pandas, file paths)
- [ ] 8.4 Update tutorials to reflect 1-based default
- [ ] 8.5 Add migration guide for users upgrading from previous versions:
  - Explain removal of `use_zero_based` parameter from range operations
  - Explain rename of `one_based` to `use_zero_based` in I/O functions
  - Explain new metadata-based approach
  - Show how to set metadata for each input type before passing to range ops
- [ ] 8.6 Document global configuration system with examples (affects I/O functions only)
- [ ] 8.7 Document DataFrame metadata tracking:
  - How coordinate system is set at I/O time (Polars via `scan_*`/`read_*`)
  - How range operations read from metadata
  - How mismatch detection works
  - `MissingCoordinateSystemError` - when and why it's raised
  - Example code for setting metadata on each input type:
    - Polars: `lf.config_meta.set(coordinate_system_zero_based=True)`
    - Pandas: `df.attrs["coordinate_system_zero_based"] = True`
    - File paths: Use `pb.scan_*()` to ensure Arrow metadata is set
- [ ] 8.8 Update `openspec/project.md` domain context section

## 9. Validation

- [x] 9.1 Run full test suite (223 passed, 2 skipped)
- [x] 9.3 Build and test package installation (maturin develop succeeded)
- [ ] 9.2 Verify bioframe compatibility tests pass - deferred
- [ ] 9.4 Test with real VCF/GFF/BAM files to verify coordinate values - deferred
