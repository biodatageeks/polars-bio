# Change: Switch default to 0-based coordinate system

GitHub Issue: https://github.com/biodatageeks/polars-bio/issues/259

## Why

The current default of 1-based coordinates creates friction for users because:

1. **Python ecosystem alignment**: Virtually the entire Python bioinformatics ecosystem (pyranges, bioframe, pybedtools) uses 0-based, half-open `[start, end)` coordinates
2. **Format inconsistency**: While VCF/GTF use 1-based positions, BED (the most common interval format) uses 0-based
3. **User expectation mismatch**: Users coming from other Python tools expect 0-based by default
4. **Current API burden**: Users must pass `use_zero_based=True` to every operation when working with 0-based data

## Architecture Analysis

### Current State

The datafusion-bio-formats crate (also maintained by biodatageeks) **normalizes all formats to 1-based**:

| Format | Native Coords | datafusion-bio-formats Output |
|--------|---------------|-------------------------------|
| VCF    | 1-based       | 1-based (unchanged)           |
| GFF/GTF| 1-based       | 1-based (unchanged)           |
| BAM    | 0-based       | 1-based (converted)           |
| BED    | 0-based       | 1-based (converted)           |

### Implementation Options

| Option | Layer | Pros | Cons |
|--------|-------|------|------|
| **A: datafusion-bio-formats** | External Rust crate | Best performance, conversion at parse time, no Arrow patching | Requires coordinated change in separate repo |
| **B: Rust binding (scan.rs)** | polars-bio Rust | Self-contained | Requires wrapping/patching Arrow tables |
| **C: Python API (io.py)** | polars-bio Python | Easy to implement | Slowest, conversion after data fetch |

### Recommendation: Option A (datafusion-bio-formats)

Add `coordinate_system_zero_based: bool` parameter (default `true`) to TableProvider constructors in datafusion-bio-formats:

```rust
// In datafusion-bio-formats vcf/src/table_provider.rs
pub fn new(
    path: String,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    thread_num: Option<usize>,
    object_storage_options: Option<HashMap<String, String>>,
    coordinate_system_zero_based: bool,  // NEW: defaults to true, if true subtract 1 from start coordinates
) -> Result<Self>
```

**Why this is better than wrapping in polars-bio:**
- Conversion happens at parse time, not as a post-processing step
- No need to wrap TableProviders or patch Arrow batches
- Single source of truth for coordinate handling
- Both repos are maintained by biodatageeks
- Cleaner architecture

## What Changes

### datafusion-bio-formats (separate PR)

**Local repo path**: `/Users/mwiewior/CLionProjects/datafusion-bio-formats`

- Add `coordinate_system_zero_based: bool` parameter to all TableProvider constructors with default `true`
- When `coordinate_system_zero_based=true` (default), convert coordinates: subtract 1 from `start` column during parsing
- Both `start` and `end` columns remain in the same coordinate system (0-based half-open `[start, end)`)
- Affects: VcfTableProvider, GffTableProvider, BamTableProvider, CramTableProvider, BedTableProvider

### polars-bio
- **BREAKING**: Change default coordinate system from 1-based to 0-based
- Add global configuration system (DataFusion-style):
  ```python
  pb.set_option("datafusion.bio.coordinate_system_zero_based", True)  # default
  pb.get_option("datafusion.bio.coordinate_system_zero_based")
  ```
- Add `one_based` parameter to I/O functions only (not range operations)
- Parameter default is `None` (uses session config); explicit value overrides
- Resolution order: explicit I/O param > session config > built-in default (0-based)
- **Remove** `use_zero_based` parameter from range operations - coordinate system is tracked on DataFrame metadata
- Track coordinate system metadata consistently across all input types:
  - **Polars LazyFrame/DataFrame**: use [polars-config-meta](https://github.com/lmmx/polars-config-meta) (auto-propagation through transforms)
  - **Pandas DataFrame**: use built-in `df.attrs` dictionary
  - **Registered DataFusion tables**: use internal registry keyed by table name
- Propagate coordinate system to output DataFrames (Polars or Pandas)
- Validate coordinate system match between inputs in range operations
- Raise `CoordinateSystemMismatchError` if inputs have different coordinate systems
- Update documentation and tutorials to reflect 0-based default
- Remove the warning that currently fires when `use_zero_based=True`

## Impact

- Affected repos: datafusion-bio-formats, polars-bio
- Affected code:
  - `datafusion-bio-formats/*/src/table_provider.rs` - Add zero_based parameter
  - `polars_bio/range_op.py` - All range operation functions
  - `polars_bio/sql.py` - I/O functions
  - `polars_bio/io.py` - I/O wrappers
  - `src/scan.rs` - Pass new parameter to TableProviders
  - `src/option.rs` - Add parameter to ReadOptions structs
  - `tests/` - Many test files need parameter updates
- **Breaking change**: Existing code using default coordinates will produce different results
- **Migration**: Users who need 1-based behavior must add `one_based=True` parameter
