## Context

polars-bio currently defaults to 1-based coordinates for genomic interval operations. This was chosen because the datafusion-bio-formats crate normalizes all input formats to 1-based. However, the Python bioinformatics ecosystem has standardized on 0-based, half-open intervals `[start, end)`, and users have requested alignment with this convention.

### Current Architecture

```
File (native)  →  datafusion-bio-formats  →  scan.rs  →  Python API  →  User
                  (normalizes to 1-based)    (passes    (documents
                                             through)   1-based)
```

**Key discovery**: The external `datafusion-bio-formats` crate already converts all formats to 1-based:
- VCF: 1-based → 1-based (unchanged)
- GFF/GTF: 1-based → 1-based (unchanged)
- BAM: 0-based (spec) → 1-based (converted)
- BED: 0-based (spec) → 1-based (converted)

### Stakeholders
- End users migrating from pyranges, bioframe, pybedtools
- Existing polars-bio users (breaking change)
- biodatageeks maintainers (both repos)

### Constraints
- Must maintain correctness for all coordinate systems
- Performance must not degrade
- Clear migration path for existing users
- Both repos maintained by same organization

## Goals / Non-Goals

### Goals
- Keep 1-based coordinates as the default (consistent with VCF/GFF native formats)
- Provide clear `use_zero_based` parameter for users who need 0-based behavior
- Implement conversion at parse time in datafusion-bio-formats (cleanest approach)
- Maintain full support for 0-based workflows via explicit parameter
- Support automatic metadata tracking so range operations can detect coordinate system

### Non-Goals
- Removing 1-based support entirely
- Changing internal algorithm implementations

## Decisions

### Decision 1: Implement conversion in datafusion-bio-formats

**Rationale**: This is the cleanest approach because:
1. Conversion happens at parse time, not as post-processing
2. No need to wrap TableProviders or patch Arrow batches in polars-bio
3. Single source of truth for coordinate handling
4. Both repos are maintained by biodatageeks

**Implementation in datafusion-bio-formats**:

```rust
// Add parameter to all TableProvider::new() functions with default true
pub fn new(
    path: String,
    // ... existing params ...
    coordinate_system_zero_based: bool,  // NEW - defaults to true (0-based coordinates)
) -> Result<Self> {
    // During record iteration:
    let start = if coordinate_system_zero_based { record.start - 1 } else { record.start };
}

// Example with default:
impl Default for VcfTableProviderConfig {
    fn default() -> Self {
        Self {
            // ... other fields ...
            coordinate_system_zero_based: true,  // Default to 0-based coordinates
        }
    }
}
```

**Affected TableProviders**:
- `VcfTableProvider` - convert `start` (pos)
- `GffTableProvider` - convert `start`
- `BamTableProvider` - convert `start`, `mate_start`
- `CramTableProvider` - convert `start`, `mate_start`
- `BedTableProvider` - keep as 0-based when `zero_based=true` (don't add 1)

**Alternatives considered**:
- **Wrap TableProviders in polars-bio**: Requires patching Arrow batches after creation, more complex
- **Python layer conversion**: Slowest option, conversion after data fetch

### Decision 2: Use `use_zero_based` parameter with default `False`

**Rationale**: Keep 1-based as default since VCF and GFF formats are natively 1-based. Users who want 0-based coordinates (e.g., for bioframe compatibility) can explicitly set `use_zero_based=True`.

### Decision 3: FilterOp mapping

Mapping (unchanged from current behavior):
- `use_zero_based=False` → `FilterOp.Weak` (1-based, closed intervals) - **DEFAULT**
- `use_zero_based=True` → `FilterOp.Strict` (0-based, half-open intervals)

### Decision 4: Coordinate columns to convert per format

**Key discovery**: The [noodles](https://github.com/zaeleus/noodles) library (used by datafusion-bio-formats) normalizes ALL positions to 1-based internally via its `Position` type:
- `Position` is explicitly "a 1-based position" ([docs](https://docs.rs/noodles-core/latest/noodles_core/position/struct.Position.html))
- `Position::MIN.get()` returns `1`, and `Position::new(0)` returns `None`
- When reading BED/BAM (0-based in file), noodles automatically converts to 1-based
- When reading VCF/GFF (1-based in file), noodles keeps as 1-based

**Current state in datafusion-bio-formats**: All formats output 1-based coordinates because noodles does the normalization.

**Key principle**: Both `start` and `end` must be in the same coordinate system. For 0-based half-open `[start, end)`:
- `start` is inclusive (first position in the interval)
- `end` is exclusive (first position after the interval)

| Format | File System | noodles Output | Conversion when `zero_based=true` |
|--------|-------------|----------------|-----------------------------------|
| VCF | 1-based closed | 1-based | `start = start - 1` |
| GFF/GTF | 1-based closed | 1-based | `start = start - 1` |
| BAM | 0-based half-open | 1-based (converted by noodles) | `start = start - 1` |
| CRAM | 0-based half-open | 1-based (converted by noodles) | `start = start - 1` |
| BED | 0-based half-open | 1-based (converted by noodles) | `start = start - 1` |

**Conversion logic** (same for ALL formats since noodles normalizes to 1-based):
- When `zero_based=true`: `start = noodles_position.get() - 1`
- When `zero_based=false` (current behavior): `start = noodles_position.get()`
- `end` remains unchanged (1-based closed end equals 0-based half-open exclusive end)

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing user code | High | Clear migration guide, version bump |
| Coordinating two repo changes | Medium | Same organization maintains both |
| Confusion during transition | Medium | Comprehensive documentation updates |

## Migration Plan

### For users upgrading from 0.18.x to 0.19.0:

1. **Default behavior is preserved (1-based coordinates)**:
   - No changes needed for existing code
   - Range operations will read coordinate system from DataFrame metadata

2. **If you want 0-based coordinates**:
   - Use `use_zero_based=True` in I/O functions: `pb.scan_vcf("file.vcf", use_zero_based=True)`
   - Or set global config: `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)`

3. **Range operations no longer have `use_zero_based` parameter**:
   - Coordinate system is read from DataFrame metadata set at I/O time
   - Mixing coordinate systems raises `CoordinateSystemMismatchError`
   - Missing metadata raises `MissingCoordinateSystemError`

### Implementation order:
1. PR to datafusion-bio-formats: Add `zero_based` parameter to all TableProviders
2. Update polars-bio Cargo.toml to use new datafusion-bio-formats revision
3. PR to polars-bio: Update API and pass parameter through

### Rollback:
- Default is already 1-based, no rollback needed for most users

## Open Questions

1. ~~**Should we provide a global configuration option?**~~
   - **Resolved**: Yes, use DataFusion-style session configuration with table-level override
   - See Decision 5 below

2. **What about SQL queries that bypass Python API?**
   - SQL queries go through same DataFusion context with registered tables
   - Tables are registered with coordinate system already applied
   - **Answer**: Handled by conversion at table registration time

### Decision 5: Global configuration with table-level override

**Design**: Follow DataFusion's pattern of session-level configuration with statement-level override.

**Implementation**:

```python
import polars_bio as pb

# Global session configuration (defaults to zero_based=False, i.e., 1-based)
# Use the constant for consistent key naming
pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)  # default (1-based)
pb.get_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED)  # returns "false"

# Or use the string key directly
pb.set_option("datafusion.bio.coordinate_system_zero_based", True)  # switch to 0-based

# Table-level override when needed
lf = pb.scan_vcf("file.vcf", use_zero_based=True)  # override for this table only

# Range operations read from DataFrame metadata (no explicit parameter)
pb.overlap(df1, df2)  # reads coordinate system from df1 and df2 metadata
```

**Note**: The option value is stored as a string ("true"/"false") in the DataFusion context. When setting with a Python bool, it's automatically converted.

**In datafusion-bio-formats**:

```rust
// TableProvider constructors accept Option<bool>
pub fn new(
    path: String,
    // ... existing params ...
    zero_based: Option<bool>,  // None = use session default, Some(v) = override
) -> Result<Self>
```

**Resolution order** (highest to lowest priority):
1. Explicit parameter on function call (`use_zero_based=True`)
2. Session-level configuration (`pb.set_option(...)`)
3. Built-in default (`zero_based=False`, i.e., 1-based)

**Coordinate system mismatch validation**:

When performing range operations on two DataFrames/tables, the library SHALL validate that both inputs use the same coordinate system. If there's a mismatch, raise `CoordinateSystemMismatchError`.

```python
# Example: This will raise an exception
df1 = pb.scan_vcf("file1.vcf", use_zero_based=False)  # 1-based (default)
df2 = pb.scan_vcf("file2.vcf", use_zero_based=True)   # 0-based

pb.overlap(df1, df2)  # Raises CoordinateSystemMismatchError
```

**Note**: Range operations do NOT have a `use_zero_based` parameter. Coordinate system is determined only at I/O time (table-level) or via global config, and is read from DataFrame metadata.

### Decision 6: Tracking coordinate system across all input/output types

**Problem**:
- Polars doesn't support DataFrame-level metadata natively ([issue #5117](https://github.com/pola-rs/polars/issues/5117))
- Need consistent handling across Polars, Pandas, and registered DataFusion tables
- Need to propagate coordinate system to output DataFrames

**Input types for range operations**:
1. **Polars LazyFrame/DataFrame** - use `polars-config-meta`
2. **Pandas DataFrame** - use `df.attrs` dictionary (built-in)
3. **Registered DataFusion tables** (Parquet, bio formats) - use Arrow schema metadata

**Arrow Schema Metadata for DataFusion Tables**:

Arrow Schema supports table-level metadata as a `HashMap<String, String>` ([Arrow Schema docs](https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html)). This is the cleanest approach for DataFusion tables because:
- Metadata is stored directly on the table's schema (not in a separate registry)
- Metadata propagates with the table through DataFusion operations
- It's the standard Arrow mechanism for custom metadata

**Implementation in datafusion-bio-formats**:

```rust
// When creating the schema for a TableProvider, include coordinate system metadata
use std::collections::HashMap;

fn create_schema_with_metadata(fields: Vec<Field>, zero_based: bool) -> Schema {
    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert(
        "bio.coordinate_system_zero_based".to_string(),
        zero_based.to_string()
    );
    Schema::new_with_metadata(fields, metadata)
}
```

**Retrieving metadata in polars-bio**:

```python
# polars_bio/_metadata.py

def get_coordinate_system_from_table(table_name: str, ctx: SessionContext) -> Optional[bool]:
    """Get coordinate system from DataFusion table's Arrow schema metadata."""
    table = ctx.table(table_name)
    schema = table.schema()
    metadata = schema.metadata
    if metadata and "bio.coordinate_system_zero_based" in metadata:
        return metadata["bio.coordinate_system_zero_based"] == "true"
    return None
```

**Solution**: Unified metadata abstraction layer

```python
# polars_bio/_metadata.py

from typing import Union, Optional
import polars as pl
import pandas as pd

METADATA_KEY = "bio.coordinate_system_zero_based"

def set_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    zero_based: bool
) -> None:
    """Set coordinate system on DataFrame."""
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        df.config_meta.set(coordinate_system_zero_based=zero_based)
    elif isinstance(df, pd.DataFrame):
        df.attrs["coordinate_system_zero_based"] = zero_based

def get_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx: Optional[SessionContext] = None
) -> Optional[bool]:
    """Get coordinate system from DataFrame or table name."""
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        return df.config_meta.get_metadata().get("coordinate_system_zero_based")
    elif isinstance(df, pd.DataFrame):
        return df.attrs.get("coordinate_system_zero_based")
    elif isinstance(df, str):  # table name - read from Arrow schema metadata
        if ctx is None:
            from polars_bio.sql import get_ctx
            ctx = get_ctx()
        table = ctx.table(df)
        schema = table.schema()
        metadata = schema.metadata or {}
        if METADATA_KEY in metadata:
            return metadata[METADATA_KEY].lower() == "true"
    return None

def validate_coordinate_systems(df1, df2, ctx=None) -> Optional[bool]:
    """Validate both inputs have same coordinate system. Raises if mismatch."""
    cs1 = get_coordinate_system(df1, ctx)
    cs2 = get_coordinate_system(df2, ctx)
    if cs1 is not None and cs2 is not None and cs1 != cs2:
        raise CoordinateSystemMismatchError(
            f"Coordinate system mismatch: "
            f"input1 is {'0-based' if cs1 else '1-based'}, "
            f"input2 is {'0-based' if cs2 else '1-based'}. "
            f"Re-read one of the inputs with matching coordinate system."
        )
    return cs1 if cs1 is not None else cs2  # return the known one, or None
```

**Output propagation**:

```python
def overlap(df1, df2, output_type="polars") -> Union[pl.LazyFrame, pd.DataFrame]:
    # Validate and get coordinate system
    coord_sys = validate_coordinate_systems(df1, df2)

    # ... perform operation ...

    # Propagate coordinate system to output
    if output_type == "polars":
        result_lf.config_meta.set(coordinate_system_zero_based=coord_sys)
    elif output_type == "pandas":
        result_df.attrs["coordinate_system_zero_based"] = coord_sys

    return result
```

**Input/Output matrix**:

| Input Type | Metadata Storage | Output Propagation |
|------------|------------------|-------------------|
| Polars LazyFrame | `polars-config-meta` | `result.config_meta.set()` |
| Polars DataFrame | `polars-config-meta` | `result.config_meta.set()` |
| Pandas DataFrame | `df.attrs` dict | `result.attrs[...]` |
| DataFusion table (str) | Arrow schema metadata | Based on output_type |
| File path (str) | Arrow schema metadata (set at registration) | Based on output_type |

**Benefits**:
- Consistent API regardless of input type
- Coordinate system always propagates to output
- Pandas uses built-in `attrs` (no extra dependency)
- Polars uses `polars-config-meta` (auto-propagation through transforms)
- DataFusion tables use Arrow schema metadata (standard mechanism, no separate registry)
- Easy migration: set `pb.set_option("datafusion.bio.coordinate_system_zero_based", False)` to restore old behavior
- Prevents silent bugs from mixing coordinate systems
- Familiar pattern for DataFusion users
