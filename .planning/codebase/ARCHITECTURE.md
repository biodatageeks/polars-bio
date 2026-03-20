# Architecture

**Analysis Date:** 2026-03-20

## Pattern Overview

**Overall:** Hybrid Python/Rust bridge with lazy execution streaming model

**Key Characteristics:**
- **Language boundary:** Python public API + Rust compute engine via PyO3 bindings
- **Query engine:** DataFusion-based execution with predicate/projection pushdown
- **Streaming:** Arrow C FFI streams to avoid GIL overhead; column-oriented data flow
- **Lazy evaluation:** LazyFrame wrapping custom I/O sources with format-specific handlers
- **Multi-format:** Unified code path for BAM/SAM/CRAM/VCF/GFF/GTF/FASTQ/FASTA/BED with format-aware options

## Layers

**Public API Layer:**
- Purpose: Expose high-level I/O and genomic operations to users
- Location: `polars_bio/__init__.py`
- Contains: Aliases and re-exports (scan/read/write/sink functions, range/pileup operations, SQL registry)
- Depends on: IOOperations, PileupOperations, IntervalOperations, SQL classes
- Used by: User code

**I/O Operations Layer:**
- Purpose: Unified interface for reading/writing genomic file formats
- Location: `polars_bio/io.py` (main entry point; 3200+ lines)
- Contains: IOOperations class with static methods for each format (read_bam, scan_vcf, write_cram, etc.)
- Depends on: Rust bindings (py_read_table, py_register_table, py_write_table), DataFusion Python, Polars
- Used by: User code directly; internally by SQL and range operations

**Query Pipeline Layer:**
- Purpose: Bridge between Polars LazyFrame/DataFrame and DataFusion execution
- Location: `polars_bio/io.py` (_read_file, _lazy_scan, _overlap_source functions)
- Key flow:
  1. `_read_file()` registers table with Rust via `py_register_table()`, extracts schema
  2. `_lazy_scan()` wraps schema in Polars IO source callback `_overlap_source()`
  3. `_overlap_source()` executed during `.collect()`: re-registers file, translates predicates, streams via Arrow C FFI
  4. Predicate pushdown via `translate_predicate()` → SQL string → DataFusion `filter()`
  5. Projection pushdown via `query_df.select(parsed_sql_expr)` at DataFusion level
  6. Client-side safety net: filter/select reapplied if pushdown fails
- Location: `polars_bio/io.py:2496-2723`
- Depends on: predicate_translator.py, Polars register_io_source, DataFusion DataFrame
- Used by: _read_file, SQL registration

**Predicate Translator Layer:**
- Purpose: Convert Polars expressions to DataFusion expressions with type validation
- Location: `polars_bio/predicate_translator.py`
- Key pattern:
  - `translate_predicate()` validates operators against format-specific column types (BAM_STRING_COLUMNS, VCF_UINT32_COLUMNS, etc.)
  - Handles unknown columns permissively (BAM tags, VCF INFO/FORMAT fields, GFF attributes)
  - Unknown columns pass validation; type errors caught at DataFusion execution time
  - Recursively translates BinaryOp (==, >, IN, etc.), UnaryOp (not), and BooleanFunction (and, or)
- Location: `polars_bio/predicate_translator.py:72-100+`
- Depends on: DataFusion col/lit/functions
- Used by: _overlap_source

**Rust/PyO3 Binding Layer:**
- Purpose: FFI boundary to Rust compute engine
- Location: `src/lib.rs`, `src/scan.rs`, `src/context.rs`
- Exposes functions:
  - `py_register_table()` - register file-backed table (BAM/VCF/GFF/etc)
  - `py_read_table()` - get DataFusion DataFrame for a registered table
  - `py_read_sql()` - execute SQL query, return DataFusion DataFrame
  - `range_operation_frame/lazy()` - spatial join operations (overlap, nearest, etc)
  - `py_pileup_depth()` - compute per-base depth
- Depends on: datafusion-bio-formats (VCF/BAM/GFF/etc readers), datafusion-bio-functions (range/pileup operations)
- Used by: Python I/O layer

**Rust Format Support:**
- Location: `src/scan.rs:1-100+`
- Table providers (one per format):
  - `VcfTableProvider` - VCF format with INFO/FORMAT field handling
  - `BamTableProvider`, `CramTableProvider` - alignment formats with tag discovery
  - `GffTableProvider`, `GtfTableProvider` - annotation with attribute parsing
  - `FastqTableProvider`, `FastaTableProvider`, `BedTableProvider` - sequence formats
  - `PairsTableProvider` - Hi-C interaction data
- All inherit from DataFusion's `TableProvider` trait

**Genomic Operations Layer:**
- Purpose: Range/interval operations and pileup analysis
- Locations:
  - `polars_bio/range_op.py` - FilterOp-based interval operations (overlap, nearest, count_overlaps, merge, cluster, complement, subtract)
  - `polars_bio/pileup_op.py` - Per-base depth computation (PileupOperations class)
  - Rust: `src/operation.rs`, `src/pileup.rs`
- Depends on: DataFusion SQL UDFs, Polars Arrow interop, coordinate system metadata
- Used by: User code via public API

**SQL/Registration Layer:**
- Purpose: Enable SQL-based table registration and querying
- Location: `polars_bio/sql.py` (SQL class)
- Pattern: `register_vcf()`, `register_gff()`, etc. call underlying py_register_table() and log table names
- Enables: `SELECT * FROM table_name` in DataFusion SQL context
- Used by: Users working with SQL interface

**Metadata Layer:**
- Purpose: Track file format metadata and coordinate systems (1-based vs 0-based)
- Location: `polars_bio/_metadata.py`, `polars_bio/metadata_extractors.py`
- Stores in DataFrame.metadata:
  - `coordinate_system_zero_based` (bool)
  - `source_format`, `source_path`
  - Format-specific: VCF sample_names, BAM header, GFF attributes
- Critical for: Range operations (FilterOp.Strict/Weak), output correctness, metadata preservation through `.collect()`
- Used by: range_op.py, io.py, user introspection

**GFF/GTF Projection Wrapper:**
- Purpose: Handle attribute field extraction with projection awareness
- Location: `polars_bio/io.py:3048-3200+` (AnnotationLazyFrameWrapper, GffLazyFrameWrapper, GtfLazyFrameWrapper classes)
- Why needed: GFF attributes column is unparsed key=value pairs; requested attr columns must be re-registered at table level
- Wraps base LazyFrame and intercepts `.select()` to trigger re-registration with appropriate attr_fields
- Used by: _read_file for GFF/GTF returns

## Data Flow

**Read Flow (scan_bam → .collect()):**

1. User calls `pb.scan_bam("file.bam")`
2. `IOOperations.scan_bam()` calls `_read_file(path, InputFormat.Bam, ...)`
3. `_read_file()`:
   - Calls `py_register_table(ctx, path, ...)` → Rust registers BamTableProvider in DataFusion context
   - Calls `py_get_table_schema(ctx, table_name)` → extracts PyArrow schema WITHOUT scanning file
   - Calls `_lazy_scan(schema)` → wraps in Polars LazyFrame with custom IO source
   - Sets metadata (coordinate_system, source format/path)
   - Returns LazyFrame
4. User applies filters/selects on LazyFrame (lazy, not executed)
5. User calls `.collect()`
6. Polars invokes `_overlap_source(with_columns, predicate, n_rows, ...)` callback:
   - Re-registers table (fresh provider state)
   - Translates Polars predicate to DataFusion Expr via `translate_predicate()`
   - Applies `query_df.filter(expr)` at DataFusion level (pushdown)
   - Applies `query_df.select(parsed_cols)` at DataFusion level (pushdown)
   - Calls `query_df.execute_stream()` → Arrow C FFI stream (GIL-free)
   - Iterates batches, converts to Polars DataFrame, applies client-side fallback if needed
   - Yields results to Polars

**Write Flow (DataFrame → sink_bam):**

1. User calls `pb.sink_bam(lazy_frame, "output.bam")`
2. Calls `_write_bam_file(lf, path, OutputFormat.Bam, ...)`
3. `_write_file()` helper:
   - Calls `.collect_into_list()` on LazyFrame to get batches
   - Converts each batch to PyArrow RecordBatch
   - Calls `py_write_table(ctx, batches, OutputFormat.Bam, ...)` in Rust
   - Rust writes directly to disk via datafusion-bio-format-bam
4. Returns row count

**Range Operation Flow (overlap example):**

1. User calls `pb.overlap(query_df, subject_df, how="inner")`
2. Calls `_validate_overlap_input()` to extract coordinate system metadata
3. Determines FilterOp (Strict for 0-based, Weak for 1-based) from metadata
4. Extracts Arrow C streams from both LazyFrames (GIL-free via `__arrow_c_stream__()`)
5. Calls `range_operation_lazy(ctx, stream1, stream2, FilterOp.Strict, ...)` in Rust
6. Rust performs spatial join using datafusion-bio-function-ranges
7. Returns PyDataFrame result

**State Management:**

- **DataFusion Context:** Module-level `ctx` in `polars_bio/context.py`; thread-safe, shared across operations
- **Table Registration:** Tables registered by path+format; fresh state per `_overlap_source()` invocation
- **Metadata:** Attached to DataFrame.metadata dict; lost on `.collect()` unless explicitly re-set
- **Option Store:** Module-level dict in `io.py` caches PyObjectStorageOptions for GFF re-registration

## Key Abstractions

**InputFormat / OutputFormat:**
- Purpose: Enum-like marker for file format (PyO3 from Rust, not hashable)
- Examples: InputFormat.Bam, InputFormat.Vcf, OutputFormat.Cram
- Usage: Passed to `py_register_table()` and `py_write_table()` to dispatch to correct TableProvider

**ReadOptions / BamReadOptions / VcfReadOptions / etc:**
- Purpose: PyO3 structs bundling format-specific read parameters
- Pattern: Created in Python, passed to Rust
- Example: VcfReadOptions includes info_fields, format_fields, samples; BamReadOptions includes tag_fields
- Used by: _read_file to configure table provider behavior

**RangeOptions / FilterOp:**
- Purpose: Parameters for spatial join operations
- FilterOp: Enum (Strict=0-based, Weak=1-based) controlling interval boundary interpretation
- RangeOptions: Includes operation type (Overlap/Nearest/etc), FilterOp, column names
- Used by: range_operation_frame/lazy Rust functions

**LazyFrame Wrappers:**
- Purpose: Preserve LazyFrame type while adding projection-aware behavior
- Examples: GffLazyFrameWrapper, GtfLazyFrameWrapper
- Pattern: Intercept `.select()` to trigger re-registration with proper attr_fields
- Implemented as: Proxy class with __getattr__ delegation to underlying LazyFrame

## Entry Points

**scan_bam:**
- Location: `polars_bio/io.py:IOOperations.scan_bam()`
- Triggers: Lazy registration → LazyFrame returned
- Responsibilities: Parameter validation, read options setup, call _read_file()

**read_bam:**
- Location: `polars_bio/io.py:IOOperations.read_bam()`
- Triggers: scan_bam().collect()
- Responsibilities: Add .collect() call to return DataFrame

**write_vcf / sink_vcf:**
- Location: `polars_bio/io.py:IOOperations.write_vcf()` / `.sink_vcf()`
- Triggers: Eager collect (write) or streaming via .collect() (sink)
- Responsibilities: Call _write_file() with OutputFormat.Vcf

**depth:**
- Location: `polars_bio/pileup_op.py:PileupOperations.depth()`
- Triggers: File path → pileup calculation via UDTF
- Responsibilities: Parameter setup, coordinate system resolution, Rust integration

**overlap / nearest / etc:**
- Location: `polars_bio/range_op.py:IntervalOperations.<op>()`
- Triggers: Two LazyFrames/DataFrames → spatial result
- Responsibilities: Metadata validation, Arrow stream extraction, Rust spatial join

## Error Handling

**Strategy:** Graceful degradation with fallback to client-side execution

**Patterns:**
- Predicate pushdown failure → logs warning, applies filter client-side (may cause full scan)
- Projection pushdown failure → logs debug, applies select client-side
- Unknown file format → raises ValueError with supported formats list
- Missing index for indexed reads → uses full scan (slower but still correct)
- Metadata missing → raises MissingCoordinateSystemError or uses global config fallback

**Cross-Cutting Concerns:**

**Logging:** Module-level logger in `polars_bio/logging.py`; uses Python logging; tracing for Rust via pyo3-log

**Validation:**
- Tag type hints validated before Rust call (format "TAG:TYPE")
- Predicate type validation in translate_predicate() per format
- Coordinate system validation in range operations (both inputs must match)
- Attribute field names validated against GFF schema

**Authentication:** Handled by object_storage_options (allow_anonymous, enable_request_payer, S3/GCS credentials via environment)

---

*Architecture analysis: 2026-03-20*
