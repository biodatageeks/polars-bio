# Architecture

**Analysis Date:** 2026-03-20

## Pattern Overview

**Overall:** Hybrid Python-Rust architecture with Apache DataFusion as the query engine backbone.

**Key Characteristics:**
- Two-layer design: Python API layer on top of Rust/DataFusion computation engine
- Format-aware table providers for genomics data (BAM/SAM/CRAM/VCF/GFF/GTF/FASTQ/FASTA/BED/PAIRS)
- Predicate and projection pushdown optimization via DataFusion
- Zero-copy Arrow C Stream integration for efficient Python-Rust data exchange
- Session-based context management for maintaining state across operations

## Layers

**Python API Layer:**
- Purpose: User-facing interface exposing genomic operations as clean, composable functions
- Location: `polars_bio/` (21 Python modules)
- Contains: High-level IO operations, range operations (overlap/nearest/coverage), SQL interface, metadata handling, coordinate system validation
- Depends on: Rust bindings (PyO3), Polars, DataFusion Python client, Arrow
- Used by: End-user scripts and Jupyter notebooks

**Rust FFI Bindings & Options Layer:**
- Purpose: PyO3 bindings and option serialization for Rust computation
- Location: `src/option.rs` (29KB)
- Contains: `InputFormat`, `OutputFormat`, `RangeOp`, `FilterOp`, read/write options for each format (`BamReadOptions`, `VcfReadOptions`, etc.)
- Depends on: PyO3, datafusion-bio-formats crates
- Used by: Python layer to configure and invoke Rust operations

**Session Context & Configuration:**
- Purpose: Centralized DataFusion session management and execution configuration
- Location: `src/context.rs` (4.1KB), `polars_bio/context.py` (4.4KB)
- Contains: `PyBioSessionContext` (Rust), `Context` singleton (Python), session config options, UDTF registration
- Depends on: DataFusion SessionContext, datafusion-bio-function-ranges
- Used by: All query execution paths

**Table Scanning & Registration:**
- Purpose: Load genomic files into DataFusion memory or streaming contexts
- Location: `src/scan.rs` (26KB), `polars_bio/io.py` (152KB)
- Contains: Table provider instantiation for each format, predicate/projection pushdown, Arrow stream consumers
- Depends on: datafusion-bio-format-* crates, Arrow C Stream FFI
- Used by: Range operations, SQL queries, direct read/write operations

**Range Operations Engine:**
- Purpose: Interval overlap, nearest, merge, and coverage calculations
- Location: `src/operation.rs` (15KB), `polars_bio/range_op.py` (36KB), `polars_bio/range_op_helpers.py` (14KB)
- Contains: COITrees-based interval joins, nearest neighbor queries, coverage/count computation
- Depends on: datafusion-bio-function-ranges, DataFusion
- Used by: `overlap()`, `nearest()`, `count_overlaps()`, `coverage()`, `merge()`, `cluster()`, `complement()`, `subtract()`

**Pileup/Depth Operations:**
- Purpose: Per-base read depth computation via CIGAR walk
- Location: `src/pileup.rs` (7.2KB), `polars_bio/pileup_op.py` (9.3KB)
- Contains: `DepthFunction` UDTF, `DepthTableProvider` for streaming depth calculation
- Depends on: datafusion-bio-function-pileup
- Used by: `pb.depth()` function for coverage analysis

**SQL Interface & Table Registration:**
- Purpose: Register genomic files as SQL-queryable tables; execute raw DataFusion SQL
- Location: `polars_bio/sql.py` (35KB)
- Contains: Table registration methods for each format, VCF INFO field auto-detection, GFF attribute handling
- Depends on: DataFusion SQL parser, table providers
- Used by: `pb.sql()`, `pb.register_*()` functions

**Predicate Translation:**
- Purpose: Convert Polars expressions to DataFusion expressions for filter pushdown
- Location: `polars_bio/predicate_translator.py` (22KB)
- Contains: Format-aware column type definitions, Polars → DataFusion expression translation
- Depends on: Polars Expr API, DataFusion functions
- Used by: IO operations for predicate pushdown on BAM/SAM/CRAM/VCF/GFF

**Metadata & Coordinate Systems:**
- Purpose: Track and validate genomic coordinate systems and VCF/BAM metadata
- Location: `polars_bio/_metadata.py` (26KB), `polars_bio/metadata_extractors.py` (14KB)
- Contains: Coordinate system validation, VCF header parsing, BAM tag type inference, metadata attachment to DataFrames
- Depends on: Polars metadata API, DataFusion-bio format readers
- Used by: Range operations, IO operations for proper coordinate interpretation

**Write Operations:**
- Purpose: Stream DataFrames back to genomic file formats
- Location: `src/write.rs` (45KB), `polars_bio/io.py` (write methods)
- Contains: VCF/FASTQ/BAM/SAM/CRAM write streaming, contig metadata preservation, FORMAT field handling
- Depends on: datafusion-bio-format-* crates insert_into() API
- Used by: `write_*()` and `sink_*()` functions

## Data Flow

**Read Path (Lazy Scan):**

1. User calls `pb.scan_bam(path, filters=...)`
2. `_lazy_scan()` in `io.py` → creates `ReadOptions` with predicates
3. Polars registers plugin via `polars_ext.py` namespace
4. DataFusion session created if needed
5. `_overlap_source()` called with Polars LazyFrame
6. Predicate validation: `translate_predicate()` converts Polars Expr → DataFusion Expr
7. `register_table()` in `scan.rs` → instantiates `BamTableProvider` (or other format provider)
8. Provider applies predicate pushdown and projection pushdown internally
9. Execution plan returned to Polars for lazy evaluation
10. User calls `.collect()` → Rust executes, streams batches back via Arrow C Streams

**Range Operation (Overlap Example):**

1. User calls `pb.overlap(df1, df2, cols1=["chrom","start","end"], cols2=["chrom","start","end"])`
2. `overlap()` in `range_op.py` validates coordinate systems from metadata
3. Input DataFrames converted to Arrow C Streams or pre-collected RecordBatches
4. `range_operation_lazy()` or `range_operation_frame()` in `lib.rs` called with stream readers
5. Streams consumed with GIL held (required for Arrow FFI export)
6. GIL released for computation: `register_frame_from_arrow_stream()` builds MemTable
7. `do_range_operation()` in `operation.rs` → applies `OverlapProvider` UDTF
8. UDTF uses COITrees algorithm (from datafusion-bio-function-ranges) to find overlaps
9. Result DataFrame returned to Python as PyDataFrame
10. Converted back to Polars LazyFrame and returned to user

**SQL Query Path:**

1. User calls `pb.register_bam("file.bam", "my_bam")`
2. `register_table()` in `scan.rs` registers table with DataFusion context
3. User calls `pb.sql("SELECT * FROM my_bam WHERE start < 1000")`
4. SQL string parsed by DataFusion
5. Predicate filter pushed to `BamTableProvider` (or other provider)
6. Projection pushed to provider for column selection
7. Provider returns filtered/projected batches
8. Results streamed back to Python as DataFrame

**Write Path:**

1. User calls `pb.write_vcf(lf, "output.vcf.gz")`
2. LazyFrame collected to DataFrame / Arrow batches
3. `write_table()` in `write.rs` → streams batches to `VcfTableProvider.insert_into()`
4. Provider writes VCF header (with preserved contig metadata) and variant records
5. File written to disk/cloud storage

**State Management:**

- Global singleton `Context` instance (`polars_bio/context.py`) holds the `BioSessionContext`
- BioSessionContext wraps DataFusion `SessionContext` + session config HashMap
- All operations access shared context: `ctx.ctx` for DataFusion operations
- Options persisted in both Python config dict and Rust context via `set_option()`
- Tokio runtime created per operation (async workflows handled with `py.allow_threads()`)

## Key Abstractions

**ReadOptions & WriteOptions:**
- Purpose: Encapsulate format-specific read/write parameters
- Examples: `BamReadOptions`, `VcfReadOptions`, `GffReadOptions`, `VcfWriteOptions`, `BamWriteOptions`
- Pattern: PyO3 classes mapping to Rust structs; passed to table providers for configuration
- Location: `src/option.rs` (read/write structs), `polars_bio/io.py` (Python wrapper functions)

**InputFormat & OutputFormat Enums:**
- Purpose: Discriminate between file types at runtime
- Examples: `InputFormat.Bam`, `InputFormat.Vcf`, `InputFormat.Gff`
- Pattern: PyO3 enums; used as dictionary keys (converted to string for hashing) in `_FORMAT_COLUMN_TYPES`
- Location: `src/option.rs`

**TableProvider Trait (DataFusion):**
- Purpose: Defines interface for lazy/streaming file readers
- Examples: `BamTableProvider`, `VcfTableProvider`, `GffTableProvider` from upstream crates
- Pattern: Implements scan(), insert_into() methods; handles predicate/projection pushdown
- Location: datafusion-bio-format-* crates (external git dependencies)

**FilterOp Enum (Coordinate System):**
- Purpose: Distinguish 0-based (Strict) from 1-based (Weak) interval filtering
- Pattern: Set based on metadata; passed to range operation UDTFs to configure boundary semantics
- Location: `src/option.rs`

**PredicateTranslator:**
- Purpose: Translate user Polars filter expressions to DataFusion Expr for pushdown
- Pattern: Generic translator with format-aware column type validation; permissive when column types unknown
- Location: `polars_bio/predicate_translator.py`

**Arrow C Stream Integration:**
- Purpose: Zero-copy data transfer from Polars LazyFrames to Rust
- Pattern: `ArrowArrayStreamReader` consumed in Rust; Polars exports via `__arrow_c_stream__()` (Polars >= 1.37.1)
- Location: `src/scan.rs` (`ArrowCStreamPartitionStream`), `src/lib.rs` (`range_operation_lazy`)

## Entry Points

**`pb.scan_*()` / `pb.read_*()` (Scan Entry Points):**
- Location: `polars_bio/io.py::IOOperations` static methods
- Triggers: User import of polars_bio and calls to scan/read functions
- Responsibilities: Parse read options, validate predicates, create LazyFrame (scan) or collect immediately (read)

**`pb.overlap()` / `pb.nearest()` / `pb.coverage()` (Range Operation Entry Points):**
- Location: `polars_bio/range_op.py` module functions
- Triggers: User calls with two DataFrames
- Responsibilities: Validate coordinate systems, convert inputs to Arrow/batches, dispatch to Rust via `range_operation_*()` functions, return result as LazyFrame

**`pb.sql()` (SQL Query Entry Point):**
- Location: `polars_bio/sql.py::SQL.sql()` static method
- Triggers: User calls with SQL string
- Responsibilities: Parse SQL, execute against registered tables, return results

**`pb.depth()` (Pileup Entry Point):**
- Location: `polars_bio/pileup_op.py::PileupOperations.depth()` static method
- Triggers: User calls with BAM/SAM/CRAM path
- Responsibilities: Configure pileup options, invoke Rust `py_pileup_depth()`, return depth LazyFrame

**`pb.register_*()` (SQL Table Registration Entry Points):**
- Location: `polars_bio/sql.py::SQL.register_*()` static methods
- Triggers: User calls to make files queryable via SQL
- Responsibilities: Auto-detect metadata (VCF INFO fields, GFF attributes), register table with DataFusion context

## Error Handling

**Strategy:** Layered validation and error propagation with context

**Patterns:**
- Predicate validation: `translate_predicate()` raises `PredicateTranslationError` if Polars expression cannot be converted
- Coordinate system validation: `validate_coordinate_systems()` raises `CoordinateSystemMismatchError` if metadata mismatches
- Option validation: `_validate_tag_type_hints()` in `io.py` validates SAM tag type codes before passing to Rust
- Metadata validation: `set_coordinate_system()` in `_metadata.py` raises `MissingCoordinateSystemError` if metadata required but absent
- Format-aware type checking: Column type sets defined per format; unknown columns (tags, INFO fields) treated permissively

## Cross-Cutting Concerns

**Logging:**
- Approach: Python `logging` module (configured via `polars_bio/logging.py::set_loglevel()`)
- Rust side: `log` and `tracing` crates, bridged via `pyo3-log`
- Usage: Debug info on option setting, table registration; info on operation execution

**Validation:**
- Approach: Multi-point validation: predicate translation, coordinate system metadata, tag type hints, column existence
- Philosophy: Fail early with descriptive errors; permissive for unknown columns (let DataFusion type-check at execution)

**Authentication:**
- Approach: Cloud storage via `PyObjectStorageOptions` (wrapper around `ObjectStorageOptions` from upstream)
- Supports: AWS S3 (with request payer option), GCS, Azure with credentials/env var configuration
- Usage: Passed to table providers for remote file access

---

*Architecture analysis: 2026-03-20*
