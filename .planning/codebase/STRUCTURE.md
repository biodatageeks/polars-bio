# Codebase Structure

**Analysis Date:** 2026-03-20

## Directory Layout

```
polars-bio/
├── src/                          # Rust source code (PyO3 extension module)
│   ├── lib.rs                    # Entry point: PyO3 module definition, range_operation_*() functions
│   ├── context.rs                # PyBioSessionContext: DataFusion session wrapper
│   ├── scan.rs                   # Table scanning: register_table(), Arrow stream consumers
│   ├── operation.rs              # Range operation implementation (overlap, nearest, coverage, merge, etc.)
│   ├── pileup.rs                 # Depth/pileup UDTF implementation
│   ├── write.rs                  # Write streaming: VCF, FASTQ, BAM, SAM, CRAM output
│   ├── option.rs                 # PyO3 option classes: InputFormat, OutputFormat, RangeOp, FilterOp, Read/WriteOptions
│   └── utils.rs                  # Utility functions
│
├── polars_bio/                   # Python package (public API)
│   ├── __init__.py               # Package initialization: re-exports all public symbols
│   ├── io.py                     # IOOperations class: scan_*(), read_*(), write_*(), describe_*()
│   ├── range_op.py               # IntervalOperations class: overlap(), nearest(), count_overlaps(), coverage(), merge(), cluster()
│   ├── range_op_helpers.py        # range_operation() dispatcher, overlap validation
│   ├── range_op_io.py             # DataFrame I/O for range operations (str paths, Polars frames, Pandas)
│   ├── pileup_op.py              # PileupOperations class: depth() method
│   ├── sql.py                    # SQL class: sql(), register_*() table registration methods
│   ├── polars_ext.py             # Polars LazyFrame namespace extension (pb.*)
│   ├── predicate_translator.py   # Polars → DataFusion Expr translation for filter pushdown
│   ├── sql_predicate_builder.py  # SQL-based predicate construction (legacy)
│   ├── context.py                # Python Context singleton, option management
│   ├── _metadata.py              # Coordinate system tracking, VCF/BAM metadata extraction
│   ├── metadata_extractors.py    # VCF header parsing, SAM tag type inference
│   ├── exceptions.py             # Custom exceptions: CoordinateSystemMismatchError, MissingCoordinateSystemError
│   ├── constants.py              # Global constants (interval column defaults, etc.)
│   ├── logging.py                # Logging configuration
│   ├── utils.py                  # Utility functions (quote SQL identifiers, etc.)
│   ├── interval_op_helpers.py    # Legacy helpers for interval operations
│   ├── range_utils.py            # Interval visualization utilities
│   └── operations.py             # Unused/legacy operations module
│
├── tests/                        # Test suite
│   ├── test_io_bam.py           # BAM reading: filters, projections, predicates, tags
│   ├── test_io_cram.py          # CRAM reading: similar to BAM
│   ├── test_io_vcf.py           # VCF reading: INFO field selection, predicates
│   ├── test_io_gff.py           # GFF/GTF reading and attribute handling
│   ├── test_io_fastq.py         # FASTQ reading
│   ├── test_io_bed.py           # BED format support
│   ├── test_io_fasta.py         # FASTA reading
│   ├── test_io_indexed.py       # Indexed file access (BAI, CSI, TBI, CRAI)
│   ├── test_io_pairs.py         # Hi-C pairs format
│   ├── test_coordinate_system_metadata.py   # Extensive coordinate system tests
│   ├── test_custom_tag_inference.py         # SAM/BAM custom tag type detection
│   ├── test_fastq_write.py      # FASTQ write functionality
│   ├── test_bioframe.py         # Bioframe API compatibility
│   ├── test_native.py           # Native range operations
│   ├── test_lazy_streaming_fix.py           # Arrow C Stream handling
│   ├── test_comprehensive_metadata.py       # Metadata persistence
│   ├── test_filter_select_attributes_bug_fix.py  # GFF attribute filter/select edge cases
│   ├── test_gff_eager_vs_lazy.py # GFF eager vs lazy collection
│   ├── test_context_options.py  # Session context options
│   ├── test_execution_plan_validation.py    # Query plan validation
│   ├── conftest.py              # Pytest fixtures
│   ├── _expected.py             # Expected test results
│   └── data/                    # Test data files (BAM, VCF, GFF, etc.)
│
├── Cargo.toml                    # Rust dependencies (DataFusion 50.3.0, PyO3 0.25.1, datafusion-bio-*)
├── pyproject.toml                # Python package metadata, dev dependencies
├── Makefile                      # Build shortcuts (maturin develop --release, pytest)
│
├── docs/                         # Documentation site (mkdocs)
├── site/                         # Generated documentation (readthedocs)
├── benchmarks/                   # Performance benchmarks
├── it/                           # Integration test scripts
├── scripts/                      # Utility scripts
└── openspec/                     # Change proposal specifications

```

## Directory Purposes

**`src/`:**
- Purpose: Rust extension module source code (compiled to `polars_bio.abi3.so`)
- Contains: PyO3 FFI bindings, DataFusion query execution, table provider integration, write streaming
- Key files: `lib.rs` (module root), `context.rs` (session), `scan.rs` (table loading), `operation.rs` (range ops)

**`polars_bio/`:**
- Purpose: Python package providing public API to users
- Contains: High-level functions, option wrappers, validation, metadata tracking, SQL interface
- Key files: `__init__.py` (re-exports), `io.py` (read/write), `range_op.py` (overlap/nearest), `sql.py` (SQL)

**`tests/`:**
- Purpose: Pytest test suite covering all data formats and operations
- Contains: Format-specific tests (BAM, VCF, GFF, FASTQ, etc.), range operation tests, metadata validation tests
- Key files: `conftest.py` (fixtures), `test_coordinate_system_metadata.py` (largest test suite)

## Key File Locations

**Entry Points:**

- `polars_bio/__init__.py`: Package initialization; re-exports all public symbols from `io.py`, `range_op.py`, `sql.py`, `pileup_op.py`
- `src/lib.rs`: PyO3 module root; defines `range_operation_frame()`, `range_operation_lazy()`, `range_operation_scan()` functions
- `polars_bio/polars_ext.py`: Registers `.pb` namespace on Polars LazyFrames

**Configuration:**

- `Cargo.toml`: Rust dependencies (DataFusion, Arrow, datafusion-bio-format-* git deps)
- `pyproject.toml`: Python package metadata, maturin config, dev dependencies
- `polars_bio/context.py`: Session configuration, option management
- `src/context.rs`: DataFusion SessionContext initialization, UDTF registration

**Core Logic:**

- `polars_bio/io.py`: IOOperations class (scan_bam, read_vcf, write_fastq, etc.) — the largest module (152KB)
- `polars_bio/range_op.py`: Range operation wrappers (overlap, nearest, count_overlaps, coverage) — 36KB
- `src/operation.rs`: Rust implementation of range operations (do_overlap, do_nearest, do_coverage) — 15KB
- `polars_bio/predicate_translator.py`: Polars Expr → DataFusion Expr translation — 22KB
- `polars_bio/sql.py`: SQL interface (sql(), register_*) — 35KB

**Testing:**

- `tests/conftest.py`: Pytest fixtures and shared setup
- `tests/test_coordinate_system_metadata.py`: Largest test file (67KB); comprehensive coordinate system validation
- `tests/test_custom_tag_inference.py`: SAM/BAM custom tag type detection (26KB)
- `tests/test_io_bam.py`: BAM format tests (31KB)

## Naming Conventions

**Files:**

- Python modules: `lowercase_with_underscores.py` (e.g., `predicate_translator.py`, `pileup_op.py`)
- Rust files: `lowercase_with_underscores.rs` (e.g., `context.rs`, `scan.rs`)
- Test files: `test_<feature>.py` (e.g., `test_io_bam.py`, `test_coordinate_system_metadata.py`)

**Directories:**

- Source: `src/` (Rust), `polars_bio/` (Python), `tests/` (tests)
- Feature-specific: `benchmarks/`, `scripts/`, `it/` (integration tests)
- Documentation: `docs/`, `site/`

## Where to Add New Code

**New Format Support (e.g., SAM variant):**
- Rust implementation: Add table provider to upstream `datafusion-bio-formats` repo (external dependency)
- Python wrapper: Add scan/read method to `IOOperations` class in `polars_bio/io.py`
- Options: Add read/write option structs in `src/option.rs`
- Registration: Add register_* method to `SQL` class in `polars_bio/sql.py`
- Tests: Add `test_io_<format>.py` file in `tests/`

**New Range Operation (e.g., binary operations):**
- Rust implementation: Implement provider in upstream `datafusion-bio-function-ranges` repo
- Python wrapper: Add method to `IntervalOperations` class in `polars_bio/range_op.py`
- Option enum: Add variant to `RangeOp` enum in `src/option.rs`
- Entry point: Add function to `polars_bio/__init__.py` exports
- Tests: Add test methods to `tests/test_native.py` or create `test_<operation>.py`

**New Utility Function:**
- Shared helpers: `polars_bio/utils.py` (for Python), `src/utils.rs` (for Rust)
- Metadata extraction: `polars_bio/metadata_extractors.py`
- Logging: `polars_bio/logging.py`

**Session Configuration:**
- Python side: Add option key/value logic to `Context.set_option()` in `polars_bio/context.py`
- Rust side: Add handling in `set_option_internal()` in `src/context.rs`
- Document: Update docstrings with option namespace and description

## Special Directories

**`tests/data/`:**
- Purpose: Contains test genomic files (small BAM, VCF, GFF, FASTQ, etc.)
- Generated: No (committed to git)
- Committed: Yes

**`target/`:**
- Purpose: Cargo build artifacts (compiled Rust, Maturin wheel staging)
- Generated: Yes (by Cargo)
- Committed: No (in .gitignore)

**`.planning/codebase/`:**
- Purpose: GSD codebase mapping documents (ARCHITECTURE.md, STRUCTURE.md, CONVENTIONS.md, etc.)
- Generated: Yes (by /gsd:map-codebase)
- Committed: Yes (to git for future reference)

**`openspec/specs/`:**
- Purpose: Change proposal specifications for major features or breaking changes
- Generated: No (manually created by developers)
- Committed: Yes

**`benchmarks/`:**
- Purpose: Performance benchmarks run via GitHub Actions
- Generated: Yes (benchmark results stored as artifacts)
- Committed: Partial (benchmark scripts committed, results stored externally)

---

*Structure analysis: 2026-03-20*
