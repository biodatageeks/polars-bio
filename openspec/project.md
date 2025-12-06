# Project Context

## Purpose
**polars-bio** is a high-performance Python library for genomics data operations built on top of Polars, Apache Arrow, and Apache DataFusion. The project provides:

- Blazing fast, memory-efficient DataFrame operations for large-scale genomic interval datasets
- Both eager and lazy (streaming/out-of-core) execution for datasets too large to fit in memory
- Native support for bioinformatics file formats (VCF, BAM, CRAM, GFF, BED, FASTQ, FASTA)
- SQL interface for querying genomic data
- Genomic range operations (overlap, nearest, count_overlaps, coverage, merge)
- Cloud storage support (S3, GCS) via Apache OpenDAL

## Tech Stack

### Core Languages
- **Python** (>=3.9, <3.14) - Primary API and user-facing interface
- **Rust** (1.86.0) - Performance-critical native extensions via PyO3

### Python Dependencies
- `polars` (>=1.30.0) - DataFrame operations
- `pyarrow` (>=21.0.0,<22) - Zero-copy data exchange
- `datafusion` (>=50.0.0,<51) - SQL query engine

**Dependency Version Policy**: Use `>=X.Y.Z,<(X+1)` format for runtime dependencies to allow compatible minor/patch updates while preventing breaking major version changes.

### Rust Dependencies
- `pyo3` (0.25.1) - Python bindings
- `datafusion` (50.3.0) - Query execution engine
- `arrow` (56.1.0) - In-memory columnar format
- `tokio` (1.42.0) - Async runtime
- `coitrees` (0.4.0) - Cache-oblivious interval trees for fast overlaps
- `sequila-core` - Genomic interval algorithms (external git dependency)
- `datafusion-bio-format-*` - Bioinformatics file format readers (external git dependencies)

### Build System
- `maturin` (>=1.0) - Build Python wheels from Rust
- `poetry` (1.8.4) - Python dependency management

### Documentation
- `mkdocs` with `mkdocs-material` theme
- `mkdocstrings-python` for API docs
- `mkdocs-jupyter` for notebook integration

## Project Conventions

### Code Style

#### Python
- **Formatter**: Black (language version: python3.12)
- **Linter**: Ruff (with `--fix` for auto-corrections)
- **Import sorting**: isort with black profile
- **Type hints**: Encouraged, checked with mypy

#### Rust
- **Formatter**: rustfmt with custom config:
  - `group_imports = "StdExternalCrate"`
  - `imports_granularity = "Module"`
  - `match_block_trailing_comma = true`
- **Linter**: Clippy (all warnings treated as errors in CI)

### Architecture Patterns

1. **Hybrid Python/Rust Architecture**:
   - Python layer (`polars_bio/`) provides high-level API
   - Rust layer (`src/`) implements performance-critical operations
   - PyO3 bridges Python and Rust via `polars_bio.polars_bio` module

2. **Lazy Evaluation**:
   - Operations return `polars.LazyFrame` by default
   - User calls `.collect()` to materialize results
   - Enables query optimization and streaming execution

3. **DataFusion Integration**:
   - SQL queries executed via DataFusion context
   - Custom table providers for bio formats (VCF, GFF, etc.)
   - Predicate and projection pushdown optimization

4. **Module Organization**:
   - `polars_bio/__init__.py` - Public API exports
   - `polars_bio/range_op.py` - Genomic range operations
   - `polars_bio/io.py` - File format I/O
   - `polars_bio/sql.py` - SQL interface
   - `polars_bio/context.py` - DataFusion context management

### Testing Strategy

- **Framework**: pytest with pytest-cov
- **Test location**: `tests/` directory
- **Test categories**:
  - Unit tests for Polars/Pandas operations (`test_polars.py`, `test_pandas.py`)
  - Native Rust function tests (`test_native.py`)
  - I/O tests (`test_io.py`, `test_parallel_io.py`)
  - Streaming/out-of-core tests (`test_streaming.py`)
  - Optimization tests (`test_predicate_pushdown.py`, `test_projection_pushdown.py`)
- **Running tests**: `make test`
- **Note**: Some tests run in isolation (overlap_algorithms, streaming, warnings)

### Git Workflow

- **Main branch**: `master`
- **Feature branches**: Typically named `issue-{number}` or descriptive names
- **CI triggers**: Push to master, PRs, tags
- **Pre-commit hooks**:
  - Python: check-ast, trailing-whitespace, isort, black
  - Rust: fmt, cargo-check

## Domain Context

### Genomic Intervals
- A genomic interval represents a region on a chromosome: `(chromosome, start, end)`
- Coordinates are typically 0-based, half-open `[start, end)`
- Common column names: `chrom`/`contig`, `start`/`chromStart`, `end`/`chromEnd`

### Key Operations
- **Overlap**: Find intervals that share genomic positions
- **Nearest**: Find closest interval (upstream/downstream/any)
- **Count overlaps**: Count how many intervals overlap each query
- **Coverage**: Calculate depth of coverage across positions
- **Merge**: Combine overlapping intervals

### File Formats
- **BED**: Tab-delimited intervals (chrom, start, end, ...)
- **VCF**: Variant Call Format (genetic variants)
- **GFF/GTF**: Gene annotations
- **BAM/CRAM**: Aligned sequence reads
- **FASTQ**: Raw sequence reads
- **FASTA**: Reference sequences

## Important Constraints

1. **Performance Critical**: Operations must handle millions of intervals efficiently
2. **Memory Efficiency**: Support out-of-core processing for large datasets
3. **Cross-Platform**: Must build wheels for Linux, macOS (arm64/x86_64), Windows
4. **Python Version Support**: 3.9 through 3.13
5. **Rust Compiler Warnings**: All warnings treated as errors in CI (`-Dwarnings`)

## External Dependencies

### Git-Based Rust Dependencies
- `sequila-core` - https://github.com/biodatageeks/sequila-native.git
- `datafusion-bio-format-*` - https://github.com/biodatageeks/datafusion-bio-formats.git

### Benchmark Comparisons
- `pyranges` - Python genomic ranges library
- `pybedtools` - Python wrapper for bedtools
- `GenomicRanges` - Bioconductor-style ranges
- `bioframe` - Pandas-based genomic operations

### Cloud Storage
- S3 and GCS via Apache OpenDAL integration
