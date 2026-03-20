# Technology Stack

**Analysis Date:** 2026-03-20

## Languages

**Primary:**
- Rust 1.91.0 - Core I/O operations, format readers/writers, range operations via PyO3 bindings
- Python 3.10+ (up to 3.14) - Public API, data manipulation wrappers, SQL utilities, extension module loading

**Secondary:**
- YAML - GitHub Actions CI/CD workflow definitions
- SQL - DataFusion query execution for distributed processing

## Runtime

**Environment:**
- Python: CPython 3.10, 3.11, 3.12, 3.13, 3.14
- Rust: 1.91.0 (specified in `rust-toolchain.toml`)
- Tokio 1.42.0 - Async runtime for Rust operations
- OS: Linux (Ubuntu 24.04), macOS (x86_64 and aarch64), Windows (x64)

**Package Manager:**
- Poetry 1.8.5+ - Python dependency management
- Cargo - Rust dependency management
- Lockfile: `poetry.lock` and `Cargo.lock` (both present)

## Frameworks

**Core:**
- Polars >= 1.37.1 - DataFrame/LazyFrame API
- DataFusion 50.x - SQL query engine and distributed computing backend
- DataFusion Python 50.1.0 - Python bindings for DataFusion
- PyO3 0.25.1 - Rust-Python FFI bindings with extension module support (`abi3` feature)

**Testing:**
- pytest ^8.3.3 - Test runner
- pytest-cov ^6.0.0 - Coverage reporting

**Build/Dev:**
- Maturin 1.7.5+ - Build tool for Python extension modules (Rust/Python)
- Black 24.10.0 - Code formatting
- isort 5.13.2 - Import sorting
- pre-commit - Git hooks for code quality
- Ruff ^0.8.2 - Linting

**Documentation:**
- MkDocs 1.6.1 - Static documentation generator
- MkDocs Material 9.5.48 - Material design theme
- mkdocs-jupyter 0.25.1 - Jupyter notebook support
- mkdocs-table-reader-plugin 3.1.0 - Markdown table rendering

## Key Dependencies

**Critical:**
- arrow 56.1.0 - Apache Arrow format support for columnar data interchange
- arrow-schema 56.1.0 - Arrow schema definitions
- arrow-array 56.1.0 - Arrow array implementations with FFI support
- polars (>=1.37.1, <1.38) - DataFrame/LazyFrame engine
- pyarrow (>=21.0.0 for Python <3.14, >=22.0.0 for >=3.14) - Arrow Python bindings

**Genomics Format Support:**
- datafusion-bio-format-vcf - VCF file reader/writer
- datafusion-bio-format-core - Base types and object storage abstraction
- datafusion-bio-format-gff - GFF/GTF format support
- datafusion-bio-format-fastq - FASTQ sequence format
- datafusion-bio-format-bam - BAM (Binary Alignment/Map) format
- datafusion-bio-format-cram - CRAM (Compressed Reference-oriented Map) format
- datafusion-bio-format-bed - BED (Browser Extensible Data) format
- datafusion-bio-format-fasta - FASTA sequence format
- datafusion-bio-format-pairs - Paired reads format
- datafusion-bio-format-gtf - GTF (Gene Transfer Format) support

**Infrastructure & Functions:**
- datafusion-bio-function-ranges - Interval overlap, nearest, merge operations
- datafusion-bio-function-pileup - Depth calculation (pileup) UDTF
- datafusion-bio-function-ranges (via git: github.com/biodatageeks/datafusion-bio-formats rev 333638a)
- datafusion-bio-function-pileup (via git: github.com/biodatageeks/datafusion-bio-functions rev 9ef64a1, default-features=false)

**Async & Utilities:**
- async-trait 0.1.86 - Async trait support
- futures 0.3.31 - Async utilities
- futures-util 0.3.31 - Async utility extensions
- tokio 1.42.0 - Async runtime with full features
- log 0.4.22 - Logging facade
- tracing 0.1.41 - Structured instrumentation
- pyo3-log 0.12.4 - Logging integration for PyO3
- serde_json 1.0 - JSON serialization
- rand 0.8.5 - Random number generation
- tqdm (>=4.67.0, <5) - Progress bars

**Optional Dependencies:**
- pandas - DataFrames compatibility (optional extras)
- bioframe - Genome annotation visualization
- matplotlib - Plotting
- pysam - Sequence/alignment utilities for testing

**Development & Benchmarking:**
- pyranges (dev, git: github.com/pyranges/pyranges rev 4f0a15)
- GenomicRanges 0.8.4 (dev)
- pybedtools 0.12.0 (dev)
- pygenomics (dev, git: gitlab.com/gtamazian/pygenomics rev 0.1.1)
- memory-profiler 0.61.0 (dev)
- py-cpuinfo 9.0.0 (dev)
- rich 13.9.4 (dev)
- psutil 6.1.1 (dev)
- jupyter ^1.1.0 (dev)
- jupyter_client ^8.6.3 (dev)

## Configuration

**Environment:**
- Python: Requires 3.10+ (specified in pyproject.toml `requires-python`)
- Polars version locked to >=1.37.1 (required for Arrow C Stream support)
- DataFusion version locked to 50.x (breaking changes in 51+)
- PyArrow version conditional: 21.0+ for Python <3.14, 22.0+ for >=3.14
- POLARS_FORCE_NEW_STREAMING env var auto-set based on Polars version (set to "1" for >=1.32)

**Build:**
- `rust-toolchain.toml`: Pins Rust 1.91.0 with rustfmt component
- `.cargo/config.toml`: Apple-specific linker flags for dynamic symbol resolution
  - `-Clink-arg=-undefined -Clink-arg=dynamic_lookup` for both x86_64 and aarch64 Darwin targets
- `pyproject.toml` maturin config: Produces `polars_bio` module (cdylib crate type)
- Maturin build settings: Uses sccache for compilation caching, manylinux=auto for Linux wheels
- Target CPU: skylake (default for Linux/Windows x86_64), apple-m1 for aarch64 macOS, skylake for x86_64 macOS

## Platform Requirements

**Development:**
- Python 3.10+ (pyenv or system)
- Rust 1.91.0 (via rustup)
- Build essentials: gcc/clang, pkg-config
- System libraries for bioinformatics: libbz2-dev, liblzma-dev, libcurl4-openssl-dev (for samtools/pysam)
- Poetry 1.8.5+ for Python dependency management
- Maturin 1.7.5+ for building extension module

**Production/Distribution:**
- CPython 3.10-3.14 with native extension support
- PyPI: Wheels built for:
  - Linux: manylinux (auto) x86_64
  - macOS: x86_64, aarch64
  - Windows: x64
- Source distribution (sdist) also available
- Installation: `pip install polars-bio`

**Deployment & Documentation:**
- Hosted on PyPI for package distribution
- Documentation built on ReadTheDocs (Ubuntu 24.04, Python 3.12)
- GitHub Pages for hosting generated docs (gh-pages branch)
- GitHub Actions runners: ubuntu-latest, macos-latest, macos-14, windows-latest

---

*Stack analysis: 2026-03-20*
