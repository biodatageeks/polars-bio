# Technology Stack

**Analysis Date:** 2026-03-20

## Languages

**Primary:**
- Rust 1.88.0+ - Core computational engine with PyO3 bindings for genomic operations
- Python 3.10-3.14 - User-facing API and high-level operations

**Secondary:**
- YAML - GitHub Actions CI/CD workflows

## Runtime

**Environment:**
- CPython (standard interpreter)
- PyPy (supported via ABI3)

**Package Manager:**
- Poetry (Python) - Version 2.1.4
- pip (Python) - Fallback dependency installation
- Cargo (Rust) - Rust package and build management
- Lockfile: `Cargo.lock` and `pyproject.toml` with pinned versions

## Frameworks

**Core:**
- Polars 1.37.1+ - High-performance DataFrame library (required)
- DataFusion 50.3.0 - SQL query engine for predicate/projection pushdown
- PyO3 0.25.1 - Python-Rust interoperability with ABI3 extension modules
- datafusion-python 50.1.0 - Python bindings for DataFusion

**Data Processing:**
- Arrow (Apache Arrow) 56.1.0 - Columnar data format and FFI interop
- arrow-array 56.1.0 - Array operations with FFI support

**Async & Concurrency:**
- Tokio 1.42.0 - Async runtime with full feature set and tracing
- futures 0.3.31 - Future combinators
- async-trait 0.1.86 - Async trait support

**Testing:**
- pytest 8.3.3+ (Python) - Test framework
- pytest-cov 6.0.0+ - Coverage measurement

**Build/Dev:**
- maturin 1.7.5+ - Rust-Python package builder
- sccache - Distributed compiler cache (GitHub Actions)

## Key Dependencies

**Critical:**
- datafusion-bio-format-vcf - VCF file format handler (git: rev 333638a)
- datafusion-bio-format-bam - BAM/SAM alignment format handler (git: rev 333638a)
- datafusion-bio-format-cram - CRAM compressed alignment format (git: rev 333638a)
- datafusion-bio-format-gff - GFF/GTF annotation format (git: rev 333638a)
- datafusion-bio-format-fastq - FASTQ sequence format (git: rev 333638a)
- datafusion-bio-format-fasta - FASTA sequence format (git: rev 333638a)
- datafusion-bio-format-bed - BED interval format (git: rev 333638a)
- datafusion-bio-format-pairs - PAIRs interaction format (git: rev 333638a)
- datafusion-bio-format-core - Core storage and object store abstraction (git: rev 333638a)

**Functions:**
- datafusion-bio-function-ranges - Range overlap/interval operations UDTF (git: rev 9ef64a1)
- datafusion-bio-function-pileup - Pileup depth calculation UDTF, default-features=false (git: rev 9ef64a1)

**Utilities:**
- pyo3-log 0.12.4 - Python logging integration with Rust
- log 0.4.22 - Rust logging facade
- tracing 0.1.41 - Structured tracing with log compatibility
- serde_json 1.0 - JSON serialization
- rand 0.8.5 - Random number generation
- tqdm 4.67.0+ - Progress bar display

**Optional:**
- pyarrow 21.0.0-23 (Python 3.10-3.13), 22.0.0-23 (Python 3.14+) - Arrow Python API
- polars-config-meta 0.3.0+ - Polars metadata support
- typing-extensions 4.14.0+ - Type hint backports
- pandas - For optional dataframe conversion (optional extra: `pip install polars-bio[pandas]`)
- pysam - For genomics testing and SAM tag validation (optional extra: `pip install polars-bio[test]`)
- bioframe 0.8.0+ - For optional visualization (optional extra: `pip install polars-bio[viz]`)
- matplotlib - For optional interval visualization (optional extra: `pip install polars-bio[viz]`)

## Configuration

**Environment:**
- POLARS_FORCE_NEW_STREAMING - Enabled for Polars >= 1.32, disabled for earlier versions (set in `polars_bio/__init__.py`)
- RUSTFLAGS (build-time) - Compiler flags for optimization and linking
  - Linux/Windows: `-Dwarnings -Ctarget-cpu=skylake` (fail on warnings, target CPU)
  - macOS x86_64: `-Dwarnings -Clink-arg=-undefined -Clink-arg=dynamic_lookup -Ctarget-cpu=skylake`
  - macOS ARM64: `-Dwarnings -Clink-arg=-undefined -Clink-arg=dynamic_lookup -Ctarget-cpu=apple-m1`
- POETRY_VERSION: 2.1.4 (CI/CD)
- TARGET_CPU: skylake (default optimization target)

**Build:**
- `pyproject.toml` - PEP 517/518 build spec with maturin backend
- `Cargo.toml` - Rust library and dependencies
- `Cargo.lock` - Locked Rust dependency versions
- `.cargo/config.toml` - macOS linking flags for dynamic symbol lookup

## Platform Requirements

**Development:**
- Rust toolchain 1.88.0+ (`rustup show` validates in CI)
- Python 3.10 minimum (3.14 maximum tested)
- Poetry 2.1.4 for reproducible builds
- System libraries for pysam optional build:
  - libbz2-dev, liblzma-dev, libcurl4-openssl-dev
  - zlib1g-dev, libdeflate-dev (for Linux testing)

**Production:**
- Any Linux distribution (x86_64) or Windows (x64) or macOS (x86_64, aarch64)
- Python 3.10-3.14
- Polars 1.37.1+
- DataFusion 50.0.0+
- PyArrow 21.0.0+ (version-dependent on Python version)

**Deployment:**
- PyPI package repository - Primary distribution channel
- Bioconda package repository - Conda distribution
- GitHub Actions - CI/CD runner for cross-platform builds

---

*Stack analysis: 2026-03-20*
