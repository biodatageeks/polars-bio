# External Integrations

**Analysis Date:** 2026-03-20

## APIs & External Services

**Genomic Data Format Libraries:**
- VCF (Variant Call Format) - `datafusion-bio-format-vcf` via DataFusion table provider
  - Reads/writes VCF files with sample subsetting and annotation support
  - Configuration: `VcfReadOptions`, `VcfWriteOptions` in `polars_bio/io.py`

- BAM/SAM/CRAM (Alignment Formats) - Via `datafusion-bio-format-bam`, `datafusion-bio-format-cram`
  - BAM/SAM: Binary and text alignment formats with tag parsing
  - CRAM: Compressed alignment format requiring `.crai` indexes (auto-discovered)
  - Configuration: `BamReadOptions`, `CramReadOptions`, tag type hints
  - Location: `polars_bio/io.py:read_bam()`, `read_sam()`, `read_cram()`

- GFF/GTF (Annotation Formats) - `datafusion-bio-format-gff`, `datafusion-bio-format-gtf`
  - Attribute field parsing and SQL query integration
  - Configuration: `GffReadOptions`, `GtfReadOptions`
  - Location: `polars_bio/io.py:read_gff()`, `read_gtf()`, `register_gff()`, `register_gtf()`

- FASTQ/FASTA (Sequence Formats) - `datafusion-bio-format-fastq`, `datafusion-bio-format-fasta`
  - Sequence read parsing with optional quality scores
  - Configuration: `FastqReadOptions`, `FastaReadOptions`
  - Location: `polars_bio/io.py:read_fastq()`, `read_fasta()`, `scan_fastq()`, `scan_fasta()`

- BED (Interval Format) - `datafusion-bio-format-bed`
  - Flexible BED3-BED12 format support with predefined schemas
  - Schemas defined in `polars_bio/io.py:SCHEMAS` dictionary
  - Location: `polars_bio/io.py:read_bed()`, `scan_bed()`

- PAIRS (Hi-C Interaction Format) - `datafusion-bio-format-pairs`
  - Contact frequency matrix format for 3D genomic analysis
  - Configuration: `PairsReadOptions`
  - Location: `polars_bio/io.py:read_pairs()`, `scan_pairs()`

**Bioinformatics Operations:**
- Range/Interval Operations - `datafusion-bio-function-ranges` UDTF
  - Overlap, nearest, merge, cluster, coverage, complement, subtract operations
  - Location: `src/operation.rs:do_range_operation()`, exposed as `pb.range_operations`
  - Python API: `polars_bio/range_op.py` wrapping Rust UDTF calls

- Pileup (Depth) Analysis - `datafusion-bio-function-pileup` UDTF
  - Calculates read depth across genomic regions from alignment files
  - Rust: `src/pileup.rs:DepthTableProvider`, `DepthFunction`
  - Python: `polars_bio/pileup_op.py:PileupOperations.depth()`
  - Configuration: `PileupOptions` PyO3 struct in `src/option.rs`

## Data Storage

**Databases:**
- Not applicable - Read/write operations are file-based only

**File Storage:**
- Local filesystem - Primary read/write storage
- Cloud object storage (via datafusion-bio-format-core):
  - AWS S3 - Full support with configurable parameters
  - Google Cloud Storage (GCS) - Full support with concurrent fetch optimization
  - Azure Blob Storage - Supported via datafusion-bio-format-core abstraction
  - HTTP/HTTPS - Read-only support for remote files

**Object Store Configuration (`PyObjectStorageOptions`):**
- `chunk_size` (int, default 8 MB) - Buffer size for remote reads; recommend 64 MB for large-scale ops
- `concurrent_fetches` (int, default 1) - GCS parallel fetch concurrency; recommend 8+ for large ops
- `allow_anonymous` (bool, default True) - GCS/S3 anonymous access flag
- `enable_request_payer` (bool, default False) - S3 requester-pays bucket support
- `max_retries` (int, default 5) - Network retry attempts for transient failures
- `timeout` (int, default 300 seconds) - Read operation timeout
- `compression_type` (str, default "auto") - BGZF/GZIP detection for FASTA/FASTQ

**Location:** `polars_bio/io.py` read/scan methods accept these parameters; passed to `datafusion-bio-format-core:ObjectStorageOptions`

**Caching:**
- None implemented - Relies on Polars lazy execution and DataFusion query caching

## Authentication & Identity

**Auth Provider:**
- None required for public data access
- AWS S3: Uses standard AWS credential chain (env vars, ~/.aws/credentials, IAM roles)
- GCS: Uses Application Default Credentials (env var, service account JSON)
- Azure: Uses SAS tokens or connection strings in path parameters

**Location:** Handled transparently by `datafusion-bio-format-core` via object store abstraction

## Monitoring & Observability

**Error Tracking:**
- None - Errors surface as Python exceptions

**Logs:**
- Rust → Python: `pyo3_log 0.12.4` bridges Rust `log` crate to Python logging
- Logging framework: `log 0.4.22` (Rust), Python `logging` module (Python)
- Tracing: `tracing 0.1.41` with log compatibility for structured logging
- Control: `polars_bio.set_loglevel()` in `polars_bio/logging.py`
- Default: Warning level and above displayed

**Location:**
- Rust logging: `src/lib.rs`, `src/context.rs` use `log::debug()`, `log::info()`, `log::error()`
- Python logging: `polars_bio/logging.py:set_loglevel()`
- Tracing instrumentation: Tokio runtime tasks with `tracing` crate

## CI/CD & Deployment

**Hosting:**
- PyPI (Python Package Index) - Primary distribution
- Bioconda (conda-forge) - Secondary distribution via automatic recipes

**CI Pipeline:**
- Platform: GitHub Actions (`.github/workflows/`)
- Python versions tested: 3.10, 3.11, 3.12, 3.13, 3.14
- Platforms built:
  - Linux x86_64 (manylinux auto, via ubuntu-latest)
  - Windows x64 (via windows-latest)
  - macOS x86_64 and aarch64 (via macos-14 and macos-latest)

**Build Process:**
- Tool: `maturin` (PyO3/maturin-action) - Mixed Rust-Python wheel builder
- Optimization: sccache for distributed compilation caching
- Artifacts: Wheels (bdist) and source distribution (sdist)
- Location: `.github/workflows/publish_to_pypi.yml`

**Release Process:**
- Trigger: Git tags matching `refs/tags/*`
- Steps:
  1. Parallel jobs build wheels for Linux/Windows/macOS/sdist
  2. Generate artifact attestation (build provenance)
  3. Upload to PyPI via OIDC trusted publisher (no stored API keys)
- Location: `.github/workflows/publish_to_pypi.yml:release` job
- Environment: `pypi` with permissions for `id-token`, `contents`, `attestations`

**Additional Workflows:**
- `update_bioconda_recipe.yml` - Auto-update bioconda recipe on releases
- `publish_documentation.yml` - Push mkdocs documentation
- `benchmark.yml` - Performance regression testing
- `claude.yml` - AI code review integration
- `claude-code-review.yml` - Supplementary code review

## Environment Configuration

**Required env vars:**
- None mandatory for basic operation
- AWS S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (if not using credential chain)
- GCS: `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)
- Polars behavior: `POLARS_FORCE_NEW_STREAMING` (auto-set in `polars_bio/__init__.py`)

**Secrets location:**
- GitHub: Repository secrets for PyPI OIDC trusted publisher token

## Webhooks & Callbacks

**Incoming:**
- None - No incoming webhook support

**Outgoing:**
- GitHub: Release artifacts uploaded via GitHub Actions API
- PyPI: Package uploads via `twine` / maturin `upload` command (OIDC-authenticated)
- Bioconda: Automatic recipe updates trigger downstream conda builds

---

*Integration audit: 2026-03-20*
