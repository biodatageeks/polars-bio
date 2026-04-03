# External Integrations

**Analysis Date:** 2026-03-20

## APIs & External Services

**BioDatageeks Custom Packages (via Git Dependencies):**
- datafusion-bio-formats (8 sub-crates for genomic formats)
  - SDK/Client: Rust crates compiled into libpolars_bio.so/dylib
  - Repository: https://github.com/biodatageeks/datafusion-bio-formats
  - Rev: 333638a

- datafusion-bio-functions (range operations, pileup)
  - SDK/Client: Rust crates compiled into libpolars_bio.so/dylib
  - Repository: https://github.com/biodatageeks/datafusion-bio-functions
  - Rev: 9ef64a1f16bedf4096ae37a0b332ce15f3a18255
  - Note: `datafusion-bio-function-pileup` compiled with `default-features=false` to avoid duplicate BAM dependency

**Remote File Access:**
- Object storage via datafusion-bio-format-core
  - Implementation: `PyObjectStorageOptions` in `src/option.rs`
  - Supports: S3, MinIO, and compatible services via object_store
  - Config options: `src/option.rs` lines 210-266
  - Chunk size, concurrent fetches, anonymous access, request payer, retry logic, timeout configurable

## Data Storage

**Databases:**
- None - This is a file-based/streaming-only system

**File Formats (I/O):**
Local filesystem and remote object storage (S3/MinIO-compatible):
- VCF (Variant Call Format) - Primary genomics format
  - Reader/Writer: `datafusion-bio-format-vcf`
  - Python API: `read_vcf()`, `scan_vcf()`, `write_vcf()`, `sink_vcf()` in `polars_bio/io.py`
  - Metadata extraction: VCF header information via `describe_vcf()`

- BAM/SAM/CRAM (Sequence Alignment Formats)
  - Readers: `datafusion-bio-format-bam`, `datafusion-bio-format-cram`
  - Python API: `read_bam()`, `read_sam()`, `read_cram()`, `scan_*()`, `write_*()`, `sink_*()`
  - Binary CIGAR handling: Configurable via `BamTableProvider::new()` binary_cigar param
  - Indexed reads: Auto-discovered BAI/CSI/TBI/CRAI indexes (handled by upstream)

- GFF/GTF (Gene/Genomic Feature Formats)
  - Reader: `datafusion-bio-format-gff`
  - Python API: `read_gff()`, `read_gtf()`, `scan_gff()`, `scan_gtf()`, `register_gff()`, `register_gtf()`
  - Attribute field handling: Re-registration via `_overlap_source()` in `polars_bio/io.py`

- FASTQ (Sequence Quality Format)
  - Reader/Writer: `datafusion-bio-format-fastq`
  - Python API: `read_fastq()`, `scan_fastq()`, `write_fastq()`, `sink_fastq()`

- FASTA (Sequence Format)
  - Reader: `datafusion-bio-format-fasta`
  - Python API: `read_fasta()`, `scan_fasta()`

- BED (Browser Extensible Data)
  - Reader: `datafusion-bio-format-bed`
  - Python API: `read_bed()`, `scan_bed()`

- PAIRS (Paired Reads Format)
  - Reader: `datafusion-bio-format-pairs`
  - Python API: `read_pairs()`, `scan_pairs()`

- Parquet & CSV
  - Native DataFusion support (standard SQL sources)
  - Python API: `read_table()`, `scan_table()`

**File Storage:**
- Local filesystem: Primary storage target
- Remote object storage: S3/MinIO-compatible via ObjectStorageOptions
  - Configuration: `PyObjectStorageOptions` class in `src/option.rs`
  - Credentials: Handled by underlying object_store library (env vars or IAM)
  - Test integration: MinIO service available in `it/docker-compose.yml`

**Caching:**
- None - Streaming architecture bypasses explicit caching

## Authentication & Identity

**Auth Provider:**
- None - Implicit (file system permissions, cloud provider IAM)

**Implementation:**
- S3/MinIO authentication: Via `PyObjectStorageOptions`
  - `allow_anonymous: bool` - Allow unsigned requests
  - `enable_request_payer: bool` - Set "requester pays" header
  - `max_retries: Option<usize>` - Retry configuration
  - `timeout: Option<usize>` - Request timeout in seconds
  - Credentials sourced from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY for S3)

## Monitoring & Observability

**Logging:**
- Framework: Rust `log` crate + `tracing` crate
- Integration: `pyo3-log` bridges Rust logs to Python logging
- Python entry point: `polars_bio/logging.py` and `set_loglevel()` function
- Configured via: `from polars_bio import set_loglevel`

**Tracing:**
- `tracing` crate 0.1.41 with log integration
- Used in async operations (Tokio runtime)

**Error Reporting:**
- Exceptions: Custom `CoordinateSystemMismatchError`, `MissingCoordinateSystemError` in `polars_bio/exceptions.py`
- Rust errors mapped to PyValueError, PyRuntimeError via PyO3

**No External Integrations:**
- No Sentry, Datadog, or similar error tracking
- No structured log shipping
- Local logging only

## CI/CD & Deployment

**Hosting:**
- PyPI (Python Package Index) - Primary distribution
- GitHub Releases - Release artifacts and changelog

**CI Pipeline:**
- GitHub Actions (workflows in `.github/workflows/`)
- Build matrix: Linux (ubuntu-latest), macOS (x86_64 and aarch64), Windows (x64)
- Python versions tested: 3.10, 3.11, 3.12, 3.13, 3.14

**Workflows:**
1. `publish_to_pypi.yml` - Main CI/CD
   - Triggered on: push to main/master, tags, PRs, workflow_dispatch
   - Tests on Linux (all Python versions)
   - Builds wheels: Linux (manylinux auto), macOS (x86_64, aarch64), Windows (x64), sdist
   - Publishes to PyPI using OIDC (id-token: write)
   - Skips on doc-only changes

2. `release.yml` - Version tagging
   - Triggered: workflow_dispatch
   - Creates GitHub tag and release via mathieudutour/github-tag-action
   - Generates changelog automatically

3. `publish_documentation.yml` - Docs deployment
   - Triggered: tag push or workflow_dispatch
   - Builds docs with mkdocs
   - Deploys to GitHub Pages (gh-pages branch)

4. `claude.yml` - Claude Code integration
   - Triggered by: @claude mentions in issues/PRs/comments
   - Uses: anthropics/claude-code-action

5. `benchmark.yml` - Performance testing
   - Scheduled benchmarks (not detailed here)

**Build Tools:**
- PyO3/maturin-action@v1 - Rust extension building
- sccache - Compilation result caching
- Target CPU optimization: skylake (Linux/Win x86_64), apple-m1 (macOS aarch64)
- RUSTFLAGS: -Dwarnings (strict warnings), -Ctarget-cpu={cpu}

**System Dependencies Installed (CI):**
- Linux: libbz2-dev, liblzma-dev, libcurl4-openssl-dev, zlib1g-dev, libdeflate-dev (for pysam)
- Poetry version pinned: 2.1.4 in CI

## Environment Configuration

**Required env vars:**
- `POLARS_FORCE_NEW_STREAMING` - Auto-set by `__init__.py` based on Polars version (>=1.32 → "1")
- DataFusion options set programmatically in `polars_bio/context.py`:
  - `datafusion.execution.target_partitions: "1"` - Single partition for deterministic execution
  - `datafusion.execution.parquet.schema_force_view_types: "true"` - Arrow view type compatibility
  - `datafusion.execution.skip_physical_aggregate_schema_check: "true"` - Schema flexibility
  - `bio.interval_join_algorithm: "coitrees"` - Interval join optimization

**Optional env vars (S3/MinIO access):**
- `AWS_ACCESS_KEY_ID` - S3 credentials (for minio: `test_user`)
- `AWS_SECRET_ACCESS_KEY` - S3 credentials (for minio: `test_secret`)
- CRAM index handling: Auto-discovery of .crai files (no config needed)

**Secrets location:**
- GitHub Actions: Repository secrets for `CLAUDE_CODE_OAUTH_TOKEN`
- No secrets stored in code (`.env` files excluded from git)

## Webhooks & Callbacks

**Incoming:**
- GitHub Actions webhooks: Issue comments, PR reviews, issue creation
- Claude Code Action: Listens for @claude mentions

**Outgoing:**
- PyPI publish: OIDC token-based (no webhook-style callbacks)
- GitHub Releases: Automatic changelog and artifact upload
- ReadTheDocs: Triggered on tag push (webhook configured externally)
- GitHub Pages: Triggered by branch push to gh-pages

## Object Storage Integration Details

**S3/MinIO Configuration (via `PyObjectStorageOptions`):**
```python
# From src/option.rs lines 210-266
class PyObjectStorageOptions:
    chunk_size: Optional[usize]           # Data chunk size in bytes
    concurrent_fetches: Optional[usize]   # Parallel fetch concurrency
    allow_anonymous: bool                 # Allow unsigned requests
    enable_request_payer: bool            # Set "requester pays" header
    max_retries: Optional[usize]          # Retry count
    timeout: Optional[usize]              # Request timeout (seconds)
    compression_type: str                 # AUTO/GZIP/BROTLI/ZSTD
```

**MinIO Test Integration:**
- Location: `it/docker-compose.yml`
- Service: minio image
- Ports: 9000 (API), 9001 (console)
- Default credentials: `test_user` / `test_secret`
- Data volume: `minio_data` (persistent)

---

*Integration audit: 2026-03-20*
