# Codebase Concerns

**Analysis Date:** 2026-03-20

## Tech Debt

**PyO3 InputFormat Hashability:**
- Issue: `InputFormat` enum from PyO3 binding is not hashable, preventing use as dictionary keys. Workaround uses string keys instead.
- Files: `polars_bio/io.py` (line 59-68), defines `_FORMAT_COLUMN_TYPES` dict with string keys like `"Bam"`, `"Sam"`, `"Cram"`
- Impact: Code is less type-safe; string keys can mask runtime errors. Type conversions needed at multiple call sites: `str(fmt).rsplit(".", 1)[-1]`
- Fix approach: Monitor upstream PyO3 releases for enum hashability support. Consider creating a wrapper enum in Rust if hashability remains unavailable.

**Arrow C Stream Consumption Race:**
- Issue: `ArrowCStreamPartitionStream` in `src/scan.rs` (lines 72-108) uses `Arc<Mutex<Option<>>>` to guard single consumption of Arrow C stream. The `.take()` pattern can panic if stream is accessed twice.
- Files: `src/scan.rs` (lines 100-106: `stream_reader.lock().unwrap().take().expect(...)`)
- Impact: If stream partition is executed multiple times (rare but possible under certain join/union scenarios), the `.expect("Arrow C Stream already consumed")` will panic with cryptic message instead of graceful error.
- Fix approach: Replace panic with proper error handling that returns `Result`. Implement backpressure to detect double-consume at partition creation time rather than execution time.

**Global Module-Level State in Predicate Translator:**
- Issue: `predicate_translator.py` uses module-level mutable globals `_ACTIVE_STRING_COLS`, `_ACTIVE_UINT32_COLS`, `_ACTIVE_FLOAT32_COLS` (lines 61-63).
- Files: `polars_bio/predicate_translator.py` (lines 61-63, 97-100)
- Impact: Not thread-safe. If multiple threads call `translate_predicate()` concurrently, global state will be corrupted. Query results will use wrong column type sets for validation.
- Fix approach: Pass column type sets as parameters through all helper functions instead of globals. Alternatively, use `threading.local()` like `conftest.py` does (line 19).

**Unvalidated Unwraps in Range Operations:**
- Issue: `src/operation.rs` contains multiple `.unwrap()` calls on required fields that should be validated at Python layer but are not.
- Files: `src/operation.rs` (lines 105-106: `columns_1.unwrap()`, `columns_2.unwrap()` in `do_nearest()`; line 135: `range_opts.filter_op.unwrap()`)
- Impact: Panics with generic message instead of user-friendly error when required parameters are missing. Users see Rust stack traces instead of clear error messages.
- Fix approach: Validate all required options in Python before calling Rust (in `range_op_helpers.py`). Add context-specific error messages in Rust error conversions.

**CSV Reader FIXME:**
- Issue: `src/scan.rs` line 287 has `CsvReadOptions::new() //FIXME: expose` - CSV read options are hard-coded with no user control.
- Files: `src/scan.rs` (line 287)
- Impact: Users cannot customize CSV parsing behavior (delimiter, quote character, encoding, header row number, etc.). Limits CSV format utility.
- Fix approach: Add `CsvReadOptions` struct to PyO3 bindings (similar to `BamReadOptions`, `VcfReadOptions`). Expose in `IOOperations.scan_csv()` method signature.

**Panic Message Placeholder in Config:**
- Issue: `src/context.rs` line 108 has `.expect("TODO: panic message")` - placeholder panic handler without descriptive message.
- Files: `src/context.rs` (line 108)
- Impact: When config option setting fails, users get unhelpful "TODO" message instead of actual error details. Debugging config issues is harder.
- Fix approach: Replace with proper error message that includes key, value, and underlying error reason.

## Known Bugs

**Test Flakiness in Streaming Mode:**
- Symptoms: `test_streaming.py` tests are flaky when run in full test suite. Tests pass when run individually.
- Files: `tests/test_streaming.py` (documented in MEMORY.md as "flaky when run in full suite")
- Trigger: Running full test suite; Arrow C Stream consumed during concurrent test execution
- Workaround: Run `test_streaming.py` tests separately: `pytest tests/test_streaming.py -v` instead of `pytest tests/test_*.py`
- Root cause: Arrow C Stream is single-consumption only (see Arrow C Stream Race above). When multiple test classes use streaming operations, race conditions occur.

**CRAM Index Auto-Discovery Fragility:**
- Symptoms: CRAM file reads fail with cryptic "index not found" error even when `.crai` file exists.
- Files: `src/scan.rs` relies on upstream `datafusion-bio-formats` (Cargo.toml git rev `333638a`)
- Trigger: CRAM files with mismatched or corrupt index; index created with older samtools versions
- Workaround: Re-index CRAM files with current samtools: `samtools index file.cram`
- Root cause: Index auto-discovery happens upstream; older/corrupt indexes cause silent failures. No validation that index matches file before use.

**Coordinate System Metadata Loss on collect():**
- Symptoms: After `lf.collect()` on a LazyFrame with coordinate system metadata set via `config_meta.set()`, metadata is lost on the resulting DataFrame.
- Files: `polars_bio/io.py` (lines 510-514, 766-770) - workaround re-sets metadata after collect
- Trigger: Using `scan_*` methods (LazyFrame) → `collect()` without explicit metadata re-application
- Workaround: Current code re-applies metadata in all read methods. Document that `.config_meta.get_metadata()` must be called before `.collect()`.
- Root cause: Polars' `polars-config-meta` library doesn't persist metadata through collect(). This is a limitation of the metadata storage mechanism.

## Security Considerations

**Object Storage Credentials in Plaintext:**
- Risk: Object storage options (AWS S3 access keys, GCS credentials) are passed as `PyObjectStorageOptions` struct. If code serializes these options or logs them, credentials leak.
- Files: `polars_bio/io.py` (all methods accepting `allow_anonymous`, `enable_request_payer`, `chunk_size` parameters); `src/option.rs` (PyO3 `PyObjectStorageOptions` struct)
- Current mitigation: Options are only used to create `ObjectStorageOptions` in Rust and are not logged or serialized. However, no explicit scrubbing in error messages.
- Recommendations:
  1. Audit all error paths to ensure credentials are not included in exception messages
  2. Add `#[serde(skip)]` attributes to credential fields if serialization is ever added
  3. Document that object storage credentials should come from environment variables or provider chains, not hardcoded parameters

**SQL Injection via Dynamic Query Building:**
- Risk: `src/operation.rs` builds SQL queries dynamically (lines 135-188, 250+) using string concatenation.
- Files: `src/operation.rs` (all `ctx.sql(&query)` calls); `polars_bio/sql.py` (query construction)
- Current mitigation: Queries are constructed from validated user inputs (table names, column names), not directly from user strings. However, column name validation is implicit.
- Recommendations:
  1. Explicitly validate table and column names against schema before building queries
  2. Use parameterized queries where possible
  3. Document that table/column names must be valid SQL identifiers

**Git Dependency on Private Repo:**
- Risk: Cargo.toml uses git dependency on `https://github.com/biodatageeks/datafusion-bio-formats.git` (rev `333638a`). Private repo or network disruption blocks builds.
- Files: `Cargo.toml` (lines 28-37, 39-40)
- Current mitigation: Hard pinned to specific rev for reproducibility
- Recommendations:
  1. Publish upstream crates to crates.io when stable
  2. Document build prerequisites (git access to biodatageeks org)
  3. Add build status checks for upstream dependency compilation

## Performance Bottlenecks

**Global Tokio Runtime Creation in Tests:**
- Problem: Each test method that calls Rust functions creates a new `Runtime::new()` in `src/lib.rs` and `src/operation.rs`. Tokio runtime creation is expensive (~10ms per instantiation).
- Files: `src/lib.rs` (multiple functions create runtime), `src/operation.rs` (line 28+), `src/context.rs` (line 73)
- Cause: Python test isolation requires clean runtime state per test, but no pooling or reuse mechanism exists
- Improvement path: Implement thread-local runtime cache that's cleared between test isolation boundaries. Measure impact with `test_lazy_streaming_fix.py` memory profiling (uses `tracemalloc`).

**Predicate Translation Permissiveness:**
- Problem: When no column type information is provided to `translate_predicate()` (no string_cols/uint32_cols/float32_cols), all operators are accepted. Type mismatches are caught by DataFusion at execution time (slower), not translation time.
- Files: `polars_bio/predicate_translator.py` (lines 72-100, 77-89 notes "permissive" behavior)
- Cause: Some predicates don't have enough type context at translation time (e.g., on dynamically created columns)
- Improvement path: Add early type inference step for common predicates before translation. Cache column type information from schema metadata.

**Memory Growth in Pileup Operations:**
- Problem: `PileupOperations.depth()` in `polars_bio/pileup_op.py` accumulates all records in memory before reducing to blocks. Large BAM files (>100GB) cause OOM.
- Files: `polars_bio/pileup_op.py` (lines 51-100), `src/pileup.rs` (uses `DenseMode` accumulation)
- Cause: Dense mode accumulation stores coverage for every position in memory. Sparse mode exists but requires larger files to be economical.
- Improvement path: Auto-select sparse mode for files >50GB. Implement streaming block output instead of full accumulation. Document `dense_mode="disable"` parameter for memory-constrained environments.

## Fragile Areas

**PyO3 Binding Compilation Coupling:**
- Files: `src/lib.rs`, `polars_bio/io.py` (all PyO3 imports)
- Why fragile: PyO3 version mismatch between Cargo.toml (0.25.1) and Python package breaks at runtime. DataFusion version mismatch (50.3.0 in Cargo vs separate pip install) causes type incompatibility.
- Safe modification:
  1. Always update Cargo.toml and `maturin develop` together
  2. Test with `python -c "import polars_bio; print(polars_bio.__version__)"` after building
  3. Run `cargo check` before `maturin develop` to catch compilation errors early

**Predicate Translator Enum Matching:**
- Files: `polars_bio/predicate_translator.py` (lines 150+, matching Polars expression types)
- Why fragile: Matches on `expr.meta.output_name()` and expression type names (strings). Polars internal expression types change between versions; matching becomes incorrect.
- Safe modification:
  1. Add unit tests for each Polars expression type supported
  2. Document minimum/maximum Polars version compatibility
  3. Add version check at module load time if using version-specific features

**Coordinate System Metadata Round-Trip:**
- Files: `polars_bio/io.py` (all read/scan methods with `set_coordinate_system()` calls)
- Why fragile: Metadata is set via polars-config-meta library, which has limited persistence. `.collect()` loses metadata; workaround re-applies. Future Polars versions may change metadata API.
- Safe modification:
  1. Always call `lf.config_meta.get_metadata()` before `.collect()` and re-apply after
  2. Test with new Polars versions to verify metadata persistence still works
  3. Document that range operations require coordinate system metadata to be present

## Scaling Limits

**Single-Partition Arrow C Stream:**
- Current capacity: Arrow C Stream from Polars LazyFrame is single-partition only. `register_frame_from_arrow_stream()` in `src/scan.rs` (lines 223-243) creates one partition per stream.
- Limit: When source LazyFrame has 1 partition but target Polars session expects 8+ partitions, parallelism is limited. DataFrame joins with multi-partition tables become unbalanced.
- Scaling path: Implement auto-repartitioning that splits Arrow C Stream into multiple partitions before registration (requires buffering batches or seeking capability).

**Tag Type Inference Sampling:**
- Current capacity: Default sample size is 100 records (parameter `infer_tag_sample_size` in `IOOperations.scan_bam()`, line 718)
- Limit: For BAM files with rare custom tags (appearing after record 100), tag type inference misses them and defaults to string
- Scaling path: Implement two-pass inference: first pass samples head, second pass scans tail if new tags found. Add user-adjustable sampling strategy.

**GFF Attribute Field Parsing:**
- Current capacity: GFF attributes parsed from single `attributes` string field. If GFF has 1000+ attributes per record, parsing overhead scales O(n)
- Limit: Files with >100 attribute fields per record show significant slowdown
- Scaling path: Implement lazy attribute parsing (parse only requested fields). Cache parsed attributes for repeated access.

## Dependencies at Risk

**Datafusion-bio-formats GitHub Dependency:**
- Risk: Three crates pinned to git rev `333638a` in Cargo.toml (lines 28-37):
  - datafusion-bio-format-vcf
  - datafusion-bio-format-core
  - datafusion-bio-format-gff
  - datafusion-bio-format-fastq
  - datafusion-bio-format-bam
  - datafusion-bio-format-cram
  - datafusion-bio-format-bed
  - datafusion-bio-format-fasta
  - datafusion-bio-format-pairs
  - datafusion-bio-format-gtf
- Impact: If upstream repo goes offline or rev is deleted, `cargo build` fails. No fallback to published crate versions.
- Migration plan: Publish stable versions to crates.io. Maintain pin to rev but add comments linking to published version number for fallback.

**DataFusion Version Rigidity:**
- Risk: `datafusion = { version = "50.3.0" }` and `datafusion-python = "50.1.0"` must match. No version range flexibility.
- Impact: Cannot upgrade to DataFusion 51+ without coordinating three crate updates simultaneously
- Migration plan: Use version ranges like `">=50.3.0, <51"` to allow patch updates. Test against min/max versions in CI.

**Unused PyO3 Features:**
- Risk: `pyo3 = { version = "0.25.1", features = ["extension-module", "abi3"] }` includes `abi3` but project may not need stable ABI
- Impact: Unnecessary constraint on Python version compatibility. ABI3 builds are slower.
- Migration plan: Benchmark with/without `abi3`. If not needed, remove feature to speed builds.

## Test Coverage Gaps

**Error Path Coverage:**
- What's not tested: Error handling in Rust FFI boundary (PyO3 exception conversion). Paths like "network error during object store read", "corrupt VCF header", "BAM file with no sequences" are not tested.
- Files: `tests/test_io_*.py` files test happy path only. No `test_io_errors.py` or similar error-specific tests.
- Risk: Errors in production use will surface with unhelpful Rust panic messages instead of Python exceptions
- Priority: **High** - Users encountering file corruption or network issues get cryptic failures

**Concurrent Access Patterns:**
- What's not tested: Multiple threads/processes accessing same registered table via `ctx.table()`. Global state in predicate translator under concurrent load.
- Files: `tests/conftest.py` uses `threading.local()` but no tests verify concurrent predicate translation
- Risk: Race conditions in multi-threaded environments (e.g., Jupyter with concurrent cells)
- Priority: **High** - Silent data corruption or wrong results in concurrent scenarios

**Large File Handling:**
- What's not tested: VCF/BAM files >10GB, GFF files with >1M records, pileup operations on whole-genome BAM
- Files: Test data in `tests/` directory is small (~MB scale); no large file tests
- Risk: OOM panics or timeouts in production when files are larger than test cases
- Priority: **Medium** - Affects production users but not all deployments

**Coordinate System Cross-Format Mixing:**
- What's not tested: Mixing 0-based BAM reads with 1-based VCF data in range operations; implicit coordinate conversion correctness
- Files: `tests/test_coordinate_system_metadata.py` exists but doesn't test cross-format range operations
- Risk: Silent incorrect results if coordinate system conversion is wrong in one direction
- Priority: **High** - Affects scientific correctness

**Index File Edge Cases:**
- What's not tested: Missing index files (should fall back to sequential read), corrupt index files (should error gracefully), index created by different tool than data (version incompatibility)
- Files: `tests/test_io_indexed.py` tests presence of index, not absence or corruption scenarios
- Risk: Unexpected panics or hangs when index is absent or mismatched
- Priority: **Medium** - Important for robustness but rare in practice

---

*Concerns audit: 2026-03-20*
