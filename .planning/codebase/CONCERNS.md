# Codebase Concerns

**Analysis Date:** 2026-03-20

## Tech Debt

**CRAM Write Limitation - MD and NM Tags Not Recoverable:**
- Issue: MD (mismatch string) and NM (edit distance) tags are written to CRAM files but cannot be read back due to noodles-cram library limitation
- Files: `polars_bio/io.py` (lines 2118-2119, 2160-2161)
- Impact: Users lose critical alignment metadata after writing to CRAM. Read-back workflows will be missing these tags
- Fix approach: Document clearly in docstrings (done), consider alternative CRAM implementation or accept BAM as workaround for tag-heavy workflows. Track upstream at https://github.com/biodatageeks/datafusion-bio-formats/issues/54

**Incomplete CSV Options Configuration:**
- Issue: CSV reader options are hardcoded with fixed delimiter and header detection; no customization exposed
- Files: `src/scan.rs` (line 16)
- Impact: Users cannot read custom delimiters or headerless CSV files
- Fix approach: Expose `CsvReadOptions` parameters via `ReadOptions` struct; add to Python API

**Global State for Table Name Generation:**
- Issue: Seven `AtomicU64` counters maintain table naming state across operations (NEAREST, OVERLAP, MERGE, COUNT_OVERLAPS, CLUSTER, COMPLEMENT, SUBTRACT)
- Files: `src/operation.rs` (lines 13-19)
- Impact: Potential counter overflow after 2^64 operations; table names predictable (security non-issue but code smell)
- Fix approach: Use UUID-based naming or add counter reset on high values

## Known Bugs

**Streaming Tests Flaky - Arrow C Stream Race Condition:**
- Symptoms: `test_streaming.py` tests fail intermittently when run in full suite; isolated runs pass
- Files: `tests/test_streaming.py` (module-level comment at line 30)
- Trigger: Running full test suite with `pytest tests/test_streaming.py` alongside other stream-consuming tests
- Root cause: Arrow C Stream exports from Polars have internal synchronization issues when multiple threads/tasks access streams simultaneously
- Workaround: Run streaming tests separately or use sequential test execution
- Fix approach: May need Polars 1.37.2+ patch; coordinate with Polars team on ArrowStreamExportable stability

**Lazy Evaluation Not Truly Lazy in GFF Operations:**
- Symptoms: GFF `_get_lazy()` helper materializes data during schema extraction
- Files: `polars_bio/interval_op_helpers.py` (TODO comment at line 1)
- Impact: Memory usage not optimized for large GFF files
- Fix approach: Defer schema inspection until collect time or refactor to lazy schema extraction

## Security Considerations

**Unwrap Chains in Rust Without Error Propagation:**
- Risk: Multiple `.unwrap()` calls on schema retrieval and table registration can panic on malformed input
- Files: `src/operation.rs` (lines 64, 105, 106, 122, 131, 137, 164, 198 - 22 total unwraps/expects)
- Current mitigation: Panic messages logged but application terminates
- Recommendations:
  - Replace `.unwrap()` with `.map_err(|e| PyRuntimeError::new_err(...))?` pattern
  - Add validation of table existence before schema extraction
  - Test with corrupted/incomplete input files

**Context Option Setting Without Error Handling:**
- Risk: Invalid DataFusion config options silently fail with generic message
- Files: `src/context.rs` (line 108)
- Current mitigation: Error logged at debug level only
- Recommendations: Validate known options before setting; return explicit error on validation failure

**Exception Handling Overly Broad in Python I/O:**
- Risk: Catching generic `Exception` masks real issues
- Files: `polars_bio/io.py` (lines 2373, 2489, 2597, 2674, 2691, 2741, 2779, 2788, 2918, 3121, 3162, 3197, 3244, 3264, 3311 - 15+ occurrences)
- Current mitigation: Exceptions logged before re-raising
- Recommendations: Catch specific exception types (IOError, ValueError, ArrowException) not bare Exception

**PyO3 Binding Type Mismatch - DataFusion Expr Incompatibility:**
- Risk: `py_read_sql()` returns PyDataFrame from polars_bio Rust crate; pip `datafusion` Expr from separate compilation unit → incompatible types
- Files: `polars_bio/io.py` (referenced in memory notes)
- Current mitigation: Bridge via `parse_sql_expr()` which creates binding-compatible Expr
- Recommendations: Document this workaround clearly; consider bundling datafusion with polars_bio to unify compilation units

## Performance Bottlenecks

**io.py Core Module Exceeds 3300 Lines:**
- Problem: Monolithic file with mixed concerns (read, write, metadata, predicates, conversion)
- Files: `polars_bio/io.py` (3355 lines)
- Cause: Historical growth; tight coupling between format handlers and option validation
- Impact: Hard to locate bugs; increased cognitive load; longer startup if imported partially
- Improvement path:
  - Extract format-specific handlers to `_handlers/bam.py`, `_handlers/vcf.py`, etc.
  - Move metadata logic to dedicated module
  - Move predicate translation to separate module (partially done)

**Predicate Translator Module-Level Global State:**
- Problem: Three global variables for column types (`_ACTIVE_STRING_COLS`, `_ACTIVE_UINT32_COLS`, `_ACTIVE_FLOAT32_COLS`) managed manually
- Files: `polars_bio/predicate_translator.py` (lines 61-63)
- Cause: Translation context passed implicitly via globals
- Impact: Not thread-safe; difficult to parallelize predicate translation
- Improvement path: Refactor to pass context as explicit parameter or use context manager

**Format Column Type Dictionary Using String Keys:**
- Problem: PyO3 `InputFormat` enum not hashable, so `_FORMAT_COLUMN_TYPES` uses string keys which require runtime format-to-string conversion
- Files: `polars_bio/io.py` (lines 60-68)
- Cause: PyO3 binding design
- Impact: Small overhead on each predicate validation; potential for key mismatch bugs
- Improvement path: Add `__hash__` to InputFormat or use enum-to-string registry pattern

**Batch Size Synchronization Manual:**
- Problem: Batch size must be manually synced between DataFusion (`datafusion.execution.batch_size`) and Polars streaming (`chunk_size`)
- Files: `polars_bio/range_op_io.py` (lines 54-55)
- Impact: Mismatches lead to suboptimal execution; easy to forget when tuning
- Improvement path: Centralize batch size configuration; add validation that both are set consistently

## Fragile Areas

**ArrowStreamExportable Integration (Polars >= 1.37.0):**
- Files: `src/lib.rs` (lines 100-160), `src/scan.rs` (lines 67-92), `tests/test_streaming.py` (lines 58-72)
- Why fragile:
  - Depends on unstable Polars internals (ArrowStreamExportable was added in 1.37.0)
  - GIL synchronization critical: Arrow FFI streams may need GIL for callbacks
  - Race conditions when multiple streams consumed in parallel
- Safe modification:
  - Test with minimum supported Polars version before releasing
  - Add feature flags for optional ArrowStreamExportable support
  - Wrap stream consumption with clear comments explaining GIL requirements
- Test coverage: `test_streaming.py::TestArrowCStreamSupport` exists but skipped in full suite

**Range Operation Table Registration and Naming:**
- Files: `src/operation.rs` (lines 145-164, 196-198)
- Why fragile:
  - SQL queries built as strings; no parameterized query support
  - Column names with special characters may break SQL
  - Table names assumed to exist after registration
- Safe modification:
  - Use DataFusion expression API instead of string concatenation
  - Add quotes around all identifiers
  - Validate table registration success before querying
- Test coverage: `test_overlap_algorithms.py` tests correctness but not edge cases (special chars, deep nesting)

**VCF Multisample Genotypes Schema - Two Formats Supported:**
- Files: `src/write.rs` (lines 60-98)
- Why fragile:
  - Supports two nested struct formats for genotypes (new and legacy)
  - Code silently falls back without indicating which format was found
  - Unknown shape returns empty Format fields with no warning
- Safe modification:
  - Add explicit error if genotypes schema is unrecognizable
  - Log which format variant was detected
  - Add schema validation in Python before write
- Test coverage: No test for schema mismatch or unexpected genotype structure

**Metadata Re-registration for GFF Attributes:**
- Files: Memory notes mention GFF re-register with attr_fields if needed; location `_overlap_source()` in `polars_bio/io.py`
- Why fragile:
  - Attribute column extraction must match downstream expectations
  - Metadata lost/corrupted if re-registration fails silently
- Safe modification:
  - Validate metadata before re-registration
  - Return error if attributes cannot be parsed
  - Add comprehensive test of attribute round-tripping

## Scaling Limits

**Atomic Counter Overflow in Table Naming:**
- Current capacity: 2^64 unique table names per counter (~18 quintillion)
- Limit: Arithmetic overflow if counter exceeds u64 max; wrapped behavior undefined in Rust
- Scaling path: Switch to UUID-based naming or reset counter periodically

**Memory Usage in Eager Operations:**
- Current capacity: Limited by available RAM (no streaming for some formats)
- Limit: Large VCF/BAM files must fit in memory for eager read operations
- Scaling path: Enforce LazyFrame returns by default; add early warning for large files

**Polars Lazy Execution Depth:**
- Current capacity: Unknown query plan complexity supported
- Limit: Deeply nested range operations (e.g., overlap(overlap(overlap(...)))) may exceed DataFusion optimizer budget
- Scaling path: Monitor query plan size; add optimization fusion passes

## Dependencies at Risk

**Pinned Git Commits for Bio Format Crates:**
- Risk: Ten `datafusion-bio-*` crates pinned to specific git commits; updates require manual Cargo.toml edits
- Files: `Cargo.toml` (lines 28-40) - all pinned to rev `333638a` and `9ef64a1f...`
- Impact: No automatic security updates; incompatible with crates.io versioning
- Migration plan:
  - Switch to crates.io published versions once stable
  - If crates.io unavailable, use github tarball releases
  - Add quarterly security audit for pinned commit status

**DataFusion Python 50.1.0 (Pre-1.0 API):**
- Risk: Pre-1.0 version; API changes expected
- Files: `Cargo.toml` line 12, `pyproject.toml` line 16
- Impact: Future versions may break PyDataFrame serialization
- Migration plan: Monitor DataFusion releases; prepare upgrade path for 51.0

**Polars Version Constraint >= 1.37.1:**
- Risk: Minimum version tied to ArrowStreamExportable feature; older Polars users cannot use GIL-free streaming
- Files: `pyproject.toml` (line 15), `Cargo.toml` (build-system line 3)
- Impact: Package requires latest Polars; some enterprises may lag versions
- Recommendations: Add fallback path for Polars < 1.37.1 that disables streaming

## Missing Critical Features

**CSV Format Not Fully Supported:**
- Problem: CSV reading implemented but options not exposed; no CSV-specific optimizations
- Blocks: Users cannot read custom delimiters, handle missing headers, or skip rows
- Priority: Low (workaround: convert to Parquet first)

**CRAM Tag Loss on Write:**
- Problem: MD/NM tags cannot round-trip through CRAM
- Blocks: Workflows requiring exact tag preservation must use BAM
- Priority: High for CRAM-focused users; affects 10% of use cases

**Predicate Pushdown for Complex Expressions:**
- Problem: Unsupported operators in predicates (e.g., `contains`, `regex`, string functions) fall back to client-side filtering
- Blocks: Efficient filtering on computed columns unavailable
- Priority: Medium; workaround is client-side but slow for large files

## Test Coverage Gaps

**Streaming Tests Isolated - Not Run in Full Suite:**
- What's not tested: Streaming behavior when other tests consume Arrow streams simultaneously
- Files: `tests/test_streaming.py` (not included in default pytest run due to race conditions)
- Risk: Streaming integration bugs may not be caught until production
- Recommendation: Fix flakiness (see Known Bugs) and integrate into CI/CD

**Error Handling for Malformed Genomic Files:**
- What's not tested: Corrupt BAM headers, missing CRAM indexes, invalid VCF records
- Files: No dedicated test file for error handling
- Risk: Panic instead of graceful error on corrupted input
- Recommendation: Add `tests/test_error_handling.py` with intentionally malformed files

**PyO3 Binding Edge Cases:**
- What's not tested: Type coercion on schema mismatch, invalid option setting, table deregistration race conditions
- Files: No tests for `BioSessionContext` error paths
- Risk: Hidden panics in Rust layer
- Recommendation: Add tests for all `PyResult` unwraps in `src/context.rs`

**Wide DataFrames (100+ Columns):**
- What's not tested: Column projection, selection, predicate pushdown with many columns
- Files: `tests/test_wide_dataframes.py` exists but only tests read/write, not optimization
- Risk: Memory bloat or O(n^2) performance on column operations
- Recommendation: Add tests for projection pushdown efficiency with 500+ column files

**GFF Attribute Field Round-Tripping:**
- What's not tested: Custom attribute parsing, re-registration with filtered attributes, metadata preservation across operations
- Files: No comprehensive test for attribute schema stability
- Risk: Silent attribute loss in complex workflows
- Recommendation: Add `tests/test_gff_attributes_comprehensive.py`

**Range Operations with Overlapping Boundary Coordinates:**
- What's not tested: Overlap/nearest with identical start/end, zero-length intervals, negative coordinates
- Files: `tests/test_overlap_algorithms.py` uses realistic data but not boundary cases
- Risk: Algorithm correctness not verified for edge cases
- Recommendation: Add parameterized tests for boundary conditions

---

*Concerns audit: 2026-03-20*
