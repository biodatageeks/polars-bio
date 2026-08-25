# Design: Cooler (.cool/.mcool) support

## Context

Cooler files are HDF5 containers following the cooler schema (v3). A *data collection* holds four groups:

- `chroms`: `name` (string), `length` (int32)
- `bins`: `chrom` (integer/enum referencing `chroms`), integer `start`/`end` coordinates, plus optional value columns — most commonly balancing weights such as `weight` (float64)
- `pixels`: `bin1_id` (int64), `bin2_id` (int64), and an integer or floating `count` dataset — upper-triangle COO sparse matrix, sorted by (`bin1_id`, `bin2_id`)
- `indexes`: `chrom_offset` (int64, maps chrom → first bin id) and `bin1_offset` (int64, maps bin1_id → first pixel row) — a CSR-style row index

A `.cool` file has one data collection at the HDF5 root. An `.mcool` file nests one data collection per resolution under `/resolutions/<binsize>`. The cooler ecosystem addresses these with URI syntax `file.mcool::/resolutions/10000`. Root attributes (`format`, `format-version`, `bin-size`, `nbins`, `nnz`, `sum`, `genome-assembly`) provide metadata without scanning data.

Unlike every other polars-bio format, noodles has no cooler support, so the provider needs generic Rust HDF5 tooling.

## Goals

- Read and lazily scan the pixels table of `.cool` and `.mcool` data collections as a genomic-interval-pair DataFrame.
- Keep the public API consistent with existing `scan_*`, `read_*`, `describe_*`, and `register_*` methods, and with the existing Hi-C `pairs` reader.
- Preserve polars-bio coordinate-system metadata and `use_zero_based` behavior.
- Push lazy projections into provider execution.
- Push genomic predicates on the first (row) axis into pixel row-range pruning via `chrom_offset`/`bin1_offset`.
- Use DataFusion `target_partitions` for parallel scans where the HDF5 stack permits.

## Non-Goals

- Writing cooler files.
- Single-cell `.scool` containers.
- Dense 2-D matrix output or a `fetch`-style matrix-selector API (region selection happens through predicates on the tabular output).
- Computing balanced counts (`count * weight1 * weight2`) inside the provider — users compose this in Polars from the exposed weight columns.
- S3/GCS/HTTP access in the first version (HDF5 needs a seekable local source; the C library reads local paths).
- Aggregation/zoomify (creating new resolutions).

## Decisions

### HDF5 access layer

Use the `hdf5-metno` crate (maintained fork of `hdf5-rust`) with the `static` feature so libhdf5 is built from source and statically linked — no system HDF5 required at runtime, which is mandatory for distributing wheels.

- Alternatives considered:
  - **System libhdf5 (dynamic)**: rejected — breaks the self-contained wheel requirement on all three platforms and bioconda packaging.
  - **`hidefix`** (pure-Rust concurrent HDF5 chunk reader): attractive for lock-free parallel reads, but read-path coverage of enum/compound types and indexing maturity must be verified; kept as a follow-up optimization, not the initial dependency.
  - **Hand-rolled HDF5 subset parser**: rejected — high effort, cooler files in the wild vary (compression filters, chunking) and libhdf5 handles all of them.
- The feasibility spike (tasks section 1) MUST validate that `hdf5-metno` + `hdf5-src` static builds succeed in the polars-bio wheel CI matrix (manylinux, macOS arm64/x86_64, Windows) and measure the build-time cost before provider work proceeds. If static linking fails on any target, that platform's wheel gates the feature behind a cargo feature flag and the proposal returns for re-scoping.
- Only the read API surface of `hdf5-metno` is used; no HDF5 write features are enabled.

### Provider location and structure

Implement provider logic in `biodatageeks/datafusion-bio-formats` as a new `datafusion-bio-format-cooler` crate, consistent with all other formats:

- `CoolerTableProvider` — opens a data collection, resolves resolution/URI, exposes schema and statistics (`nnz` as row count).
- `CoolerExec` — streams pixel row ranges as Arrow record batches.
- A small `CoolerMetadata` reader for `describe_cool` (resolutions, bin size, chroms, nnz, assembly) that never touches `pixels`.

This polars-bio OpenSpec change is the authoritative tracker for both repositories.

### URI and resolution handling

- `scan_cool(path, resolution=None)` accepts:
  - a `.cool` path — `resolution` optional; if given it MUST match the file's `bin-size`, otherwise error.
  - an `.mcool` path plus `resolution=N` — resolves to `/resolutions/N`; a missing resolution errors listing available ones.
  - a cooler URI `file.mcool::/resolutions/N` — the `::` suffix wins; passing a conflicting `resolution` errors.
  - an `.mcool` path with exactly one stored resolution and no `resolution` — resolves to it; multiple resolutions without `resolution` errors listing them.
- Detection is by HDF5 structure (`format` attribute / presence of `/resolutions`), not file extension, so nonstandard extensions still work.
- One API pair covers both formats; there is no separate `scan_mcool`.

### API shape

- `scan_cool(path, resolution=None, join_bins=True, include_weights=False, projection_pushdown=True, predicate_pushdown=True, use_zero_based=None)`
- `read_cool(...)` — eager wrapper, same signature.
- `describe_cool(path)` — returns a DataFrame of data collections: resolution/bin size, bin type, chrom count, nbins, nnz, sum, assembly, format version.
- `register_cool(path, name=None, resolution=None, join_bins=True, include_weights=False, use_zero_based=None)` for SQL.
- Scan partitioning follows the session's `datafusion.execution.target_partitions` setting rather than a per-call thread or parallel flag.

### Schema

Default output (`join_bins=True`), one row per pixel:

- `chrom1: Utf8`, `start1: UInt32`, `end1: UInt32`
- `chrom2: Utf8`, `start2: UInt32`, `end2: UInt32`
- `count`: Int32, Int64, UInt32, UInt64, or Float64 as required to preserve the stored numeric range (detected from the HDF5 dataset dtype)
- with `include_weights=True` and a `bins/weight` column present: `weight1: Float64`, `weight2: Float64` (NaN where bins are unbalanced/filtered)

With `join_bins=False` the raw COO view is exposed instead: `bin1_id: Int64`, `bin2_id: Int64`, `count` — no coordinate conversion applies.

The full `bins` table fits comfortably in memory at any realistic resolution (~3.1M rows for human 1 kb bins), so the provider loads `bins` + `chroms` once per scan and performs the pixel→coordinate join by direct array indexing while streaming pixel chunks.

Coordinate conversion follows the BED/BigWig convention: cooler is natively 0-based half-open; with 1-based output (`use_zero_based=False` or global default), emit `start + 1` on both `start1` and `start2` and keep ends closed.

### Predicate pushdown

The pixels table is sorted by `bin1_id` and CSR-indexed by `bin1_offset`, so first-axis genomic predicates map to contiguous pixel row ranges:

- `chrom1 = "..."` / `chrom1 IN (...)` → chrom → bin range via `chrom_offset` → pixel row range via `bin1_offset`.
- Conjunctions with `start1`/`end1` range comparisons narrow the bin range (bins are fixed-width or indexed, so position → bin id is arithmetic for fixed `bin-size` and binary search otherwise).
- Filters are reported `Inexact`; the existing client-side reapply invariant preserves exact semantics.
- `chrom2`/`start2`/`end2` and `count` predicates are residual (provider-side row filtering after decode at most); within a bin1 block `bin2_id` is sorted, so second-axis binary-search pruning is a possible follow-up, not initial scope.

### Projection pushdown

- Only projected arrays are materialized: e.g. projecting `count` alone never touches `bins`; projecting only first-axis coordinates skips `bin2` lookups and `count`.
- Empty projection (`count(*)`) reads no pixel data and can serve row counts from `bin1_offset`/`nnz` (per pruned range: offset arithmetic).
- `CoolerExec` implements `DisplayAs` output (e.g. `CoolerExec: projection=[chrom1, start1, count]`) for plan-inspection tests.

### Parallel execution

- Partition planning splits the (possibly predicate-pruned) pixel row space into `target_partitions` contiguous ranges aligned to bin1 boundaries via `bin1_offset`.
- Constraint: libhdf5 is not concurrency-friendly — `hdf5-metno` serializes all calls behind a global lock, so parallel partitions contend on raw HDF5 reads. Each partition therefore reads coarse HDF5 chunks under the lock and does Arrow building, joining, and filtering outside it. Speedup is real but lock-bound; this is documented, and the session defaults `datafusion.execution.target_partitions` to 1.
- Evaluating `hidefix` for lock-free chunk decompression is an explicit follow-up if profiling shows the global lock dominating.

### Storage scope

Local filesystem only. Remote paths (s3://, gs://, http://) fail with a clear not-supported error at the Python layer, consistent with the BBI first version.

### Test oracle: the `cooler` reference implementation

The Python `cooler` package (https://github.com/open2c/cooler) is the reference implementation of the format and serves as the oracle for both correctness and performance, mirroring how oxbow is used as the independent reference reader for GFF/GTF pushdown regression:

- **Fixture generation**: test `.cool`/`.mcool` fixtures are generated with `cooler` (`cooler cload`, `cooler zoomify`, `cooler balance`) from small synthetic pairs, so fixtures are guaranteed spec-conformant and carry real weights, multiple resolutions, and standard attributes. Fixture-generation scripts are committed; generated files are small and committed too.
- **Correctness parity**: every scan mode (full scan, projected, predicate-pushed region, `join_bins=False`, `include_weights=True`, each resolution of an `.mcool`) is asserted row-for-row equal to the equivalent `cooler` query (`Cooler.pixels()[:]` with `join=True/False`, `Cooler.matrix(as_pixels=True, join=True).fetch(region)` for region queries), after normalizing coordinate system (cooler emits 0-based half-open) and dtypes. `describe_cool` output is checked against `cooler.fileops.list_coolers` and `Cooler.info`.
- **Performance benchmark**: benchmarks live in the existing `biodatageeks/bioformats-benchmark` repository, following its `bench_<format>_<library>.py` convention: `bench_cool_polars_bio.py` (eager and lazy/streaming variants, plus a region-query workload) versus `bench_cool_cooler.py` (the `cooler` chunked-pandas LazyFrame workaround from oxbow#180 as the baseline, plus `Cooler.matrix(as_pixels=True).fetch` for the region workload), sharing a `cool_common.py` for workload definitions. `setup.sh` downloads and checksum-verifies a realistically sized public `.mcool` (4DN/GEO), and the README library/variant/data tables are extended. Metrics follow the repo standard: wall time, peak memory, and partition scalability for polars-bio. This benchmark is the acceptance signal that the feature beats the clunky generator workaround.
- `cooler` is a dev/test-only dependency (test extras), never a runtime dependency.

## Risks / Trade-offs

- **Static HDF5 wheel builds** are the dominant risk (CMake + C toolchain on three platforms, build-time cost, potential bioconda interplay). → Mitigated by the gating spike in tasks section 1; nothing else starts until it passes.
- **libhdf5 global lock** limits parallel scaling. → Coarse chunk reads under the lock, heavy work outside; `hidefix` follow-up.
- **Schema variance in the wild** (float counts, missing weights, cooler schema v2 files, nonstandard bin value columns). → Detect dtypes from HDF5 metadata; unsupported layouts error with the offending dataset path; fixtures include a v2 or float-count file if obtainable.
- **`chrom` stored as HDF5 enum vs plain int** varies between writers. → Read as integer index into `chroms/name` regardless of enum typing.
- **Huge pixel tables** (billions of rows at 1 kb). → Streaming chunked reads bounded by batch size; no full materialization on the provider side.

## Migration Plan

Purely additive: a new capability with new APIs; no existing behavior changes. Rollback is removing the new crate dependency and APIs.

## Spike results (task 1.2, 2026-08-25)

The static-HDF5 feasibility spike PASSED on every wheel-CI target (polars-bio run 32836622787; spike crate preserved with full results in `datafusion-bio-formats/sandbox/cooler-spike`):

| Target | Result | Job time |
|---|---|---|
| manylinux2014 x86_64 (glibc 2.17) | pass | 11m59s |
| ubuntu-latest | pass | 11m30s |
| Windows MSVC x64 | pass | 23m42s |
| macOS aarch64 | pass | 13m30s |
| macOS x86_64 (cross-compiled on arm64 runner) | pass | 9m33s |

Local marginal HDF5 build cost ~35s (macOS arm64); binaries link only system libs. **Key finding:** the `zlib` feature is mandatory alongside `static` — without it, attribute reads work but gzip-compressed dataset reads fail at runtime with a filter-plugin path error, so a build-only spike would have passed while every real cooler file failed.

## Open Questions

- ~~Does `hdf5-metno`'s static build work on Windows CI with the MSVC toolchain used for polars-bio wheels?~~ Yes — see spike results above.
- Should `describe_cool` also list available bin value columns (weights) so users can discover `include_weights` applicability? (Cheap; decide during implementation.)
