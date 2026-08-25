# Tasks: add-cool-mcool-support

## 1. HDF5 Feasibility Spike (gates everything below)

- [x] 1.1 Coordinate this cross-repository feature from the polars-bio plan and maintain a repository-local provider record in `biodatageeks/datafusion-bio-formats` for its requirements and archival.
- [x] 1.2 Prototype `hdf5-metno` with the `static` feature: build in the polars-bio wheel CI matrix (manylinux x86_64/aarch64, macOS arm64/x86_64, Windows MSVC) and record build-time cost; if any target fails, stop and re-scope (feature flag or alternative crate) before continuing.
- [x] 1.3 Prototype opening a cooler-generated `.cool` and `.mcool`, reading root attributes, `chroms`, `bins` (including enum-typed `chrom`), `indexes`, and a chunked slice of `pixels`.
- [x] 1.4 Confirm the provider crate builds against the DataFusion/Arrow versions pinned by polars-bio and `datafusion-bio-formats`.

## 2. Test Fixtures from the cooler Oracle

- [x] 2.1 Write a committed fixture-generation script using `cooler` (`cload`, `zoomify`, `balance`) producing: a small `.cool`, a multi-resolution balanced `.mcool` (≥2 chromosomes, ≥2 resolutions), a single-resolution `.mcool`, and a float-count variant.
- [x] 2.2 Commit the generated fixtures and add `cooler` to the dev/test extras only.

## 3. DataFusion Cooler Provider (`datafusion-bio-format-cooler`)

- [x] 3.1 Implement data-collection resolution: `.cool` root, `.mcool` + resolution, `::` URI parsing, ambiguity/missing-resolution errors listing available resolutions.
- [x] 3.2 Implement `CoolerMetadata` (attributes, chroms, resolutions) without touching `pixels`.
- [x] 3.3 Implement `CoolerTableProvider` schema: joined mode (`chrom1..count`), raw COO mode, optional `weight1`/`weight2`, and lossless Int32/Int64/UInt32/UInt64/Float64 `count` selection from the stored dtype.
- [x] 3.4 Implement `CoolerExec` streaming full scans: load `bins`+`chroms` once, stream `pixels` in chunks, join by array indexing, emit Arrow batches.
- [x] 3.5 Implement projection pushdown (skip non-projected datasets; `count(*)` from index/nnz arithmetic) and `DisplayAs` plan output.
- [x] 3.6 Implement first-axis predicate pruning via `chrom_offset`/`bin1_offset` (chrom equality/membership + start1/end1 range conjunctions), reported inexact.
- [x] 3.7 Implement partition planning along bin1 boundaries honoring `target_partitions`, with coarse HDF5 reads under the library lock and Arrow building outside it.
- [x] 3.8 Add provider tests: full scan, projection, region pruning, raw COO, weights, float count, partition-count equivalence, error cases.

## 4. Rust/PyO3 Integration (polars-bio)

- [x] 4.1 Add `InputFormat.Cool` and `CoolReadOptions` (resolution, join_bins, include_weights, coordinate flag) in `src/option.rs`; extend `ReadOptions`.
- [x] 4.2 Add the `datafusion-bio-format-cooler` git dependency and register the provider in `src/scan.rs`; expose new classes via `src/lib.rs`.
- [x] 4.3 Extend internal format detection/routing where explicit formats are expected.

## 5. Python API Integration

- [x] 5.1 Add `IOOperations.scan_cool`, `read_cool`, and `describe_cool` with remote-path rejection and cooler-URI support.
- [x] 5.2 Add `SQL.register_cool`.
- [x] 5.3 Export new APIs from `polars_bio/__init__.py`.
- [x] 5.4 Add cooler column-type metadata to `predicate_translator.py` string/uint32/float sets and `_FORMAT_COLUMN_TYPES` (string keys).
- [x] 5.5 Extend `metadata_extractors.py` so source format, path, resolution, and coordinate-system metadata are visible on cooler scans.

## 6. Python Tests (cooler as correctness oracle)

- [x] 6.1 Parity tests vs `cooler`: full joined scan vs `Cooler.pixels(join=True)[:]`, raw COO vs `join=False`, weighted mode vs cooler weight columns, per-resolution `.mcool` scans — row-for-row after coordinate/dtype normalization.
- [x] 6.2 Region-query parity: pushed `chrom1`/`start1`/`end1` filters vs `Cooler.matrix(as_pixels=True, join=True).fetch(region)`, with pushdown on == pushdown off == cooler.
- [x] 6.3 `describe_cool` vs `cooler.fileops.list_coolers` + `Cooler.info`.
- [x] 6.4 Coordinate-system tests: `use_zero_based=True`/`False`/global default.
- [x] 6.5 Projection pushdown tests: projected columns only, plan display shows `CoolerExec` projection, `count(*)` fast path.
- [x] 6.6 Parallelism test: identical row sets at `target_partitions` 1 vs 4.
- [x] 6.7 Error tests: ambiguous/missing resolution, conflicting URI+argument, remote paths, non-cooler HDF5 file.
- [x] 6.8 SQL tests: `register_cool` + `pb.sql` query.

## 7. Benchmark vs the cooler Oracle (in `biodatageeks/bioformats-benchmark`)

- [x] 7.1 Extend `setup.sh` to download and SHA-256-verify a realistically sized public `.mcool` (4DN/GEO) plus a smaller `.cool`.
- [x] 7.2 Add `benchmarks/cool_common.py` with shared workload definitions: full eager read, lazy/streaming collect, count, and a region query.
- [x] 7.3 Add `benchmarks/bench_cool_polars_bio.py` (eager + lazy/streaming + region-pushdown workloads, partition scalability like the BBI benchmarks).
- [x] 7.4 Add `benchmarks/bench_cool_cooler.py`: the `cooler` chunked-pandas LazyFrame workaround (oxbow#180 baseline) for full scans and `Cooler.matrix(as_pixels=True, join=True).fetch` for region queries.
- [x] 7.5 Add a `verify_cool_equivalence.py`-style parity check between both benchmark outputs (precedent: `verify_bcf_equivalence.py`).
- [x] 7.6 Update the bioformats-benchmark README tables (libraries, variants, data requirements) and record results as the acceptance evidence for this change.

## 8. Documentation and Release Notes

- [x] 8.1 API docs for `scan_cool`, `read_cool`, `describe_cool`, `register_cool` (resolution/URI semantics, weights, coordinate systems).
- [x] 8.2 Document storage limitations (local-only), parallelism caveats (HDF5 lock), and non-goals (.scool, balancing computation, dense matrices).
- [x] 8.3 Add a Hi-C example combining `scan_pairs` and `scan_cool`.
- [x] 8.4 Add changelog entry.
