# Change: Add Cooler (.cool/.mcool) read/scan support

## Why

Cooler `.cool` (single-resolution) and `.mcool` (multi-resolution) files are the de facto standard for storing Hi-C contact matrices as HDF5 containers. polars-bio already reads the upstream 4DN `pairs` format, but the binned contact-matrix stage of Hi-C pipelines has no native reader, so users fall back to `cooler`'s pandas-chunk API and lose lazy/streaming Polars execution (the motivation raised in abdenlab/oxbow#180). Both formats share the same cooler data-collection schema, so one scanner covers both.

## What Changes

- Add explicit `scan_cool` and `read_cool` APIs that handle both `.cool` files and `.mcool` files (via a `resolution` parameter and cooler-style `path::/resolutions/N` URI syntax).
- Add `describe_cool` to list resolutions and data-collection metadata (bin size, chromosomes, nnz, genome assembly) without scanning pixels.
- Add SQL registration API `register_cool`.
- Implement a DataFusion table provider for cooler data collections in the companion `biodatageeks/datafusion-bio-formats` repository, reading HDF5 via Rust HDF5 tooling (no dependency on the Python `cooler`/`h5py` stack).
- Expose the pixels table joined with bin coordinates as `chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, `count`, with optional per-side balancing-weight columns.
- Support coordinate-system conversion and metadata consistent with existing interval formats (cooler is natively 0-based half-open).
- Support projection pushdown so non-projected bin-coordinate, count, and weight arrays are not materialized.
- Support genomic predicate pushdown for `chrom1`/`start1`/`end1` filters using the cooler `indexes/chrom_offset` and `indexes/bin1_offset` datasets to bound pixel row ranges.
- Support parallel scans by splitting the pixel table into row ranges along bin1 boundaries for DataFusion partitions, within the concurrency limits of the HDF5 library.
- Keep object-store support out of the first implementation; local filesystem only.

## Impact

- Affected specs: `cooler-io` (new capability)
- Planning coordination: this polars-bio change is the original cross-repository feature plan; `datafusion-bio-formats` also carries a repository-local `add-cool-mcool-support` record for its provider-specific requirements and archival.
- Affected companion format-provider checkout:
  - `biodatageeks/datafusion-bio-formats`
  - new `datafusion-bio-format-cooler` crate (HDF5 reader + `CoolerTableProvider`/`CoolerExec`)
  - `datafusion-bio-format-core` helpers if shared projection/filter/partition code is needed
- Affected benchmark suite:
  - `biodatageeks/bioformats-benchmark`: new `bench_cool_polars_bio.py` / `bench_cool_cooler.py` / `cool_common.py`, `setup.sh` `.mcool` fixture download, README tables
- Affected polars-bio code:
  - `Cargo.toml`, `Cargo.lock`
  - `src/option.rs` (new `CoolReadOptions`, `InputFormat.Cool`)
  - `src/scan.rs`, `src/lib.rs`
  - `polars_bio/io.py` (`scan_cool`, `read_cool`, `describe_cool`, `_FORMAT_COLUMN_TYPES`)
  - `polars_bio/sql.py` (`register_cool`)
  - `polars_bio/__init__.py`
  - `polars_bio/predicate_translator.py`, `polars_bio/metadata_extractors.py`
  - docs and tests
- Build/packaging risk: the HDF5 dependency must be statically linked into wheels for Linux, macOS (arm64/x86_64), and Windows; this is validated by an explicit feasibility spike before provider work starts.
