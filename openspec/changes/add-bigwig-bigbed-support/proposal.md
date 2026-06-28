# Change: Add BigWig and BigBed read/scan support

## Why

BigWig and BigBed are common indexed UCSC binary interval formats for genomic signal tracks and annotation tracks. polars-bio should read them through the same lazy DataFusion and Polars pipeline used by existing genomic formats, while preserving projection pushdown, genomic predicate pruning, and parallel execution where the file index makes that practical.

## What Changes

- Add explicit `scan_bigwig`, `read_bigwig`, `scan_bigbed`, and `read_bigbed` APIs.
- Add SQL registration APIs for BigWig and BigBed tables.
- Use the Rust `bigtools` crate as the low-level BBI parser and interval reader.
- Implement DataFusion table providers for BigWig and BigBed in the companion `biodatageeks/datafusion-bio-formats` repository.
- Expose BigWig as `chrom`, `start`, `end`, `value`.
- Expose BigBed as `chrom`, `start`, `end`, plus autoSQL-derived fields when available, with a `rest` fallback when a usable schema is unavailable.
- Support coordinate-system conversion and metadata consistent with existing interval formats.
- Support projection pushdown by only building requested Arrow arrays and only parsing projected BigBed extra fields.
- Support genomic predicate pushdown for `chrom`, `start`, and `end` filters by using BigWig/BigBed interval indexes.
- Support parallel scans by splitting full scans and pushed genomic regions into DataFusion partitions and reopening independent bigtools readers per partition.
- Keep object-store support out of the first implementation except for clearly validated local paths and optionally HTTP(S) if bigtools remote reading is enabled and tested.

## Impact

- Affected specs: `bbi-io`
- Planning ownership: this polars-bio OpenSpec change is the single authoritative plan for both polars-bio integration and the companion format-provider work. Do not create a separate mirrored OpenSpec change in `datafusion-bio-formats` for this feature.
- Affected companion format-provider checkout:
  - `biodatageeks/datafusion-bio-formats`
  - BigWig and BigBed provider crates or a new shared BBI provider crate inside that checkout
  - `datafusion-bio-format-core` helpers if shared projection, filter, or partition code is needed
- Affected polars-bio code:
  - `Cargo.toml`, `Cargo.lock`
  - `src/option.rs`
  - `src/scan.rs`
  - `src/lib.rs`
  - `polars_bio/io.py`
  - `polars_bio/sql.py`
  - `polars_bio/__init__.py`
  - `polars_bio/predicate_translator.py`
  - `polars_bio/metadata_extractors.py`
  - docs and tests
