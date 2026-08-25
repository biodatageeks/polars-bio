# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cooler Hi-C contact matrix support (`.cool`/`.mcool`): `read_cool()`,
  `scan_cool()`, `describe_cool()`, and `register_cool()`. Both file layouts
  are handled through a `resolution` argument or the cooler URI syntax
  (`file.mcool::/resolutions/10000`). Pixels are joined with bin coordinates
  by default (`chrom1..count`, optional `weight1`/`weight2` balancing
  weights), with a raw COO mode (`join_bins=False`); `count` is `Int32` or
  `Float64` following the stored dtype. First-axis predicate pushdown prunes
  pixel row ranges through the cooler CSR indexes, projection pushdown reads
  only the needed HDF5 datasets, `count(*)` is served without touching pixel
  data, and parallel scans split rows along bin1 boundaries. HDF5 is
  statically linked — no system HDF5 or Python `cooler` required. Outputs are
  validated row-for-row against the reference `cooler` implementation.
  Local filesystem paths only in this version.

### Fixed

- Numeric predicate pushdown now works on the Polars scan path for every
  format. The Polars optimizer casts filter literals to the column dtype
  before they reach the io-plugin callback (`{"Scalar": {"UInt32": N}}`
  instead of the untyped `Dyn` int), and the predicate translator silently
  kept such conjuncts client-side — so filters like
  `pl.col("start") >= 20_000_000` never pruned at the reader level (results
  were still correct via client-side re-filtering). Typed integer and float
  scalar kinds now translate; a cooler region query went from 1.95 s to
  0.23 s with the fix.

### Changed

- Updated `datafusion-bio-formats` to v1.11.0 and
  `datafusion-bio-functions` to v0.18.0.
- BigWig and BigBed full scans now use their built-in BBI index to balance
  compressed blocks across `datafusion.execution.target_partitions`. No sidecar
  index is required, and partitioned scans preserve every row exactly once.

## [0.34.0] - 2026-08-20

### Added

- BGEN 1.2/1.3 support: `read_bgen()`, `scan_bgen()`, `describe_bgen()`, and
  `register_bgen()`. `genotype_output="probability"` preserves every
  format-defined probability state, `genotype_output="dosage"` emits the
  expected copy count of `alleles[1]`, and a neighbouring `.bgi` index is used
  for `chrom`/`rsid`/`id`/`start`/`end` predicate pushdown.
  `probability_layout="fixed"` stores probabilities as a fixed-width list per
  sample, dropping the per-sample offsets, for files whose variants all store
  the same number of states.
- PLINK 2 PGEN support: `read_pgen()`, `scan_pgen()`, `describe_pgen()`, and
  `register_pgen()`. One row is one PVAR variant; `genotype_fields` selects the
  children of the `genotypes` struct — `"GT"`, `"ALT_COUNT"`, `"PHASED"`,
  `"DS"`, `"DS_STORED"`, `"HDS"` — and `max_range_gap`, `max_range_bytes`, and
  `batch_soft_byte_limit` tune read coalescing for object storage.
- `read_pgen_matrix()` and `read_bgen_matrix()` return a whole cohort as a dense
  `(variants, samples)` NumPy array — `values`, `positions`, and `sample_names`
  — instead of a DataFrame. Genotypes are decoded straight to their final
  address rather than consolidated from Arrow batches, and rows stay in source
  order at every thread count. PGEN takes `field="ALT_COUNT"` (`int8`) or
  `field="DS"` (`float32`); BGEN is ALT dosage only, because its probabilities
  are variable width and have no single dense shape.
- `genotype_fields` on `read_bgen()`, `scan_bgen()`, and `register_bgen()`
  selects the children of the BGEN `genotypes` struct — the output mode's value
  child (`"DS"` or `"GP"`) and `"PLOIDY"` — so a dosage read can skip the
  per-genotype ploidy byte.
- Dedicated `read_bcf()`, `scan_bcf()`, `describe_bcf()`, and `register_bcf()`
  APIs. BCF keeps
  `genotype_output="string"|"dosage"`, CSI predicate pushdown, and parallel
  partition processing.
- polars-bio is now published on Bioconda:
  `conda install -c conda-forge -c bioconda polars-bio` (linux-64, osx-64,
  osx-arm64). Its one unpackaged dependency, `polars-config-meta`, was published
  on conda-forge first (#426)

### Changed

- **Breaking:** `read_vcf()`, `scan_vcf()`, `describe_vcf()`, and
  `register_vcf()` now accept text VCF only. The read/scan methods no longer
  expose `genotype_output` or the deprecated, ignored `genotype_encoding_raw`
  argument. Use the corresponding BCF methods for `.bcf` input.
- `genotype_encoding_raw` remains available on `read_vcf_zarr()` and
  `scan_vcf_zarr()`, where it controls the returned genotype representation.

### Fixed

- BCF metadata now identifies its source format as `bcf`, and format detection
  for VCF/BCF range operations ignores signed-URL query parameters and fragments.

### Documentation

- Quickstart documents the conda/mamba install alongside pip, with the platform
  and extras caveats, and the README carries Bioconda version, downloads and
  platform badges. The `update_bioconda_recipe` workflow is removed — Bioconda's
  autobump bot already watches the recipe's PyPI source URL and Mergify
  auto-merges its bump PRs (#438)

## [0.33.1] - 2026-08-02

### Added

- Indexed CRAM scans now return the unplaced, unmapped reads at the end of a file.
  A whole-file `scan_cram`/`read_cram` returns the same records whether or not a
  `.crai` is present; previously the indexed path dropped them silently, so a file
  with 300 mapped and 200 unmapped reads yielded 300 rows with an index alongside
  it and 500 without one. Region queries are unchanged — they ask for placed reads
  by definition. BAM was never affected

### Fixed

- `pb.depth(..., use_zero_based=True)` returned 0-based **closed** coordinate
  blocks instead of the documented 0-based half-open, so every block covered one
  base fewer than its 1-based counterpart and summing block widths understated
  coverage. `pos_end` is now exclusive in 0-based mode. Fixed upstream in
  `datafusion-bio-function-pileup`
  (biodatageeks/datafusion-bio-functions#204, #205); verified against
  `mosdepth --fast-mode` and `samtools depth -a` (#427)
- Reading a CRAM whose bases series uses Huffman coding no longer aborts the query
  with `not yet implemented`. The underlying `noodles` fork has been rebased onto
  upstream master, which picks up the implementation of `Byte::decode_take` for
  Huffman (zaeleus/noodles#393). This affected both `pb.scan_cram`/`pb.read_cram`
  and `pb.depth` on such files (#429)
- Reading a CRAM whose `.crai` holds more than one record no longer fails with
  `invalid digit found in string`. The index reader was reusing its line buffer
  without clearing it, concatenating each record onto the previous one

### Changed

- `datafusion-bio-formats` now points at the rebased `noodles` fork. All `noodles`
  crates resolve to a single revision — previously two divergent revisions were
  pinned at once, so duplicate copies of `noodles-core`, `noodles-csi` and
  `noodles-bgzf` were built

### Documentation

- Document how unmapped reads behave across indexed and sequential scans (see
  [Reading files](https://biodatageeks.github.io/polars-bio/features/reading/#unmapped-reads-and-indexed-scans))
- Explain how to suppress processed-row progress output with `TQDM_DISABLE`
  (#432)

## [0.33.0] - 2026-07-05

### Added

- **FastQC quality control** (`pb.fastqc` / `SELECT * FROM fastqc(...)`): streaming FastQC over FASTQ files (plain, `.gz`, BGZF) in a single out-of-core pass. All 12 core modules implemented and bit-exact against FastQC 0.12.1 (`--nogroup`): `basic_stats`, `per_base_quality`, `per_seq_quality`, `per_base_content`, `per_seq_gc`, `per_base_n`, `seq_length`, `overrepresented`, `adapter_content`, `dup_levels`, `per_tile_quality`, `kmer_content`. Parallel accumulate-then-merge yields partition-invariant output; on a 26.5M-read BGZF it runs ~12× faster than FastQC at 8 cores (#420).

### Fixed

- Prevent a Python 3.12/3.13 segfault when an eager `pb.overlap` is followed by
  a lazy operation. Python-owned Arrow batches registered in the singleton
  context were being freed off-GIL on a tokio worker; batches are now deep-copied
  on registration (#395, #422)

## [0.32.0] - 2026-06-30

### Added
- BigWig and BigBed I/O APIs (#393)
  - `read_bigwig()`, `scan_bigwig()`, `register_bigwig()` and
    `read_bigbed()`, `scan_bigbed()`, `register_bigbed()`
  - Local and cloud storage input, eager/lazy/register access patterns
- VCF Zarr `describe` and registration APIs (#391)
  - `describe_vcf_zarr()` to introspect the logical VCF schema of a store
  - `register_vcf_zarr()` to register a VCF Zarr store as a DataFusion table
- `register_fasta()` to register a FASTA file as a DataFusion table, completing
  the eager/lazy/register triad for FASTA

### Changed
- Robust predicate & projection pushdown across formats (#407, fixes #396)
- Bumped the DataFusion stack to 53 and raised the pyarrow floor (#392)

### Fixed
- `scan_fastq` / `read_fastq` now read **all** members of multi-member
  (concatenated / block) gzip files (pigz, bgzip-as-gzip, fastp output).
  Previously only the first gzip member was decoded, which silently dropped
  reads or raised `DataFusion error: External(Kind(UnexpectedEof))` depending
  on where the member boundary fell. Fixed via the upstream
  datafusion-bio-formats bump (#408)
- `SELECT count(*)` on a FASTQ table registered via `register_fastq()` (#412)
- Removed the unsupported `parallel` kwarg from `register_fastq` (#409, #410)
- Normalize FASTQ columns before writing (#401)
- `read_bed` / `scan_bed` now emit correct 0-based half-open coordinates
  (#413, #415)
- Consume the upstream bare VCF INFO key parser fix (#389)

## [0.31.0] - 2026-05-13

### Added
- VCF Zarr read support (#382)
  - `read_vcf_zarr()` and `scan_vcf_zarr()` for array-native variant analytics
  - Lazy scans, eager reads, projection pushdown, INFO/FORMAT field selection,
    sample selection, raw typed genotype values, and genomic predicate pruning
    via the VCZ region index
  - Backed by a new `datafusion-bio-formats` VCF Zarr provider using the Rust
    `zarrs` crate

## [0.30.0] - 2026-04-29

### Added
- Overlap `left` output mode (#377)

### Changed
- Optimized range operations (#378)

## [0.29.0] - 2026-04-24

### Changed
- Improved eager partitioning (#374)

### Fixed
- Parallelize LazyFrame Arrow C stream inputs (#371)

## [0.28.0] - 2026-04-09

### Added
- Typed BAM/SAM tag roundtrip support (#366)

## [0.27.1] - 2026-04-05

### Fixed
- GTF `attr_fields` now returns all values for duplicate keys (#358, #359)
- BAM/CRAM write position off-by-one (#356, #357)

## [0.27.0] - 2026-04-03

### Added
- FASTA write and sink support (#353)
  - `write_fasta()` and `sink_fasta()`

### Fixed
- Handle INFO/FORMAT column name collision in single-sample VCFs (#354)

## [0.26.0] - 2026-03-07

### Added
- GTF format support with full read/scan/register pipeline (#336)
  - `read_gtf()`, `scan_gtf()`, `register_gtf()` for reading GTF files
  - Attribute flattening via `attr_fields` parameter
  - Predicate pushdown and projection pushdown support
  - Coordinate system support (0-based / 1-based)
  - Compressed file support (gzip, bgzf)
  - Object storage support (S3, GCS, Azure)
- Auto-infer custom SAM tag types from file sampling (#335)
  - New `infer_tag_types` parameter (default: True) for BAM/SAM/CRAM scan/read/describe/pileup
  - New `infer_tag_sample_size` parameter to control sampling depth
  - New `tag_type_hints` parameter for explicit type overrides (format: `["pt:i", "de:f"]`)
  - Previously unknown tags defaulted to Utf8; now correctly inferred as Int32/Float32/etc.
  - 26 nanopore-specific custom tags tested end-to-end

### Fixed
- **Critical**: Coalesce partitions before single-file writes (#338)
  - When `target_partitions > 1`, data from partitions 1..N was silently dropped
  - Affects VCF, BAM, CRAM, FASTQ write paths
- Preserve contig metadata (##contig lines) in VCF write/sink output (#340)
- Multisample VCF memory optimization (#331)
- No-coordinate BAM regression test added (#332)

### Security
- Updated pypdf to 6.7.5 to resolve 4 CVEs (#341)

### Changed
- Deduplicated GffLazyFrameWrapper / GtfLazyFrameWrapper into shared AnnotationLazyFrameWrapper
- Renamed `execute_streaming_write` to `execute_fastq_streaming_write`
- Extracted shared `execute_write()` for all format writers

## [0.22.0] - 2025-02-12

### Added
- Pairs (Hi-C) format scan/read support (#290)
  - `read_pairs()`, `scan_pairs()`, `register_pairs()` for reading Hi-C `.pairs` / `.pairs.gz` / `.pairs.bgz` files
  - Tabix-indexed querying with predicate pushdown on chr1/pos1, residual filters on chr2/pos2
  - Projection pushdown support
- New `template_length` (TLEN) column for BAM/SAM/CRAM (#294)
  - Non-nullable `Int32` column — schema grows from 11 to 12 core columns
- Non-nullable `mapping_quality` (MAPQ) for BAM/SAM/CRAM (#294)
  - Now `UInt32` — value 255 is preserved instead of becoming null
- Non-nullable `name` (QNAME) for BAM/SAM/CRAM (#294)
  - `*` is preserved as a string value instead of becoming null

### Changed
- Bumped datafusion-bio-formats to 0.3.0 (#292)

## [0.21.0] - 2025-02-09

### Added
- VCF and FASTQ write/sink support (#276)
- BAM/CRAM write support using datafusion-bio-formats (#283)
- SAM format read/write support (#285)
  - `read_sam()`, `scan_sam()`, `register_sam()` for reading SAM files
  - SAM write support via the unified write pipeline
- BAM optional tag support via `tag_fields` parameter (#281)
  - Support for ~40 common SAM tags (NM, AS, MD, XS, RG, CB, UB, etc.)
  - Zero-overhead design: tags only parsed when requested
  - Tag-based filtering in SQL queries
  - Projection pushdown optimization for tag columns
  - Added `tag_fields` parameter to:
    - `read_bam()` and `scan_bam()` functions
    - `register_bam()` SQL function
  - CRAM functions (`read_cram`, `scan_cram`, `register_cram`) accept `tag_fields` parameter but currently ignore it with a warning (CRAM tag support coming in future release)
- Indexed reads with predicate pushdown for BAM, CRAM, VCF, and GFF (#286)
  - Index files (BAI/CSI, CRAI, TBI) are auto-discovered by the upstream DataFusion providers
  - New `predicate_pushdown` parameter on `scan_bam`/`read_bam`, `scan_vcf`/`read_vcf`, `scan_cram`/`read_cram`
  - Polars filter expressions (e.g., `pl.col("chrom") == "chr1"`) are converted to SQL WHERE clauses and pushed down to DataFusion for index-based random access
  - SQL path (`register_*` + `pb.sql("SELECT ... WHERE ...")`) works automatically after dependency bump
  - Automatic parallel partitioning by chromosome when index files are present
- Parsing-level projection pushdown for BAM, CRAM, and VCF (#288)
  - Unprojected fields are skipped entirely during record parsing (no string formatting, sequence decoding, map lookups, or memory allocation)
  - Activates automatically when `.select()` or SQL column projection is used
  - `COUNT(*)` queries use an empty projection path — no dummy fields are parsed
- Schema inspection with automatic tag discovery
  - `describe_bam()` - Get comprehensive schema information from BAM files with automatic tag discovery
    - Samples records (default: 100) to discover all present optional tags
    - Returns detailed metadata: column names, data types, nullability, category (core/tag), SAM type, and descriptions
    - Fast operation - only samples N records instead of reading entire file
    - Perfect for exploring unfamiliar BAM files
  - `describe_cram()` - Get schema information from CRAM files

### Changed
- Updated datafusion-bio-formats dependency
  - Integrated upstream PR #51: BAM/CRAM write support
  - Integrated upstream PR #61: indexed & parallel reads for BAM/CRAM/VCF/GFF
  - Integrated upstream `describe()` method with tag auto-discovery
  - Integrated upstream PR #64: parsing-level projection pushdown for BAM, CRAM, and VCF
- Changed `projection_pushdown` default from `False` to `True` for all I/O methods and range operations
  - Applies to: `scan_*`/`read_*`, `overlap()`, `nearest()`, `count_overlaps()`, `coverage()`, `merge()`
  - To opt out, pass `projection_pushdown=False`
- Unified FastqTableProvider with auto parallel reads (#287)

### Fixed
- Move mkdocs-glightbox to dev dependencies (#280)

### Removed
- Removed dead `IndexedBam` and `IndexedVcf` enum variants (indexed reads are now handled automatically by upstream providers)

## [0.20.1] - 2024-01-28

Previous releases...
