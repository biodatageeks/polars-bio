# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **FastQC quality control** (`pb.fastqc` / `SELECT * FROM fastqc(...)`): streaming FastQC over FASTQ files (plain, `.gz`, BGZF) in a single out-of-core pass. All 12 core modules implemented and bit-exact against FastQC 0.12.1 (`--nogroup`): `basic_stats`, `per_base_quality`, `per_seq_quality`, `per_base_content`, `per_seq_gc`, `per_base_n`, `seq_length`, `overrepresented`, `adapter_content`, `dup_levels`, `per_tile_quality`, `kmer_content`. Parallel accumulate-then-merge yields partition-invariant output; on a 26.5M-read BGZF it runs ~12× faster than FastQC at 8 cores.

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
