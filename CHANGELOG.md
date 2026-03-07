# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
