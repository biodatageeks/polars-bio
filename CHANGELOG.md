# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Indexed reads with predicate pushdown for BAM, CRAM, VCF, and GFF
  - Index files (BAI/CSI, CRAI, TBI) are auto-discovered by the upstream DataFusion providers
  - New `predicate_pushdown` parameter on `scan_bam`/`read_bam`, `scan_vcf`/`read_vcf`, `scan_cram`/`read_cram`
  - Polars filter expressions (e.g., `pl.col("chrom") == "chr1"`) are converted to SQL WHERE clauses and pushed down to DataFusion for index-based random access
  - SQL path (`register_*` + `pb.sql("SELECT ... WHERE ...")`) works automatically after dependency bump
  - Automatic parallel partitioning by chromosome when index files are present
- BAM optional tag support via `tag_fields` parameter
  - Support for ~40 common SAM tags (NM, AS, MD, XS, RG, CB, UB, etc.)
  - Zero-overhead design: tags only parsed when requested
  - Tag-based filtering in SQL queries
  - Projection pushdown optimization for tag columns
  - Added `tag_fields` parameter to:
    - `read_bam()` and `scan_bam()` functions
    - `register_bam()` SQL function
  - CRAM functions (`read_cram`, `scan_cram`, `register_cram`) accept `tag_fields` parameter but currently ignore it with a warning (CRAM tag support coming in future release)
- Schema inspection with automatic tag discovery
  - `describe_bam()` - Get comprehensive schema information from BAM files with automatic tag discovery
    - Samples records (default: 100) to discover all present optional tags
    - Returns detailed metadata: column names, data types, nullability, category (core/tag), SAM type, and descriptions
    - Fast operation - only samples N records instead of reading entire file
    - Perfect for exploring unfamiliar BAM files
  - `describe_cram()` - Get schema information from CRAM files

### Changed
- Updated datafusion-bio-formats dependency to rev `84605fcd03bf570db141400bbf5a78ae7e7f0d35`
  - Integrated upstream PR #61: indexed & parallel reads for BAM/CRAM/VCF/GFF
  - Integrated upstream `describe()` method with tag auto-discovery
  - Enhanced schema inspection capabilities
  - Integrated upstream PR #64: parsing-level projection pushdown for BAM, CRAM, and VCF
    - Unprojected fields are skipped entirely during record parsing (no string formatting, sequence decoding, map lookups, or memory allocation)
    - Activates automatically when `.select()` or SQL column projection is used
    - `COUNT(*)` queries use an empty projection path — no dummy fields are parsed
- Changed `projection_pushdown` default from `False` to `True` for all I/O methods and range operations
  - Applies to: `scan_*`/`read_*`, `overlap()`, `nearest()`, `count_overlaps()`, `coverage()`, `merge()`
  - To opt out, pass `projection_pushdown=False`

### Removed
- Removed dead `IndexedBam` and `IndexedVcf` enum variants (indexed reads are now handled automatically by upstream providers)

## [0.20.1] - 2024-01-28

Previous releases...
