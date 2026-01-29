# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- BAM optional tag support via `tag_fields` parameter
  - Support for ~40 common SAM tags (NM, AS, MD, XS, RG, CB, UB, etc.)
  - Zero-overhead design: tags only parsed when requested
  - Tag-based filtering in SQL queries
  - Projection pushdown optimization for tag columns
  - Added `tag_fields` parameter to:
    - `read_bam()` and `scan_bam()` functions
    - `register_bam()` SQL function
  - CRAM functions (`read_cram`, `scan_cram`, `register_cram`) accept `tag_fields` parameter but currently ignore it with a warning (CRAM tag support coming in future release)
- Schema inspection functions
  - `describe_bam()` - Get schema information (column names and types) from BAM files
  - `describe_cram()` - Get schema information from CRAM files
  - Support for including tag fields in schema inspection
  - Only reads first row for fast schema retrieval

### Changed
- Updated datafusion-bio-formats dependency to rev 610c0c9257d2ba1d193a5d5592682cb30722a05a

## [0.20.1] - 2024-01-28

Previous releases...
