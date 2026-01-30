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
- Schema inspection with automatic tag discovery
  - `describe_bam()` - Get comprehensive schema information from BAM files with automatic tag discovery
    - Samples records (default: 100) to discover all present optional tags
    - Returns detailed metadata: column names, data types, nullability, category (core/tag), SAM type, and descriptions
    - Fast operation - only samples N records instead of reading entire file
    - Perfect for exploring unfamiliar BAM files
  - `describe_cram()` - Get schema information from CRAM files

### Changed
- Updated datafusion-bio-formats dependency to rev `d7ac1a8331c5a12a858145421acdcebe64a6c7d8`
  - Integrated upstream `describe()` method with tag auto-discovery
  - Enhanced schema inspection capabilities

## [0.20.1] - 2024-01-28

Previous releases...
