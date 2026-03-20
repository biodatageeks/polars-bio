# datafusion-bio-format-core

Core utilities and shared functionality for DataFusion bioinformatics file format table providers.

## Purpose

This crate provides common infrastructure used by all datafusion-bio-format-* crates, including:

- **Object Storage Support**: Integration with cloud storage providers (GCS, S3, Azure Blob Storage) via OpenDAL
- **Compression Detection**: Automatic detection and handling of GZIP and BGZF compression
- **Table Utilities**: Shared utilities for implementing DataFusion table providers
- **Streaming I/O**: Efficient streaming readers for large biological data files

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-bio-format-core = { path = "../bio-format-core" }
```

## Usage

This crate is primarily used as a dependency by other datafusion-bio-format crates and provides internal utilities. Most users will not interact with this crate directly, but will use format-specific crates like:

- `datafusion-bio-format-fastq` for FASTQ files
- `datafusion-bio-format-vcf` for VCF files
- `datafusion-bio-format-bam` for BAM files
- `datafusion-bio-format-bed` for BED files
- `datafusion-bio-format-gff` for GFF files
- `datafusion-bio-format-fasta` for FASTA files
- `datafusion-bio-format-cram` for CRAM files

### Object Storage Example

```rust
use datafusion_bio_format_core::object_store::create_object_store;

// Create an object store for GCS
let object_store = create_object_store("gs://my-bucket/data.fastq.gz").await?;
```

### Compression Detection

```rust
use datafusion_bio_format_core::compression::detect_compression;

let compression_type = detect_compression(&file_path)?;
```

## Features

- Support for multiple cloud storage providers
- Automatic compression detection and decompression
- BGZF parallel reading support
- Efficient memory usage for large files

## License

Licensed under Apache-2.0
