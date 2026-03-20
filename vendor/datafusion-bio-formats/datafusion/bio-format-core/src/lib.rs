//! Core utilities for DataFusion bioinformatics table providers
//!
//! This crate provides shared infrastructure for implementing Apache DataFusion table providers
//! for various bioinformatics file formats. It includes:
//!
//! - **Object Storage Integration**: Support for reading from local files and cloud storage
//!   providers (GCS, S3, Azure Blob Storage) via OpenDAL
//! - **Compression Support**: Automatic detection and handling of GZIP and BGZF compression
//! - **Table Utilities**: Helper functions and types for implementing DataFusion table providers
//! - **Streaming I/O**: Efficient streaming readers optimized for large biological data files
//!
//! ## Usage
//!
//! This crate is primarily used as a dependency by format-specific crates in the
//! datafusion-bio-format family. Most users will interact with format-specific crates rather
//! than using this core crate directly.
//!
//! ### Example: Compression Detection
//!
//! ```rust,no_run
//! use datafusion_bio_format_core::object_storage::{get_compression_type, CompressionType, ObjectStorageOptions};
//!
//! # async fn example() -> Result<(), opendal::Error> {
//! // Detect compression type of a file
//! let compression = get_compression_type(
//!     "data/sample.fastq.gz".to_string(),
//!     Some(CompressionType::AUTO),
//!     ObjectStorageOptions::default()
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`object_storage`]: Cloud and local storage integration via OpenDAL
//! - [`table_utils`]: Utilities for implementing DataFusion table providers

#![warn(missing_docs)]

/// Key for storing coordinate system metadata in Arrow schema.
///
/// When set to "true", coordinates are 0-based half-open `[start, end)`.
/// When set to "false", coordinates are 1-based closed `[start, end]`.
///
/// This metadata is stored in the Arrow schema's metadata HashMap and allows
/// downstream consumers to correctly interpret coordinate values.
pub const COORDINATE_SYSTEM_METADATA_KEY: &str = "bio.coordinate_system_zero_based";

/// Bioinformatics metadata key constants and utilities
pub mod metadata;

// Re-export commonly used metadata keys, types, and utilities
pub use metadata::{
    BAM_BINARY_CIGAR_KEY, BAM_COMMENTS_KEY, BAM_FILE_FORMAT_VERSION_KEY, BAM_GROUP_ORDER_KEY,
    BAM_PROGRAM_INFO_KEY, BAM_READ_GROUPS_KEY, BAM_REFERENCE_SEQUENCES_KEY, BAM_SORT_ORDER_KEY,
    BAM_SUBSORT_ORDER_KEY, BAM_TAG_DESCRIPTION_KEY, BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY,
    ProgramMetadata, ReadGroupMetadata, ReferenceSequenceMetadata, extract_header_metadata,
    from_json_string, to_json_string,
};
/// Alignment utilities shared between BAM and CRAM formats
pub mod alignment_utils;
/// Calculated tags (MD, NM) for alignment records
pub mod calculated_tags;
/// Genomic filter extraction from SQL expressions for index-based queries
pub mod genomic_filter;
/// Index file discovery utilities for BAI/CSI/CRAI/TBI
pub mod index_utils;
/// Object storage integration for cloud and local file access
pub mod object_storage;
/// Balanced partition assignment for indexed genomic reads
pub mod partition_balancer;
/// Shared record-level filter evaluation for all formats
pub mod record_filter;
/// Table utilities for building DataFusion table providers
pub mod table_utils;
/// Tag registry for BAM/CRAM alignment tags
pub mod tag_registry;
/// Shared test utilities for projection pushdown and execution plan analysis
pub mod test_utils;

#[cfg(test)]
mod tests {
    mod object_storage;
}
