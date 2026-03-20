//! Index file discovery utilities for bioinformatics formats.
//!
//! Locates companion index files (BAI, CSI, CRAI, TBI) for genomic data files,
//! following standard naming conventions used by samtools/htslib.

use log::debug;
use std::path::Path;

/// Supported index file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexFormat {
    /// BAM index (.bai) — supports BAM files
    BAI,
    /// Coordinate-sorted index (.csi) — supports BAM, VCF, GFF
    CSI,
    /// CRAM index (.crai) — supports CRAM files
    CRAI,
    /// Tabix index (.tbi) — supports bgzipped VCF, GFF, BED, Pairs
    TBI,
}

/// Try to locate an index file for a given data file (local only).
///
/// Returns `None` if no index file is found at any of the conventional paths.
///
/// # Naming conventions
/// - BAM: `{path}.bai`, `{path_without_ext}.bai`
/// - CRAM: `{path}.crai`
/// - VCF: `{path}.tbi`, `{path}.csi`
/// - GFF: `{path}.tbi`, `{path}.csi`
pub fn discover_index_path(data_file: &str, format: IndexFormat) -> Option<String> {
    let candidates = get_index_candidates(data_file, format);
    let result = candidates.into_iter().find(|path| Path::new(path).exists());
    match &result {
        Some(path) => debug!("Discovered {format:?} index for {data_file}: {path}"),
        None => debug!("No {format:?} index found for {data_file}"),
    }
    result
}

/// Get a list of candidate index file paths for a given data file and format.
/// Returned paths are ordered by preference (most common convention first).
fn get_index_candidates(data_file: &str, format: IndexFormat) -> Vec<String> {
    match format {
        IndexFormat::BAI => {
            // Convention 1: file.bam.bai
            // Convention 2: file.bai (replacing .bam extension)
            let mut candidates = vec![format!("{}.bai", data_file)];
            if let Some(stripped) = data_file.strip_suffix(".bam") {
                candidates.push(format!("{stripped}.bai"));
            }
            candidates
        }
        IndexFormat::CSI => {
            vec![format!("{}.csi", data_file)]
        }
        IndexFormat::CRAI => {
            vec![format!("{}.crai", data_file)]
        }
        IndexFormat::TBI => {
            vec![format!("{}.tbi", data_file)]
        }
    }
}

/// Try to discover any available index for a BAM file.
/// Tries BAI first (more common), then CSI.
pub fn discover_bam_index(data_file: &str) -> Option<(String, IndexFormat)> {
    if let Some(path) = discover_index_path(data_file, IndexFormat::BAI) {
        return Some((path, IndexFormat::BAI));
    }
    if let Some(path) = discover_index_path(data_file, IndexFormat::CSI) {
        return Some((path, IndexFormat::CSI));
    }
    None
}

/// Try to discover a CRAI index for a CRAM file.
pub fn discover_cram_index(data_file: &str) -> Option<(String, IndexFormat)> {
    discover_index_path(data_file, IndexFormat::CRAI).map(|path| (path, IndexFormat::CRAI))
}

/// Try to discover any available index for a bgzipped VCF file.
/// Tries TBI first (more common), then CSI.
pub fn discover_vcf_index(data_file: &str) -> Option<(String, IndexFormat)> {
    if let Some(path) = discover_index_path(data_file, IndexFormat::TBI) {
        return Some((path, IndexFormat::TBI));
    }
    if let Some(path) = discover_index_path(data_file, IndexFormat::CSI) {
        return Some((path, IndexFormat::CSI));
    }
    None
}

/// Try to discover any available index for a bgzipped Pairs file.
/// Tries TBI first, then CSI, then `.px2` (pairix index, treated as TBI-compatible).
pub fn discover_pairs_index(data_file: &str) -> Option<(String, IndexFormat)> {
    if let Some(path) = discover_index_path(data_file, IndexFormat::TBI) {
        return Some((path, IndexFormat::TBI));
    }
    if let Some(path) = discover_index_path(data_file, IndexFormat::CSI) {
        return Some((path, IndexFormat::CSI));
    }
    // Pairix (.px2) index is tabix-compatible
    let px2_path = format!("{data_file}.px2");
    if Path::new(&px2_path).exists() {
        debug!("Discovered pairix (.px2) index for {data_file}: {px2_path}");
        return Some((px2_path, IndexFormat::TBI));
    }
    None
}

/// Try to discover any available index for a bgzipped GFF file.
/// Tries TBI first, then CSI.
pub fn discover_gff_index(data_file: &str) -> Option<(String, IndexFormat)> {
    if let Some(path) = discover_index_path(data_file, IndexFormat::TBI) {
        return Some((path, IndexFormat::TBI));
    }
    if let Some(path) = discover_index_path(data_file, IndexFormat::CSI) {
        return Some((path, IndexFormat::CSI));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    #[test]
    fn test_get_index_candidates_bam() {
        let candidates = get_index_candidates("/data/test.bam", IndexFormat::BAI);
        assert_eq!(candidates, vec!["/data/test.bam.bai", "/data/test.bai"]);
    }

    #[test]
    fn test_get_index_candidates_cram() {
        let candidates = get_index_candidates("/data/test.cram", IndexFormat::CRAI);
        assert_eq!(candidates, vec!["/data/test.cram.crai"]);
    }

    #[test]
    fn test_get_index_candidates_vcf() {
        let candidates = get_index_candidates("/data/test.vcf.gz", IndexFormat::TBI);
        assert_eq!(candidates, vec!["/data/test.vcf.gz.tbi"]);
    }

    #[test]
    fn test_discover_index_path_exists() {
        let dir = TempDir::new().unwrap();
        let bam_path = dir.path().join("test.bam");
        let bai_path = dir.path().join("test.bam.bai");

        // Create dummy files
        File::create(&bam_path).unwrap();
        File::create(&bai_path).unwrap();

        let result = discover_index_path(bam_path.to_str().unwrap(), IndexFormat::BAI);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), bai_path.to_str().unwrap());
    }

    #[test]
    fn test_discover_index_path_not_found() {
        let result = discover_index_path("/nonexistent/test.bam", IndexFormat::BAI);
        assert!(result.is_none());
    }

    #[test]
    fn test_discover_bam_index_bai_convention() {
        let dir = TempDir::new().unwrap();
        let bam_path = dir.path().join("test.bam");
        let bai_path = dir.path().join("test.bam.bai");

        File::create(&bam_path).unwrap();
        File::create(&bai_path).unwrap();

        let result = discover_bam_index(bam_path.to_str().unwrap());
        assert!(result.is_some());
        let (path, format) = result.unwrap();
        assert_eq!(format, IndexFormat::BAI);
        assert_eq!(path, bai_path.to_str().unwrap());
    }

    #[test]
    fn test_discover_bam_index_alt_convention() {
        let dir = TempDir::new().unwrap();
        let bam_path = dir.path().join("test.bam");
        let bai_path = dir.path().join("test.bai");

        File::create(&bam_path).unwrap();
        File::create(&bai_path).unwrap();

        let result = discover_bam_index(bam_path.to_str().unwrap());
        assert!(result.is_some());
        let (path, format) = result.unwrap();
        assert_eq!(format, IndexFormat::BAI);
        assert_eq!(path, bai_path.to_str().unwrap());
    }
}
