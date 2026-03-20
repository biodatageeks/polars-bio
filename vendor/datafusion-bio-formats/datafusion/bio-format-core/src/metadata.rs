//! Generic bioinformatics metadata handling for Arrow schemas
//!
//! This module provides standardized metadata key constants and serialization
//! utilities for storing bioinformatics file headers in Arrow schema metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Generic Metadata Keys (All Formats)
// ============================================================================

/// Coordinate system: "true" = 0-based half-open [start, end), "false" = 1-based closed [start, end]
pub const COORDINATE_SYSTEM_METADATA_KEY: &str = "bio.coordinate_system_zero_based";

/// File format version (e.g., "VCFv4.3", "BAM/1.6")
pub const BIO_FILE_FORMAT_VERSION_KEY: &str = "bio.file_format_version";

/// Compression type (e.g., "GZIP", "BGZF", "NONE")
pub const BIO_COMPRESSION_TYPE_KEY: &str = "bio.compression_type";

/// Source file URI
pub const BIO_SOURCE_URI_KEY: &str = "bio.source_uri";

// ============================================================================
// VCF-Specific Metadata Keys
// ============================================================================

// Schema-level metadata

/// VCF file format version (e.g., "VCFv4.3") stored in schema metadata
pub const VCF_FILE_FORMAT_KEY: &str = "bio.vcf.file_format";

/// VCF FILTER definitions stored as JSON array of FilterMetadata
pub const VCF_FILTERS_KEY: &str = "bio.vcf.filters";

/// VCF CONTIG definitions stored as JSON array of ContigMetadata
pub const VCF_CONTIGS_KEY: &str = "bio.vcf.contigs";

/// VCF ALT allele definitions stored as JSON array of AltAlleleMetadata
pub const VCF_ALTERNATIVE_ALLELES_KEY: &str = "bio.vcf.alternative_alleles";

/// VCF sample names stored as JSON array of strings
pub const VCF_SAMPLE_NAMES_KEY: &str = "bio.vcf.samples";

/// VCF FORMAT definitions stored as JSON object keyed by FORMAT ID
///
/// Value type is `HashMap<String, VcfFieldMetadata>`, where the outer key is FORMAT ID
/// (e.g., `GT`, `DP`) and the value contains `number`, `type`, and `description`.
pub const VCF_FORMAT_FIELDS_KEY: &str = "bio.vcf.format_fields";

/// Sample names stored in the genotypes field metadata as a JSON array.
/// Present on the `genotypes` struct field in multi-sample columnar schemas.
pub const VCF_GENOTYPES_SAMPLE_NAMES_KEY: &str = "bio.vcf.genotypes.sample_names";

// Field-level metadata

/// VCF field description stored in field metadata
pub const VCF_FIELD_DESCRIPTION_KEY: &str = "bio.vcf.field.description";

/// VCF field type (Integer, Float, String, Flag) stored in field metadata
pub const VCF_FIELD_TYPE_KEY: &str = "bio.vcf.field.type";

/// VCF field number (1, A, R, G, .) stored in field metadata
pub const VCF_FIELD_NUMBER_KEY: &str = "bio.vcf.field.number";

/// VCF field type category (INFO or FORMAT) stored in field metadata
pub const VCF_FIELD_FIELD_TYPE_KEY: &str = "bio.vcf.field.field_type";

/// VCF FORMAT field ID for multi-sample columns (e.g., "GT" for Sample1_GT)
pub const VCF_FIELD_FORMAT_ID_KEY: &str = "bio.vcf.field.format_id";

// ============================================================================
// BAM-Specific Metadata Keys
// ============================================================================

/// BAM file format version (e.g., "1.6") stored in schema metadata
pub const BAM_FILE_FORMAT_VERSION_KEY: &str = "bio.bam.file_format_version";

/// BAM sort order (e.g., "coordinate", "queryname", "unsorted") stored in schema metadata
pub const BAM_SORT_ORDER_KEY: &str = "bio.bam.sort_order";

/// BAM group order (e.g., "none", "query", "reference") stored in schema metadata
pub const BAM_GROUP_ORDER_KEY: &str = "bio.bam.group_order";

/// BAM subsort order stored in schema metadata
pub const BAM_SUBSORT_ORDER_KEY: &str = "bio.bam.subsort_order";

/// BAM reference sequences (contigs) stored as JSON array
pub const BAM_REFERENCE_SEQUENCES_KEY: &str = "bio.bam.reference_sequences";

/// BAM read groups (@RG) stored as JSON array
pub const BAM_READ_GROUPS_KEY: &str = "bio.bam.read_groups";

/// BAM program info (@PG) stored as JSON array
pub const BAM_PROGRAM_INFO_KEY: &str = "bio.bam.program_info";

/// BAM comments (@CO) stored as JSON array
pub const BAM_COMMENTS_KEY: &str = "bio.bam.comments";

/// BAM optional tag name (e.g., "NM", "MD") stored in field metadata
pub const BAM_TAG_TAG_KEY: &str = "bio.bam.tag.tag";

/// BAM optional tag SAM type (e.g., "i", "Z", "f") stored in field metadata
pub const BAM_TAG_TYPE_KEY: &str = "bio.bam.tag.type";

/// BAM optional tag description stored in field metadata
pub const BAM_TAG_DESCRIPTION_KEY: &str = "bio.bam.tag.description";

/// Whether the CIGAR column uses binary encoding (raw LE u32 ops) instead of string
pub const BAM_BINARY_CIGAR_KEY: &str = "bio.bam.binary_cigar";

// ============================================================================
// GFF-Specific Metadata Keys (For Future Use)
// ============================================================================

/// GFF version (e.g., "3") stored in schema metadata
pub const GFF_VERSION_KEY: &str = "bio.gff.version";

/// GFF directives (##gff-version, ##genome-build, etc.) stored as JSON object
pub const GFF_DIRECTIVES_KEY: &str = "bio.gff.directives";

/// GFF sequence-region directives stored as JSON array
pub const GFF_SEQUENCE_REGIONS_KEY: &str = "bio.gff.sequence_regions";

// ============================================================================
// Pairs-Specific Metadata Keys (Hi-C contact data)
// ============================================================================

/// Pairs format version (e.g., "1.0") from the `## pairs format` header line
pub const PAIRS_FORMAT_VERSION_KEY: &str = "bio.pairs.format_version";

/// Pairs sort order (e.g., "chr1-chr2-pos1-pos2") from the `#sorted:` header line
pub const PAIRS_SORTED_KEY: &str = "bio.pairs.sorted";

/// Pairs shape (e.g., "upper triangle") from the `#shape:` header line
pub const PAIRS_SHAPE_KEY: &str = "bio.pairs.shape";

/// Pairs genome assembly (e.g., "hg38") from the `#genome_assembly:` header line
pub const PAIRS_GENOME_ASSEMBLY_KEY: &str = "bio.pairs.genome_assembly";

/// Pairs chromosome sizes stored as JSON array of `{"name": "chr1", "length": 249250621}`
pub const PAIRS_CHROMSIZES_KEY: &str = "bio.pairs.chromsizes";

/// Pairs column names stored as JSON array of strings
pub const PAIRS_COLUMNS_KEY: &str = "bio.pairs.columns";

// ============================================================================
// BED-Specific Metadata Keys (For Future Use)
// ============================================================================

/// BED variant type (BED3, BED6, BED12, etc.) stored in schema metadata
pub const BED_VARIANT_KEY: &str = "bio.bed.variant";

/// BED track metadata (name, description, color, etc.) stored as JSON object
pub const BED_TRACK_METADATA_KEY: &str = "bio.bed.track_metadata";

/// BED browser directives stored as JSON array
pub const BED_BROWSER_LINES_KEY: &str = "bio.bed.browser_lines";

// ============================================================================
// Shared Metadata Structures
// ============================================================================

/// Generic filter metadata (used by VCF, potentially others)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FilterMetadata {
    /// Filter ID (e.g., "PASS", "LowQual")
    pub id: String,
    /// Filter description
    pub description: String,
}

/// Generic contig/reference sequence metadata
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContigMetadata {
    /// Contig/chromosome ID (e.g., "chr1", "1")
    pub id: String,
    /// Contig length in base pairs (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    /// Additional metadata key-value pairs
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: HashMap<String, String>,
}

/// Alternative allele metadata (VCF-specific)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AltAlleleMetadata {
    /// ALT allele ID (e.g., "DEL", "INS", "DUP")
    pub id: String,
    /// ALT allele description
    pub description: String,
}

/// VCF field metadata for INFO/FORMAT definitions.
///
/// This is used for schema-level `bio.vcf.format_fields` metadata to preserve
/// header fidelity when field-level metadata is nested (e.g., multisample
/// `genotypes.values.*`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct VcfFieldMetadata {
    /// VCF Number value (e.g., "1", "A", "R", "G", ".")
    pub number: String,
    /// VCF Type value (e.g., "Integer", "Float", "String", "Flag")
    #[serde(rename = "type")]
    pub field_type: String,
    /// Header description text
    pub description: String,
}

// ============================================================================
// BAM/CRAM Alignment Metadata Structures
// ============================================================================

/// Reference sequence metadata for BAM/CRAM headers (@SQ lines)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceSequenceMetadata {
    /// Reference sequence name (e.g., "chr1")
    pub name: String,
    /// Reference sequence length in base pairs
    pub length: usize,
    /// Additional @SQ fields (AS, UR, M5, SP, etc.) keyed by 2-char tag name
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub other_fields: HashMap<String, String>,
}

/// Read group metadata for BAM/CRAM headers (@RG lines)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadGroupMetadata {
    /// Read group ID (required)
    pub id: String,
    /// Sample name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<String>,
    /// Platform (e.g., "ILLUMINA")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    /// Library
    #[serde(skip_serializing_if = "Option::is_none")]
    pub library: Option<String>,
    /// Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Additional @RG fields keyed by 2-char tag name
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub other_fields: HashMap<String, String>,
}

/// Program info metadata for BAM/CRAM headers (@PG lines)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramMetadata {
    /// Program ID (required)
    pub id: String,
    /// Program name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Program version
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Command line
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_line: Option<String>,
    /// Additional @PG fields keyed by 2-char tag name
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub other_fields: HashMap<String, String>,
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Serialize a value to JSON string, returning empty string on failure
pub fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::new())
}

/// Deserialize from JSON string, returning None on failure
pub fn from_json_string<'a, T: Deserialize<'a>>(json: &'a str) -> Option<T> {
    serde_json::from_str(json).ok()
}

/// Extracts header metadata from a SAM/BAM/CRAM header into a HashMap
/// suitable for storing in Arrow schema metadata.
///
/// This function serializes all header sections (@HD, @SQ, @RG, @PG, @CO)
/// into `bio.bam.*` metadata keys, enabling round-trip read/write fidelity.
pub fn extract_header_metadata(header: &noodles_sam::Header) -> HashMap<String, String> {
    let mut metadata = HashMap::new();

    // @HD: file format version and sort order
    if let Some(hd) = header.header() {
        metadata.insert(
            BAM_FILE_FORMAT_VERSION_KEY.to_string(),
            hd.version().to_string(),
        );

        use noodles_sam::header::record::value::map::header::tag;
        if let Some(sort_order) = hd.other_fields().get(&tag::SORT_ORDER) {
            metadata.insert(
                BAM_SORT_ORDER_KEY.to_string(),
                String::from_utf8_lossy(sort_order.as_ref()).to_string(),
            );
        }
        if let Some(group_order) = hd.other_fields().get(&tag::GROUP_ORDER) {
            metadata.insert(
                BAM_GROUP_ORDER_KEY.to_string(),
                String::from_utf8_lossy(group_order.as_ref()).to_string(),
            );
        }
        if let Some(subsort_order) = hd.other_fields().get(&tag::SUBSORT_ORDER) {
            metadata.insert(
                BAM_SUBSORT_ORDER_KEY.to_string(),
                String::from_utf8_lossy(subsort_order.as_ref()).to_string(),
            );
        }
    }

    // @SQ: reference sequences (including all optional fields like AS, UR, M5, SP)
    let ref_sequences = header.reference_sequences();
    if !ref_sequences.is_empty() {
        let ref_seq_metadata: Vec<ReferenceSequenceMetadata> = ref_sequences
            .iter()
            .map(|(name, map)| {
                let mut other = HashMap::new();
                for (tag, value) in map.other_fields() {
                    let tag_bytes: &[u8; 2] = tag.as_ref();
                    if let Ok(tag_str) = std::str::from_utf8(tag_bytes) {
                        other.insert(
                            tag_str.to_string(),
                            String::from_utf8_lossy(value.as_ref()).to_string(),
                        );
                    }
                }
                ReferenceSequenceMetadata {
                    name: String::from_utf8_lossy(name.as_ref()).to_string(),
                    length: map.length().get(),
                    other_fields: other,
                }
            })
            .collect();
        metadata.insert(
            BAM_REFERENCE_SEQUENCES_KEY.to_string(),
            to_json_string(&ref_seq_metadata),
        );
    }

    // @RG: read groups (including all optional fields)
    let read_groups = header.read_groups();
    if !read_groups.is_empty() {
        use noodles_sam::header::record::value::map::read_group::tag;
        let rg_metadata: Vec<ReadGroupMetadata> = read_groups
            .iter()
            .map(|(id, map)| {
                let fields = map.other_fields();
                let mut other = HashMap::new();
                for (t, value) in fields.iter() {
                    if *t != tag::SAMPLE
                        && *t != tag::PLATFORM
                        && *t != tag::LIBRARY
                        && *t != tag::DESCRIPTION
                    {
                        let tag_bytes: &[u8; 2] = t.as_ref();
                        if let Ok(tag_str) = std::str::from_utf8(tag_bytes) {
                            other.insert(
                                tag_str.to_string(),
                                String::from_utf8_lossy(value.as_ref()).to_string(),
                            );
                        }
                    }
                }
                ReadGroupMetadata {
                    id: String::from_utf8_lossy(id.as_ref()).to_string(),
                    sample: fields
                        .get(&tag::SAMPLE)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    platform: fields
                        .get(&tag::PLATFORM)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    library: fields
                        .get(&tag::LIBRARY)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    description: fields
                        .get(&tag::DESCRIPTION)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    other_fields: other,
                }
            })
            .collect();
        metadata.insert(
            BAM_READ_GROUPS_KEY.to_string(),
            to_json_string(&rg_metadata),
        );
    }

    // @PG: programs (including all optional fields)
    let programs = header.programs();
    let programs_map = programs.as_ref();
    if !programs_map.is_empty() {
        use noodles_sam::header::record::value::map::program::tag;
        let pg_metadata: Vec<ProgramMetadata> = programs_map
            .iter()
            .map(|(id, map)| {
                let fields = map.other_fields();
                let mut other = HashMap::new();
                for (t, value) in fields.iter() {
                    if *t != tag::NAME && *t != tag::VERSION && *t != tag::COMMAND_LINE {
                        let tag_bytes: &[u8; 2] = t.as_ref();
                        if let Ok(tag_str) = std::str::from_utf8(tag_bytes) {
                            other.insert(
                                tag_str.to_string(),
                                String::from_utf8_lossy(value.as_ref()).to_string(),
                            );
                        }
                    }
                }
                ProgramMetadata {
                    id: String::from_utf8_lossy(id.as_ref()).to_string(),
                    name: fields
                        .get(&tag::NAME)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    version: fields
                        .get(&tag::VERSION)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    command_line: fields
                        .get(&tag::COMMAND_LINE)
                        .map(|v| String::from_utf8_lossy(v.as_ref()).to_string()),
                    other_fields: other,
                }
            })
            .collect();
        metadata.insert(
            BAM_PROGRAM_INFO_KEY.to_string(),
            to_json_string(&pg_metadata),
        );
    }

    // @CO: comments
    let comments = header.comments();
    if !comments.is_empty() {
        let comment_strings: Vec<String> = comments
            .iter()
            .map(|c| String::from_utf8_lossy(c.as_ref()).to_string())
            .collect();
        metadata.insert(
            BAM_COMMENTS_KEY.to_string(),
            to_json_string(&comment_strings),
        );
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use noodles_sam as sam;
    use noodles_sam::header::record::value::Map;
    use noodles_sam::header::record::value::map::{
        Program, ReadGroup, ReferenceSequence, header::Version,
    };
    use std::num::NonZeroUsize;

    fn build_test_header() -> sam::Header {
        let version = Version::new(1, 6);
        let mut header_map = Map::<sam::header::record::value::map::Header>::new(version);

        use noodles_sam::header::record::value::map::header::tag;
        header_map
            .other_fields_mut()
            .insert(tag::SORT_ORDER, "coordinate".into());

        let mut rg_map = Map::<ReadGroup>::default();
        {
            use noodles_sam::header::record::value::map::read_group::tag;
            rg_map
                .other_fields_mut()
                .insert(tag::SAMPLE, "SAMPLE1".into());
            rg_map
                .other_fields_mut()
                .insert(tag::PLATFORM, "ILLUMINA".into());
        }

        let mut pg_map = Map::<Program>::default();
        {
            use noodles_sam::header::record::value::map::program::tag;
            pg_map.other_fields_mut().insert(tag::NAME, "bwa".into());
            pg_map
                .other_fields_mut()
                .insert(tag::VERSION, "0.7.17".into());
        }

        sam::Header::builder()
            .set_header(header_map)
            .add_reference_sequence(
                "chr1",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(249250621).unwrap()),
            )
            .add_reference_sequence(
                "chr2",
                Map::<ReferenceSequence>::new(NonZeroUsize::new(242193529).unwrap()),
            )
            .add_read_group("RG1", rg_map)
            .add_program("bwa", pg_map)
            .add_comment("Test comment")
            .build()
    }

    #[test]
    fn test_extract_header_metadata() {
        let header = build_test_header();
        let metadata = extract_header_metadata(&header);

        // Check version
        assert_eq!(
            metadata.get(BAM_FILE_FORMAT_VERSION_KEY),
            Some(&"1.6".to_string())
        );

        // Check sort order
        assert_eq!(
            metadata.get(BAM_SORT_ORDER_KEY),
            Some(&"coordinate".to_string())
        );

        // Check reference sequences
        let ref_seqs: Vec<ReferenceSequenceMetadata> =
            from_json_string(metadata.get(BAM_REFERENCE_SEQUENCES_KEY).unwrap()).unwrap();
        assert_eq!(ref_seqs.len(), 2);
        assert_eq!(ref_seqs[0].name, "chr1");
        assert_eq!(ref_seqs[0].length, 249250621);
        assert_eq!(ref_seqs[1].name, "chr2");
        assert_eq!(ref_seqs[1].length, 242193529);

        // Check read groups
        let rgs: Vec<ReadGroupMetadata> =
            from_json_string(metadata.get(BAM_READ_GROUPS_KEY).unwrap()).unwrap();
        assert_eq!(rgs.len(), 1);
        assert_eq!(rgs[0].id, "RG1");
        assert_eq!(rgs[0].sample.as_deref(), Some("SAMPLE1"));
        assert_eq!(rgs[0].platform.as_deref(), Some("ILLUMINA"));

        // Check programs
        let pgs: Vec<ProgramMetadata> =
            from_json_string(metadata.get(BAM_PROGRAM_INFO_KEY).unwrap()).unwrap();
        assert_eq!(pgs.len(), 1);
        assert_eq!(pgs[0].id, "bwa");
        assert_eq!(pgs[0].name.as_deref(), Some("bwa"));
        assert_eq!(pgs[0].version.as_deref(), Some("0.7.17"));

        // Check comments
        let comments: Vec<String> =
            from_json_string(metadata.get(BAM_COMMENTS_KEY).unwrap()).unwrap();
        assert_eq!(comments, vec!["Test comment"]);
    }

    #[test]
    fn test_extract_empty_header() {
        let header = sam::Header::default();
        let metadata = extract_header_metadata(&header);

        // Empty header should not produce reference_sequences, read_groups, etc.
        assert!(!metadata.contains_key(BAM_REFERENCE_SEQUENCES_KEY));
        assert!(!metadata.contains_key(BAM_READ_GROUPS_KEY));
        assert!(!metadata.contains_key(BAM_PROGRAM_INFO_KEY));
        assert!(!metadata.contains_key(BAM_COMMENTS_KEY));
    }
}
