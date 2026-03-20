//! VCF header builder for constructing VCF headers from Arrow schemas
//!
//! This module provides functionality for building VCF header lines from Arrow schemas,
//! enabling round-trip VCF read/write operations. Header information is reconstructed from:
//! - Schema-level metadata: file format version, FILTER, CONTIG, and ALT definitions (stored as JSON)
//! - Field-level metadata: INFO/FORMAT field descriptions, types, and numbers (using `bio.vcf.field.*` keys)
//!
//! When metadata is not available, sensible defaults are generated from Arrow types.

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use datafusion_bio_format_core::metadata::{
    AltAlleleMetadata, ContigMetadata, FilterMetadata, VCF_ALTERNATIVE_ALLELES_KEY,
    VCF_CONTIGS_KEY, VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_FILTERS_KEY, from_json_string,
};
use std::collections::HashSet;

/// Index of the CHROM column in VCF schema
pub const CHROM_IDX: usize = 0;
/// Index of the START (POS) column in VCF schema
pub const START_IDX: usize = 1;
/// Index of the END column in VCF schema
pub const END_IDX: usize = 2;
/// Index of the ID column in VCF schema
pub const ID_IDX: usize = 3;
/// Index of the REF column in VCF schema
pub const REF_IDX: usize = 4;
/// Index of the ALT column in VCF schema
pub const ALT_IDX: usize = 5;
/// Index of the QUAL column in VCF schema
pub const QUAL_IDX: usize = 6;
/// Index of the FILTER column in VCF schema
pub const FILTER_IDX: usize = 7;
/// Number of core VCF columns (before INFO fields)
pub const CORE_FIELD_COUNT: usize = 8;

/// Builds VCF header lines from an Arrow schema with INFO and FORMAT field definitions
///
/// Reconstructs VCF header lines from schema metadata:
/// - File format version from `bio.vcf.file_format` (defaults to VCFv4.3)
/// - FILTER definitions from `bio.vcf.filters` (JSON array)
/// - CONTIG definitions from `bio.vcf.contigs` (JSON array)
/// - ALT allele definitions from `bio.vcf.alternative_alleles` (JSON array)
/// - INFO/FORMAT field metadata from `bio.vcf.field.*` keys
///
/// When metadata is absent, defaults are inferred from Arrow types.
///
/// # Arguments
///
/// * `schema` - The Arrow schema containing field definitions and metadata
/// * `info_fields` - List of INFO field names to include
/// * `format_fields` - List of FORMAT field names per sample
/// * `sample_names` - List of sample names from the original VCF
///
/// # Returns
///
/// A vector of VCF header lines (without the column header line)
pub fn build_vcf_header_lines(
    schema: &SchemaRef,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
) -> Result<Vec<String>> {
    let mut lines = Vec::new();

    // Get file format from schema metadata (default to VCFv4.3)
    let schema_metadata = schema.metadata();
    let file_format = schema_metadata
        .get(VCF_FILE_FORMAT_KEY)
        .map(|s| s.as_str())
        .unwrap_or("VCFv4.3");
    lines.push(format!("##fileformat={file_format}"));

    // Add FILTER definitions from metadata using shared utilities
    if let Some(filters_json) = schema_metadata.get(VCF_FILTERS_KEY) {
        if let Some(filters) = from_json_string::<Vec<FilterMetadata>>(filters_json) {
            for filter in filters {
                if filter.id != "PASS" {
                    // PASS is implicit
                    lines.push(format!(
                        "##FILTER=<ID={},Description=\"{}\">",
                        filter.id, filter.description
                    ));
                }
            }
        }
    }

    // Add CONTIG definitions from metadata using shared utilities
    if let Some(contigs_json) = schema_metadata.get(VCF_CONTIGS_KEY) {
        if let Some(contigs) = from_json_string::<Vec<ContigMetadata>>(contigs_json) {
            for contig in contigs {
                let mut line = format!("##contig=<ID={}", contig.id);
                if let Some(length) = contig.length {
                    line.push_str(&format!(",length={length}"));
                }
                line.push('>');
                lines.push(line);
            }
        }
    }

    // Add ALT definitions from metadata using shared utilities
    if let Some(alts_json) = schema_metadata.get(VCF_ALTERNATIVE_ALLELES_KEY) {
        if let Some(alts) = from_json_string::<Vec<AltAlleleMetadata>>(alts_json) {
            for alt in alts {
                lines.push(format!(
                    "##ALT=<ID={},Description=\"{}\">",
                    alt.id, alt.description
                ));
            }
        }
    }

    // Add INFO field definitions
    for info_name in info_fields {
        // Look up field by name to support any column order
        if let Ok(field_idx) = schema.index_of(info_name) {
            let field = schema.field(field_idx);
            let (vcf_type, number, description) = get_info_field_metadata(field, info_name);

            lines.push(format!(
                "##INFO=<ID={info_name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ));
        }
    }

    // Add FORMAT field definitions
    let unique_format_fields: HashSet<_> = format_fields.iter().collect();

    for format_name in unique_format_fields {
        // Find the first occurrence to get the type (try both naming conventions)
        if let Some(field) = find_format_field(schema, format_name, sample_names) {
            let (vcf_type, number, description) = get_format_field_metadata(field, format_name);

            lines.push(format!(
                "##FORMAT=<ID={format_name},Number={number},Type={vcf_type},Description=\"{description}\">"
            ));
        }
    }

    Ok(lines)
}

/// Extracts VCF metadata from an INFO field, using stored metadata if available
///
/// Reads metadata from `bio.vcf.field.*` keys. If not present, generates defaults
/// from the Arrow data type.
fn get_info_field_metadata(field: &Field, field_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // Get stored VCF metadata using bio.vcf.field.* keys
    let vcf_type = metadata
        .get(VCF_FIELD_TYPE_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_type(field.data_type()).to_string());

    let number = metadata
        .get(VCF_FIELD_NUMBER_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(field.data_type()).to_string());

    let description = metadata
        .get(VCF_FIELD_DESCRIPTION_KEY)
        .cloned()
        .unwrap_or_else(|| format!("{field_name} field"));

    (vcf_type, number, description)
}

/// Extracts VCF metadata from a FORMAT field, using stored metadata if available
///
/// Reads metadata from `bio.vcf.field.*` keys. If not present, generates defaults
/// from the Arrow data type (with special handling for GT fields).
/// In the columnar multi-sample schema the field type is `List<T>`; the function
/// unwraps one List level to derive the scalar VCF type for the header.
fn get_format_field_metadata(field: &Field, format_name: &str) -> (String, String, String) {
    let metadata = field.metadata();

    // For columnar multi-sample fields, unwrap List<T>/LargeList<T> → T for type inference.
    let scalar_type = match field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => inner.data_type(),
        other => other,
    };

    // Get stored VCF metadata using bio.vcf.field.* keys
    let vcf_type = metadata
        .get(VCF_FIELD_TYPE_KEY)
        .cloned()
        .unwrap_or_else(|| {
            // GT is always a string
            if format_name == "GT" {
                "String".to_string()
            } else {
                arrow_type_to_vcf_type(scalar_type).to_string()
            }
        });

    let number = metadata
        .get(VCF_FIELD_NUMBER_KEY)
        .cloned()
        .unwrap_or_else(|| arrow_type_to_vcf_number(scalar_type).to_string());

    let description = metadata
        .get(VCF_FIELD_DESCRIPTION_KEY)
        .cloned()
        .unwrap_or_else(|| format!("{format_name} format field"));

    (vcf_type, number, description)
}

/// Finds a FORMAT field in the schema by name (handles both single and multi-sample naming)
fn find_format_field<'a>(
    schema: &'a SchemaRef,
    format_name: &str,
    _sample_names: &[String],
) -> Option<&'a Field> {
    // First try direct name lookup (single sample case)
    if let Ok(idx) = schema.index_of(format_name) {
        return Some(schema.field(idx));
    }

    // Columnar multisample schema: genotypes: Struct<GT: List<T>, GQ: List<T>, ...>
    if let Ok(idx) = schema.index_of("genotypes") {
        let genotypes_field = schema.field(idx);
        if let DataType::Struct(struct_fields) = genotypes_field.data_type() {
            if let Some(field) = struct_fields.iter().find(|f| f.name() == format_name) {
                // field is List<T>; return a reference so callers can extract metadata.
                // The caller needs to unwrap List to get scalar type for VCF header.
                return Some(field.as_ref());
            }
        }
    }

    None
}

/// Builds the VCF column header line
///
/// # Arguments
///
/// * `sample_names` - List of sample names
///
/// # Returns
///
/// The column header line (starting with #CHROM)
pub fn build_vcf_column_header(sample_names: &[String]) -> String {
    let mut header = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO".to_string();

    if !sample_names.is_empty() {
        header.push_str("\tFORMAT");
        for sample in sample_names {
            header.push('\t');
            header.push_str(sample);
        }
    }

    header
}

/// Converts Arrow DataType to VCF type string
fn arrow_type_to_vcf_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int32 | DataType::Int64 => "Integer",
        DataType::Float32 | DataType::Float64 => "Float",
        DataType::Boolean => "Flag",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "String",
        DataType::List(inner) | DataType::LargeList(inner) => match inner.data_type() {
            DataType::Int32 | DataType::Int64 => "Integer",
            DataType::Float32 | DataType::Float64 => "Float",
            _ => "String",
        },
        _ => "String",
    }
}

/// Converts Arrow DataType to VCF Number string
fn arrow_type_to_vcf_number(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "0",                          // Flag type
        DataType::List(_) | DataType::LargeList(_) => ".", // Variable length
        _ => "1",                                          // Single value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_build_vcf_header_lines_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        assert!(lines.iter().any(|l| l.contains("##fileformat=")));
        assert!(lines.iter().any(|l| l.contains("##INFO=<ID=DP")));
    }

    #[test]
    fn test_build_vcf_header_lines_with_metadata() {
        // Create field with VCF metadata using new bio.vcf.field.* keys
        let mut dp_metadata = HashMap::new();
        dp_metadata.insert(
            VCF_FIELD_DESCRIPTION_KEY.to_string(),
            "Read Depth".to_string(),
        );
        dp_metadata.insert(VCF_FIELD_NUMBER_KEY.to_string(), "1".to_string());
        dp_metadata.insert(VCF_FIELD_TYPE_KEY.to_string(), "Integer".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true).with_metadata(dp_metadata),
        ]));

        let info_fields = vec!["DP".to_string()];
        let format_fields = vec![];
        let sample_names = vec![];

        let lines =
            build_vcf_header_lines(&schema, &info_fields, &format_fields, &sample_names).unwrap();

        // Should use the original description from metadata
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Description=\"Read Depth\""))
        );
    }

    #[test]
    fn test_build_vcf_column_header_no_samples() {
        let header = build_vcf_column_header(&[]);
        assert_eq!(header, "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
    }

    #[test]
    fn test_build_vcf_column_header_with_samples() {
        let header = build_vcf_column_header(&["SAMPLE1".to_string(), "SAMPLE2".to_string()]);
        assert!(header.contains("FORMAT"));
        assert!(header.contains("SAMPLE1"));
        assert!(header.contains("SAMPLE2"));
    }

    #[test]
    fn test_find_format_field_single_sample() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("GT", DataType::Utf8, true),
        ]));

        let field = find_format_field(&schema, "GT", &["SAMPLE1".to_string()]);
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GT");
    }

    #[test]
    fn test_find_format_field_multi_sample() {
        // Columnar schema: genotypes: Struct<GT: List<Utf8>, DP: List<Int32>>
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let field = find_format_field(
            &schema,
            "GT",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GT");
    }

    #[test]
    fn test_find_format_field_columnar_struct() {
        // Columnar schema with multiple FORMAT fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let field = find_format_field(
            &schema,
            "GQ",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(field.is_some());
        assert_eq!(field.unwrap().name(), "GQ");

        // Non-existent field returns None
        let missing = find_format_field(
            &schema,
            "AD",
            &["SAMPLE1".to_string(), "SAMPLE2".to_string()],
        );
        assert!(missing.is_none());
    }

    #[test]
    fn test_format_type_inference_from_large_list() {
        // Columnar schema with LargeList children (DataFusion default from named_struct)
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new(
                "genotypes",
                DataType::Struct(
                    vec![
                        Field::new(
                            "GT",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Utf8View,
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            "DP",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::LargeList(Arc::new(Field::new(
                                "item",
                                DataType::Int32,
                                true,
                            ))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        let samples = vec!["S1".to_string(), "S2".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string(), "GQ".to_string()];
        let lines = build_vcf_header_lines(&schema, &[], &format_fields, &samples).unwrap();

        // GT should be Type=String
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=GT") && l.contains("Type=String")),
            "GT should be Type=String. Lines: {lines:?}"
        );
        // DP should be Type=Integer (not Type=String)
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=DP") && l.contains("Type=Integer")),
            "DP should be Type=Integer. Lines: {lines:?}"
        );
        // GQ should be Type=Integer (not Type=String)
        assert!(
            lines
                .iter()
                .any(|l| l.contains("ID=GQ") && l.contains("Type=Integer")),
            "GQ should be Type=Integer. Lines: {lines:?}"
        );
    }
}
