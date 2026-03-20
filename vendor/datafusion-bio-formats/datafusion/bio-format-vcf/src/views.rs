//! Dual-view support for VCF tables.
//!
//! When a multi-sample VCF table is registered, this module can auto-register
//! a companion `{table}_long` view that unnests the columnar genotypes into
//! one row per variant×sample.

use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use datafusion_bio_format_core::metadata::VCF_GENOTYPES_SAMPLE_NAMES_KEY;

/// Registers a `{table_name}_long` view that unnests columnar genotypes
/// into per-sample rows with a `sample_id` column.
///
/// The view uses `generate_series` + array indexing to expand each list
/// element into its own row, producing one row per variant×sample.
///
/// # Arguments
///
/// * `ctx` - The DataFusion session context
/// * `table_name` - The base VCF table name
/// * `sample_names` - Ordered list of sample names
/// * `format_fields` - List of FORMAT field names in the genotypes struct
///
/// # Example
///
/// ```sql
/// -- After registration:
/// SELECT sample_id, GT, GQ, DP FROM vcf_table_long WHERE sample_id = 'NA12878'
/// ```
pub async fn register_vcf_long_view(
    ctx: &SessionContext,
    table_name: &str,
    sample_names: &[String],
    format_fields: &[String],
) -> Result<()> {
    if sample_names.is_empty() || format_fields.is_empty() {
        return Ok(());
    }

    let long_view_name = format!("{table_name}_long");

    // Build a CASE expression to map ordinal → sample_name
    let mut case_parts = Vec::new();
    for (i, name) in sample_names.iter().enumerate() {
        case_parts.push(format!("WHEN s.idx = {} THEN '{}'", i + 1, name));
    }
    let sample_id_expr = format!("CASE {} END AS sample_id", case_parts.join(" "));

    // Build field extraction expressions: genotypes.GT[s.idx] AS GT, ...
    let field_exprs: Vec<String> = format_fields
        .iter()
        .map(|field| format!(r#"genotypes."{field}"[s.idx] AS "{field}""#))
        .collect();

    // Core columns (everything except genotypes)
    let core_cols = r#"chrom, start, "end", id, "ref", alt, qual, filter"#;

    let num_samples = sample_names.len();

    // Build the view SQL using CROSS JOIN with generate_series
    let sql = format!(
        r#"CREATE VIEW "{long_view_name}" AS
SELECT {core_cols},
    {sample_id_expr},
    {field_exprs}
FROM "{table_name}"
CROSS JOIN generate_series(1, {num_samples}) AS s(idx)"#,
        field_exprs = field_exprs.join(",\n    "),
    );

    ctx.sql(&sql).await?;
    Ok(())
}

/// Attempts to extract genotype field names and sample names from a registered
/// VCF table's schema, then registers the long view.
///
/// This is a convenience wrapper that reads the schema metadata to determine
/// sample names and format fields automatically.
pub async fn auto_register_vcf_long_view(ctx: &SessionContext, table_name: &str) -> Result<()> {
    let table = ctx.table_provider(table_name).await?;
    let schema = table.schema();

    // Check if this is a multi-sample table with genotypes column
    let genotypes_idx = match schema.index_of("genotypes") {
        Ok(idx) => idx,
        Err(_) => return Ok(()), // Not a multi-sample table
    };

    let genotypes_field = schema.field(genotypes_idx);

    // Extract sample names from genotypes field metadata
    let sample_names: Vec<String> = genotypes_field
        .metadata()
        .get(VCF_GENOTYPES_SAMPLE_NAMES_KEY)
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();

    if sample_names.is_empty() {
        return Ok(());
    }

    // Extract format field names from the struct children
    let format_fields: Vec<String> = if let datafusion::arrow::datatypes::DataType::Struct(fields) =
        genotypes_field.data_type()
    {
        fields.iter().map(|f| f.name().clone()).collect()
    } else {
        return Ok(());
    };

    register_vcf_long_view(ctx, table_name, &sample_names, &format_fields).await
}
