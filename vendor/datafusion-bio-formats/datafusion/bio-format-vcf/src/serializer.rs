//! Serializer for converting Arrow RecordBatches to VCF records
//!
//! This module provides functionality for converting DataFusion Arrow data back
//! to VCF format for writing to files.

use datafusion::arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, LargeListArray, LargeStringArray,
    ListArray, RecordBatch, StringArray, StringViewArray, StructArray, UInt32Array,
};
use datafusion::common::{DataFusionError, Result};

/// Enum to hold StringArray, LargeStringArray, or StringViewArray reference.
/// This allows handling standard Arrow Utf8, Polars LargeUtf8, and DataFusion Utf8View types.
enum StringColumnRef<'a> {
    Small(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl StringColumnRef<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            StringColumnRef::Small(arr) => arr.value(i),
            StringColumnRef::Large(arr) => arr.value(i),
            StringColumnRef::View(arr) => arr.value(i),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            StringColumnRef::Small(arr) => Array::is_null(*arr, i),
            StringColumnRef::Large(arr) => Array::is_null(*arr, i),
            StringColumnRef::View(arr) => Array::is_null(*arr, i),
        }
    }
}

/// Formats a float value for VCF output, matching C's `%g` formatting
/// (6 significant digits, trailing zeros trimmed).
///
/// - Fixed notation when the exponent is in [-4, 6) (matching C `%g` rules)
/// - Scientific notation otherwise
/// - Trailing zeros and unnecessary decimal points removed
/// - `NaN` → `"."` (VCF missing value)
fn format_vcf_float(v: f64) -> String {
    if v.is_nan() {
        return ".".to_string();
    }
    // Use Rust's {:.*e} to get scientific form, then decide notation
    // C %g uses 6 significant digits by default
    let formatted = format!("{v:.5e}"); // 5 digits after point = 6 sig digits
    // Parse the exponent
    let (mantissa_str, exp_str) = formatted.split_once('e').unwrap();
    let exp: i32 = exp_str.parse().unwrap();

    if (-4..6).contains(&exp) {
        // Use fixed notation with enough decimal places for 6 sig digits
        let decimal_places = if exp >= 0 {
            let dp = 5 - exp; // 6 sig digits - (exp+1) integer digits
            if dp < 0 { 0 } else { dp as usize }
        } else {
            (5 - exp) as usize // more decimals needed for small numbers
        };
        let fixed = format!("{v:.decimal_places$}");
        // Trim trailing zeros after decimal point
        if fixed.contains('.') {
            let trimmed = fixed.trim_end_matches('0').trim_end_matches('.');
            if trimmed.is_empty() || trimmed == "-" {
                "0".to_string()
            } else {
                trimmed.to_string()
            }
        } else {
            fixed
        }
    } else {
        // Scientific notation: reconstruct from parsed parts
        let mantissa_trimmed = mantissa_str.trim_end_matches('0').trim_end_matches('.');
        if exp >= 0 {
            format!("{mantissa_trimmed}e+{exp:02}")
        } else {
            format!(
                "{mantissa_trimmed}e-{exp_abs:02}",
                exp_abs = exp.unsigned_abs()
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Batch-level resolved genotypes: eliminate per-row downcasts & array slices
// ---------------------------------------------------------------------------

/// Pre-resolved typed values from a list column's inner array.
/// Resolved once per batch to eliminate per-element downcast chains.
enum TypedValues<'a> {
    Int32(&'a Int32Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    /// Fallback for nested or uncommon types (e.g. List<List<T>>).
    Other(&'a dyn Array),
}

/// Pre-resolved field data for a single FORMAT field in the genotypes struct.
enum ResolvedFieldData<'a> {
    List {
        list: &'a ListArray,
        values: TypedValues<'a>,
    },
    LargeList {
        list: &'a LargeListArray,
        values: TypedValues<'a>,
    },
    /// Field not found in the genotypes struct.
    Missing,
}

/// Pre-resolved genotype field (name + resolved data).
struct ResolvedGenotypeField<'a> {
    name: &'a str,
    data: ResolvedFieldData<'a>,
}

/// Pre-resolved genotypes for an entire batch. Type resolution is done once
/// per batch instead of per-row x per-field x per-sample.
struct ResolvedGenotypes<'a> {
    struct_array: &'a StructArray,
    fields: Vec<ResolvedGenotypeField<'a>>,
}

/// Resolves the typed values array from an Arrow array (single downcast per batch per field).
fn resolve_typed_values(array: &dyn Array) -> TypedValues<'_> {
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        TypedValues::Int32(a)
    } else if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        TypedValues::Float32(a)
    } else if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        TypedValues::Float64(a)
    } else if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        TypedValues::Utf8(a)
    } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        TypedValues::LargeUtf8(a)
    } else if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
        TypedValues::Utf8View(a)
    } else {
        TypedValues::Other(array)
    }
}

/// Resolves the field data for a single FORMAT column (list array + typed values).
fn resolve_field_data(array: &dyn Array) -> ResolvedFieldData<'_> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let values = resolve_typed_values(list.values().as_ref());
        ResolvedFieldData::List { list, values }
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = resolve_typed_values(list.values().as_ref());
        ResolvedFieldData::LargeList { list, values }
    } else {
        ResolvedFieldData::Missing
    }
}

/// Pre-resolves all genotype columns for a batch. Returns `None` if there is
/// no `genotypes` struct column (single-sample flat schema).
fn resolve_batch_genotypes<'a>(
    batch: &'a RecordBatch,
    format_fields: &'a [String],
) -> Option<ResolvedGenotypes<'a>> {
    let idx = batch.schema().index_of("genotypes").ok()?;
    let col = batch.column(idx);
    let struct_array = col.as_any().downcast_ref::<StructArray>()?;

    let fields = format_fields
        .iter()
        .map(|field_name| {
            let data = match struct_array.column_by_name(field_name) {
                Some(list_col) => resolve_field_data(list_col.as_ref()),
                None => ResolvedFieldData::Missing,
            };
            ResolvedGenotypeField {
                name: field_name.as_str(),
                data,
            }
        })
        .collect();

    Some(ResolvedGenotypes {
        struct_array,
        fields,
    })
}

/// Checks if a single value at `flat_idx` is missing (would produce "." in VCF output).
fn is_value_missing(values: &TypedValues, flat_idx: usize) -> bool {
    match values {
        TypedValues::Int32(a) => a.is_null(flat_idx),
        TypedValues::Float32(a) => a.is_null(flat_idx) || a.value(flat_idx).is_nan(),
        TypedValues::Float64(a) => a.is_null(flat_idx) || a.value(flat_idx).is_nan(),
        TypedValues::Utf8(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::LargeUtf8(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::Utf8View(a) => {
            a.is_null(flat_idx) || {
                let s = a.value(flat_idx);
                s.is_empty() || s == "."
            }
        }
        TypedValues::Other(a) => a.is_null(flat_idx),
    }
}

/// Writes a single typed value at `flat_idx` directly into the line buffer.
fn write_typed_value(values: &TypedValues, flat_idx: usize, buf: &mut String) -> Result<()> {
    use std::fmt::Write;
    match values {
        TypedValues::Int32(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                write!(buf, "{}", a.value(flat_idx)).unwrap();
            }
        }
        TypedValues::Float32(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                buf.push_str(&format_vcf_float(a.value(flat_idx) as f64));
            }
        }
        TypedValues::Float64(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                buf.push_str(&format_vcf_float(a.value(flat_idx)));
            }
        }
        TypedValues::Utf8(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::LargeUtf8(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::Utf8View(a) => {
            if a.is_null(flat_idx) {
                buf.push('.');
            } else {
                let s = a.value(flat_idx);
                if s.is_empty() {
                    buf.push('.');
                } else {
                    buf.push_str(s);
                }
            }
        }
        TypedValues::Other(a) => {
            let val = extract_sample_value_string(*a, flat_idx)?;
            buf.push_str(&val);
        }
    }
    Ok(())
}

impl ResolvedFieldData<'_> {
    /// Checks whether all samples have missing values for this field at the given row.
    fn is_all_missing(&self, row: usize, num_samples: usize) -> bool {
        match self {
            ResolvedFieldData::Missing => true,
            ResolvedFieldData::List { list, values } => {
                if list.is_null(row) {
                    return true;
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if count == 0 {
                    return true;
                }
                for i in 0..num_samples.min(count) {
                    if !is_value_missing(values, start + i) {
                        return false;
                    }
                }
                true
            }
            ResolvedFieldData::LargeList { list, values } => {
                if list.is_null(row) {
                    return true;
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if count == 0 {
                    return true;
                }
                for i in 0..num_samples.min(count) {
                    if !is_value_missing(values, start + i) {
                        return false;
                    }
                }
                true
            }
        }
    }

    /// Writes a single sample's value directly to the line buffer.
    fn write_value(&self, row: usize, sample_idx: usize, buf: &mut String) -> Result<()> {
        match self {
            ResolvedFieldData::Missing => {
                buf.push('.');
                Ok(())
            }
            ResolvedFieldData::List { list, values } => {
                if list.is_null(row) {
                    buf.push('.');
                    return Ok(());
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if sample_idx >= count {
                    buf.push('.');
                    return Ok(());
                }
                write_typed_value(values, start + sample_idx, buf)
            }
            ResolvedFieldData::LargeList { list, values } => {
                if list.is_null(row) {
                    buf.push('.');
                    return Ok(());
                }
                let offsets = list.offsets();
                let start = offsets[row] as usize;
                let count = offsets[row + 1] as usize - start;
                if sample_idx >= count {
                    buf.push('.');
                    return Ok(());
                }
                write_typed_value(values, start + sample_idx, buf)
            }
        }
    }
}

/// Writes FORMAT and sample columns directly to the line buffer using pre-resolved genotypes.
fn write_resolved_format_and_samples(
    resolved: &ResolvedGenotypes,
    row: usize,
    num_samples: usize,
    line: &mut String,
) -> Result<()> {
    if resolved.struct_array.is_null(row) {
        return Ok(());
    }

    // Compute which fields to keep (drop all-missing fields, matching bcftools behavior)
    let keep: Vec<bool> = resolved
        .fields
        .iter()
        .map(|f| !f.data.is_all_missing(row, num_samples))
        .collect();

    if !keep.iter().any(|&k| k) {
        return Ok(());
    }

    // Write FORMAT column
    line.push('\t');
    let mut first = true;
    for (field, &k) in resolved.fields.iter().zip(keep.iter()) {
        if !k {
            continue;
        }
        if !first {
            line.push(':');
        }
        line.push_str(field.name);
        first = false;
    }

    // Write each sample's values
    for sample_idx in 0..num_samples {
        line.push('\t');
        first = true;
        for (field, &k) in resolved.fields.iter().zip(keep.iter()) {
            if !k {
                continue;
            }
            if !first {
                line.push(':');
            }
            field.data.write_value(row, sample_idx, line)?;
            first = false;
        }
    }

    Ok(())
}

/// A serialized VCF record as a string line
pub struct VcfRecordLine {
    /// The VCF line (without newline)
    pub line: String,
}

/// Converts an Arrow RecordBatch to a vector of VCF record lines.
///
/// The RecordBatch must have columns matching VCF schema names. Columns are
/// looked up by name, so the order in the batch does not matter.
///
/// # Arguments
///
/// * `batch` - The Arrow RecordBatch to convert
/// * `info_fields` - Names of INFO fields to include
/// * `format_fields` - Names of FORMAT fields (unique list)
/// * `sample_names` - Names of samples
/// * `coordinate_system_zero_based` - If true, coordinates are 0-based half-open (need +1 for VCF)
///
/// # Returns
///
/// A vector of VCF record lines that can be written to a file
///
/// # Errors
///
/// Returns an error if required columns are missing or have wrong types
pub fn batch_to_vcf_lines(
    batch: &RecordBatch,
    info_fields: &[String],
    format_fields: &[String],
    sample_names: &[String],
    coordinate_system_zero_based: bool,
) -> Result<Vec<VcfRecordLine>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Look up core columns by name
    let chroms = get_string_column_by_name(batch, "chrom")?;
    let starts = get_u32_column_by_name(batch, "start")?;
    let ids = get_string_column_by_name(batch, "id")?;
    let refs = get_string_column_by_name(batch, "ref")?;
    let alts = get_string_column_by_name(batch, "alt")?;
    let quals = get_optional_f64_column_by_name(batch, "qual")?;
    let filters = get_string_column_by_name(batch, "filter")?;

    // Build column index maps for INFO and FORMAT fields
    let info_columns = build_info_column_map(batch, info_fields);
    let num_samples = sample_names.len();

    // Pre-resolve genotype columns for batch-level type resolution (multisample fast path)
    let resolved_genotypes = if !sample_names.is_empty() && !format_fields.is_empty() {
        resolve_batch_genotypes(batch, format_fields)
    } else {
        None
    };
    let format_columns = if resolved_genotypes.is_none() && num_samples == 1 {
        Some(build_format_column_map(batch, format_fields, sample_names))
    } else {
        None
    };

    let mut records = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        // CHROM
        let chrom = chroms.value(row);

        // POS (convert from 0-based to 1-based if needed)
        let pos = if coordinate_system_zero_based {
            starts.value(row) + 1
        } else {
            starts.value(row)
        };

        // ID
        let id_str = if ids.is_null(row) || ids.value(row).is_empty() {
            ".".to_string()
        } else {
            ids.value(row).to_string()
        };

        // REF
        let ref_str = refs.value(row);

        // ALT (convert pipe separator back to comma)
        let alt_value = alts.value(row);
        let alt_str = if alt_value.is_empty() || alt_value == "." {
            ".".to_string()
        } else {
            alt_value.replace('|', ",")
        };

        // QUAL
        let qual_str = if quals.is_null(row) {
            ".".to_string()
        } else {
            format!("{:.2}", quals.value(row))
        };

        // FILTER
        let filter_str = if filters.is_null(row) || filters.value(row).is_empty() {
            ".".to_string()
        } else {
            filters.value(row).to_string()
        };

        // INFO
        let info_str = build_info_string(batch, row, info_fields, &info_columns)?;

        // Build the VCF line
        let mut line = format!(
            "{chrom}\t{pos}\t{id_str}\t{ref_str}\t{alt_str}\t{qual_str}\t{filter_str}\t{info_str}"
        );

        // FORMAT and samples
        if let Some(ref resolved) = resolved_genotypes {
            write_resolved_format_and_samples(resolved, row, num_samples, &mut line)?;
        } else if !sample_names.is_empty() && !format_fields.is_empty() {
            let (format_str, samples_str) = build_format_and_samples(
                batch,
                row,
                format_fields,
                sample_names,
                format_columns.as_ref(),
            )?;
            if !format_str.is_empty() {
                line.push('\t');
                line.push_str(&format_str);
                for sample in &samples_str {
                    line.push('\t');
                    line.push_str(sample);
                }
            }
        }

        records.push(VcfRecordLine { line });
    }

    Ok(records)
}

/// Gets a string column from the batch by name (supports both Utf8 and LargeUtf8)
fn get_string_column_by_name<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<StringColumnRef<'a>> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    let column = batch.column(idx);

    // Try StringArray, LargeStringArray, then StringViewArray
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(StringColumnRef::Small(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(StringColumnRef::Large(arr));
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
        return Ok(StringColumnRef::View(arr));
    }

    Err(DataFusionError::Execution(format!(
        "Column '{name}' must be Utf8, LargeUtf8, or Utf8View type"
    )))
}

/// Gets a u32 column from the batch by name
fn get_u32_column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be UInt32 type")))
}

/// Gets an optional f64 column from the batch by name
fn get_optional_f64_column_by_name<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float64Array> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        DataFusionError::Execution(format!("Required column '{name}' not found in batch"))
    })?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("Column '{name}' must be Float64 type")))
}

/// Builds a map from INFO field name to column index
fn build_info_column_map(
    batch: &RecordBatch,
    info_fields: &[String],
) -> std::collections::HashMap<String, usize> {
    let mut map = std::collections::HashMap::new();
    for field_name in info_fields {
        if let Ok(idx) = batch.schema().index_of(field_name) {
            map.insert(field_name.clone(), idx);
        }
    }
    map
}

/// Builds a map from (sample_name, format_field) to column index
fn build_format_column_map(
    batch: &RecordBatch,
    format_fields: &[String],
    sample_names: &[String],
) -> std::collections::HashMap<(String, String), usize> {
    let mut map = std::collections::HashMap::new();
    let single_sample = sample_names.len() == 1;

    for sample_name in sample_names {
        for format_field in format_fields {
            // Column naming: single sample uses just format name, multi-sample uses sample_format
            let column_name = if single_sample {
                format_field.clone()
            } else {
                format!("{sample_name}_{format_field}")
            };

            if let Ok(idx) = batch.schema().index_of(&column_name) {
                map.insert((sample_name.clone(), format_field.clone()), idx);
            }
        }
    }
    map
}

/// Builds the INFO string from INFO columns
fn build_info_string(
    batch: &RecordBatch,
    row: usize,
    info_fields: &[String],
    info_columns: &std::collections::HashMap<String, usize>,
) -> Result<String> {
    let mut info_parts = Vec::new();

    for field_name in info_fields {
        let col_idx = match info_columns.get(field_name) {
            Some(&idx) => idx,
            None => continue, // Column not in batch, skip
        };

        let column = batch.column(col_idx);
        if column.is_null(row) {
            continue;
        }

        if let Some(value_str) = extract_info_value_string(column.as_ref(), row)? {
            if value_str == "true" {
                // Flag type - just include the name
                info_parts.push(field_name.clone());
            } else if value_str != "false" {
                info_parts.push(format!("{field_name}={value_str}"));
            }
        }
    }

    if info_parts.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(info_parts.join(";"))
    }
}

/// Extracts an INFO value as a string from an Arrow array at a specific row
/// Supports both standard Arrow types and Polars "Large" variants (LargeUtf8, LargeList)
fn extract_info_value_string(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(format_vcf_float(arr.value(row) as f64)));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(format_vcf_float(arr.value(row))));
    }

    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    // Handle Utf8 (StringArray), LargeUtf8 (LargeStringArray), and Utf8View (StringViewArray)
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(arr.value(row).to_string()));
    }

    // Handle both List and LargeList
    if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(None);
        }
        return Ok(Some(value_strings.join(",")));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(None);
        }
        return Ok(Some(value_strings.join(",")));
    }

    Ok(None)
}

/// Extracts values from a list array as strings
/// Supports both standard Arrow types and Polars "Large" variants
fn extract_list_values(array: &dyn Array) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let len = array.len();

    if let Some(int_arr) = array.as_any().downcast_ref::<Int32Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(int_arr.value(i).to_string());
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float32Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(format_vcf_float(float_arr.value(i) as f64));
            }
        }
    } else if let Some(float_arr) = array.as_any().downcast_ref::<Float64Array>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(format_vcf_float(float_arr.value(i)));
            }
        }
    } else if let Some(str_arr) = array.as_any().downcast_ref::<StringArray>() {
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(str_arr.value(i).to_string());
            }
        }
    } else if let Some(str_arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        // Handle LargeUtf8 (Polars default string type)
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(str_arr.value(i).to_string());
            }
        }
    } else if let Some(str_arr) = array.as_any().downcast_ref::<StringViewArray>() {
        // Handle Utf8View (DataFusion default in certain operations)
        for i in 0..len {
            if array.is_null(i) {
                values.push(".".to_string());
            } else {
                values.push(str_arr.value(i).to_string());
            }
        }
    }

    Ok(values)
}

/// Builds FORMAT string and sample values using name-based column lookup
fn build_format_and_samples(
    batch: &RecordBatch,
    row: usize,
    format_fields: &[String],
    sample_names: &[String],
    format_columns: Option<&std::collections::HashMap<(String, String), usize>>,
) -> Result<(String, Vec<String>)> {
    if sample_names.is_empty() || format_fields.is_empty() {
        return Ok((String::new(), Vec::new()));
    }

    // Multisample sources keep FORMAT data in nested `genotypes` even when
    // only a subset (including one sample) is selected for output.
    let has_nested_genotypes = batch.schema().column_with_name("genotypes").is_some();
    if has_nested_genotypes {
        let field_values =
            collect_nested_multisample_values(batch, row, format_fields, sample_names)?;
        let (format_str, samples) =
            filter_all_missing_format_fields(format_fields, &field_values, sample_names.len());
        return Ok((format_str, samples));
    }

    let format_columns = format_columns.ok_or_else(|| {
        DataFusionError::Execution("Missing single-sample FORMAT column mapping".to_string())
    })?;

    // Collect values in field × sample order for filtering
    let mut field_values: Vec<Vec<String>> = Vec::with_capacity(format_fields.len());
    for format_field in format_fields {
        let mut sample_vals = Vec::with_capacity(sample_names.len());
        for sample_name in sample_names {
            let key = (sample_name.clone(), format_field.clone());
            let value = match format_columns.get(&key) {
                Some(&col_idx) => {
                    let column = batch.column(col_idx);
                    extract_sample_value_string(column.as_ref(), row)?
                }
                None => ".".to_string(),
            };
            sample_vals.push(value);
        }
        field_values.push(sample_vals);
    }

    let (format_str, samples) =
        filter_all_missing_format_fields(format_fields, &field_values, sample_names.len());
    Ok((format_str, samples))
}

/// Collects per-field, per-sample values from the nested genotypes column.
/// Returns a 2D structure: `values[field_idx][sample_idx]`.
fn collect_nested_multisample_values(
    batch: &RecordBatch,
    row: usize,
    format_fields: &[String],
    sample_names: &[String],
) -> Result<Vec<Vec<String>>> {
    let genotypes_idx = batch.schema().index_of("genotypes").map_err(|_| {
        DataFusionError::Execution(
            "Multisample output requires a 'genotypes' column in nested schema".to_string(),
        )
    })?;
    let genotypes_col = batch.column(genotypes_idx);
    let num_samples = sample_names.len();

    if genotypes_col.is_null(row) {
        return Ok(vec![
            vec![".".to_string(); num_samples];
            format_fields.len()
        ]);
    }

    // Columnar layout: genotypes is Struct<GT: List<Utf8>, GQ: List<Int32>, ...>
    let genotypes_struct = genotypes_col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Column 'genotypes' must be Struct (columnar layout)".to_string(),
            )
        })?;

    let mut field_values = Vec::with_capacity(format_fields.len());
    for format_field in format_fields {
        let mut sample_vals = Vec::with_capacity(num_samples);
        for sample_idx in 0..num_samples {
            let val = if let Some(list_col) = genotypes_struct.column_by_name(format_field) {
                extract_list_element_as_string(list_col.as_ref(), row, sample_idx)?
            } else {
                ".".to_string()
            };
            sample_vals.push(val);
        }
        field_values.push(sample_vals);
    }
    Ok(field_values)
}

/// Filters out FORMAT fields where ALL samples have missing values for this row.
/// Returns the filtered FORMAT string and filtered sample value strings.
/// This matches bcftools behavior of omitting per-row all-missing FORMAT fields.
fn filter_all_missing_format_fields(
    format_fields: &[String],
    field_values: &[Vec<String>],
    num_samples: usize,
) -> (String, Vec<String>) {
    let keep: Vec<bool> = field_values
        .iter()
        .map(|sample_vals| sample_vals.iter().any(|v| v != "."))
        .collect();

    let filtered_fields: Vec<&str> = format_fields
        .iter()
        .zip(keep.iter())
        .filter(|&(_, &k)| k)
        .map(|(f, _)| f.as_str())
        .collect();
    let format_str = filtered_fields.join(":");

    let mut samples = Vec::with_capacity(num_samples);
    for sample_idx in 0..num_samples {
        let vals: Vec<&str> = field_values
            .iter()
            .zip(keep.iter())
            .filter(|&(_, &k)| k)
            .map(|(sv, _)| sv[sample_idx].as_str())
            .collect();
        samples.push(vals.join(":"));
    }

    (format_str, samples)
}

/// Extracts a single element from a list column at [row][element_idx] as a string.
fn extract_list_element_as_string(
    array: &dyn Array,
    row: usize,
    element_idx: usize,
) -> Result<String> {
    if array.is_null(row) {
        return Ok(".".to_string());
    }

    // Try List<T>
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let inner = list.value(row);
        if element_idx >= inner.len() || inner.is_null(element_idx) {
            return Ok(".".to_string());
        }
        return extract_sample_value_string(inner.as_ref(), element_idx);
    }
    // Try LargeList<T>
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let inner = list.value(row);
        if element_idx >= inner.len() || inner.is_null(element_idx) {
            return Ok(".".to_string());
        }
        return extract_sample_value_string(inner.as_ref(), element_idx);
    }

    Ok(".".to_string())
}

/// Extracts a sample/FORMAT value as a string from an Arrow array
/// Supports both standard Arrow types and Polars "Large" variants (LargeUtf8, LargeList)
fn extract_sample_value_string(array: &dyn Array, row: usize) -> Result<String> {
    if array.is_null(row) {
        return Ok(".".to_string());
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(arr.value(row).to_string());
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(format_vcf_float(arr.value(row) as f64));
    }

    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(format_vcf_float(arr.value(row)));
    }

    // Handle Utf8 (StringArray), LargeUtf8 (LargeStringArray), and Utf8View (StringViewArray)
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        let s = arr.value(row);
        if s.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(s.to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        let s = arr.value(row);
        if s.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(s.to_string());
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
        let s = arr.value(row);
        if s.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(s.to_string());
    }

    // Handle both List and LargeList
    if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(value_strings.join(","));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = arr.value(row);
        let value_strings = extract_list_values(&values)?;
        if value_strings.is_empty() {
            return Ok(".".to_string());
        }
        return Ok(value_strings.join(","));
    }

    Ok(".".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Builder, ListBuilder, StringBuilder, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_batch_to_vcf_lines_basic() {
        let schema = create_test_schema();

        let chroms = StringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]); // 0-based
        let ends = UInt32Array::from(vec![100u32]);
        let ids = StringArray::from(vec![Some("rs123")]);
        let refs = StringArray::from(vec!["A"]);
        let alts = StringArray::from(vec!["G"]);
        let quals = Float64Array::from(vec![Some(30.0)]);
        let filters = StringArray::from(vec![Some("PASS")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        assert!(lines[0].line.starts_with("chr1\t100\t")); // Position should be 100 (1-based)
    }

    #[test]
    fn test_batch_to_vcf_lines_null_values() {
        let schema = create_test_schema();

        let chroms = StringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]);
        let ends = UInt32Array::from(vec![100u32]);
        let ids = StringArray::from(vec![None::<&str>]);
        let refs = StringArray::from(vec!["A"]);
        let alts = StringArray::from(vec!["."]);
        let quals = Float64Array::from(vec![None]);
        let filters = StringArray::from(vec![None::<&str>]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        // Check that null values are represented as "."
        assert!(lines[0].line.contains("\t.\t.\t.\t.")); // id, alt, qual, filter, info
    }

    #[test]
    fn test_batch_to_vcf_lines_multi_sample() {
        // Columnar schema: genotypes: Struct<GT: List<Utf8>, DP: List<Int32>>
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

        // Build columnar genotypes: GT = ["0/1", "1/1"], DP = [25, 30]
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);
        let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(25);
        dp_builder.values().append_value(30);
        dp_builder.append(true);
        let dp_array = Arc::new(dp_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let genotypes = Arc::new(
            StructArray::try_new(
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
                vec![gt_array, dp_array],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string(), "SAMPLE2".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();

        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;

        // Should have FORMAT column and two sample columns
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25")); // SAMPLE1
        assert!(line.contains("1/1:30")); // SAMPLE2
    }

    #[test]
    fn test_batch_to_vcf_lines_columnar_multisample_single_selected_sample() {
        // Columnar schema with 2 samples in genotypes, but only 1 sample_name selected for output
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

        // Build columnar genotypes with 1 sample: GT = ["0/1"], DP = [25]
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.append(true);
        let gt_array = Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let mut dp_builder = ListBuilder::new(Int32Builder::new());
        dp_builder.values().append_value(25);
        dp_builder.append(true);
        let dp_array = Arc::new(dp_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>;

        let genotypes = Arc::new(
            StructArray::try_new(
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
                vec![gt_array, dp_array],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();
        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25"));
    }

    #[test]
    fn test_batch_to_vcf_lines_single_sample() {
        // Schema with FORMAT fields for single sample (no sample prefix)
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("ref", DataType::Utf8, false),
            Field::new("alt", DataType::Utf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("GT", DataType::Utf8, true),
            Field::new("DP", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some("rs123")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                Arc::new(StringArray::from(vec![Some("0/1")])),
                Arc::new(Int32Array::from(vec![Some(25)])),
            ],
        )
        .unwrap();

        let sample_names = vec!["SAMPLE1".to_string()];
        let format_fields = vec!["GT".to_string(), "DP".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();

        assert_eq!(lines.len(), 1);
        let line = &lines[0].line;

        // Should have FORMAT column and one sample column
        assert!(line.contains("GT:DP"));
        assert!(line.contains("0/1:25"));
    }

    #[test]
    fn test_batch_to_vcf_lines_large_string_array() {
        // Test with LargeUtf8 (LargeStringArray) - Polars default string type
        let schema = Arc::new(Schema::new(vec![
            Field::new("chrom", DataType::LargeUtf8, false),
            Field::new("start", DataType::UInt32, false),
            Field::new("end", DataType::UInt32, false),
            Field::new("id", DataType::LargeUtf8, true),
            Field::new("ref", DataType::LargeUtf8, false),
            Field::new("alt", DataType::LargeUtf8, false),
            Field::new("qual", DataType::Float64, true),
            Field::new("filter", DataType::LargeUtf8, true),
        ]));

        let chroms = LargeStringArray::from(vec!["chr1"]);
        let starts = UInt32Array::from(vec![99u32]); // 0-based
        let ends = UInt32Array::from(vec![100u32]);
        let ids = LargeStringArray::from(vec![Some("rs456")]);
        let refs = LargeStringArray::from(vec!["C"]);
        let alts = LargeStringArray::from(vec!["T"]);
        let quals = Float64Array::from(vec![Some(45.0)]);
        let filters = LargeStringArray::from(vec![Some("PASS")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(chroms),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(ids),
                Arc::new(refs),
                Arc::new(alts),
                Arc::new(quals),
                Arc::new(filters),
            ],
        )
        .unwrap();

        let lines = batch_to_vcf_lines(&batch, &[], &[], &[], true).unwrap();

        assert_eq!(lines.len(), 1);
        // Position should be 100 (1-based), ID should be rs456
        assert!(
            lines[0]
                .line
                .starts_with("chr1\t100\trs456\tC\tT\t45.00\tPASS")
        );
    }

    #[test]
    fn test_format_vcf_float() {
        // 6 significant digits, trailing zeros trimmed
        assert_eq!(format_vcf_float(0.000312305), "0.000312305");
        assert_eq!(format_vcf_float(0.5), "0.5");
        assert_eq!(format_vcf_float(1.0), "1");
        assert_eq!(format_vcf_float(0.0), "0");
        assert_eq!(format_vcf_float(1234.57), "1234.57");
        assert_eq!(format_vcf_float(0.1), "0.1");
        assert_eq!(format_vcf_float(1e-5), "1e-05");
        assert_eq!(format_vcf_float(1e7), "1e+07");
        assert_eq!(format_vcf_float(f64::NAN), ".");
        // Edge cases
        assert_eq!(format_vcf_float(100.0), "100");
        assert_eq!(format_vcf_float(0.001), "0.001");
        assert_eq!(format_vcf_float(999999.0), "999999");
        assert_eq!(format_vcf_float(1e-4), "0.0001");
        assert_eq!(format_vcf_float(1e6), "1e+06");
        assert_eq!(format_vcf_float(-0.5), "-0.5");
        assert_eq!(format_vcf_float(123456.0), "123456");
    }

    #[test]
    fn test_format_drops_all_missing_fields() {
        // Multi-sample with GT, GQ, PL where PL is all-missing for all samples
        // PL should be dropped from FORMAT (matching bcftools behavior)
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
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "GQ",
                            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                            true,
                        ),
                        Field::new(
                            "PL",
                            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));

        // GT = ["0/1", "1/1"], GQ = [30, 40], PL = [null, null] (all missing)
        let mut gt_builder = ListBuilder::new(StringBuilder::new());
        gt_builder.values().append_value("0/1");
        gt_builder.values().append_value("1/1");
        gt_builder.append(true);

        let mut gq_builder = ListBuilder::new(Int32Builder::new());
        gq_builder.values().append_value(30);
        gq_builder.values().append_value(40);
        gq_builder.append(true);

        // PL: all null values for both samples
        let mut pl_builder = ListBuilder::new(StringBuilder::new());
        pl_builder.values().append_null();
        pl_builder.values().append_null();
        pl_builder.append(true);

        let genotypes = Arc::new(
            StructArray::try_new(
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
                    Field::new(
                        "PL",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                ]
                .into(),
                vec![
                    Arc::new(gt_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                    Arc::new(gq_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                    Arc::new(pl_builder.finish()) as Arc<dyn datafusion::arrow::array::Array>,
                ],
                None,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["chr1"])),
                Arc::new(UInt32Array::from(vec![99u32])),
                Arc::new(UInt32Array::from(vec![100u32])),
                Arc::new(StringArray::from(vec![Some(".")])),
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(StringArray::from(vec!["G"])),
                Arc::new(Float64Array::from(vec![Some(30.0)])),
                Arc::new(StringArray::from(vec![Some("PASS")])),
                genotypes,
            ],
        )
        .unwrap();

        let sample_names = vec!["S1".to_string(), "S2".to_string()];
        let format_fields = vec!["GT".to_string(), "GQ".to_string(), "PL".to_string()];

        let lines = batch_to_vcf_lines(&batch, &[], &format_fields, &sample_names, true).unwrap();
        let line = &lines[0].line;
        let parts: Vec<&str> = line.split('\t').collect();

        // FORMAT should drop all-missing PL field
        assert_eq!(parts[8], "GT:GQ");
        // Sample values should have 2 components (PL omitted)
        assert_eq!(parts[9], "0/1:30");
        assert_eq!(parts[10], "1/1:40");
    }
}
