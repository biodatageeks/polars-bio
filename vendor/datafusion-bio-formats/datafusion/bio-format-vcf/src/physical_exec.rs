use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Write};
use std::sync::Arc;

use crate::storage::{
    IndexedVcfReader, VcfLocalReader, VcfRecordFields, VcfRemoteReader, open_local_vcf_sync,
};
use crate::table_provider::{format_to_arrow_type, info_to_arrow_type};
use async_stream::__private::AsyncStream;
use async_stream::try_stream;
use datafusion::arrow::array::{
    Array, ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, ListBuilder,
    StringBuilder, StructArray, UInt32Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_bio_format_core::genomic_filter::GenomicRegion;
use datafusion_bio_format_core::partition_balancer::PartitionAssignment;
use datafusion_bio_format_core::record_filter::evaluate_record_filters;
use datafusion_bio_format_core::{
    metadata::{
        VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY,
        VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    },
    object_storage::{
        CompressionType, ObjectStorageOptions, StorageType, get_compression_type, get_storage_type,
    },
    table_utils::{OptionalField, builders_to_arrays},
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{StreamExt, TryStreamExt};
use log::{debug, info, warn};
use noodles_vcf::Header;
use noodles_vcf::header::record::value::map::format::{Number as FormatNumber, Type as FormatType};
use noodles_vcf::header::{Formats, Infos};
use noodles_vcf::variant::Record;
use noodles_vcf::variant::record::info::field::{Value, value::Array as ValueArray};
use noodles_vcf::variant::record::samples::series::value::genotype::Phasing;
use noodles_vcf::variant::record::{AlternateBases, Filters, Ids, ReferenceBases, Samples};
use std::str;

/// Joins an iterator of displayable items into a reusable buffer with a separator,
/// avoiding intermediate Vec allocation.
fn join_into<I, D>(buf: &mut String, iter: I, sep: char)
where
    I: Iterator<Item = D>,
    D: std::fmt::Display,
{
    buf.clear();
    let mut first = true;
    for item in iter {
        if !first {
            buf.push(sep);
        }
        first = false;
        let _ = write!(buf, "{item}");
    }
}

// Soft budgets for multisample FORMAT work per batch.
const MULTISAMPLE_TARGET_GENOTYPE_CELLS_PER_BATCH: usize = 100_000;
const MULTISAMPLE_TARGET_GENOTYPE_BYTES_PER_BATCH: usize = 8_000_000;
const MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE: usize = 8;
const MULTISAMPLE_ESTIMATED_SAMPLE_ID_BYTES_PER_ENTRY: usize = 16;
const MULTISAMPLE_ESTIMATED_VALUE_BYTES_PER_CELL: usize = 8;
const MULTISAMPLE_MAX_INITIAL_BUILDER_ROWS: usize = 32;
const MULTISAMPLE_BUILDER_RECYCLE_INTERVAL_BATCHES: usize = 8;
const STREAM_CHANNEL_BUFFERED_BATCHES: usize = 4;

fn count_requested_format_fields(format_fields: &Option<Vec<String>>, formats: &Formats) -> usize {
    match format_fields {
        Some(fields) => fields.len(),
        None => formats.keys().count(),
    }
}

fn choose_effective_batch_size(
    requested_batch_size: usize,
    any_format_projected: bool,
    format_fields: &Option<Vec<String>>,
    selected_sample_names: &[String],
    source_sample_names: &[String],
    formats: &Formats,
) -> usize {
    // Adaptive reduction is only useful for multisample FORMAT work.
    if !any_format_projected || source_sample_names.len() <= 1 || selected_sample_names.is_empty() {
        return requested_batch_size.max(1);
    }

    let format_field_count = count_requested_format_fields(format_fields, formats).max(1);
    let cells_per_row = selected_sample_names
        .len()
        .saturating_mul(format_field_count);
    if cells_per_row == 0 {
        return requested_batch_size.max(1);
    }

    let estimated_bytes_per_sample = MULTISAMPLE_ESTIMATED_SAMPLE_ID_BYTES_PER_ENTRY
        .saturating_add(
            format_field_count.saturating_mul(MULTISAMPLE_ESTIMATED_VALUE_BYTES_PER_CELL),
        );
    let estimated_bytes_per_row = selected_sample_names
        .len()
        .saturating_mul(estimated_bytes_per_sample)
        .max(1);

    let max_by_cells = (MULTISAMPLE_TARGET_GENOTYPE_CELLS_PER_BATCH / cells_per_row).max(1);
    let max_by_bytes =
        (MULTISAMPLE_TARGET_GENOTYPE_BYTES_PER_BATCH / estimated_bytes_per_row).max(1);
    let mut effective = requested_batch_size.min(max_by_cells).min(max_by_bytes);

    // Keep moderate batches only when both budgets allow it.
    if max_by_cells >= MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE
        && max_by_bytes >= MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE
        && requested_batch_size > MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE
    {
        effective = effective.max(MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE);
    }

    if effective < requested_batch_size {
        debug!(
            "Reducing effective VCF batch size from {} to {} (selected_samples={}, format_fields={}, target_cells={}, target_bytes={})",
            requested_batch_size,
            effective,
            selected_sample_names.len(),
            format_field_count,
            MULTISAMPLE_TARGET_GENOTYPE_CELLS_PER_BATCH,
            MULTISAMPLE_TARGET_GENOTYPE_BYTES_PER_BATCH
        );
    }

    effective.max(1)
}

fn choose_initial_builder_batch_size(
    effective_batch_size: usize,
    any_format_projected: bool,
    source_sample_names: &[String],
) -> usize {
    if any_format_projected && source_sample_names.len() > 1 {
        effective_batch_size.clamp(1, MULTISAMPLE_MAX_INITIAL_BUILDER_ROWS)
    } else {
        effective_batch_size.max(1)
    }
}

fn adjust_effective_batch_size_by_observed_format_bytes(
    requested_batch_size: usize,
    current_effective_batch_size: usize,
    any_format_projected: bool,
    source_sample_names: &[String],
    batch_rows: usize,
    format_arrays: Option<&Vec<Arc<dyn Array>>>,
) -> usize {
    if !any_format_projected || source_sample_names.len() <= 1 || batch_rows == 0 {
        return current_effective_batch_size;
    }

    let Some(format_arrays) = format_arrays else {
        return current_effective_batch_size;
    };
    if format_arrays.is_empty() {
        return current_effective_batch_size;
    }

    let format_bytes = format_arrays
        .iter()
        .map(|array| array.get_array_memory_size())
        .sum::<usize>();
    if format_bytes == 0 {
        return current_effective_batch_size;
    }

    let bytes_per_row = format_bytes.div_ceil(batch_rows).max(1);
    let max_by_observed_bytes =
        (MULTISAMPLE_TARGET_GENOTYPE_BYTES_PER_BATCH / bytes_per_row).max(1);
    let mut next_effective = requested_batch_size.min(max_by_observed_bytes);
    if max_by_observed_bytes >= MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE
        && requested_batch_size > MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE
    {
        next_effective = next_effective.max(MULTISAMPLE_MIN_EFFECTIVE_BATCH_SIZE);
    }

    if next_effective != current_effective_batch_size {
        debug!(
            "Adjusting effective VCF batch size from {current_effective_batch_size} to {next_effective} based on observed FORMAT bytes/row={bytes_per_row} (target_bytes={MULTISAMPLE_TARGET_GENOTYPE_BYTES_PER_BATCH})"
        );
    }

    next_effective.max(1)
}

/// Precomputed flags indicating which core VCF columns are needed based on projection.
struct ProjectionFlags {
    chrom: bool,
    start: bool,
    end: bool,
    id: bool,
    reference: bool,
    alt: bool,
    qual: bool,
    filter: bool,
    any_info: bool,
    any_format: bool,
}

impl ProjectionFlags {
    fn new(projection: &Option<Vec<usize>>, num_info_fields: usize) -> Self {
        let contains = |idx: usize| {
            projection
                .as_ref()
                .is_none_or(|p: &Vec<usize>| p.contains(&idx))
        };
        Self {
            chrom: contains(0),
            start: contains(1),
            end: contains(2),
            id: contains(3),
            reference: contains(4),
            alt: contains(5),
            qual: contains(6),
            filter: contains(7),
            any_info: projection
                .as_ref()
                .is_none_or(|p| p.iter().any(|&i| i >= 8 && i < 8 + num_info_fields)),
            any_format: projection
                .as_ref()
                .is_none_or(|p| p.iter().any(|&i| i >= 8 + num_info_fields)),
        }
    }
}

/// Arrow builders for the 8 core VCF columns, replacing Vec<T> accumulators
/// to eliminate double-buffering (Vec → Arrow copy at batch boundary).
struct CoreBatchBuilders {
    chrom: Option<StringBuilder>,
    start: Option<UInt32Builder>,
    end: Option<UInt32Builder>,
    id: Option<StringBuilder>,
    reference: Option<StringBuilder>,
    alt: Option<StringBuilder>,
    qual: Option<Float64Builder>,
    filter: Option<StringBuilder>,
}

impl CoreBatchBuilders {
    fn new(flags: &ProjectionFlags, batch_size: usize) -> Self {
        Self {
            chrom: if flags.chrom {
                Some(StringBuilder::with_capacity(batch_size, batch_size * 8))
            } else {
                None
            },
            start: if flags.start {
                Some(UInt32Builder::with_capacity(batch_size))
            } else {
                None
            },
            end: if flags.end {
                Some(UInt32Builder::with_capacity(batch_size))
            } else {
                None
            },
            id: if flags.id {
                Some(StringBuilder::with_capacity(batch_size, batch_size * 8))
            } else {
                None
            },
            reference: if flags.reference {
                Some(StringBuilder::with_capacity(batch_size, batch_size * 4))
            } else {
                None
            },
            alt: if flags.alt {
                Some(StringBuilder::with_capacity(batch_size, batch_size * 8))
            } else {
                None
            },
            qual: if flags.qual {
                Some(Float64Builder::with_capacity(batch_size))
            } else {
                None
            },
            filter: if flags.filter {
                Some(StringBuilder::with_capacity(batch_size, batch_size * 8))
            } else {
                None
            },
        }
    }

    #[inline]
    fn append_chrom(&mut self, value: &str) {
        if let Some(b) = &mut self.chrom {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_start(&mut self, value: u32) {
        if let Some(b) = &mut self.start {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_end(&mut self, value: u32) {
        if let Some(b) = &mut self.end {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_id(&mut self, value: &str) {
        if let Some(b) = &mut self.id {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_ref(&mut self, value: &str) {
        if let Some(b) = &mut self.reference {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_alt(&mut self, value: &str) {
        if let Some(b) = &mut self.alt {
            b.append_value(value);
        }
    }

    #[inline]
    fn append_qual(&mut self, value: Option<f64>) {
        if let Some(b) = &mut self.qual {
            match value {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
    }

    #[inline]
    fn append_filter(&mut self, value: &str) {
        if let Some(b) = &mut self.filter {
            b.append_value(value);
        }
    }

    /// Finishes all active builders and returns the 8 core arrays.
    fn finish(&mut self) -> [Option<Arc<dyn Array>>; 8] {
        [
            self.chrom
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.start
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.end
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.id
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.reference
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.alt
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.qual
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
            self.filter
                .as_mut()
                .map(|b| Arc::new(b.finish()) as Arc<dyn Array>),
        ]
    }
}

/// Constructs a DataFusion RecordBatch from Arrow builder arrays + INFO/FORMAT arrays.
///
/// Unlike `build_record_batch` which copies from Vec<T> into Arrow arrays, this function
/// takes pre-built Arrow arrays from `CoreBatchBuilders::finish()`.
fn build_record_batch_from_builders(
    schema: SchemaRef,
    core_arrays: [Option<Arc<dyn Array>>; 8],
    infos: Option<&Vec<Arc<dyn Array>>>,
    formats: Option<&Vec<Arc<dyn Array>>>,
    num_info_fields: usize,
    projection: &Option<Vec<usize>>,
    record_count: usize,
) -> datafusion::error::Result<RecordBatch> {
    let format_start_idx = 8 + num_info_fields;

    let arrays = match projection {
        None => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(8 + num_info_fields);
            for a in core_arrays.iter().flatten() {
                arrays.push(a.clone());
            }
            if let Some(info_arrays) = infos {
                arrays.extend_from_slice(info_arrays);
            }
            if let Some(format_arrays) = formats {
                arrays.extend_from_slice(format_arrays);
            }
            arrays
        }
        Some(proj_ids) => {
            let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(proj_ids.len());
            for &i in proj_ids {
                match i {
                    0..=7 => {
                        if let Some(a) = &core_arrays[i] {
                            arrays.push(a.clone());
                        }
                    }
                    idx if idx < format_start_idx => {
                        if let Some(info_arrays) = infos {
                            arrays.push(info_arrays[idx - 8].clone());
                        }
                    }
                    idx => {
                        if let Some(format_arrays) = formats {
                            arrays.push(format_arrays[idx - format_start_idx].clone());
                        }
                    }
                }
            }
            arrays
        }
    };

    if arrays.is_empty() {
        let options = datafusion::arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(record_count));
        RecordBatch::try_new_with_options(schema, arrays, &options)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {e:?}")))
    } else {
        RecordBatch::try_new(schema, arrays)
            .map_err(|e| DataFusionError::Execution(format!("Error creating batch: {e:?}")))
    }
}

/// Iterates all INFO fields in a single pass using `info.iter(header)` and dispatches
/// via HashMap, avoiding the O(N*M) cost of calling `info.get(header, key)` per field.
fn load_infos_single_pass(
    record: &dyn Record,
    header: &Header,
    info_data_types: &[DataType],
    info_builders: &mut [OptionalField],
    info_name_to_index: &HashMap<String, usize>,
    info_populated: &mut [bool],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    let num_builders = info_builders.len();
    if num_builders == 0 {
        return Ok(());
    }

    // Reset populated tracking
    info_populated.fill(false);

    let info = record.info();
    for result in info.iter(header) {
        let (key, value) = result.map_err(|e| {
            datafusion::arrow::error::ArrowError::InvalidArgumentError(format!(
                "Error reading INFO field: {e}"
            ))
        })?;

        if let Some(&idx) = info_name_to_index.get(key) {
            info_populated[idx] = true;
            let data_type = &info_data_types[idx];
            let builder = &mut info_builders[idx];

            match value {
                Some(Value::Integer(v)) => {
                    builder.append_int(v)?;
                }
                Some(Value::Array(ValueArray::Integer(values))) => {
                    let ints: Vec<Option<i32>> = values.iter().map(|v| v.ok().flatten()).collect();
                    builder.append_array_int_nullable(ints)?;
                }
                Some(Value::Array(ValueArray::Float(values))) => {
                    let floats: Vec<Option<f32>> =
                        values.iter().map(|v| v.ok().flatten()).collect();
                    builder.append_array_float_nullable(floats)?;
                }
                Some(Value::Float(v)) => {
                    builder.append_float(v)?;
                }
                Some(Value::String(v)) => {
                    builder.append_string(&v)?;
                }
                Some(Value::Array(ValueArray::String(values))) => {
                    let strings: Vec<Option<String>> = values
                        .iter()
                        .map(|v| v.ok().flatten().map(|s| s.to_string()))
                        .collect();
                    builder.append_array_string_nullable(strings)?;
                }
                Some(Value::Flag) => {
                    builder.append_boolean(true)?;
                }
                None => {
                    if data_type == &DataType::Boolean {
                        builder.append_boolean(false)?;
                    } else {
                        builder.append_null()?;
                    }
                }
                _ => {
                    return Err(datafusion::arrow::error::ArrowError::InvalidArgumentError(
                        format!("Unsupported INFO value type for field '{key}'"),
                    ));
                }
            }
        }
    }

    // Backfill nulls for fields not present in this record
    for idx in 0..num_builders {
        if !info_populated[idx] {
            if info_data_types[idx] == DataType::Boolean {
                info_builders[idx].append_boolean(false)?;
            } else {
                info_builders[idx].append_null()?;
            }
        }
    }

    Ok(())
}

fn get_variant_end(record: &dyn Record, header: &Header) -> u32 {
    let ref_len = record.reference_bases().len();
    let alt_len = record.alternate_bases().len();
    //check if all are single base ACTG
    if ref_len == 1
        && alt_len == 1
        && record
            .reference_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c == b'A' || c == b'C' || c == b'G' || c == b'T')
        && record
            .alternate_bases()
            .iter()
            .map(|c| c.unwrap())
            .all(|c| c.eq("A") || c.eq("C") || c.eq("G") || c.eq("T"))
    {
        record.variant_start().unwrap().unwrap().get() as u32
    } else {
        record.variant_end(header).unwrap().get() as u32
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_local_vcf(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> datafusion::error::Result<impl futures::Stream<Item = datafusion::error::Result<RecordBatch>>>
{
    // let mut count: usize = 0;
    let mut batch_num = 0;
    let schema = Arc::clone(&schema_ref);
    let file_path = file_path.clone();
    let mut reader = VcfLocalReader::new(
        file_path.clone(),
        object_storage_options.unwrap_or_default(),
    )
    .await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let formats = header.formats();
    let mut record_num: usize = 0;
    let mut batch_row_count: usize = 0;
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields.clone(), infos, &mut info_builders);
    let mut num_info_fields = info_builders.0.len();
    let flags = ProjectionFlags::new(&projection, num_info_fields);
    let mut effective_batch_size = choose_effective_batch_size(
        batch_size,
        flags.any_format,
        &format_fields,
        &sample_names,
        &source_sample_names,
        formats,
    );
    let initial_builder_batch_size = choose_initial_builder_batch_size(
        effective_batch_size,
        flags.any_format,
        &source_sample_names,
    );
    info_builders = (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(
        initial_builder_batch_size,
        info_fields.clone(),
        infos,
        &mut info_builders,
    );
    num_info_fields = info_builders.0.len();

    // Build INFO name→index HashMap for single-pass iteration
    // Uses owned String keys (~20-30 short strings cloned once per query) to avoid
    // borrow conflicts with the mutable builders and async generator lifetime issues
    let info_name_to_index: HashMap<String, usize> = info_builders
        .0
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut info_populated = vec![false; num_info_fields];

    let mut format_mode = init_format_mode(
        initial_builder_batch_size,
        format_fields.clone(),
        &sample_names,
        &source_sample_names,
        formats,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let has_format_fields = format_mode.has_fields();

    let mut builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);

    let has_residual_filters = !residual_filters.is_empty();
    let needs_start = flags.start || has_residual_filters;

    let stream = try_stream! {
        let mut join_buf = String::with_capacity(64);

        let mut records = reader.read_records();
        while let Some(result) = records.next().await {
            let record = result?;

            let start_val = if needs_start {
                let start_pos_1based = record.variant_start().unwrap()?.get() as u32;
                Some(if coordinate_system_zero_based {
                    start_pos_1based - 1
                } else {
                    start_pos_1based
                })
            } else {
                None
            };

            let chrom_for_filters = if has_residual_filters {
                Some(record.reference_sequence_name().to_string())
            } else {
                None
            };
            let end_val = if flags.end || has_residual_filters {
                Some(get_variant_end(&record, &header))
            } else {
                None
            };

            if has_residual_filters {
                let fields = VcfRecordFields {
                    chrom: chrom_for_filters.clone(),
                    start: start_val,
                    end: end_val,
                };
                if !evaluate_record_filters(&fields, &residual_filters) {
                    continue;
                }
            }

            if flags.chrom {
                if let Some(chrom) = chrom_for_filters.as_deref() {
                    builders.append_chrom(chrom);
                } else {
                    builders.append_chrom(record.reference_sequence_name());
                }
            }
            if flags.start {
                builders.append_start(start_val.unwrap());
            }
            if flags.end { builders.append_end(end_val.unwrap()); }
            if flags.id { join_into(&mut join_buf, record.ids().iter(), ';'); builders.append_id(&join_buf); }
            if flags.reference { builders.append_ref(record.reference_bases()); }
            if flags.alt { join_into(&mut join_buf, record.alternate_bases().iter().map(|v| v.unwrap_or(".")), '|'); builders.append_alt(&join_buf); }
            if flags.qual { builders.append_qual(record.quality_score().transpose()?.map(|v| v as f64)); }
            if flags.filter { join_into(&mut join_buf, record.filters().iter(&header).map(|v| v.unwrap_or(".")), ';'); builders.append_filter(&join_buf); }
            if flags.any_info { load_infos_single_pass(&record, &header, &info_builders.1, &mut info_builders.2, &info_name_to_index, &mut info_populated)?; }
            if has_format_fields && flags.any_format {
                format_mode.append_record(&record, &header)?;
            }
            record_num += 1;
            batch_row_count += 1;

            if limit.is_some_and(|lim| record_num >= lim) {
                break;
            }

            if batch_row_count == effective_batch_size {
                debug!("Record number: {record_num}");
                let info_arrays = if flags.any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && flags.any_format {
                    Some(
                        format_mode
                            .finish_arrays()
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                effective_batch_size = adjust_effective_batch_size_by_observed_format_bytes(
                    batch_size,
                    effective_batch_size,
                    flags.any_format,
                    &source_sample_names,
                    batch_row_count,
                    format_arrays.as_ref(),
                );
                let core_arrays = builders.finish();
                let batch = build_record_batch_from_builders(
                    schema.clone(),
                    core_arrays,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    batch_row_count,
                )?;
                batch_num += 1;
                batch_row_count = 0;
                debug!("Batch number: {batch_num}");
                yield batch;
            }
        }
        if batch_row_count > 0 {
            let info_arrays = if flags.any_info {
                Some(builders_to_arrays(&mut info_builders.2))
            } else {
                None
            };
            let format_arrays = if has_format_fields && flags.any_format {
                Some(
                    format_mode
                        .finish_arrays()
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                )
            } else {
                None
            };
            let core_arrays = builders.finish();
            let batch = build_record_batch_from_builders(
                schema.clone(),
                core_arrays,
                info_arrays.as_ref(),
                format_arrays.as_ref(),
                num_info_fields,
                &projection,
                batch_row_count,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

/// Reads a local VCF file using a synchronous thread with buffer reuse.
///
/// Uses `read_record(&mut record)` to reuse a single record buffer across reads,
/// avoiding per-record clone allocations. Sends batches via a bounded channel for
/// backpressure. Falls back to the async `get_local_vcf` for GZIP compression.
#[allow(clippy::too_many_arguments)]
async fn get_local_vcf_sync(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    compression_type: CompressionType,
    limit: Option<usize>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<
        Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    >(STREAM_CHANNEL_BUFFERED_BATCHES);

    std::thread::spawn(move || {
        let read_and_send = || -> Result<(), DataFusionError> {
            let (mut reader, header) = open_local_vcf_sync(&file_path, compression_type)
                .map_err(|e| DataFusionError::Execution(format!("Failed to open VCF: {e}")))?;

            let infos = header.infos();
            let formats = header.formats();

            // Initialize INFO builders
            let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
                (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(batch_size, info_fields.clone(), infos, &mut info_builders);
            let mut num_info_fields = info_builders.0.len();
            let flags = ProjectionFlags::new(&projection, num_info_fields);
            let mut effective_batch_size = choose_effective_batch_size(
                batch_size,
                flags.any_format,
                &format_fields,
                &sample_names,
                &source_sample_names,
                formats,
            );
            let initial_builder_batch_size = choose_initial_builder_batch_size(
                effective_batch_size,
                flags.any_format,
                &source_sample_names,
            );
            info_builders = (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(
                initial_builder_batch_size,
                info_fields.clone(),
                infos,
                &mut info_builders,
            );
            num_info_fields = info_builders.0.len();

            let info_name_to_index: HashMap<String, usize> = info_builders
                .0
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut info_populated = vec![false; num_info_fields];

            let mut format_mode = init_format_mode(
                initial_builder_batch_size,
                format_fields.clone(),
                &sample_names,
                &source_sample_names,
                formats,
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let has_format_fields = format_mode.has_fields();

            let mut builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);
            let mut join_buf = String::with_capacity(64);

            let has_residual_filters = !residual_filters.is_empty();
            let needs_start = flags.start || has_residual_filters;
            let mut record = noodles_vcf::Record::default();
            let mut record_num = 0usize;
            let mut batch_row_count: usize = 0;

            loop {
                match reader.read_record(&mut record) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let start_val = if needs_start {
                            let start_pos_1based = record
                                .variant_start()
                                .ok_or_else(|| {
                                    DataFusionError::Execution("Missing variant start".to_string())
                                })?
                                .map_err(|e| {
                                    DataFusionError::Execution(format!("VCF position error: {e}"))
                                })?
                                .get() as u32;
                            Some(if coordinate_system_zero_based {
                                start_pos_1based - 1
                            } else {
                                start_pos_1based
                            })
                        } else {
                            None
                        };

                        let chrom_for_filters = if has_residual_filters {
                            Some(record.reference_sequence_name().to_string())
                        } else {
                            None
                        };
                        let end_val = if flags.end || has_residual_filters {
                            Some(get_variant_end(&record, &header))
                        } else {
                            None
                        };

                        if has_residual_filters {
                            let fields = VcfRecordFields {
                                chrom: chrom_for_filters.clone(),
                                start: start_val,
                                end: end_val,
                            };
                            if !evaluate_record_filters(&fields, &residual_filters) {
                                continue;
                            }
                        }

                        if flags.chrom {
                            if let Some(chrom) = chrom_for_filters.as_deref() {
                                builders.append_chrom(chrom);
                            } else {
                                builders.append_chrom(record.reference_sequence_name());
                            }
                        }
                        if flags.start {
                            builders.append_start(start_val.unwrap());
                        }
                        if flags.end {
                            builders.append_end(end_val.unwrap());
                        }
                        if flags.id {
                            join_into(&mut join_buf, record.ids().iter(), ';');
                            builders.append_id(&join_buf);
                        }
                        if flags.reference {
                            builders.append_ref(record.reference_bases());
                        }
                        if flags.alt {
                            join_into(
                                &mut join_buf,
                                record.alternate_bases().iter().map(|v| v.unwrap_or(".")),
                                '|',
                            );
                            builders.append_alt(&join_buf);
                        }
                        if flags.qual {
                            builders.append_qual(
                                match record.quality_score() {
                                    Some(Ok(v)) => Some(v as f64),
                                    _ => None, // "." → None (missing), non-float string → None
                                },
                            );
                        }
                        if flags.filter {
                            join_into(
                                &mut join_buf,
                                record.filters().iter(&header).map(|v| v.unwrap_or(".")),
                                ';',
                            );
                            builders.append_filter(&join_buf);
                        }
                        if flags.any_info {
                            load_infos_single_pass(
                                &record,
                                &header,
                                &info_builders.1,
                                &mut info_builders.2,
                                &info_name_to_index,
                                &mut info_populated,
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }
                        if has_format_fields && flags.any_format {
                            format_mode
                                .append_record(&record, &header)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }

                        record_num += 1;
                        batch_row_count += 1;

                        if limit.is_some_and(|lim| record_num >= lim) {
                            break;
                        }

                        if batch_row_count == effective_batch_size {
                            debug!("Record number: {record_num}");
                            let info_arrays = if flags.any_info {
                                Some(builders_to_arrays(&mut info_builders.2))
                            } else {
                                None
                            };
                            let format_arrays =
                                if has_format_fields && flags.any_format {
                                    Some(format_mode.finish_arrays().map_err(|e| {
                                        DataFusionError::ArrowError(Box::new(e), None)
                                    })?)
                                } else {
                                    None
                                };
                            effective_batch_size =
                                adjust_effective_batch_size_by_observed_format_bytes(
                                    batch_size,
                                    effective_batch_size,
                                    flags.any_format,
                                    &source_sample_names,
                                    batch_row_count,
                                    format_arrays.as_ref(),
                                );
                            let core_arrays = builders.finish();
                            let batch = build_record_batch_from_builders(
                                Arc::clone(&schema),
                                core_arrays,
                                info_arrays.as_ref(),
                                format_arrays.as_ref(),
                                num_info_fields,
                                &projection,
                                batch_row_count,
                            )?;

                            let mut pending = Ok(batch);
                            loop {
                                match tx.try_send(pending) {
                                    Ok(()) => break,
                                    Err(e) if e.is_disconnected() => return Ok(()),
                                    Err(e) => {
                                        pending = e.into_inner();
                                        std::thread::yield_now();
                                    }
                                }
                            }

                            batch_row_count = 0;
                        }
                    }
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!("VCF read error: {e}")));
                    }
                }
            }

            // Remaining records
            if batch_row_count > 0 {
                let info_arrays = if flags.any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && flags.any_format {
                    Some(
                        format_mode
                            .finish_arrays()
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                let core_arrays = builders.finish();
                let batch = build_record_batch_from_builders(
                    Arc::clone(&schema),
                    core_arrays,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    batch_row_count,
                )?;
                let mut pending = Ok(batch);
                loop {
                    match tx.try_send(pending) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(e) => {
                            pending = e.into_inner();
                            std::thread::yield_now();
                        }
                    }
                }
            }

            debug!("Local VCF sync scan: {record_num} records");
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(e),
            )));
        }
    });

    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

#[allow(clippy::too_many_arguments)]
async fn get_remote_vcf_stream(
    file_path: String,
    schema: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> datafusion::error::Result<
    AsyncStream<datafusion::error::Result<RecordBatch>, impl Future<Output = ()> + Sized>,
> {
    let mut reader = VcfRemoteReader::new(
        file_path.clone(),
        object_storage_options.unwrap_or_default(),
    )
    .await;
    let header = reader.read_header().await?;
    let infos = header.infos();
    let formats = header.formats();
    let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
        (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(batch_size, info_fields.clone(), infos, &mut info_builders);
    let mut num_info_fields = info_builders.0.len();
    let flags = ProjectionFlags::new(&projection, num_info_fields);
    let mut effective_batch_size = choose_effective_batch_size(
        batch_size,
        flags.any_format,
        &format_fields,
        &sample_names,
        &source_sample_names,
        formats,
    );
    let initial_builder_batch_size = choose_initial_builder_batch_size(
        effective_batch_size,
        flags.any_format,
        &source_sample_names,
    );
    info_builders = (Vec::new(), Vec::new(), Vec::new());
    set_info_builders(
        initial_builder_batch_size,
        info_fields.clone(),
        infos,
        &mut info_builders,
    );
    num_info_fields = info_builders.0.len();

    // Build INFO name→index HashMap for single-pass iteration
    // Uses owned String keys (~20-30 short strings cloned once per query) to avoid
    // borrow conflicts with the mutable builders and async generator lifetime issues
    let info_name_to_index: HashMap<String, usize> = info_builders
        .0
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect();
    let mut info_populated = vec![false; num_info_fields];

    let mut format_mode = init_format_mode(
        initial_builder_batch_size,
        format_fields.clone(),
        &sample_names,
        &source_sample_names,
        formats,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let has_format_fields = format_mode.has_fields();

    let has_residual_filters = !residual_filters.is_empty();
    let needs_start = flags.start || has_residual_filters;

    let stream = try_stream! {
        let mut builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);
        let mut join_buf = String::with_capacity(64);

        debug!("Info fields: {:?}", info_builders);

        let mut record_num: usize = 0;
        let mut batch_row_count: usize = 0;
        let mut batch_num = 0;

        let mut records = reader.read_records().await;
        while let Some(result) = records.next().await {
            let record = result?;

            let start_val = if needs_start {
                let start_pos_1based = record.variant_start().unwrap()?.get() as u32;
                Some(if coordinate_system_zero_based {
                    start_pos_1based - 1
                } else {
                    start_pos_1based
                })
            } else {
                None
            };

            let chrom_for_filters = if has_residual_filters {
                Some(record.reference_sequence_name().to_string())
            } else {
                None
            };
            let end_val = if flags.end || has_residual_filters {
                Some(get_variant_end(&record, &header))
            } else {
                None
            };

            if has_residual_filters {
                let fields = VcfRecordFields {
                    chrom: chrom_for_filters.clone(),
                    start: start_val,
                    end: end_val,
                };
                if !evaluate_record_filters(&fields, &residual_filters) {
                    continue;
                }
            }

            if flags.chrom {
                if let Some(chrom) = chrom_for_filters.as_deref() {
                    builders.append_chrom(chrom);
                } else {
                    builders.append_chrom(record.reference_sequence_name());
                }
            }
            if flags.start {
                builders.append_start(start_val.unwrap());
            }
            if flags.end { builders.append_end(end_val.unwrap()); }
            if flags.id { join_into(&mut join_buf, record.ids().iter(), ';'); builders.append_id(&join_buf); }
            if flags.reference { builders.append_ref(record.reference_bases()); }
            if flags.alt { join_into(&mut join_buf, record.alternate_bases().iter().map(|v| v.unwrap_or(".")), '|'); builders.append_alt(&join_buf); }
            if flags.qual { builders.append_qual(record.quality_score().transpose()?.map(|v| v as f64)); }
            if flags.filter { join_into(&mut join_buf, record.filters().iter(&header).map(|v| v.unwrap_or(".")), ';'); builders.append_filter(&join_buf); }
            if flags.any_info { load_infos_single_pass(&record, &header, &info_builders.1, &mut info_builders.2, &info_name_to_index, &mut info_populated)?; }
            if has_format_fields && flags.any_format {
                format_mode.append_record(&record, &header)?;
            }
            record_num += 1;
            batch_row_count += 1;

            if limit.is_some_and(|lim| record_num >= lim) {
                break;
            }

            if batch_row_count == effective_batch_size {
                debug!("Record number: {record_num}");
                let info_arrays = if flags.any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && flags.any_format {
                    Some(
                        format_mode
                            .finish_arrays()
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                effective_batch_size = adjust_effective_batch_size_by_observed_format_bytes(
                    batch_size,
                    effective_batch_size,
                    flags.any_format,
                    &source_sample_names,
                    batch_row_count,
                    format_arrays.as_ref(),
                );
                let core_arrays = builders.finish();
                let batch = build_record_batch_from_builders(
                    schema.clone(),
                    core_arrays,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    batch_row_count,
                )?;
                batch_num += 1;
                batch_row_count = 0;
                debug!("Batch number: {batch_num}");
                yield batch;
            }
        }
        if batch_row_count > 0 {
            let info_arrays = if flags.any_info {
                Some(builders_to_arrays(&mut info_builders.2))
            } else {
                None
            };
            let format_arrays = if has_format_fields && flags.any_format {
                Some(
                    format_mode
                        .finish_arrays()
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                )
            } else {
                None
            };
            let core_arrays = builders.finish();
            let batch = build_record_batch_from_builders(
                schema.clone(),
                core_arrays,
                info_arrays.as_ref(),
                format_arrays.as_ref(),
                num_info_fields,
                &projection,
                batch_row_count,
            )?;
            yield batch;
        }
    };
    Ok(stream)
}

fn set_info_builders(
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    infos: &Infos,
    info_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) {
    let fields: Vec<String> = match info_fields {
        Some(fields) => fields,
        None => infos.keys().map(|k| k.to_string()).collect(),
    };
    for f in fields {
        let data_type = info_to_arrow_type(infos, &f);
        let field = OptionalField::new(&data_type, batch_size).unwrap();
        info_builders.0.push(f);
        info_builders.1.push(data_type);
        info_builders.2.push(field);
    }
}

/// Holds builders for per-sample FORMAT fields.
/// Format builders are organized as: for each sample, for each format field.
/// This matches the column ordering: sample1_field1, sample1_field2, sample2_field1, sample2_field2, etc.
type FormatBuilders = (Vec<String>, Vec<String>, Vec<DataType>, Vec<OptionalField>);

#[derive(Debug, Clone)]
enum ParsedFormatValue {
    Int(i32),
    Float(f32),
    String(String),
    ArrayIntRange { start: usize, len: usize },
    ArrayFloatRange { start: usize, len: usize },
    ArrayStringRange { start: usize, len: usize },
}

struct ParsedArrayPools<'a> {
    int_values: &'a [Option<i32>],
    float_values: &'a [Option<f32>],
    string_values: &'a [Option<String>],
}

#[inline]
fn pool_slice<T>(pool: &[Option<T>], start: usize, len: usize) -> Option<&[Option<T>]> {
    let end = start.checked_add(len)?;
    pool.get(start..end)
}

/// Multisample FORMAT builder that emits a single columnar `genotypes` column:
/// `Struct<GT: List<Utf8>, GQ: List<Int32>, DP: List<Int32>, ...>`.
/// Each list has N elements (one per selected sample) in sample order.
struct MultiSampleFormatBuilder {
    sample_names: Vec<String>,
    header_index_to_output_index: Vec<Option<usize>>,
    /// Schema fields for the genotypes struct (each is List<T>).
    list_fields: Vec<Field>,
    /// Scalar types for each FORMAT field (before wrapping in List).
    scalar_types: Vec<DataType>,
    field_to_index: HashMap<String, usize>,
    // Reusable per-record scratch: flattened [sample_idx * num_fields + field_idx].
    parsed_values: Vec<Option<ParsedFormatValue>>,
    // Tracks which flattened positions were populated in the current record.
    touched_indices: Vec<usize>,
    // Reusable pools for array FORMAT values to avoid per-value Vec allocations.
    array_int_pool: Vec<Option<i32>>,
    array_float_pool: Vec<Option<f32>>,
    array_string_pool: Vec<Option<String>>,
    // Reusable buffer for GT string formatting to avoid per-sample allocation.
    gt_buf: String,
    batch_size: usize,
    batches_since_recycle: usize,
    /// One ListBuilder per FORMAT field.
    format_list_builders: Vec<Box<dyn ArrayBuilder>>,
}

/// Creates one `ListBuilder<T>` per FORMAT field for the columnar layout.
fn build_columnar_list_builders(
    scalar_types: &[DataType],
    sample_count: usize,
    batch_size: usize,
) -> Result<Vec<Box<dyn ArrayBuilder>>, datafusion::arrow::error::ArrowError> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(scalar_types.len());
    // Inner capacity: each list row has `sample_count` elements.
    let inner_cap = batch_size * sample_count;
    for scalar_type in scalar_types {
        let list_builder: Box<dyn ArrayBuilder> = match scalar_type {
            DataType::Utf8 => Box::new(ListBuilder::with_capacity(
                StringBuilder::with_capacity(inner_cap, inner_cap * 8),
                batch_size,
            )),
            DataType::Int32 => Box::new(ListBuilder::with_capacity(
                Int32Builder::with_capacity(inner_cap),
                batch_size,
            )),
            DataType::Float32 => Box::new(ListBuilder::with_capacity(
                Float32Builder::with_capacity(inner_cap),
                batch_size,
            )),
            DataType::Boolean => Box::new(ListBuilder::with_capacity(
                BooleanBuilder::with_capacity(inner_cap),
                batch_size,
            )),
            DataType::List(inner) => match inner.data_type() {
                DataType::Int32 => Box::new(ListBuilder::with_capacity(
                    ListBuilder::with_capacity(Int32Builder::with_capacity(inner_cap), inner_cap),
                    batch_size,
                )),
                DataType::Float32 => Box::new(ListBuilder::with_capacity(
                    ListBuilder::with_capacity(Float32Builder::with_capacity(inner_cap), inner_cap),
                    batch_size,
                )),
                DataType::Utf8 => Box::new(ListBuilder::with_capacity(
                    ListBuilder::with_capacity(
                        StringBuilder::with_capacity(inner_cap, inner_cap * 8),
                        inner_cap,
                    ),
                    batch_size,
                )),
                other => {
                    return Err(datafusion::arrow::error::ArrowError::SchemaError(format!(
                        "Unsupported nested list type in columnar FORMAT: {other:?}"
                    )));
                }
            },
            other => {
                return Err(datafusion::arrow::error::ArrowError::SchemaError(format!(
                    "Unsupported FORMAT data type in columnar layout: {other:?}"
                )));
            }
        };
        builders.push(list_builder);
    }
    Ok(builders)
}

impl MultiSampleFormatBuilder {
    fn new(
        sample_names: Vec<String>,
        selected_header_indices: Vec<usize>,
        source_sample_count: usize,
        value_fields: Vec<datafusion::arrow::datatypes::Field>,
        batch_size: usize,
    ) -> Result<Self, datafusion::arrow::error::ArrowError> {
        // value_fields have scalar types (e.g., Utf8, Int32); we produce List<T> for each.
        let scalar_types: Vec<DataType> =
            value_fields.iter().map(|f| f.data_type().clone()).collect();
        let format_list_builders =
            build_columnar_list_builders(&scalar_types, sample_names.len(), batch_size)?;

        // Build list_fields for the genotypes struct schema.
        let list_fields: Vec<Field> = value_fields
            .iter()
            .map(|vf| {
                Field::new(
                    vf.name(),
                    DataType::List(Arc::new(Field::new("item", vf.data_type().clone(), true))),
                    true,
                )
                .with_metadata(vf.metadata().clone())
            })
            .collect();

        let field_to_index = value_fields
            .iter()
            .enumerate()
            .map(|(i, field)| (field.name().to_string(), i))
            .collect();
        let mut header_index_to_output_index = vec![None; source_sample_count];
        for (output_idx, header_idx) in selected_header_indices.into_iter().enumerate() {
            if header_idx < source_sample_count {
                header_index_to_output_index[header_idx] = Some(output_idx);
            }
        }
        let num_fields = scalar_types.len();
        let num_samples = sample_names.len();
        let parsed_values = vec![None; num_samples * num_fields];
        let touched_indices = Vec::with_capacity(num_samples * num_fields);

        Ok(Self {
            sample_names,
            header_index_to_output_index,
            list_fields,
            scalar_types,
            field_to_index,
            parsed_values,
            touched_indices,
            array_int_pool: Vec::new(),
            array_float_pool: Vec::new(),
            array_string_pool: Vec::new(),
            gt_buf: String::new(),
            batch_size,
            batches_since_recycle: 0,
            format_list_builders,
        })
    }

    fn append_record(
        &mut self,
        record: &dyn Record,
        header: &Header,
    ) -> Result<(), datafusion::arrow::error::ArrowError> {
        use noodles_vcf::variant::record::samples::series::Value as SV;
        use noodles_vcf::variant::record::samples::series::value::Array as SamplesArray;

        for idx in self.touched_indices.drain(..) {
            self.parsed_values[idx] = None;
        }
        self.array_int_pool.clear();
        self.array_float_pool.clear();
        self.array_string_pool.clear();

        let samples = match record.samples() {
            Ok(s) => s,
            Err(_) => {
                // Append null list for each FORMAT field
                for (field_idx, builder) in self.format_list_builders.iter_mut().enumerate() {
                    append_null_list(builder.as_mut(), &self.scalar_types[field_idx])?;
                }
                return Ok(());
            }
        };

        let num_fields = self.scalar_types.len();
        let mut remaining_selected = self.sample_names.len();
        for (header_sample_idx, sample) in samples.iter().enumerate() {
            let Some(Some(output_sample_idx)) = self
                .header_index_to_output_index
                .get(header_sample_idx)
                .copied()
            else {
                continue;
            };

            for result in sample.iter(header) {
                let (key, value) = result.map_err(|e| {
                    datafusion::arrow::error::ArrowError::InvalidArgumentError(format!(
                        "Error reading FORMAT field: {e}"
                    ))
                })?;
                let Some(&idx) = self.field_to_index.get(key) else {
                    continue;
                };

                let parsed_value = if key == "GT" {
                    match value {
                        Some(SV::Genotype(gt)) => {
                            self.gt_buf.clear();
                            let mut first = true;
                            for (allele, phasing) in gt.iter().flatten() {
                                if !first {
                                    match phasing {
                                        Phasing::Phased => self.gt_buf.push('|'),
                                        Phasing::Unphased => self.gt_buf.push('/'),
                                    }
                                }
                                first = false;
                                match allele {
                                    Some(a) => {
                                        let _ = write!(self.gt_buf, "{a}");
                                    }
                                    None => self.gt_buf.push('.'),
                                }
                            }
                            Some(ParsedFormatValue::String(self.gt_buf.clone()))
                        }
                        _ => None,
                    }
                } else {
                    match value {
                        Some(SV::Integer(v)) => Some(ParsedFormatValue::Int(v)),
                        Some(SV::Float(v)) => Some(ParsedFormatValue::Float(v)),
                        Some(SV::String(v)) => Some(ParsedFormatValue::String(v.to_string())),
                        Some(SV::Character(c)) => Some(ParsedFormatValue::String(c.to_string())),
                        Some(SV::Array(arr)) => match arr {
                            SamplesArray::Integer(values) => {
                                let start = self.array_int_pool.len();
                                for v in values.iter() {
                                    self.array_int_pool.push(v.ok().flatten());
                                }
                                Some(ParsedFormatValue::ArrayIntRange {
                                    start,
                                    len: self.array_int_pool.len() - start,
                                })
                            }
                            SamplesArray::Float(values) => {
                                let start = self.array_float_pool.len();
                                for v in values.iter() {
                                    self.array_float_pool.push(v.ok().flatten());
                                }
                                Some(ParsedFormatValue::ArrayFloatRange {
                                    start,
                                    len: self.array_float_pool.len() - start,
                                })
                            }
                            SamplesArray::String(values) => {
                                let start = self.array_string_pool.len();
                                for v in values.iter() {
                                    self.array_string_pool
                                        .push(v.ok().flatten().map(|s| s.to_string()));
                                }
                                Some(ParsedFormatValue::ArrayStringRange {
                                    start,
                                    len: self.array_string_pool.len() - start,
                                })
                            }
                            SamplesArray::Character(values) => {
                                let start = self.array_string_pool.len();
                                for v in values.iter() {
                                    self.array_string_pool
                                        .push(v.ok().flatten().map(|c| c.to_string()));
                                }
                                Some(ParsedFormatValue::ArrayStringRange {
                                    start,
                                    len: self.array_string_pool.len() - start,
                                })
                            }
                        },
                        Some(SV::Genotype(_)) | None => None,
                    }
                };

                if let Some(value) = parsed_value {
                    let flat_idx = output_sample_idx * num_fields + idx;
                    self.parsed_values[flat_idx] = Some(value);
                    self.touched_indices.push(flat_idx);
                }
            }

            remaining_selected -= 1;
            if remaining_selected == 0 {
                break;
            }
        }

        // Build: for each FORMAT field, append a list of N sample values.
        let array_pools = ParsedArrayPools {
            int_values: &self.array_int_pool,
            float_values: &self.array_float_pool,
            string_values: &self.array_string_pool,
        };
        let num_samples = self.sample_names.len();
        for (field_idx, builder) in self.format_list_builders.iter_mut().enumerate() {
            let scalar_type = &self.scalar_types[field_idx];
            append_list_of_samples(
                builder.as_mut(),
                scalar_type,
                &self.parsed_values,
                &array_pools,
                field_idx,
                num_fields,
                num_samples,
            )?;
        }

        Ok(())
    }

    fn finish_array(&mut self) -> Result<Arc<dyn Array>, datafusion::arrow::error::ArrowError> {
        let arrays: Vec<Arc<dyn Array>> = self
            .format_list_builders
            .iter_mut()
            .map(|b| b.finish())
            .collect();

        let struct_array = StructArray::try_new(self.list_fields.clone().into(), arrays, None)?;

        self.batches_since_recycle += 1;
        if self.batches_since_recycle >= MULTISAMPLE_BUILDER_RECYCLE_INTERVAL_BATCHES {
            self.format_list_builders = build_columnar_list_builders(
                &self.scalar_types,
                self.sample_names.len(),
                self.batch_size,
            )?;
            self.batches_since_recycle = 0;
            self.parsed_values.fill(None);
            self.touched_indices.clear();
            self.array_int_pool.clear();
            self.array_float_pool.clear();
            self.array_string_pool.clear();
        }

        Ok(Arc::new(struct_array))
    }
}

/// Appends a null list to a list builder for a given scalar type.
fn append_null_list(
    builder: &mut dyn ArrayBuilder,
    scalar_type: &DataType,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    match scalar_type {
        DataType::Utf8 => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<StringBuilder>>()
            .unwrap()
            .append(false),
        DataType::Int32 => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Int32Builder>>()
            .unwrap()
            .append(false),
        DataType::Float32 => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Float32Builder>>()
            .unwrap()
            .append(false),
        DataType::Boolean => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<BooleanBuilder>>()
            .unwrap()
            .append(false),
        DataType::List(inner) => match inner.data_type() {
            DataType::Int32 => builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<ListBuilder<Int32Builder>>>()
                .unwrap()
                .append(false),
            DataType::Float32 => builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<ListBuilder<Float32Builder>>>()
                .unwrap()
                .append(false),
            DataType::Utf8 => builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<ListBuilder<StringBuilder>>>()
                .unwrap()
                .append(false),
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

/// Appends one list of N sample values for a single FORMAT field.
#[allow(clippy::too_many_arguments)]
fn append_list_of_samples(
    builder: &mut dyn ArrayBuilder,
    scalar_type: &DataType,
    parsed_values: &[Option<ParsedFormatValue>],
    array_pools: &ParsedArrayPools<'_>,
    field_idx: usize,
    num_fields: usize,
    num_samples: usize,
) -> Result<(), datafusion::arrow::error::ArrowError> {
    match scalar_type {
        DataType::Utf8 => {
            let lb = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<StringBuilder>>()
                .unwrap();
            for sample_idx in 0..num_samples {
                let flat = sample_idx * num_fields + field_idx;
                match &parsed_values[flat] {
                    Some(ParsedFormatValue::String(v)) => lb.values().append_value(v),
                    Some(ParsedFormatValue::Int(v)) => lb.values().append_value(v.to_string()),
                    Some(ParsedFormatValue::Float(v)) => lb.values().append_value(format!("{v}")),
                    _ => lb.values().append_null(),
                }
            }
            lb.append(true);
        }
        DataType::Int32 => {
            let lb = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Int32Builder>>()
                .unwrap();
            for sample_idx in 0..num_samples {
                let flat = sample_idx * num_fields + field_idx;
                match &parsed_values[flat] {
                    Some(ParsedFormatValue::Int(v)) => lb.values().append_value(*v),
                    Some(ParsedFormatValue::ArrayIntRange { start, len }) => {
                        if let Some(values) = pool_slice(array_pools.int_values, *start, *len) {
                            if let Some(v) = values.iter().find_map(|v| *v) {
                                lb.values().append_value(v);
                            } else {
                                lb.values().append_null();
                            }
                        } else {
                            lb.values().append_null();
                        }
                    }
                    _ => lb.values().append_null(),
                }
            }
            lb.append(true);
        }
        DataType::Float32 => {
            let lb = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Float32Builder>>()
                .unwrap();
            for sample_idx in 0..num_samples {
                let flat = sample_idx * num_fields + field_idx;
                match &parsed_values[flat] {
                    Some(ParsedFormatValue::Float(v)) => lb.values().append_value(*v),
                    Some(ParsedFormatValue::ArrayFloatRange { start, len }) => {
                        if let Some(values) = pool_slice(array_pools.float_values, *start, *len) {
                            if let Some(v) = values.iter().find_map(|v| *v) {
                                lb.values().append_value(v);
                            } else {
                                lb.values().append_null();
                            }
                        } else {
                            lb.values().append_null();
                        }
                    }
                    _ => lb.values().append_null(),
                }
            }
            lb.append(true);
        }
        DataType::Boolean => {
            let lb = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<BooleanBuilder>>()
                .unwrap();
            for sample_idx in 0..num_samples {
                let _flat = sample_idx * num_fields + field_idx;
                lb.values().append_null();
            }
            lb.append(true);
        }
        DataType::List(inner) => match inner.data_type() {
            DataType::Int32 => {
                let lb = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<ListBuilder<Int32Builder>>>()
                    .unwrap();
                for sample_idx in 0..num_samples {
                    let flat = sample_idx * num_fields + field_idx;
                    match &parsed_values[flat] {
                        Some(ParsedFormatValue::ArrayIntRange { start, len }) => {
                            if let Some(values) = pool_slice(array_pools.int_values, *start, *len) {
                                for v in values {
                                    match v {
                                        Some(v) => lb.values().values().append_value(*v),
                                        None => lb.values().values().append_null(),
                                    }
                                }
                                lb.values().append(true);
                            } else {
                                lb.values().append(false);
                            }
                        }
                        Some(ParsedFormatValue::Int(v)) => {
                            lb.values().values().append_value(*v);
                            lb.values().append(true);
                        }
                        _ => lb.values().append(false),
                    }
                }
                lb.append(true);
            }
            DataType::Float32 => {
                let lb = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<ListBuilder<Float32Builder>>>()
                    .unwrap();
                for sample_idx in 0..num_samples {
                    let flat = sample_idx * num_fields + field_idx;
                    match &parsed_values[flat] {
                        Some(ParsedFormatValue::ArrayFloatRange { start, len }) => {
                            if let Some(values) = pool_slice(array_pools.float_values, *start, *len)
                            {
                                for v in values {
                                    match v {
                                        Some(v) => lb.values().values().append_value(*v),
                                        None => lb.values().values().append_null(),
                                    }
                                }
                                lb.values().append(true);
                            } else {
                                lb.values().append(false);
                            }
                        }
                        Some(ParsedFormatValue::Float(v)) => {
                            lb.values().values().append_value(*v);
                            lb.values().append(true);
                        }
                        _ => lb.values().append(false),
                    }
                }
                lb.append(true);
            }
            DataType::Utf8 => {
                let lb = builder
                    .as_any_mut()
                    .downcast_mut::<ListBuilder<ListBuilder<StringBuilder>>>()
                    .unwrap();
                for sample_idx in 0..num_samples {
                    let flat = sample_idx * num_fields + field_idx;
                    match &parsed_values[flat] {
                        Some(ParsedFormatValue::ArrayStringRange { start, len }) => {
                            if let Some(values) =
                                pool_slice(array_pools.string_values, *start, *len)
                            {
                                for v in values {
                                    match v {
                                        Some(v) => lb.values().values().append_value(v),
                                        None => lb.values().values().append_null(),
                                    }
                                }
                                lb.values().append(true);
                            } else {
                                lb.values().append(false);
                            }
                        }
                        Some(ParsedFormatValue::String(v)) => {
                            lb.values().values().append_value(v);
                            lb.values().append(true);
                        }
                        _ => lb.values().append(false),
                    }
                }
                lb.append(true);
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

enum FormatMode {
    None,
    Single {
        format_builders: FormatBuilders,
        format_field_to_index: HashMap<String, usize>,
        num_format_fields: usize,
        format_populated: Vec<bool>,
        sample_count: usize,
    },
    Multi {
        builder: Box<MultiSampleFormatBuilder>,
    },
}

impl FormatMode {
    fn has_fields(&self) -> bool {
        !matches!(self, Self::None)
    }

    fn append_record(
        &mut self,
        record: &dyn Record,
        header: &Header,
    ) -> Result<(), datafusion::arrow::error::ArrowError> {
        match self {
            Self::None => Ok(()),
            Self::Single {
                format_builders,
                format_field_to_index,
                num_format_fields,
                format_populated,
                sample_count,
            } => load_formats_single_pass(
                record,
                header,
                *sample_count,
                &format_builders.2,
                &mut format_builders.3,
                format_field_to_index,
                *num_format_fields,
                format_populated,
            ),
            Self::Multi { builder } => builder.append_record(record, header),
        }
    }

    fn finish_arrays(
        &mut self,
    ) -> Result<Vec<Arc<dyn Array>>, datafusion::arrow::error::ArrowError> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::Single {
                format_builders, ..
            } => Ok(builders_to_arrays(&mut format_builders.3)),
            Self::Multi { builder } => Ok(vec![builder.finish_array()?]),
        }
    }
}

fn resolve_selected_sample_indices(
    selected_sample_names: &[String],
    source_sample_names: &[String],
) -> Vec<usize> {
    let source_name_to_index: HashMap<&str, usize> = source_sample_names
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.as_str(), idx))
        .collect();

    selected_sample_names
        .iter()
        .filter_map(|name| {
            let idx = source_name_to_index.get(name.as_str()).copied();
            if idx.is_none() {
                warn!("Selected sample '{name}' not found in source header; skipping");
            }
            idx
        })
        .collect()
}

fn init_format_mode(
    batch_size: usize,
    format_fields: Option<Vec<String>>,
    sample_names: &[String],
    source_sample_names: &[String],
    formats: &Formats,
) -> Result<FormatMode, datafusion::arrow::error::ArrowError> {
    let field_names: Vec<String> = match format_fields {
        Some(tags) => tags,
        None => formats.keys().map(|k| k.to_string()).collect(),
    };
    if field_names.is_empty() || sample_names.is_empty() || source_sample_names.is_empty() {
        return Ok(FormatMode::None);
    }

    if source_sample_names.len() == 1 {
        let mut format_builders: FormatBuilders = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        set_format_builders(
            batch_size,
            Some(field_names),
            sample_names,
            formats,
            &mut format_builders,
        );
        let num_format_fields = format_builders.0.len();
        let format_field_to_index: HashMap<String, usize> = format_builders
            .1
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        let format_populated = vec![false; num_format_fields];
        Ok(FormatMode::Single {
            format_builders,
            format_field_to_index,
            num_format_fields,
            format_populated,
            sample_count: sample_names.len(),
        })
    } else {
        let selected_sample_indices =
            resolve_selected_sample_indices(sample_names, source_sample_names);
        if selected_sample_indices.is_empty() {
            return Ok(FormatMode::None);
        }

        let value_fields = field_names
            .iter()
            .map(|name| {
                let data_type = format_to_arrow_type(formats, name);
                let mut metadata = HashMap::new();
                if let Some(format_info) = formats.get(name.as_str()) {
                    metadata.insert(
                        VCF_FIELD_DESCRIPTION_KEY.to_string(),
                        format_info.description().to_string(),
                    );
                    metadata.insert(
                        VCF_FIELD_TYPE_KEY.to_string(),
                        format_type_to_string(&format_info.ty()),
                    );
                    metadata.insert(
                        VCF_FIELD_NUMBER_KEY.to_string(),
                        format_number_to_string(format_info.number()),
                    );
                }
                metadata.insert(VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string());
                metadata.insert(VCF_FIELD_FORMAT_ID_KEY.to_string(), name.clone());
                datafusion::arrow::datatypes::Field::new(name.clone(), data_type, true)
                    .with_metadata(metadata)
            })
            .collect::<Vec<_>>();
        let builder = MultiSampleFormatBuilder::new(
            sample_names.to_vec(),
            selected_sample_indices,
            source_sample_names.len(),
            value_fields,
            batch_size,
        )?;
        Ok(FormatMode::Multi {
            builder: Box::new(builder),
        })
    }
}

fn format_number_to_string(number: FormatNumber) -> String {
    match number {
        FormatNumber::Count(n) => n.to_string(),
        FormatNumber::AlternateBases => "A".to_string(),
        FormatNumber::ReferenceAlternateBases => "R".to_string(),
        FormatNumber::Samples => "G".to_string(),
        FormatNumber::Unknown => ".".to_string(),
        FormatNumber::LocalAlternateBases => "LA".to_string(),
        FormatNumber::LocalReferenceAlternateBases => "LR".to_string(),
        FormatNumber::LocalSamples => "LG".to_string(),
        FormatNumber::Ploidy => "P".to_string(),
        FormatNumber::BaseModifications => "M".to_string(),
    }
}

fn format_type_to_string(ty: &FormatType) -> String {
    match ty {
        FormatType::Integer => "Integer".to_string(),
        FormatType::Float => "Float".to_string(),
        FormatType::Character => "Character".to_string(),
        FormatType::String => "String".to_string(),
    }
}

fn set_format_builders(
    batch_size: usize,
    format_fields: Option<Vec<String>>,
    sample_names: &[String],
    formats: &Formats,
    format_builders: &mut FormatBuilders,
) {
    // If format_fields is None, include all FORMAT fields from header
    let fields: Vec<String> = match format_fields {
        Some(tags) => tags,
        None => formats.keys().map(|k| k.to_string()).collect(),
    };
    for sample_name in sample_names {
        for f in &fields {
            let data_type = format_to_arrow_type(formats, f);
            let field = OptionalField::new(&data_type, batch_size).unwrap();
            format_builders.0.push(sample_name.clone()); // sample name
            format_builders.1.push(f.clone()); // format field name
            format_builders.2.push(data_type);
            format_builders.3.push(field);
        }
    }
}

/// Iterates all FORMAT fields per-sample in a single pass using `sample.iter(header)`,
/// avoiding the O(N*M) cost of calling `samples.select(header, field)` per field
/// and `series.get(header, sample_idx)` per sample.
#[allow(clippy::too_many_arguments)]
fn load_formats_single_pass(
    record: &dyn Record,
    header: &Header,
    sample_count: usize,
    format_data_types: &[DataType],
    format_builders: &mut [OptionalField],
    format_field_to_index: &HashMap<String, usize>,
    num_format_fields: usize,
    format_populated: &mut [bool],
) -> Result<(), datafusion::arrow::error::ArrowError> {
    use noodles_vcf::variant::record::samples::series::Value as SV;
    use noodles_vcf::variant::record::samples::series::value::Array as SamplesArray;

    let samples = match record.samples() {
        Ok(s) => s,
        Err(_) => {
            // If samples can't be read, append null for all format fields
            for builder in format_builders.iter_mut() {
                builder.append_null()?;
            }
            return Ok(());
        }
    };

    if num_format_fields == 0 {
        return Ok(());
    }

    for (sample_idx, sample) in samples.iter().enumerate().take(sample_count) {
        let base_builder_idx = sample_idx * num_format_fields;

        // Reset populated tracking for this sample
        format_populated.fill(false);

        for result in sample.iter(header) {
            let (key, value) = result.map_err(|e| {
                datafusion::arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Error reading FORMAT field: {e}"
                ))
            })?;

            let local_idx = match format_field_to_index.get(key) {
                Some(&idx) => idx,
                None => continue,
            };

            format_populated[local_idx] = true;
            let builder_idx = base_builder_idx + local_idx;
            let builder = &mut format_builders[builder_idx];
            let data_type = &format_data_types[builder_idx];

            // Handle GT (genotype) specially - always convert to string
            if key == "GT" {
                match value {
                    Some(SV::Genotype(gt)) => {
                        let mut gt_str = String::new();
                        let mut first = true;
                        for (allele, phasing) in gt.iter().flatten() {
                            if !first {
                                match phasing {
                                    Phasing::Phased => gt_str.push('|'),
                                    Phasing::Unphased => gt_str.push('/'),
                                }
                            }
                            first = false;
                            match allele {
                                Some(a) => {
                                    let _ = write!(gt_str, "{a}");
                                }
                                None => gt_str.push('.'),
                            }
                        }
                        builder.append_string(&gt_str)?;
                    }
                    _ => builder.append_null()?,
                }
                continue;
            }

            // Handle other FORMAT fields
            match value {
                Some(SV::Integer(v)) => builder.append_int(v)?,
                Some(SV::Float(v)) => builder.append_float(v)?,
                Some(SV::String(v)) => builder.append_string(&v)?,
                Some(SV::Character(c)) => {
                    let mut buf = [0u8; 4];
                    builder.append_string(c.encode_utf8(&mut buf))?;
                }
                Some(SV::Array(arr)) => match arr {
                    SamplesArray::Integer(values) => {
                        if values.iter().all(|v| v.ok().flatten().is_none()) {
                            builder.append_null()?;
                        } else if matches!(data_type, DataType::Int32) {
                            if let Some(first) = values.iter().find_map(|v| v.ok().flatten()) {
                                builder.append_int(first)?;
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_array_int_nullable_iter(
                                values.iter().map(|v| v.ok().flatten()),
                            )?;
                        }
                    }
                    SamplesArray::Float(values) => {
                        if values.iter().all(|v| v.ok().flatten().is_none()) {
                            builder.append_null()?;
                        } else if matches!(data_type, DataType::Float32) {
                            if let Some(first) = values.iter().find_map(|v| v.ok().flatten()) {
                                builder.append_float(first)?;
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_array_float_nullable_iter(
                                values.iter().map(|v| v.ok().flatten()),
                            )?;
                        }
                    }
                    SamplesArray::String(values) => {
                        if values.iter().all(|v| v.ok().flatten().is_none()) {
                            builder.append_null()?;
                        } else if matches!(data_type, DataType::Utf8) {
                            if let Some(first) = values.iter().find_map(|v| v.ok().flatten()) {
                                builder.append_string(first.as_ref())?;
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_array_string_nullable_iter(
                                values
                                    .iter()
                                    .map(|v| v.ok().flatten().map(|s| s.to_string())),
                            )?;
                        }
                    }
                    SamplesArray::Character(values) => {
                        if values.iter().all(|v| v.ok().flatten().is_none()) {
                            builder.append_null()?;
                        } else if matches!(data_type, DataType::Utf8) {
                            if let Some(first) = values.iter().find_map(|v| v.ok().flatten()) {
                                builder.append_string(&first.to_string())?;
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_array_string_nullable_iter(
                                values
                                    .iter()
                                    .map(|v| v.ok().flatten().map(|c| c.to_string())),
                            )?;
                        }
                    }
                },
                Some(SV::Genotype(_)) => {
                    // Genotype should have been handled above
                    builder.append_null()?;
                }
                None => builder.append_null()?,
            }
        }

        // Backfill nulls for FORMAT fields not present in this sample
        for local_idx in 0..num_format_fields {
            if !format_populated[local_idx] {
                format_builders[base_builder_idx + local_idx].append_null()?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn get_stream(
    file_path: String,
    schema_ref: SchemaRef,
    batch_size: usize,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    projection: Option<Vec<usize>>,
    object_storage_options: Option<ObjectStorageOptions>,
    coordinate_system_zero_based: bool,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    // Open the BGZF-indexed VCF using IndexedReader.

    let file_path = file_path.clone();
    let store_type = get_storage_type(file_path.clone());
    let schema = schema_ref.clone();

    match store_type {
        StorageType::LOCAL => {
            let opts = object_storage_options.clone().unwrap_or_default();
            let compression_type = get_compression_type(
                file_path.clone(),
                opts.compression_type.clone(),
                opts.clone(),
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to detect compression: {e}"))
            })?;

            if matches!(compression_type, CompressionType::GZIP) {
                // GZIP: fall back to the async stream-based reader
                let stream = get_local_vcf(
                    file_path.clone(),
                    schema.clone(),
                    batch_size,
                    info_fields,
                    format_fields,
                    sample_names,
                    source_sample_names,
                    projection,
                    object_storage_options,
                    coordinate_system_zero_based,
                    residual_filters,
                    limit,
                )
                .await?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
            } else {
                // BGZF / PLAIN: sync thread with buffer reuse
                get_local_vcf_sync(
                    file_path.clone(),
                    schema_ref,
                    batch_size,
                    info_fields,
                    format_fields,
                    sample_names,
                    source_sample_names,
                    projection,
                    coordinate_system_zero_based,
                    residual_filters,
                    compression_type,
                    limit,
                )
                .await
            }
        }
        StorageType::GCS | StorageType::S3 | StorageType::AZBLOB => {
            let stream = get_remote_vcf_stream(
                file_path.clone(),
                schema.clone(),
                batch_size,
                info_fields,
                format_fields,
                sample_names,
                source_sample_names,
                projection,
                object_storage_options,
                coordinate_system_zero_based,
                residual_filters,
                limit,
            )
            .await?;
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
        }
        _ => unimplemented!("Unsupported storage type: {:?}", store_type),
    }
}

#[allow(dead_code)]
pub struct VcfExec {
    pub(crate) file_path: String,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) info_fields: Option<Vec<String>>,
    pub(crate) format_fields: Option<Vec<String>>,
    /// Samples included in output (all or filtered subset).
    pub(crate) sample_names: Vec<String>,
    /// All sample names from source VCF header.
    pub(crate) source_sample_names: Vec<String>,
    pub(crate) cache: PlanProperties,
    pub(crate) limit: Option<usize>,
    pub(crate) object_storage_options: Option<ObjectStorageOptions>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed coordinates
    pub(crate) coordinate_system_zero_based: bool,
    /// Partition assignments for index-based reading (None = full scan)
    pub(crate) partition_assignments: Option<Vec<PartitionAssignment>>,
    /// Path to the index file (TBI/CSI)
    pub(crate) index_path: Option<String>,
    /// Residual filters for record-level evaluation
    pub(crate) residual_filters: Vec<Expr>,
}

impl Debug for VcfExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VcfExec")
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for VcfExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let proj_str = match &self.projection {
            Some(_) => {
                let col_names: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                col_names.join(", ")
            }
            None => "*".to_string(),
        };
        write!(f, "VcfExec: projection=[{proj_str}]")
    }
}

impl ExecutionPlan for VcfExec {
    fn name(&self) -> &str {
        "VCFExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let proj_cols = match &self.projection {
            Some(_) => self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", "),
            None => "*".to_string(),
        };
        info!(
            "{}: executing partition={} with projection=[{}]",
            self.name(),
            partition,
            proj_cols
        );
        let batch_size = context.session_config().batch_size();
        let schema = self.schema.clone();

        // Use indexed reading when partition assignments and index are available
        if let (Some(assignments), Some(index_path)) =
            (&self.partition_assignments, &self.index_path)
        {
            if partition < assignments.len() {
                let regions = assignments[partition].regions.clone();
                let file_path = self.file_path.clone();
                let index_path = index_path.clone();
                let projection = self.projection.clone();
                let coord_zero_based = self.coordinate_system_zero_based;
                let info_fields = self.info_fields.clone();
                let format_fields = self.format_fields.clone();
                let sample_names = self.sample_names.clone();
                let source_sample_names = self.source_sample_names.clone();
                let residual_filters = self.residual_filters.clone();

                let limit = self.limit;
                let fut = get_indexed_vcf_stream(
                    file_path,
                    index_path,
                    regions,
                    schema.clone(),
                    batch_size,
                    projection,
                    coord_zero_based,
                    info_fields,
                    format_fields,
                    sample_names,
                    source_sample_names,
                    residual_filters,
                    limit,
                );
                let stream = futures::stream::once(fut).try_flatten();
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
            }
        }

        // Fallback: full scan (original path)
        let fut = get_stream(
            self.file_path.clone(),
            schema.clone(),
            batch_size,
            self.info_fields.clone(),
            self.format_fields.clone(),
            self.sample_names.clone(),
            self.source_sample_names.clone(),
            self.projection.clone(),
            self.object_storage_options.clone(),
            self.coordinate_system_zero_based,
            self.residual_filters.clone(),
            self.limit,
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Build a noodles Region from a GenomicRegion.
fn build_noodles_region(region: &GenomicRegion) -> Result<noodles_core::Region, DataFusionError> {
    let to_position =
        |label: &str, value: u64| -> Result<noodles_core::Position, DataFusionError> {
            let n = usize::try_from(value).map_err(|_| {
                DataFusionError::Execution(format!(
                    "Invalid region '{}': {label} position {value} exceeds platform size",
                    region.chrom
                ))
            })?;
            noodles_core::Position::try_from(n).map_err(|_| {
                DataFusionError::Execution(format!(
                    "Invalid region '{}': {label} position must be >= 1 (got {value})",
                    region.chrom
                ))
            })
        };

    match (region.start, region.end) {
        (Some(start), Some(end)) => {
            if end < start {
                return Err(DataFusionError::Execution(format!(
                    "Invalid region '{}': end ({end}) is less than start ({start})",
                    region.chrom
                )));
            }

            let start_pos = to_position("start", start)?;
            let end_pos = to_position("end", end)?;
            Ok(noodles_core::Region::new(
                region.chrom.clone(),
                start_pos..=end_pos,
            ))
        }
        (Some(start), None) => {
            let start_pos = to_position("start", start)?;
            Ok(noodles_core::Region::new(region.chrom.clone(), start_pos..))
        }
        (None, Some(end)) => {
            let end_pos = to_position("end", end)?;
            Ok(noodles_core::Region::new(region.chrom.clone(), ..=end_pos))
        }
        (None, None) => Ok(noodles_core::Region::new(region.chrom.clone(), ..)),
    }
}

/// Get a streaming RecordBatch stream from an indexed VCF file for one or more regions.
///
/// Uses `thread::spawn` + `mpsc::channel(1)` for streaming I/O with backpressure.
/// Each partition processes its assigned regions sequentially, keeping only ~2 batches
/// in memory at a time (constant memory regardless of data volume).
#[allow(clippy::too_many_arguments)]
async fn get_indexed_vcf_stream(
    file_path: String,
    index_path: String,
    regions: Vec<GenomicRegion>,
    schema_ref: SchemaRef,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    coordinate_system_zero_based: bool,
    info_fields: Option<Vec<String>>,
    format_fields: Option<Vec<String>>,
    sample_names: Vec<String>,
    source_sample_names: Vec<String>,
    residual_filters: Vec<Expr>,
    limit: Option<usize>,
) -> datafusion::error::Result<SendableRecordBatchStream> {
    let schema = schema_ref.clone();
    let (mut tx, rx) = futures::channel::mpsc::channel::<
        Result<RecordBatch, datafusion::arrow::error::ArrowError>,
    >(STREAM_CHANNEL_BUFFERED_BATCHES);

    std::thread::spawn(move || {
        let mut read_and_send = || -> Result<(), DataFusionError> {
            let mut indexed_reader =
                IndexedVcfReader::new(&file_path, &index_path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open indexed VCF: {e}"))
                })?;

            let header = indexed_reader.header().clone();
            let infos = header.infos();
            let formats = header.formats();

            // Initialize INFO builders
            let mut info_builders: (Vec<String>, Vec<DataType>, Vec<OptionalField>) =
                (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(batch_size, info_fields.clone(), infos, &mut info_builders);
            let mut num_info_fields = info_builders.0.len();
            let flags = ProjectionFlags::new(&projection, num_info_fields);
            let mut effective_batch_size = choose_effective_batch_size(
                batch_size,
                flags.any_format,
                &format_fields,
                &sample_names,
                &source_sample_names,
                formats,
            );
            let initial_builder_batch_size = choose_initial_builder_batch_size(
                effective_batch_size,
                flags.any_format,
                &source_sample_names,
            );
            info_builders = (Vec::new(), Vec::new(), Vec::new());
            set_info_builders(
                initial_builder_batch_size,
                info_fields.clone(),
                infos,
                &mut info_builders,
            );
            num_info_fields = info_builders.0.len();

            // Build INFO name→index HashMap for single-pass iteration
            let info_name_to_index: HashMap<String, usize> = info_builders
                .0
                .iter()
                .enumerate()
                .map(|(i, name)| (name.clone(), i))
                .collect();
            let mut info_populated = vec![false; num_info_fields];

            let mut format_mode = init_format_mode(
                initial_builder_batch_size,
                format_fields.clone(),
                &sample_names,
                &source_sample_names,
                formats,
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let has_format_fields = format_mode.has_fields();

            let mut builders = CoreBatchBuilders::new(&flags, initial_builder_batch_size);
            let mut join_buf = String::with_capacity(64);
            let has_residual_filters = !residual_filters.is_empty();
            let needs_start_for_filters = flags.start || has_residual_filters;

            let mut total_records = 0usize;
            let mut batch_record_count = 0usize;

            'regions: for region in &regions {
                if region.unmapped_tail {
                    continue;
                }

                let region_start_1based = region.start.map(|s| s as u32);
                let region_end_1based = region.end.map(|e| e as u32);

                let noodles_region = build_noodles_region(region)?;

                let records = match indexed_reader.query(&noodles_region) {
                    Ok(records) => records,
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("does not exist in reference sequences") {
                            debug!(
                                "Skipping region {:?}: chromosome not in VCF reference sequences",
                                region.chrom
                            );
                            continue;
                        }
                        return Err(DataFusionError::Execution(format!(
                            "VCF region query failed: {e}"
                        )));
                    }
                };

                for result in records {
                    let record = result.map_err(|e| {
                        DataFusionError::Execution(format!("VCF record read error: {e}"))
                    })?;

                    let needs_start_for_region =
                        region_start_1based.is_some() || region_end_1based.is_some();
                    let needs_start = needs_start_for_filters || needs_start_for_region;
                    let (start_pos, start_val) = if needs_start {
                        let start_pos = record
                            .variant_start()
                            .ok_or_else(|| {
                                DataFusionError::Execution("Missing variant start".to_string())
                            })?
                            .map_err(|e| {
                                DataFusionError::Execution(format!("VCF position error: {e}"))
                            })?
                            .get() as u32;
                        let start_val = if coordinate_system_zero_based {
                            start_pos - 1
                        } else {
                            start_pos
                        };
                        (Some(start_pos), Some(start_val))
                    } else {
                        (None, None)
                    };

                    if let Some(rs) = region_start_1based {
                        if start_pos.unwrap() < rs {
                            continue;
                        }
                    }
                    if let Some(re) = region_end_1based {
                        if start_pos.unwrap() > re {
                            continue;
                        }
                    }

                    let chrom_for_filters = if has_residual_filters {
                        Some(record.reference_sequence_name().to_string())
                    } else {
                        None
                    };
                    let end_val = if flags.end || has_residual_filters {
                        Some(get_variant_end(&record, &header))
                    } else {
                        None
                    };

                    if has_residual_filters {
                        let fields = VcfRecordFields {
                            chrom: chrom_for_filters.clone(),
                            start: start_val,
                            end: end_val,
                        };
                        if !evaluate_record_filters(&fields, &residual_filters) {
                            continue;
                        }
                    }

                    if flags.chrom {
                        if let Some(chrom) = chrom_for_filters.as_deref() {
                            builders.append_chrom(chrom);
                        } else {
                            builders.append_chrom(record.reference_sequence_name());
                        }
                    }
                    if flags.start {
                        builders.append_start(start_val.unwrap());
                    }
                    if flags.end {
                        builders.append_end(end_val.unwrap());
                    }
                    if flags.id {
                        join_into(&mut join_buf, record.ids().iter(), ';');
                        builders.append_id(&join_buf);
                    }
                    if flags.reference {
                        builders.append_ref(record.reference_bases());
                    }
                    if flags.alt {
                        join_into(
                            &mut join_buf,
                            record.alternate_bases().iter().map(|v| v.unwrap_or(".")),
                            '|',
                        );
                        builders.append_alt(&join_buf);
                    }
                    if flags.qual {
                        builders.append_qual(
                            record
                                .quality_score()
                                .transpose()
                                .map_err(|e| {
                                    DataFusionError::Execution(format!("VCF qual error: {e}"))
                                })?
                                .map(|v| v as f64),
                        );
                    }
                    if flags.filter {
                        join_into(
                            &mut join_buf,
                            record.filters().iter(&header).map(|v| v.unwrap_or(".")),
                            ';',
                        );
                        builders.append_filter(&join_buf);
                    }
                    if flags.any_info {
                        load_infos_single_pass(
                            &record,
                            &header,
                            &info_builders.1,
                            &mut info_builders.2,
                            &info_name_to_index,
                            &mut info_populated,
                        )
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    }
                    if has_format_fields && flags.any_format {
                        format_mode
                            .append_record(&record, &header)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    }

                    total_records += 1;
                    batch_record_count += 1;

                    if limit.is_some_and(|lim| total_records >= lim) {
                        break 'regions;
                    }

                    if batch_record_count == effective_batch_size {
                        let info_arrays = if flags.any_info {
                            Some(builders_to_arrays(&mut info_builders.2))
                        } else {
                            None
                        };
                        let format_arrays = if has_format_fields && flags.any_format {
                            Some(
                                format_mode
                                    .finish_arrays()
                                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                            )
                        } else {
                            None
                        };
                        effective_batch_size = adjust_effective_batch_size_by_observed_format_bytes(
                            batch_size,
                            effective_batch_size,
                            flags.any_format,
                            &source_sample_names,
                            batch_record_count,
                            format_arrays.as_ref(),
                        );
                        let core_arrays = builders.finish();
                        let batch = build_record_batch_from_builders(
                            Arc::clone(&schema),
                            core_arrays,
                            info_arrays.as_ref(),
                            format_arrays.as_ref(),
                            num_info_fields,
                            &projection,
                            batch_record_count,
                        )?;

                        let mut pending = Ok(batch);
                        loop {
                            match tx.try_send(pending) {
                                Ok(()) => break,
                                Err(e) if e.is_disconnected() => return Ok(()),
                                Err(e) => {
                                    pending = e.into_inner();
                                    std::thread::yield_now();
                                }
                            }
                        }

                        batch_record_count = 0;
                    }
                }
            }

            // Remaining records
            if batch_record_count > 0 {
                let info_arrays = if flags.any_info {
                    Some(builders_to_arrays(&mut info_builders.2))
                } else {
                    None
                };
                let format_arrays = if has_format_fields && flags.any_format {
                    Some(
                        format_mode
                            .finish_arrays()
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                    )
                } else {
                    None
                };
                let core_arrays = builders.finish();
                let batch = build_record_batch_from_builders(
                    Arc::clone(&schema),
                    core_arrays,
                    info_arrays.as_ref(),
                    format_arrays.as_ref(),
                    num_info_fields,
                    &projection,
                    batch_record_count,
                )?;
                let mut pending = Ok(batch);
                loop {
                    match tx.try_send(pending) {
                        Ok(()) => break,
                        Err(e) if e.is_disconnected() => break,
                        Err(e) => {
                            pending = e.into_inner();
                            std::thread::yield_now();
                        }
                    }
                }
            }

            debug!(
                "Indexed VCF scan: {} records for {} regions",
                total_records,
                regions.len()
            );
            Ok(())
        };
        if let Err(e) = read_and_send() {
            let _ = tx.try_send(Err(datafusion::arrow::error::ArrowError::ExternalError(
                Box::new(e),
            )));
        }
    });

    // Stream batches from the channel
    let stream = rx.map(|item| item.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema_ref, stream)))
}

#[cfg(test)]
mod build_noodles_region_tests {
    use super::build_noodles_region;
    use datafusion_bio_format_core::genomic_filter::GenomicRegion;

    #[test]
    fn supports_contig_names_with_colons() {
        let region = GenomicRegion {
            chrom: "HLA-A*01:01:01:02N".to_string(),
            start: None,
            end: None,
            unmapped_tail: false,
        };

        let parsed = build_noodles_region(&region).expect("region should be valid");
        let name_bytes: &[u8] = parsed.name().as_ref();
        assert_eq!(
            name_bytes, b"HLA-A*01:01:01:02N",
            "region name should be preserved exactly"
        );
        assert_eq!(parsed.interval().start().map(|p| p.get()), None);
        assert_eq!(parsed.interval().end().map(|p| p.get()), None);
    }

    #[test]
    fn supports_intervals_for_contig_names_with_colons() {
        let region = GenomicRegion {
            chrom: "HLA-A*01:01:01:02N".to_string(),
            start: Some(10),
            end: Some(20),
            unmapped_tail: false,
        };

        let parsed = build_noodles_region(&region).expect("region should be valid");
        let name_bytes: &[u8] = parsed.name().as_ref();
        assert_eq!(
            name_bytes, b"HLA-A*01:01:01:02N",
            "region name should be preserved exactly"
        );
        assert_eq!(parsed.interval().start().map(|p| p.get()), Some(10));
        assert_eq!(parsed.interval().end().map(|p| p.get()), Some(20));
    }

    #[test]
    fn rejects_zero_positions() {
        let region = GenomicRegion {
            chrom: "chr1".to_string(),
            start: Some(0),
            end: None,
            unmapped_tail: false,
        };

        let err = build_noodles_region(&region).expect_err("zero start must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("start position must be >= 1"));
    }
}
