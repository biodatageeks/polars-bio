//! Write module for VCF and FASTQ file output support using datafusion-bio-formats.
//!
//! This module provides streaming write functionality that leverages the `insert_into()` API
//! from datafusion-bio-formats TableProviders for efficient, memory-conscious writes.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_DESCRIPTION_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
};
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use futures::{Stream, StreamExt};
use log::info;

use crate::option::{OutputFormat, WriteOptions};

/// Build field metadata HashMap from a VCF meta object.
///
/// Extracts "number", "type", and "description" fields from a JSON metadata object
/// and converts them to VCF field metadata keys.
fn build_field_metadata_from_vcf_meta(
    meta_obj: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, String> {
    use serde_json::Value;

    let mut field_metadata = HashMap::new();

    if let Some(Value::String(number)) = meta_obj.get("number") {
        field_metadata.insert(VCF_FIELD_NUMBER_KEY.to_string(), number.clone());
    }
    if let Some(Value::String(ty)) = meta_obj.get("type") {
        field_metadata.insert(VCF_FIELD_TYPE_KEY.to_string(), ty.clone());
    }
    if let Some(Value::String(desc)) = meta_obj.get("description") {
        field_metadata.insert(VCF_FIELD_DESCRIPTION_KEY.to_string(), desc.clone());
    }

    field_metadata
}

/// Extract FORMAT field names from nested multisample `genotypes` schema.
///
/// Expected shape:
/// list<struct<sample_id: Utf8, values: struct<GT: ..., DP: ..., ...>>>
fn extract_format_fields_from_nested_genotypes(schema: &SchemaRef) -> Vec<String> {
    let Some(genotypes_field) = schema.fields().iter().find(|f| f.name() == "genotypes") else {
        return Vec::new();
    };

    let list_item = match genotypes_field.data_type() {
        DataType::List(inner) | DataType::LargeList(inner) => inner.as_ref(),
        _ => return Vec::new(),
    };

    let genotype_struct_fields = match list_item.data_type() {
        DataType::Struct(fields) => fields,
        _ => return Vec::new(),
    };

    let Some(values_field) = genotype_struct_fields.iter().find(|f| f.name() == "values") else {
        return Vec::new();
    };

    let value_struct_fields = match values_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => return Vec::new(),
    };

    value_struct_fields
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

/// Consume a write stream and count total rows written.
///
/// The WriteExec returns batches with a "count" column containing the number of rows written.
/// This helper function consumes the stream and aggregates the total row count.
async fn consume_write_stream(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Result<u64, DataFusionError> {
    let mut total_rows: u64 = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        // WriteExec returns a batch with a "count" column containing the number of rows written
        if let Some(count_col) = batch.column_by_name("count") {
            if let Some(count_array) = count_col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            {
                if count_array.len() > 0 && !count_array.is_null(0) {
                    total_rows += count_array.value(0);
                }
            }
        } else {
            // Fallback: count rows in the batch itself
            total_rows += batch.num_rows() as u64;
        }
    }
    Ok(total_rows)
}

/// Apply VCF metadata from JSON strings to an Arrow schema.
///
/// This parses the metadata JSON and adds VCF field metadata to the schema fields,
/// allowing the VCF writer to generate proper headers with Number, Type, and Description.
fn apply_vcf_metadata_to_schema(
    schema: &SchemaRef,
    info_meta_json: Option<String>,
    format_meta_json: Option<String>,
    sample_names_json: Option<String>,
) -> Result<(Vec<String>, Vec<String>, Vec<String>, SchemaRef), DataFusionError> {
    use serde_json::Value;

    // Parse INFO field metadata
    let info_meta: HashMap<String, Value> = if let Some(json) = info_meta_json {
        serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Internal(format!("Failed to parse INFO field metadata JSON: {}", e))
        })?
    } else {
        HashMap::new()
    };

    // Parse FORMAT field metadata
    let format_meta: HashMap<String, Value> = if let Some(json) = format_meta_json {
        serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Internal(format!("Failed to parse FORMAT field metadata JSON: {}", e))
        })?
    } else {
        HashMap::new()
    };

    // Parse sample names
    let sample_names: Vec<String> = if let Some(json) = sample_names_json {
        serde_json::from_str(&json).map_err(|e| {
            DataFusionError::Internal(format!("Failed to parse sample names JSON: {}", e))
        })?
    } else {
        Vec::new()
    };

    // Build new schema with metadata
    let mut new_fields = Vec::new();
    let mut info_fields = Vec::new();
    let mut format_fields = Vec::new();
    let nested_format_fields = extract_format_fields_from_nested_genotypes(schema);

    // Core VCF columns that are not INFO or FORMAT
    let core_columns: std::collections::HashSet<&str> = [
        "chrom", "start", "end", "id", "ref", "alt", "qual", "filter",
    ]
    .into_iter()
    .collect();

    for field in schema.fields() {
        let name = field.name();

        if core_columns.contains(name.as_str()) {
            new_fields.push(field.as_ref().clone());
            continue;
        }

        // Handle multisample nested schema: genotypes: list<struct<sample_id, values<FORMAT...>>>
        // Attach FORMAT metadata directly to nested `values` fields so the VCF writer can
        // preserve Number/Type/Description in output headers.
        if name == "genotypes" {
            let nested_with_meta = (|| -> Option<Field> {
                let (item_field, is_large_list) = match field.data_type() {
                    DataType::List(item) => (item, false),
                    DataType::LargeList(item) => (item, true),
                    _ => return None,
                };

                let genotype_struct_fields = match item_field.data_type() {
                    DataType::Struct(fields) => fields,
                    _ => return None,
                };

                let mut rebuilt_genotype_fields = Vec::with_capacity(genotype_struct_fields.len());

                for genotype_field in genotype_struct_fields.iter() {
                    if genotype_field.name() != "values" {
                        rebuilt_genotype_fields
                            .push(std::sync::Arc::new(genotype_field.as_ref().clone()));
                        continue;
                    }

                    let values_struct_fields = match genotype_field.data_type() {
                        DataType::Struct(fields) => fields,
                        _ => {
                            rebuilt_genotype_fields
                                .push(std::sync::Arc::new(genotype_field.as_ref().clone()));
                            continue;
                        },
                    };

                    let mut rebuilt_values_fields = Vec::with_capacity(values_struct_fields.len());

                    for value_field in values_struct_fields.iter() {
                        let format_id = value_field.name();
                        let mut rebuilt_value_field = value_field.as_ref().clone();

                        if let Some(serde_json::Value::Object(meta_obj)) =
                            format_meta.get(format_id)
                        {
                            let field_metadata = build_field_metadata_from_vcf_meta(meta_obj);
                            rebuilt_value_field = rebuilt_value_field.with_metadata(field_metadata);
                        }

                        if !format_fields.contains(format_id) {
                            format_fields.push(format_id.to_string());
                        }

                        rebuilt_values_fields.push(std::sync::Arc::new(rebuilt_value_field));
                    }

                    let values_type = DataType::Struct(rebuilt_values_fields.into());
                    let mut rebuilt_values_field = Field::new(
                        genotype_field.name(),
                        values_type,
                        genotype_field.is_nullable(),
                    );
                    if !genotype_field.metadata().is_empty() {
                        rebuilt_values_field =
                            rebuilt_values_field.with_metadata(genotype_field.metadata().clone());
                    }

                    rebuilt_genotype_fields.push(std::sync::Arc::new(rebuilt_values_field));
                }

                let rebuilt_item_type = DataType::Struct(rebuilt_genotype_fields.into());
                let mut rebuilt_item_field = Field::new(
                    item_field.name(),
                    rebuilt_item_type,
                    item_field.is_nullable(),
                );
                if !item_field.metadata().is_empty() {
                    rebuilt_item_field =
                        rebuilt_item_field.with_metadata(item_field.metadata().clone());
                }

                let rebuilt_list_type = if is_large_list {
                    DataType::LargeList(std::sync::Arc::new(rebuilt_item_field))
                } else {
                    DataType::List(std::sync::Arc::new(rebuilt_item_field))
                };

                let mut rebuilt_genotypes_field =
                    Field::new(name, rebuilt_list_type, field.is_nullable());
                if !field.metadata().is_empty() {
                    rebuilt_genotypes_field =
                        rebuilt_genotypes_field.with_metadata(field.metadata().clone());
                }
                Some(rebuilt_genotypes_field)
            })();

            if let Some(genotypes_field) = nested_with_meta {
                new_fields.push(genotypes_field);
                continue;
            }
        }

        // Check if this is an INFO field
        if let Some(meta_value) = info_meta.get(name) {
            if let Value::Object(meta_obj) = meta_value {
                let field_metadata = build_field_metadata_from_vcf_meta(meta_obj);

                info_fields.push(name.clone());
                new_fields.push(field.as_ref().clone().with_metadata(field_metadata));
                continue;
            }
        }

        // Check if this is a FORMAT field
        // Patterns: {sample}_{format} (multi-sample) OR {format} (single-sample)
        let mut is_format = false;

        // First try multi-sample pattern: {sample}_{format}
        for format_name in format_meta.keys() {
            for sample in &sample_names {
                let col_pattern = format!("{}_{}", sample, format_name);
                if name == &col_pattern {
                    if let Some(Value::Object(meta_obj)) = format_meta.get(format_name) {
                        let field_metadata = build_field_metadata_from_vcf_meta(meta_obj);

                        if !format_fields.contains(format_name) {
                            format_fields.push(format_name.clone());
                        }
                        new_fields.push(field.as_ref().clone().with_metadata(field_metadata));
                        is_format = true;
                        break;
                    }
                }
            }
            if is_format {
                break;
            }
        }

        // If not found, try single-sample pattern: column name equals format_id directly
        if !is_format {
            if let Some(Value::Object(meta_obj)) = format_meta.get(name) {
                let field_metadata = build_field_metadata_from_vcf_meta(meta_obj);

                if !format_fields.contains(name) {
                    format_fields.push(name.clone());
                }
                new_fields.push(field.as_ref().clone().with_metadata(field_metadata));
                is_format = true;
            }
        }

        if !is_format {
            // Keep field as-is (might be detected by heuristics later)
            new_fields.push(field.as_ref().clone());
        }
    }

    // Multisample nested schema (`genotypes`) has no top-level FORMAT columns, so the
    // loop above cannot discover FORMAT ids from column names. In that case, derive the
    // output FORMAT list from nested `genotypes.values` field names.
    if format_fields.is_empty() && !nested_format_fields.is_empty() {
        format_fields = nested_format_fields;
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));

    Ok((info_fields, format_fields, sample_names, new_schema))
}

/// Write a DataFrame to a file in the specified format using streaming.
///
/// This function uses datafusion-bio-formats' `insert_into()` API for true streaming writes,
/// processing data batch-by-batch without materializing the entire dataset in memory.
///
/// # Arguments
/// * `ctx` - The DataFusion SessionContext
/// * `df` - The DataFrame to write (can be backed by a lazy query)
/// * `path` - Output file path (compression auto-detected from extension)
/// * `format` - Output format (VCF or FASTQ)
/// * `write_options` - Optional write options for compression and coordinate handling
///
/// # Returns
/// The number of rows written, or an error if the write fails.
pub(crate) async fn write_table(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    format: OutputFormat,
    write_options: Option<WriteOptions>,
) -> Result<u64, DataFusionError> {
    info!("Streaming write to {} as {}", path, format);

    match format {
        OutputFormat::Vcf => write_vcf_streaming(ctx, df, path, write_options).await,
        OutputFormat::Fastq => write_fastq_streaming(ctx, df, path, write_options).await,
        OutputFormat::Bam | OutputFormat::Sam => {
            write_bam_streaming(ctx, df, path, write_options).await
        },
        OutputFormat::Cram => write_cram_streaming(ctx, df, path, write_options).await,
    }
}

/// Stream write a DataFrame to VCF format.
async fn write_vcf_streaming(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    write_options: Option<WriteOptions>,
) -> Result<u64, DataFusionError> {
    // Extract VCF-specific options
    let (zero_based, vcf_metadata) = if let Some(opts) = &write_options {
        if let Some(vcf_opts) = &opts.vcf_write_options {
            (
                vcf_opts.zero_based,
                Some((
                    vcf_opts.info_fields_metadata.clone(),
                    vcf_opts.format_fields_metadata.clone(),
                    vcf_opts.sample_names.clone(),
                )),
            )
        } else {
            (true, None)
        }
    } else {
        (true, None)
    };

    // Execute streaming write with VCF metadata for header generation
    execute_vcf_streaming_write(ctx, df, path, zero_based, vcf_metadata).await
}

/// Execute VCF streaming write with metadata support.
///
/// This function handles VCF-specific write logic including:
/// - Applying VCF field metadata to the schema for proper header generation
/// - Creating the VCF table provider and executing the streaming write
async fn execute_vcf_streaming_write(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    zero_based: bool,
    vcf_metadata: Option<(Option<String>, Option<String>, Option<String>)>,
) -> Result<u64, DataFusionError> {
    // Get the schema from the DataFrame
    let schema = df.schema().inner().clone();

    // Apply VCF metadata to the converted schema, or fall back to heuristics
    let (info_fields, format_fields, sample_names, schema_with_metadata) =
        if let Some((info_meta, format_meta, sample_meta)) = vcf_metadata {
            // Parse metadata and add to schema
            apply_vcf_metadata_to_schema(&schema, info_meta, format_meta, sample_meta)?
        } else {
            // Fall back to heuristics
            let (info_fields, format_fields, sample_names) =
                extract_vcf_fields_from_schema(&schema);
            (info_fields, format_fields, sample_names, schema)
        };

    info!(
        "VCF write: zero_based={}, info_fields={:?}, format_fields={:?}, samples={:?}",
        zero_based, info_fields, format_fields, sample_names
    );

    // Create VCF table provider for write with the annotated schema
    let provider = VcfTableProvider::new_for_write(
        path.to_string(),
        schema_with_metadata.clone(),
        info_fields,
        format_fields,
        sample_names,
        zero_based,
    );

    // Create the logical plan for the source data
    let logical_plan = df.logical_plan().clone();

    // Create a physical plan for reading the source data
    let state = ctx.state();
    let input_plan = state.create_physical_plan(&logical_plan).await?;

    // Wrap the input plan with a schema override to include VCF metadata
    // This ensures the VCF writer sees the correct field metadata for header generation
    let wrapped_input = Arc::new(SchemaOverrideExec::new(input_plan, schema_with_metadata));

    // Get the write execution plan via insert_into
    let write_plan = Arc::new(provider)
        .insert_into(&state, wrapped_input, InsertOp::Overwrite)
        .await?;

    // Execute the write plan - this streams batches through the writer
    let task_ctx = ctx.task_ctx();
    let stream = write_plan.execute(0, task_ctx)?;

    // Consume the stream to execute the write
    let total_rows = consume_write_stream(stream).await?;

    info!("Successfully wrote {} rows to output", total_rows);
    Ok(total_rows)
}

/// Wrapper ExecutionPlan that overrides the schema to include VCF metadata.
///
/// This is needed because DataFusion's physical plan doesn't preserve Arrow field metadata
/// through DataFrame operations like cast(). This wrapper allows us to inject the VCF
/// metadata that the bio-formats library expects for proper header generation.
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use std::any::Any;

#[derive(Debug)]
struct SchemaOverrideExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl SchemaOverrideExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            schema,
            cache,
        }
    }
}

impl DisplayAs for SchemaOverrideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SchemaOverrideExec")
    }
}

impl ExecutionPlan for SchemaOverrideExec {
    fn name(&self) -> &str {
        "SchemaOverrideExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SchemaOverrideExec::new(
            children[0].clone(),
            self.schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(SchemaOverrideStream {
            input: input_stream,
            schema: self.schema.clone(),
        }))
    }
}

/// Stream adapter that overrides the schema of record batches
struct SchemaOverrideStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl Stream for SchemaOverrideStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Reconcile any type drift (including nested field metadata) with the
                // override schema before constructing the output batch.
                let mut columns = Vec::with_capacity(batch.num_columns());
                for (idx, (column, target_field)) in batch
                    .columns()
                    .iter()
                    .zip(self.schema.fields().iter())
                    .enumerate()
                {
                    if column.data_type() == target_field.data_type() {
                        columns.push(column.clone());
                        continue;
                    }

                    match cast(column.as_ref(), target_field.data_type()) {
                        Ok(casted) => columns.push(casted),
                        Err(e) => {
                            log::debug!(
                                "Schema override cast fallback for column '{}' at index {} from {:?} to {:?}: {}",
                                target_field.name(),
                                idx,
                                column.data_type(),
                                target_field.data_type(),
                                e
                            );
                            columns.push(column.clone());
                        },
                    }
                }

                match RecordBatch::try_new(self.schema.clone(), columns) {
                    Ok(new_batch) => Poll::Ready(Some(Ok(new_batch))),
                    Err(e) => {
                        log::debug!("Schema override batch fallback to input schema: {}", e);
                        Poll::Ready(Some(Ok(batch)))
                    },
                }
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SchemaOverrideStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream write a DataFrame to FASTQ format.
async fn write_fastq_streaming(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    _write_options: Option<WriteOptions>,
) -> Result<u64, DataFusionError> {
    // Create FASTQ table provider - it doesn't need special write constructor
    // since the schema is fixed (name, description, sequence, quality_scores)
    let provider = FastqTableProvider::new(path.to_string(), None)?;

    // Execute streaming write via insert_into
    execute_streaming_write(ctx, df, Arc::new(provider)).await
}

/// Execute a streaming write using the TableProvider's insert_into() method.
///
/// This is the core streaming mechanism that processes data batch-by-batch.
async fn execute_streaming_write(
    ctx: &SessionContext,
    df: DataFrame,
    provider: Arc<dyn TableProvider>,
) -> Result<u64, DataFusionError> {
    // Create the logical plan for the source data
    let logical_plan = df.logical_plan().clone();

    // Create a physical plan for reading the source data
    let state = ctx.state();
    let input_plan = state.create_physical_plan(&logical_plan).await?;

    // Get the write execution plan via insert_into
    let write_plan = provider
        .insert_into(&state, input_plan, InsertOp::Overwrite)
        .await?;

    // Execute the write plan - this streams batches through the writer
    let task_ctx = ctx.task_ctx();
    let stream = write_plan.execute(0, task_ctx)?;

    // Consume the stream to execute the write
    let total_rows = consume_write_stream(stream).await?;

    info!("Successfully wrote {} rows to output", total_rows);
    Ok(total_rows)
}

/// Extract INFO fields, FORMAT fields, and sample names from an Arrow schema.
///
/// This analyzes the schema metadata to identify which columns are INFO fields
/// vs FORMAT fields, and extracts sample names from FORMAT column naming patterns.
fn extract_vcf_fields_from_schema(schema: &SchemaRef) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut info_fields = Vec::new();
    let mut format_fields = Vec::new();
    let mut sample_names = Vec::new();
    let mut seen_samples = std::collections::HashSet::new();

    // Core VCF columns that are not INFO or FORMAT
    let core_columns: std::collections::HashSet<&str> = [
        "chrom", "start", "end", "id", "ref", "alt", "qual", "filter",
    ]
    .into_iter()
    .collect();

    for field in schema.fields() {
        let name = field.name();

        // Skip core columns
        if core_columns.contains(name.as_str()) {
            continue;
        }

        // Check field metadata for VCF field type
        let metadata = field.metadata();
        if let Some(field_type) = metadata.get("vcf_field_type") {
            match field_type.as_str() {
                "INFO" => {
                    info_fields.push(name.clone());
                },
                "FORMAT" => {
                    // Extract format field ID and sample name
                    if let Some(format_id) = metadata.get("vcf_format_id") {
                        if !format_fields.contains(format_id) {
                            format_fields.push(format_id.clone());
                        }
                    }
                    // Try to extract sample name from column name pattern: {sample}_{format}
                    if let Some(format_id) = metadata.get("vcf_format_id") {
                        if name.ends_with(&format!("_{}", format_id)) {
                            let sample = name
                                .strip_suffix(&format!("_{}", format_id))
                                .unwrap_or(name);
                            if !sample.is_empty() && seen_samples.insert(sample.to_string()) {
                                sample_names.push(sample.to_string());
                            }
                        }
                    }
                },
                _ => {},
            }
        } else {
            // No metadata - use conservative heuristics
            // Boolean fields are typically INFO Flag types, not FORMAT
            // Only consider FORMAT if it's a clear pattern match with typical FORMAT types
            let is_likely_format = match field.data_type() {
                // GT is always String
                DataType::Utf8 | DataType::LargeUtf8 => {
                    // Only if it ends with known FORMAT fields like GT
                    name.ends_with("_GT")
                },
                // DP, GQ, MIN_DP are Int32
                DataType::Int32 => {
                    name.ends_with("_DP") || name.ends_with("_GQ") || name.ends_with("_MIN_DP")
                },
                // AD, PL are List<Int32> or LargeList<Int32>
                DataType::List(inner) | DataType::LargeList(inner) => {
                    matches!(inner.data_type(), DataType::Int32 | DataType::Float32)
                        && (name.ends_with("_AD")
                            || name.ends_with("_PL")
                            || name.ends_with("_VAF"))
                },
                // VAF can be Float32 or List<Float32>
                DataType::Float32 => name.ends_with("_VAF"),
                // Boolean is typically INFO Flag, not FORMAT
                DataType::Boolean => false,
                _ => false,
            };

            if is_likely_format {
                if let Some((sample, format_field)) = parse_format_column_name(name) {
                    if !format_fields.contains(&format_field) {
                        format_fields.push(format_field);
                    }
                    if seen_samples.insert(sample.clone()) {
                        sample_names.push(sample);
                    }
                } else {
                    // Doesn't match pattern, treat as INFO
                    info_fields.push(name.clone());
                }
            } else {
                // Assume it's an INFO field
                info_fields.push(name.clone());
            }
        }
    }

    // Fallback for nested multisample schema where FORMAT ids are carried in
    // `genotypes.values` rather than top-level columns.
    if format_fields.is_empty() {
        let nested_format_fields = extract_format_fields_from_nested_genotypes(schema);
        if !nested_format_fields.is_empty() {
            format_fields = nested_format_fields;
            info_fields.retain(|name| name != "genotypes");
        }
    }

    (info_fields, format_fields, sample_names)
}

/// Parse a column name to extract sample name and FORMAT field.
///
/// Expected pattern: {sample_name}_{FORMAT_FIELD}
/// where FORMAT_FIELD is typically uppercase (GT, DP, GQ, AD, etc.)
fn parse_format_column_name(name: &str) -> Option<(String, String)> {
    // Common FORMAT field patterns (uppercase)
    let format_patterns = ["GT", "DP", "GQ", "AD", "PL", "VAF", "MIN_DP"];

    for pattern in format_patterns {
        if name.ends_with(&format!("_{}", pattern)) {
            let sample = name
                .strip_suffix(&format!("_{}", pattern))
                .unwrap_or("")
                .to_string();
            if !sample.is_empty() {
                return Some((sample, pattern.to_string()));
            }
        }
    }

    // Try generic pattern: ends with _{UPPERCASE}
    if let Some(idx) = name.rfind('_') {
        let suffix = &name[idx + 1..];
        if suffix
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            && !suffix.is_empty()
        {
            let sample = name[..idx].to_string();
            if !sample.is_empty() {
                return Some((sample, suffix.to_string()));
            }
        }
    }

    None
}

/// Stream write a DataFrame to BAM format.
async fn write_bam_streaming(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    write_options: Option<WriteOptions>,
) -> Result<u64, DataFusionError> {
    let (zero_based, tag_fields, header_metadata, sort_on_write) =
        if let Some(opts) = &write_options {
            if let Some(bam_opts) = &opts.bam_write_options {
                (
                    bam_opts.zero_based,
                    bam_opts.tag_fields.clone(),
                    bam_opts.header_metadata.clone(),
                    bam_opts.sort_on_write,
                )
            } else {
                (true, None, None, false)
            }
        } else {
            (true, None, None, false)
        };

    execute_bam_streaming_write(
        ctx,
        df,
        path,
        zero_based,
        tag_fields,
        header_metadata,
        sort_on_write,
    )
    .await
}

/// Execute BAM streaming write with metadata support.
async fn execute_bam_streaming_write(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    zero_based: bool,
    tag_fields: Option<Vec<String>>,
    header_metadata: Option<String>,
    sort_on_write: bool,
) -> Result<u64, DataFusionError> {
    let schema = df.schema().inner().clone();

    // Auto-detect tag fields if not specified
    let tags = tag_fields.unwrap_or_else(|| extract_tag_fields_from_schema(&schema));

    info!(
        "BAM write: zero_based={}, tags={:?}, header_metadata={:?}",
        zero_based,
        tags,
        header_metadata.as_ref().map(|_| "present")
    );

    // Add BAM tag metadata to schema (required for serialization)
    let schema_with_metadata = add_bam_tag_metadata(schema, &tags);

    // Merge header metadata (reference sequences, read groups, programs, etc.) into schema
    let schema_with_metadata = merge_header_metadata(schema_with_metadata, &header_metadata);

    // Create BAM table provider for write
    let provider = BamTableProvider::new_for_write(
        path.to_string(),
        schema_with_metadata.clone(),
        Some(tags),
        zero_based,
        sort_on_write,
    );

    // Create the logical plan for the source data
    let logical_plan = df.logical_plan().clone();

    // Create a physical plan for reading the source data
    let state = ctx.state();
    let input_plan = state.create_physical_plan(&logical_plan).await?;

    // Wrap the input plan with a schema override to include BAM tag metadata
    // and header metadata for full header reconstruction
    let wrapped_input = Arc::new(SchemaOverrideExec::new(input_plan, schema_with_metadata));

    // Get the write execution plan via insert_into
    let write_plan = Arc::new(provider)
        .insert_into(&state, wrapped_input, InsertOp::Overwrite)
        .await?;

    // Execute the write plan - this streams batches through the writer
    let task_ctx = ctx.task_ctx();
    let stream = write_plan.execute(0, task_ctx)?;

    // Consume the stream to execute the write
    let total_rows = consume_write_stream(stream).await?;

    info!("Successfully wrote {} rows to output", total_rows);
    Ok(total_rows)
}

/// Stream write a DataFrame to CRAM format.
async fn write_cram_streaming(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    write_options: Option<WriteOptions>,
) -> Result<u64, DataFusionError> {
    let (zero_based, reference_path, tag_fields, header_metadata, sort_on_write) =
        if let Some(opts) = &write_options {
            if let Some(cram_opts) = &opts.cram_write_options {
                (
                    cram_opts.zero_based,
                    cram_opts.reference_path.clone(),
                    cram_opts.tag_fields.clone(),
                    cram_opts.header_metadata.clone(),
                    cram_opts.sort_on_write,
                )
            } else {
                // Default: no reference (reference-free CRAM)
                (true, None, None, None, false)
            }
        } else {
            // Default: no reference (reference-free CRAM)
            (true, None, None, None, false)
        };

    execute_cram_streaming_write(
        ctx,
        df,
        path,
        zero_based,
        reference_path,
        tag_fields,
        header_metadata,
        sort_on_write,
    )
    .await
}

/// Execute CRAM streaming write with metadata support.
async fn execute_cram_streaming_write(
    ctx: &SessionContext,
    df: DataFrame,
    path: &str,
    zero_based: bool,
    reference_path: Option<String>,
    tag_fields: Option<Vec<String>>,
    header_metadata: Option<String>,
    sort_on_write: bool,
) -> Result<u64, DataFusionError> {
    let schema = df.schema().inner().clone();
    let tags = tag_fields.unwrap_or_else(|| extract_tag_fields_from_schema(&schema));

    info!(
        "CRAM write: zero_based={}, reference={:?}, tags={:?}, header_metadata={:?}",
        zero_based,
        reference_path,
        tags,
        header_metadata.as_ref().map(|_| "present")
    );

    // Add BAM tag metadata to schema (required for serialization)
    let schema_with_metadata = add_bam_tag_metadata(schema, &tags);

    // Merge header metadata (reference sequences, read groups, programs, etc.) into schema
    let schema_with_metadata = merge_header_metadata(schema_with_metadata, &header_metadata);

    // Create CRAM table provider for write
    let provider = CramTableProvider::new_for_write(
        path.to_string(),
        schema_with_metadata.clone(),
        reference_path,
        Some(tags),
        zero_based,
        sort_on_write,
    );

    // Create the logical plan for the source data
    let logical_plan = df.logical_plan().clone();

    // Create a physical plan for reading the source data
    let state = ctx.state();
    let input_plan = state.create_physical_plan(&logical_plan).await?;

    // Wrap the input plan with a schema override to include BAM tag metadata
    // and header metadata for full header reconstruction
    let wrapped_input = Arc::new(SchemaOverrideExec::new(input_plan, schema_with_metadata));

    // Get the write execution plan via insert_into
    let write_plan = Arc::new(provider)
        .insert_into(&state, wrapped_input, InsertOp::Overwrite)
        .await?;

    // Execute the write plan
    let task_ctx = ctx.task_ctx();
    let stream = write_plan.execute(0, task_ctx)?;

    // Consume the stream to execute the write
    let total_rows = consume_write_stream(stream).await?;

    info!("Successfully wrote {} rows to output", total_rows);
    Ok(total_rows)
}

/// Extract tag field names from schema (columns beyond 12 core BAM columns)
fn extract_tag_fields_from_schema(schema: &SchemaRef) -> Vec<String> {
    let core_columns: std::collections::HashSet<&str> = [
        "name",
        "chrom",
        "start",
        "end",
        "flags",
        "cigar",
        "mapping_quality",
        "mate_chrom",
        "mate_start",
        "sequence",
        "quality_scores",
        "template_length",
    ]
    .into_iter()
    .collect();

    schema
        .fields()
        .iter()
        .filter_map(|field| {
            if !core_columns.contains(field.name().as_str()) {
                Some(field.name().clone())
            } else {
                None
            }
        })
        .collect()
}

/// Add BAM tag type metadata to schema fields
/// This infers the SAM type from Arrow data type and adds it to field metadata
/// Merges header metadata from the JSON string into the Arrow schema metadata.
///
/// The header_metadata JSON has keys like "reference_sequences", "read_groups", etc.
/// These need to be mapped to `bio.bam.*` keys in the schema metadata so that
/// `build_bam_header()` can reconstruct the full SAM header during write.
fn merge_header_metadata(schema: SchemaRef, header_metadata: &Option<String>) -> SchemaRef {
    let header_json = match header_metadata {
        Some(json) if !json.is_empty() && json != "null" => json,
        _ => return schema,
    };

    let parsed: serde_json::Value = match serde_json::from_str(header_json) {
        Ok(v) => v,
        Err(_) => return schema,
    };

    let obj = match parsed.as_object() {
        Some(o) => o,
        None => return schema,
    };

    // Map Python metadata keys to bio.bam.* schema metadata keys
    let key_mapping = [
        ("file_format_version", "bio.bam.file_format_version"),
        ("sort_order", "bio.bam.sort_order"),
        ("reference_sequences", "bio.bam.reference_sequences"),
        ("read_groups", "bio.bam.read_groups"),
        ("program_info", "bio.bam.program_info"),
        ("comments", "bio.bam.comments"),
    ];

    let mut metadata = schema.metadata().clone();
    for (py_key, schema_key) in &key_mapping {
        if let Some(value) = obj.get(*py_key) {
            // Values from Python are already JSON strings (double-serialized),
            // so extract the inner string directly
            let val_str = match value {
                serde_json::Value::String(s) => s.clone(),
                _ => value.to_string(),
            };
            if !val_str.is_empty() && val_str != "null" {
                metadata.insert(schema_key.to_string(), val_str);
            }
        }
    }

    Arc::new(Schema::new_with_metadata(
        schema.fields().to_vec(),
        metadata,
    ))
}

fn add_bam_tag_metadata(schema: SchemaRef, tag_fields: &[String]) -> SchemaRef {
    use datafusion_bio_format_core::{BAM_TAG_TAG_KEY, BAM_TAG_TYPE_KEY};
    use std::collections::HashMap;

    let mut new_fields = Vec::new();
    let tag_set: std::collections::HashSet<&str> = tag_fields.iter().map(|s| s.as_str()).collect();

    for field in schema.fields().iter() {
        if tag_set.contains(field.name().as_str()) {
            // This is a tag field - add metadata
            // First check if the field already has BAM_TAG_TYPE_KEY metadata (preserve existing)
            let sam_type = if let Some(existing_type) = field.metadata().get(BAM_TAG_TYPE_KEY) {
                // Preserve existing tag type metadata (e.g., from read_bam)
                existing_type.chars().next().unwrap_or('Z')
            } else {
                // Infer SAM type from Arrow data type
                match field.data_type() {
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => 'i',
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 'i',
                    DataType::Float32 | DataType::Float64 => 'f',
                    DataType::Utf8 | DataType::LargeUtf8 => 'Z',
                    // Handle array/list types for BAM 'B' tags (integer/byte arrays)
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                        'B'
                    },
                    _ => 'Z', // Default to string for unknown types
                }
            };

            let mut field_metadata = HashMap::new();
            field_metadata.insert(BAM_TAG_TYPE_KEY.to_string(), sam_type.to_string());
            field_metadata.insert(BAM_TAG_TAG_KEY.to_string(), field.name().clone());

            new_fields.push(field.as_ref().clone().with_metadata(field_metadata));
        } else {
            new_fields.push(field.as_ref().clone());
        }
    }

    Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_format_column_name() {
        assert_eq!(
            parse_format_column_name("NA12878_GT"),
            Some(("NA12878".to_string(), "GT".to_string()))
        );
        assert_eq!(
            parse_format_column_name("sample1_DP"),
            Some(("sample1".to_string(), "DP".to_string()))
        );
        assert_eq!(
            parse_format_column_name("my_sample_GQ"),
            Some(("my_sample".to_string(), "GQ".to_string()))
        );
        assert_eq!(parse_format_column_name("chrom"), None);
        assert_eq!(parse_format_column_name("AF"), None); // No sample prefix
    }
}
