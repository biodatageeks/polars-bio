//! Write execution plan for VCF files
//!
//! This module provides the physical execution plan for writing DataFusion
//! query results to VCF files.

use crate::serializer::batch_to_vcf_lines;
use crate::writer::{VcfCompressionType, VcfLocalWriter};
use datafusion::arrow::array::{
    Array, LargeListArray, ListArray, RecordBatch, StructArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::StreamExt;
use log::debug;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for writing VCF files
///
/// This execution plan consumes input RecordBatches, converts them to VCF records,
/// and writes them to a local file with optional compression.
///
/// The plan returns a single-row RecordBatch with a `count` column containing
/// the total number of records written.
pub struct VcfWriteExec {
    /// The input execution plan providing data to write
    input: Arc<dyn ExecutionPlan>,
    /// Path to the output VCF file
    output_path: String,
    /// Compression type for the output file
    compression: VcfCompressionType,
    /// INFO field names in schema order
    info_fields: Vec<String>,
    /// FORMAT field names (unique list)
    format_fields: Vec<String>,
    /// Sample names from the original VCF
    sample_names: Vec<String>,
    /// Whether the input coordinates are 0-based (need +1 for VCF output)
    coordinate_system_zero_based: bool,
    /// Schema-level metadata from the source VCF table provider (contigs, filters, etc.)
    /// that may be lost during DataFusion query plan projections.
    source_metadata: Option<HashMap<String, String>>,
    /// Cached plan properties
    cache: PlanProperties,
}

impl VcfWriteExec {
    /// Creates a new VCF write execution plan
    ///
    /// # Arguments
    ///
    /// * `input` - The input execution plan providing RecordBatches to write
    /// * `output_path` - Path to the output VCF file
    /// * `compression` - Compression type (defaults to auto-detection from path)
    /// * `info_fields` - List of INFO field names
    /// * `format_fields` - List of FORMAT field names
    /// * `sample_names` - List of sample names
    /// * `coordinate_system_zero_based` - Whether input uses 0-based coordinates
    ///
    /// # Returns
    ///
    /// A new `VcfWriteExec` instance
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_path: String,
        compression: Option<VcfCompressionType>,
        info_fields: Vec<String>,
        format_fields: Vec<String>,
        sample_names: Vec<String>,
        coordinate_system_zero_based: bool,
        source_metadata: Option<HashMap<String, String>>,
    ) -> Self {
        let compression =
            compression.unwrap_or_else(|| VcfCompressionType::from_path(&output_path));

        // Output schema is a single count column
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let cache = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            input,
            output_path,
            compression,
            info_fields,
            format_fields,
            sample_names,
            coordinate_system_zero_based,
            source_metadata,
            cache,
        }
    }

    /// Returns the output file path
    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    /// Returns the compression type
    pub fn compression(&self) -> VcfCompressionType {
        self.compression
    }

    /// Returns the output schema
    fn schema(&self) -> SchemaRef {
        self.cache.eq_properties.schema().clone()
    }
}

impl Debug for VcfWriteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VcfWriteExec")
            .field("output_path", &self.output_path)
            .field("compression", &self.compression)
            .field("info_fields", &self.info_fields)
            .field("format_fields", &self.format_fields)
            .field("sample_names", &self.sample_names)
            .finish()
    }
}

impl DisplayAs for VcfWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "VcfWriteExec: path={}, compression={:?}, info_fields={}, format_fields={}, samples={}",
            self.output_path,
            self.compression,
            self.info_fields.len(),
            self.format_fields.len(),
            self.sample_names.len()
        )
    }
}

impl ExecutionPlan for VcfWriteExec {
    fn name(&self) -> &str {
        "VcfWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "VcfWriteExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(VcfWriteExec::new(
            children[0].clone(),
            self.output_path.clone(),
            Some(self.compression),
            self.info_fields.clone(),
            self.format_fields.clone(),
            self.sample_names.clone(),
            self.coordinate_system_zero_based,
            self.source_metadata.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "VcfWriteExec::execute partition={}, path={}",
            partition, self.output_path
        );

        let output_path = self.output_path.clone();
        let compression = self.compression;
        let info_fields = self.info_fields.clone();
        let format_fields = self.format_fields.clone();
        let sample_names = self.sample_names.clone();
        let coordinate_system_zero_based = self.coordinate_system_zero_based;
        let source_metadata = self.source_metadata.clone();
        let input = self.input.execute(0, context)?;
        let input_schema = self.input.schema();
        let output_schema = self.schema();

        let stream = futures::stream::once(async move {
            write_vcf_stream(
                input,
                input_schema,
                output_path,
                compression,
                info_fields,
                format_fields,
                sample_names,
                coordinate_system_zero_based,
                output_schema,
                source_metadata,
            )
            .await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Checks whether a schema has a `genotypes` column with Struct type containing List children.
fn schema_has_genotypes_struct(schema: &SchemaRef) -> bool {
    if let Ok(idx) = schema.index_of("genotypes") {
        matches!(schema.field(idx).data_type(), DataType::Struct(_))
    } else {
        false
    }
}

/// Infers sample names from the first batch by inspecting the genotypes struct column.
/// Returns `SAMPLE_0`, `SAMPLE_1`, ... based on the length of the first List child.
fn infer_sample_names_from_batch(batch: &RecordBatch) -> Vec<String> {
    let genotypes_idx = match batch.schema().index_of("genotypes") {
        Ok(idx) => idx,
        Err(_) => return Vec::new(),
    };
    let genotypes_col = batch.column(genotypes_idx);
    let genotypes_struct = match genotypes_col.as_any().downcast_ref::<StructArray>() {
        Some(s) => s,
        None => return Vec::new(),
    };

    // Find the first non-null List child to determine sample count.
    // Try both ListArray and LargeListArray since DataFusion may produce either.
    for col_idx in 0..genotypes_struct.num_columns() {
        let child = genotypes_struct.column(col_idx);
        if let Some(list) = child.as_any().downcast_ref::<ListArray>() {
            for row in 0..list.len() {
                if !list.is_null(row) {
                    let num_samples = list.value(row).len();
                    return (0..num_samples).map(|i| format!("SAMPLE_{i}")).collect();
                }
            }
        }
        if let Some(list) = child.as_any().downcast_ref::<LargeListArray>() {
            for row in 0..list.len() {
                if !list.is_null(row) {
                    let num_samples = list.value(row).len();
                    return (0..num_samples).map(|i| format!("SAMPLE_{i}")).collect();
                }
            }
        }
    }

    Vec::new()
}

/// Merges source metadata into the input schema. Source metadata provides
/// defaults; input schema metadata wins on conflict.
fn merge_metadata_into_schema(
    input_schema: &SchemaRef,
    source_metadata: &Option<HashMap<String, String>>,
) -> SchemaRef {
    let Some(source) = source_metadata else {
        return input_schema.clone();
    };
    let mut merged = source.clone();
    // Input schema metadata wins on conflict
    for (k, v) in input_schema.metadata() {
        merged.insert(k.clone(), v.clone());
    }
    Arc::new(input_schema.as_ref().clone().with_metadata(merged))
}

/// Writes records from a stream to a VCF file
#[allow(clippy::too_many_arguments)]
async fn write_vcf_stream(
    mut input: SendableRecordBatchStream,
    input_schema: SchemaRef,
    output_path: String,
    compression: VcfCompressionType,
    info_fields: Vec<String>,
    format_fields: Vec<String>,
    mut sample_names: Vec<String>,
    coordinate_system_zero_based: bool,
    output_schema: SchemaRef,
    source_metadata: Option<HashMap<String, String>>,
) -> Result<RecordBatch> {
    let mut writer = VcfLocalWriter::with_compression(&output_path, compression)?;

    // Merge source metadata (contigs, filters, etc.) into the schema used for header writing.
    let header_schema = merge_metadata_into_schema(&input_schema, &source_metadata);

    let mut total_count: u64 = 0;

    // When sample_names is empty but the input has a genotypes struct,
    // defer header writing until the first batch to infer sample count.
    if sample_names.is_empty() && schema_has_genotypes_struct(&input_schema) {
        if let Some(first_batch_result) = input.next().await {
            let first_batch = first_batch_result?;
            sample_names = infer_sample_names_from_batch(&first_batch);
            writer.write_header(&header_schema, &info_fields, &format_fields, &sample_names)?;

            if first_batch.num_rows() > 0 {
                let records = batch_to_vcf_lines(
                    &first_batch,
                    &info_fields,
                    &format_fields,
                    &sample_names,
                    coordinate_system_zero_based,
                )?;
                total_count += records.len() as u64;
                writer.write_records(&records)?;
            }
        } else {
            // Empty result — write header with no samples
            writer.write_header(&header_schema, &info_fields, &format_fields, &sample_names)?;
        }
    } else {
        // Standard path: write header immediately
        writer.write_header(&header_schema, &info_fields, &format_fields, &sample_names)?;
    }

    // Continue processing remaining batches
    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;

        if batch.num_rows() == 0 {
            continue;
        }

        let records = batch_to_vcf_lines(
            &batch,
            &info_fields,
            &format_fields,
            &sample_names,
            coordinate_system_zero_based,
        )?;

        total_count += records.len() as u64;
        writer.write_records(&records)?;
    }

    writer.finish()?;

    debug!("VcfWriteExec: wrote {total_count} records to {output_path}");

    // Return a batch with the count
    let count_array = Arc::new(UInt64Array::from(vec![total_count]));
    RecordBatch::try_new(output_schema, vec![count_array])
        .map_err(|e| DataFusionError::Execution(format!("Failed to create result batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_vcf_write_exec_properties() {
        let schema = create_test_schema();
        let output_path = "/tmp/test.vcf".to_string();

        let write_exec = VcfWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema)),
            output_path.clone(),
            None,
            vec![],
            vec![],
            vec![],
            true,
            None,
        );

        assert_eq!(write_exec.output_path(), output_path);
        assert_eq!(write_exec.compression(), VcfCompressionType::Plain);
        assert_eq!(write_exec.name(), "VcfWriteExec");
    }

    #[test]
    fn test_compression_detection_in_write_exec() {
        let schema = create_test_schema();

        // Test BGZF detection
        let bgzf_exec = VcfWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.vcf.bgz".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            true,
            None,
        );
        assert_eq!(bgzf_exec.compression(), VcfCompressionType::Bgzf);

        // Test GZIP detection
        let gzip_exec = VcfWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.vcf.gz".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            true,
            None,
        );
        assert_eq!(gzip_exec.compression(), VcfCompressionType::Gzip);
    }

    #[test]
    fn test_merge_metadata_into_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "chrom",
            DataType::Utf8,
            false,
        )]));

        // No source metadata → schema unchanged
        let result = merge_metadata_into_schema(&schema, &None);
        assert!(result.metadata().is_empty());

        // Source metadata merged into empty schema metadata
        let mut source = HashMap::new();
        source.insert("bio.vcf.contigs".to_string(), "[\"chr1\"]".to_string());
        source.insert("bio.vcf.filters".to_string(), "[\"PASS\"]".to_string());
        let result = merge_metadata_into_schema(&schema, &Some(source.clone()));
        assert_eq!(
            result.metadata().get("bio.vcf.contigs").unwrap(),
            "[\"chr1\"]"
        );
        assert_eq!(
            result.metadata().get("bio.vcf.filters").unwrap(),
            "[\"PASS\"]"
        );

        // Input schema metadata wins on conflict
        let mut input_meta = HashMap::new();
        input_meta.insert("bio.vcf.contigs".to_string(), "[\"chr2\"]".to_string());
        let schema_with_meta = Arc::new(schema.as_ref().clone().with_metadata(input_meta));
        let result = merge_metadata_into_schema(&schema_with_meta, &Some(source));
        assert_eq!(
            result.metadata().get("bio.vcf.contigs").unwrap(),
            "[\"chr2\"]"
        );
        assert_eq!(
            result.metadata().get("bio.vcf.filters").unwrap(),
            "[\"PASS\"]"
        );
    }

    #[test]
    fn test_source_metadata_threaded_through_with_new_children() {
        let schema = create_test_schema();
        let mut source_meta = HashMap::new();
        source_meta.insert("bio.vcf.contigs".to_string(), "[\"chr1\"]".to_string());

        let write_exec = Arc::new(VcfWriteExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )),
            "/tmp/test.vcf".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            true,
            Some(source_meta.clone()),
        ));

        // with_new_children should preserve source_metadata
        let new_child = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let rebuilt = write_exec.with_new_children(vec![new_child]).unwrap();
        let rebuilt_exec = rebuilt.as_any().downcast_ref::<VcfWriteExec>().unwrap();
        assert_eq!(rebuilt_exec.source_metadata, Some(source_meta));
    }
}
