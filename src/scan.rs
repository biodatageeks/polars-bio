use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use arrow_schema::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_bed::table_provider::{BEDFields, BedTableProvider};
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use datafusion_bio_format_fasta::table_provider::FastaTableProvider;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use datafusion_bio_format_gtf::table_provider::GtfTableProvider;
use datafusion_bio_format_pairs::table_provider::PairsTableProvider;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use futures::Stream;
use log::info;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::debug;

use crate::context::PyBioSessionContext;
use crate::option::{
    BamReadOptions, BedReadOptions, CramReadOptions, FastaReadOptions, FastqReadOptions,
    GffReadOptions, GtfReadOptions, InputFormat, PairsReadOptions, ReadOptions, VcfReadOptions,
};

/// Maximum number of RecordBatches buffered per partition when fanning out a LazyFrame
/// Arrow C stream into multiple DataFusion partitions.
///
/// Keeping this small preserves streaming behavior and provides backpressure to the
/// dispatcher thread instead of allowing it to accumulate the full input in memory.
const ARROW_STREAM_PARTITION_BUFFER_SIZE: usize = 2;

/// A PartitionStream that yields pre-collected RecordBatches.
/// Used to enable multi-partition parallel execution for DataFrame inputs.
///
/// Note: RecordBatch::clone() is cheap - it uses Arc internally for column data,
/// so cloning only increments reference counts (O(num_columns)), not copying data.
#[derive(Debug)]
struct RecordBatchPartitionStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl RecordBatchPartitionStream {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl PartitionStream for RecordBatchPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // Clone is cheap: RecordBatch uses Arc internally, so this just increments
        // reference counts for each column (O(num_columns)), no data copying.
        let batches = self.batches.clone();
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// A PartitionStream that consumes an Arrow C Stream directly.
/// Enables GIL-free streaming from Polars LazyFrame via ArrowStreamExportable.
///
/// This approach uses Polars' `__arrow_c_stream__()` method (available since Polars 1.37.1)
/// to export data via Arrow C FFI, eliminating per-batch GIL acquisition.
pub struct ArrowCStreamPartitionStream {
    schema: SchemaRef,
    stream_reader: Arc<Mutex<Option<ArrowArrayStreamReader>>>,
}

impl ArrowCStreamPartitionStream {
    pub fn new(schema: SchemaRef, stream_reader: ArrowArrayStreamReader) -> Self {
        Self {
            schema,
            stream_reader: Arc::new(Mutex::new(Some(stream_reader))),
        }
    }
}

impl std::fmt::Debug for ArrowCStreamPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowCStreamPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for ArrowCStreamPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let reader = self
            .stream_reader
            .lock()
            .unwrap()
            .take()
            .expect("Arrow C Stream already consumed");
        Box::pin(ArrowCStreamBatchStream::new(reader, self.schema.clone()))
    }
}

/// RecordBatchStream that reads from ArrowArrayStreamReader (no GIL needed).
///
/// After the initial stream export from Python (single GIL acquisition),
/// all batch iteration happens in pure Rust without any Python interaction.
pub struct ArrowCStreamBatchStream {
    reader: ArrowArrayStreamReader,
    schema: SchemaRef,
}

impl ArrowCStreamBatchStream {
    pub fn new(reader: ArrowArrayStreamReader, schema: SchemaRef) -> Self {
        Self { reader, schema }
    }
}

impl Stream for ArrowCStreamBatchStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // For single-partition execution we keep the implementation simple and read
        // directly from the Arrow C stream on the consumer task. The multi-partition
        // path uses a dedicated OS thread plus bounded channels so probe-side
        // parallelism does not depend on synchronous reads inside `poll_next`.
        match self.reader.next() {
            Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
            Some(Err(e)) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ArrowCStreamBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// A PartitionStream backed by a receiver fed from a shared Arrow C stream.
///
/// The source stream is read once and batches are dispatched round-robin to each
/// partition receiver so DataFusion can execute probe-side partitions in parallel.
pub struct ArrowCStreamFanoutPartitionStream {
    schema: SchemaRef,
    receiver: Arc<Mutex<Option<Receiver<Result<RecordBatch, DataFusionError>>>>>,
}

impl ArrowCStreamFanoutPartitionStream {
    pub fn new(
        schema: SchemaRef,
        receiver: Receiver<Result<RecordBatch, DataFusionError>>,
    ) -> Self {
        Self {
            schema,
            receiver: Arc::new(Mutex::new(Some(receiver))),
        }
    }
}

impl std::fmt::Debug for ArrowCStreamFanoutPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowCStreamFanoutPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for ArrowCStreamFanoutPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("Arrow C Stream partition already consumed");

        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });

        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}

/// Split eager RecordBatches into row-balanced partitions.
///
/// DataFrame inputs often arrive as a small number of large Arrow batches
/// (for example one batch per Parquet row group). Treating those batches as
/// indivisible caps parallelism at `batches.len()`, even when session
/// `target_partitions` is larger. We instead slice large batches into
/// zero-copy RecordBatch views so eager inputs can fill the requested
/// partitions.
fn partition_record_batches(
    batches: Vec<RecordBatch>,
    target_partitions: usize,
) -> Vec<Vec<RecordBatch>> {
    if batches.is_empty() {
        return vec![vec![]];
    }

    let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    if total_rows == 0 {
        return vec![vec![]];
    }

    let num_partitions = target_partitions.max(1).min(total_rows);
    let base_rows_per_partition = total_rows / num_partitions;
    let partitions_with_extra_row = total_rows % num_partitions;
    let mut partition_batches: Vec<Vec<RecordBatch>> =
        (0..num_partitions).map(|_| Vec::new()).collect();
    let mut partition_index = 0usize;
    let mut partition_rows_remaining =
        base_rows_per_partition + usize::from(partition_index < partitions_with_extra_row);

    for batch in batches {
        let batch_rows = batch.num_rows();
        if batch_rows == 0 {
            continue;
        }

        let mut offset = 0usize;
        while offset < batch_rows {
            if partition_rows_remaining == 0 && partition_index + 1 < num_partitions {
                partition_index += 1;
                partition_rows_remaining = base_rows_per_partition
                    + usize::from(partition_index < partitions_with_extra_row);
            }

            let slice_len = (batch_rows - offset).min(partition_rows_remaining);
            partition_batches[partition_index].push(batch.slice(offset, slice_len));
            offset += slice_len;
            partition_rows_remaining -= slice_len;
        }
    }

    partition_batches
}

/// Distributes RecordBatches into PartitionStreams for parallel execution.
fn create_partition_streams(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    target_partitions: usize,
) -> Vec<Arc<dyn PartitionStream>> {
    partition_record_batches(batches, target_partitions)
        .into_iter()
        .map(|batches| {
            Arc::new(RecordBatchPartitionStream::new(schema.clone(), batches))
                as Arc<dyn PartitionStream>
        })
        .collect()
}

fn dispatch_arrow_stream_batches(
    mut stream_reader: ArrowArrayStreamReader,
    senders: Vec<Sender<Result<RecordBatch, DataFusionError>>>,
) {
    let partition_count = senders.len();

    for (batch_index, batch_result) in stream_reader.by_ref().enumerate() {
        match batch_result {
            Ok(batch) => {
                let partition_idx = batch_index % partition_count;
                if senders[partition_idx].blocking_send(Ok(batch)).is_err() {
                    // The query stopped consuming this stream, so stop dispatching.
                    return;
                }
            },
            Err(error) => {
                let message = format!("Arrow C Stream read failed: {}", error);
                for sender in &senders {
                    let _ = sender.blocking_send(Err(DataFusionError::Execution(message.clone())));
                }
                return;
            },
        }
    }
}

fn create_arrow_stream_partition_streams(
    schema: SchemaRef,
    stream_reader: ArrowArrayStreamReader,
    target_partitions: usize,
) -> Vec<Arc<dyn PartitionStream>> {
    let num_partitions = target_partitions.max(1);

    if num_partitions == 1 {
        return vec![
            Arc::new(ArrowCStreamPartitionStream::new(schema, stream_reader))
                as Arc<dyn PartitionStream>,
        ];
    }

    let mut senders = Vec::with_capacity(num_partitions);
    let mut partitions = Vec::with_capacity(num_partitions);

    for _ in 0..num_partitions {
        let (sender, receiver) = channel(ARROW_STREAM_PARTITION_BUFFER_SIZE);
        senders.push(sender);
        partitions.push(Arc::new(ArrowCStreamFanoutPartitionStream::new(
            schema.clone(),
            receiver,
        )) as Arc<dyn PartitionStream>);
    }

    // The bounded fanout queues rely on downstream consumers polling partitions
    // concurrently. That is true for the current DataFusion `execute_stream()`
    // path, which coalesces multi-partition plans by spawning one task per input
    // partition. If a future caller were to drain these partitions strictly
    // sequentially, this fanout design would need to be revisited.
    thread::Builder::new()
        .name("arrow-c-stream-fanout".to_string())
        .spawn(move || dispatch_arrow_stream_batches(stream_reader, senders))
        .expect("failed to spawn Arrow C Stream fanout thread");

    partitions
}

fn register_frame_from_arrow_stream_with_partitions(
    py_ctx: &PyBioSessionContext,
    stream_reader: ArrowArrayStreamReader,
    schema: Arc<arrow::datatypes::Schema>,
    table_name: String,
    target_partitions: usize,
) {
    let ctx = &py_ctx.ctx;
    let partitions =
        create_arrow_stream_partition_streams(schema.clone(), stream_reader, target_partitions);
    let table_source = StreamingTable::try_new(schema, partitions).unwrap();

    ctx.deregister_table(&table_name).unwrap();
    ctx.register_table(&table_name, Arc::new(table_source))
        .unwrap();
}

pub(crate) fn register_frame(
    py_ctx: &PyBioSessionContext,
    df: PyArrowType<ArrowArrayStreamReader>,
    table_name: String,
) {
    // Get the schema from the stream reader before consuming it
    let reader_schema = df.0.schema();
    let batches =
        df.0.collect::<Result<Vec<RecordBatch>, ArrowError>>()
            .unwrap();
    // Use the reader's schema (which is always available) instead of batches[0].schema()
    // This handles the case when batches is empty
    let schema = reader_schema;
    register_frame_from_batches(py_ctx, batches, schema, table_name);
}

/// Register a table from pre-collected RecordBatches and schema.
/// This is used when Arrow streams are consumed with GIL held (to avoid segfault),
/// and then the batches are passed to this function for registration without GIL.
///
/// Uses StreamingTable with multi-partition support for parallel execution,
/// distributing batches across partitions using round-robin for better performance.
pub(crate) fn register_frame_from_batches(
    py_ctx: &PyBioSessionContext,
    batches: Vec<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
    table_name: String,
) {
    let ctx = &py_ctx.ctx;

    // Get target partitions from session config for parallel execution
    let target_partitions = ctx.state().config().options().execution.target_partitions;

    // Create partition streams for parallel execution (instead of single-partition MemTable)
    let partitions = create_partition_streams(schema.clone(), batches, target_partitions);

    // Use StreamingTable instead of MemTable for multi-partition support
    let table_source = StreamingTable::try_new(schema, partitions).unwrap();

    ctx.deregister_table(&table_name).unwrap();
    ctx.register_table(&table_name, Arc::new(table_source))
        .unwrap();
}

/// Register a table from an Arrow C Stream (from Polars LazyFrame via ArrowStreamExportable).
/// This enables GIL-free streaming by using Arrow FFI instead of Python iterators.
///
/// The stream is extracted from a Polars LazyFrame's `__arrow_c_stream__()` method,
/// which provides direct Arrow C Stream access without per-batch GIL acquisition.
pub(crate) fn register_frame_from_arrow_stream(
    py_ctx: &PyBioSessionContext,
    stream_reader: ArrowArrayStreamReader,
    schema: Arc<arrow::datatypes::Schema>,
    table_name: String,
) {
    let target_partitions = py_ctx
        .ctx
        .state()
        .config()
        .options()
        .execution
        .target_partitions;
    register_frame_from_arrow_stream_with_partitions(
        py_ctx,
        stream_reader,
        schema,
        table_name,
        target_partitions,
    );
}

/// Register an Arrow C stream as a single-partition streaming table.
///
/// Writers use this path so their input row order does not depend on session-level
/// partition fanout. Range operations should continue to use the partition-aware path.
pub(crate) fn register_frame_from_arrow_stream_single_partition(
    py_ctx: &PyBioSessionContext,
    stream_reader: ArrowArrayStreamReader,
    schema: Arc<arrow::datatypes::Schema>,
    table_name: String,
) {
    register_frame_from_arrow_stream_with_partitions(py_ctx, stream_reader, schema, table_name, 1);
}

pub(crate) fn get_input_format(path: &str) -> InputFormat {
    let path = path.to_lowercase();
    if path.ends_with(".parquet") {
        InputFormat::Parquet
    } else if path.ends_with(".csv") {
        InputFormat::Csv
    } else if path.ends_with(".bed") {
        InputFormat::Bed
    } else if path.ends_with(".vcf") || path.ends_with(".vcf.gz") || path.ends_with(".vcf.bgz") {
        InputFormat::Vcf
    } else if path.ends_with(".gff") || path.ends_with(".gff.gz") || path.ends_with(".gff.bgz") {
        InputFormat::Gff
    } else if path.ends_with(".gtf") || path.ends_with(".gtf.gz") || path.ends_with(".gtf.bgz") {
        InputFormat::Gtf
    } else if path.ends_with(".cram") {
        InputFormat::Cram
    } else if path.ends_with(".sam") {
        InputFormat::Sam
    } else if path.ends_with(".pairs")
        || path.ends_with(".pairs.gz")
        || path.ends_with(".pairs.bgz")
    {
        InputFormat::Pairs
    } else {
        panic!("Unsupported format")
    }
}

pub(crate) async fn register_table(
    ctx: &SessionContext,
    path: &str,
    table_name: &str,
    format: InputFormat,
    read_options: Option<ReadOptions>,
) -> String {
    ctx.deregister_table(table_name).unwrap();
    match format {
        InputFormat::Parquet => ctx
            .register_parquet(table_name, path, ParquetReadOptions::new())
            .await
            .unwrap(),
        InputFormat::Csv => {
            let csv_read_options = CsvReadOptions::new() //FIXME: expose
                .delimiter(b',')
                .has_header(true);
            ctx.register_csv(table_name, path, csv_read_options)
                .await
                .unwrap()
        },
        InputFormat::Fastq => {
            let fastq_read_options = match &read_options {
                Some(options) => match options.clone().fastq_read_options {
                    Some(opts) => opts,
                    _ => FastqReadOptions::default(),
                },
                _ => FastqReadOptions::default(),
            };
            let table_provider = FastqTableProvider::new(
                path.to_string(),
                fastq_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register FASTQ table");
        },
        InputFormat::Vcf => {
            let vcf_read_options = match &read_options {
                Some(options) => match options.clone().vcf_read_options {
                    Some(vcf_read_options) => vcf_read_options,
                    _ => VcfReadOptions::default(),
                },
                _ => VcfReadOptions::default(),
            };
            info!(
                "Registering VCF table {} with options: {:?}",
                table_name, vcf_read_options
            );
            let table_provider = VcfTableProvider::new_with_samples(
                path.to_string(),
                vcf_read_options.info_fields,
                vcf_read_options.format_fields,
                vcf_read_options.samples,
                vcf_read_options.object_storage_options.clone(),
                vcf_read_options.zero_based,
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register VCF table");
        },
        InputFormat::Gff => {
            let gff_read_options = match &read_options {
                Some(options) => match options.clone().gff_read_options {
                    Some(gff_read_options) => gff_read_options,
                    _ => GffReadOptions::default(),
                },
                _ => GffReadOptions::default(),
            };
            info!(
                "Registering GFF table {} with options: {:?}",
                table_name, gff_read_options
            );
            let table_provider = GffTableProvider::new(
                path.to_string(),
                gff_read_options.attr_fields,
                gff_read_options.object_storage_options.clone(),
                gff_read_options.zero_based,
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register GFF table");
        },
        InputFormat::Bam | InputFormat::Sam => {
            let bam_read_options = match &read_options {
                Some(options) => match options.clone().bam_read_options {
                    Some(bam_read_options) => bam_read_options,
                    _ => BamReadOptions::default(),
                },
                _ => BamReadOptions::default(),
            };
            info!(
                "Registering {} table {} with options: {:?}",
                format, table_name, bam_read_options
            );
            let table_provider = BamTableProvider::new(
                path.to_string(),
                bam_read_options.object_storage_options.clone(),
                bam_read_options.zero_based,
                bam_read_options.tag_fields.clone(),
                false,
                bam_read_options.infer_tag_types,
                bam_read_options.infer_tag_sample_size,
                bam_read_options.tag_type_hints.clone(),
            )
            .await
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register BAM/SAM table");
        },
        InputFormat::Bed => {
            let bed_read_options = match &read_options {
                Some(options) => match options.clone().bed_read_options {
                    Some(bed_read_options) => bed_read_options,
                    _ => BedReadOptions::default(),
                },
                _ => BedReadOptions::default(),
            };
            info!(
                "Registering BED table {} with options: {:?}",
                table_name, bed_read_options
            );
            let table_provider = BedTableProvider::new(
                path.to_string(),
                BEDFields::BED4,
                bed_read_options.object_storage_options.clone(),
                bed_read_options.zero_based,
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register BED table");
        },

        InputFormat::Fasta => {
            let fasta_read_options = match &read_options {
                Some(options) => match options.clone().fasta_read_options {
                    Some(fasta_read_options) => fasta_read_options,
                    _ => FastaReadOptions::default(),
                },
                _ => FastaReadOptions::default(),
            };
            info!(
                "Registering FASTA table {} with options: {:?}",
                table_name, fasta_read_options
            );

            let table_provider = FastaTableProvider::new(
                path.to_string(),
                fasta_read_options.object_storage_options.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register FASTA table");
        },
        InputFormat::Cram => {
            let cram_read_options = match &read_options {
                Some(options) => match options.clone().cram_read_options {
                    Some(cram_read_options) => cram_read_options,
                    _ => CramReadOptions::default(),
                },
                _ => CramReadOptions::default(),
            };
            info!(
                "Registering CRAM table {} with options: {:?}",
                table_name, cram_read_options
            );
            let table_provider = CramTableProvider::new(
                path.to_string(),
                cram_read_options.reference_path,
                cram_read_options.object_storage_options.clone(),
                cram_read_options.zero_based,
                cram_read_options.tag_fields.clone(),
                false,
                cram_read_options.infer_tag_types,
                cram_read_options.infer_tag_sample_size,
                cram_read_options.tag_type_hints.clone(),
            )
            .await
            .expect("Failed to create CRAM table provider. Check that the file exists and requested tags are valid.");
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register CRAM table");
        },
        InputFormat::Pairs => {
            let pairs_read_options = match &read_options {
                Some(options) => match options.clone().pairs_read_options {
                    Some(pairs_read_options) => pairs_read_options,
                    _ => PairsReadOptions::default(),
                },
                _ => PairsReadOptions::default(),
            };
            info!(
                "Registering PAIRS table {} with options: {:?}",
                table_name, pairs_read_options
            );
            let table_provider = PairsTableProvider::new(
                path.to_string(),
                pairs_read_options.object_storage_options.clone(),
                pairs_read_options.zero_based,
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register PAIRS table");
        },
        InputFormat::Gtf => {
            let gtf_read_options = match &read_options {
                Some(options) => match options.clone().gtf_read_options {
                    Some(gtf_read_options) => gtf_read_options,
                    _ => GtfReadOptions::default(),
                },
                _ => GtfReadOptions::default(),
            };
            info!(
                "Registering GTF table {} with options: {:?}",
                table_name, gtf_read_options
            );
            let table_provider = GtfTableProvider::new(
                path.to_string(),
                gtf_read_options.attr_fields,
                gtf_read_options.object_storage_options.clone(),
                gtf_read_options.zero_based,
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register GTF table");
        },
    };
    table_name.to_string()
}

pub(crate) fn maybe_register_table(
    df_path_or_table: String,
    default_table: &String,
    read_options: Option<ReadOptions>,
    ctx: &SessionContext,
    rt: &Runtime,
) -> String {
    let ext: Vec<&str> = df_path_or_table.split('.').collect();
    debug!("ext: {:?}", ext);
    if ext.len() == 1 {
        return df_path_or_table;
    }
    match ext.last() {
        Some(_ext) => {
            rt.block_on(register_table(
                ctx,
                &df_path_or_table,
                default_table,
                get_input_format(&df_path_or_table),
                read_options,
            ));
            default_table.to_string()
        },
        _ => df_path_or_table,
    }
    .to_string()
}
#[pyo3::pyfunction]
#[pyo3(signature = (py_ctx, path, object_storage_options=None, zero_based=true, tag_fields=None, sample_size=None))]
pub fn py_describe_bam(
    py: pyo3::Python<'_>,
    py_ctx: &crate::context::PyBioSessionContext,
    path: String,
    object_storage_options: Option<crate::option::PyObjectStorageOptions>,
    zero_based: bool,
    tag_fields: Option<Vec<String>>,
    sample_size: Option<usize>,
) -> pyo3::PyResult<datafusion_python::dataframe::PyDataFrame> {
    use crate::option::pyobject_storage_options_to_object_storage_options;
    use datafusion::datasource::MemTable;
    use pyo3::exceptions::PyRuntimeError;

    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let object_storage_opts =
            pyobject_storage_options_to_object_storage_options(object_storage_options);

        let df = rt
            .block_on(async {
                // Create table provider
                let table_provider = BamTableProvider::new(
                    path.clone(),
                    object_storage_opts,
                    zero_based,
                    tag_fields,
                    false,
                    true,
                    100,
                    None,
                )
                .await
                .map_err(|e| format!("Failed to create BAM table provider: {}", e))?;

                // Call describe - returns a DataFrame
                let describe_df = table_provider
                    .describe(ctx, sample_size)
                    .await
                    .map_err(|e| format!("Failed to describe BAM: {}", e))?;

                // Execute to get RecordBatches
                let batches = describe_df
                    .collect()
                    .await
                    .map_err(|e| format!("Failed to collect batches: {}", e))?;

                // Register result as temporary table
                let schema = if let Some(first_batch) = batches.first() {
                    first_batch.schema()
                } else {
                    return Err("No batches returned from describe".to_string());
                };

                let mem_table = MemTable::try_new(schema, vec![batches])
                    .map_err(|e| format!("Failed to create memory table: {}", e))?;
                let random_table_name = format!("bam_schema_{}", rand::random::<u32>());
                ctx.register_table(random_table_name.clone(), Arc::new(mem_table))
                    .map_err(|e| format!("Failed to register table: {}", e))?;
                let df = ctx
                    .table(random_table_name)
                    .await
                    .map_err(|e| format!("Failed to get table: {}", e))?;
                Ok::<datafusion::dataframe::DataFrame, String>(df)
            })
            .map_err(|e| PyRuntimeError::new_err(format!("BAM schema extraction failed: {}", e)))?;
        Ok(datafusion_python::dataframe::PyDataFrame::new(df))
    })
}

#[pyo3::pyfunction]
#[pyo3(signature = (py_ctx, path, reference_path=None, object_storage_options=None, zero_based=true, tag_fields=None, sample_size=None))]
#[allow(unused_variables)]
pub fn py_describe_cram(
    py: pyo3::Python<'_>,
    py_ctx: &crate::context::PyBioSessionContext,
    path: String,
    reference_path: Option<String>,
    object_storage_options: Option<crate::option::PyObjectStorageOptions>,
    zero_based: bool,
    tag_fields: Option<Vec<String>>,
    sample_size: Option<usize>,
) -> pyo3::PyResult<datafusion_python::dataframe::PyDataFrame> {
    use crate::option::pyobject_storage_options_to_object_storage_options;
    use datafusion::datasource::MemTable;
    use pyo3::exceptions::PyRuntimeError;

    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let object_storage_opts =
            pyobject_storage_options_to_object_storage_options(object_storage_options);

        let df = rt
            .block_on(async {
                let table_provider = CramTableProvider::new(
                    path.clone(),
                    reference_path,
                    object_storage_opts,
                    zero_based,
                    tag_fields,
                    false,
                    true,
                    100,
                    None,
                )
                .await
                .map_err(|e| format!("Failed to create CRAM table provider: {}", e))?;

                let describe_df = table_provider
                    .describe(ctx, sample_size)
                    .await
                    .map_err(|e| format!("Failed to describe CRAM: {}", e))?;

                let batches = describe_df
                    .collect()
                    .await
                    .map_err(|e| format!("Failed to collect batches: {}", e))?;

                let schema = if let Some(first_batch) = batches.first() {
                    first_batch.schema()
                } else {
                    return Err("No batches returned from describe".to_string());
                };

                let mem_table = MemTable::try_new(schema, vec![batches])
                    .map_err(|e| format!("Failed to create memory table: {}", e))?;
                let random_table_name = format!("cram_schema_{}", rand::random::<u32>());
                ctx.register_table(random_table_name.clone(), Arc::new(mem_table))
                    .map_err(|e| format!("Failed to register table: {}", e))?;
                let df = ctx
                    .table(random_table_name)
                    .await
                    .map_err(|e| format!("Failed to get table: {}", e))?;
                Ok::<datafusion::dataframe::DataFrame, String>(df)
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("CRAM schema extraction failed: {}", e))
            })?;
        Ok(datafusion_python::dataframe::PyDataFrame::new(df))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::partition_record_batches;
    use super::RecordBatch;

    fn make_batch(start: i32, len: usize) -> RecordBatch {
        let contigs = StringArray::from_iter_values((0..len).map(|_| "chr1"));
        let starts = Int32Array::from_iter_values((0..len).map(|offset| start + offset as i32));
        let ends = Int32Array::from_iter_values((0..len).map(|offset| start + offset as i32 + 10));
        let schema = Arc::new(Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("start", DataType::Int32, false),
            Field::new("end", DataType::Int32, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(contigs), Arc::new(starts), Arc::new(ends)],
        )
        .unwrap()
    }

    fn partition_sizes(partitions: &[Vec<RecordBatch>]) -> Vec<usize> {
        partitions
            .iter()
            .map(|partition| partition.iter().map(RecordBatch::num_rows).sum())
            .collect()
    }

    #[test]
    fn eager_partitioning_splits_large_batches_to_fill_target_partitions() {
        let batches = vec![make_batch(0, 10), make_batch(10, 10)];

        let partitions = partition_record_batches(batches, 4);

        assert_eq!(partitions.len(), 4);
        assert_eq!(partition_sizes(&partitions), vec![5, 5, 5, 5]);
        assert_eq!(
            partitions
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![5, 5, 5, 5]
        );
    }

    #[test]
    fn eager_partitioning_balances_rows_when_total_rows_not_divisible() {
        let batches = vec![make_batch(0, 10), make_batch(10, 3)];

        let partitions = partition_record_batches(batches, 4);

        assert_eq!(partitions.len(), 4);
        assert_eq!(partition_sizes(&partitions), vec![4, 3, 3, 3]);
    }

    #[test]
    fn eager_partitioning_caps_partitions_at_total_rows() {
        let batches = vec![make_batch(0, 2)];

        let partitions = partition_record_batches(batches, 8);

        assert_eq!(partitions.len(), 2);
        assert_eq!(partition_sizes(&partitions), vec![1, 1]);
    }

    #[test]
    fn eager_partitioning_merges_batches_when_target_partitions_is_smaller() {
        let batches = vec![
            make_batch(0, 2),
            make_batch(2, 2),
            make_batch(4, 2),
            make_batch(6, 2),
        ];

        let partitions = partition_record_batches(batches, 2);

        assert_eq!(partitions.len(), 2);
        assert_eq!(partition_sizes(&partitions), vec![4, 4]);
    }

    #[test]
    fn eager_partitioning_uses_one_partition_when_target_is_zero() {
        let batches = vec![make_batch(0, 3), make_batch(3, 2)];

        let partitions = partition_record_batches(batches, 0);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partition_sizes(&partitions), vec![5]);
    }
}
