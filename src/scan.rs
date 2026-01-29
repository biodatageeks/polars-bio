use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

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
use datafusion_bio_format_fastq::bgzf_parallel_reader::BgzfFastqTableProvider as BgzfParallelFastqTableProvider;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use datafusion_bio_format_gff::bgzf_parallel_reader::BgzfGffTableProvider as BgzfParallelGffTableProvider;
use datafusion_bio_format_gff::table_provider::GffTableProvider;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use futures::Stream;
use log::info;
use tokio::runtime::Runtime;
use tracing::debug;

use crate::context::PyBioSessionContext;
use crate::option::{
    BamReadOptions, BedReadOptions, CramReadOptions, FastaReadOptions, FastqReadOptions,
    GffReadOptions, InputFormat, ReadOptions, VcfReadOptions,
};

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

/// Distributes RecordBatches into PartitionStreams for parallel execution.
/// Uses round-robin distribution to balance batches across partitions.
fn create_partition_streams(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    target_partitions: usize,
) -> Vec<Arc<dyn PartitionStream>> {
    if batches.is_empty() {
        // Return single empty partition
        return vec![Arc::new(RecordBatchPartitionStream::new(schema, vec![]))];
    }

    let num_partitions = target_partitions.min(batches.len()).max(1);
    let mut partition_batches: Vec<Vec<RecordBatch>> =
        (0..num_partitions).map(|_| Vec::new()).collect();

    // Round-robin distribution
    for (i, batch) in batches.into_iter().enumerate() {
        partition_batches[i % num_partitions].push(batch);
    }

    partition_batches
        .into_iter()
        .filter(|p| !p.is_empty())
        .map(|batches| {
            Arc::new(RecordBatchPartitionStream::new(schema.clone(), batches))
                as Arc<dyn PartitionStream>
        })
        .collect()
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
    let ctx = &py_ctx.ctx;

    // Create a single partition stream that reads from Arrow C Stream
    let partition = Arc::new(ArrowCStreamPartitionStream::new(
        schema.clone(),
        stream_reader,
    )) as Arc<dyn PartitionStream>;

    // Use StreamingTable with the Arrow C Stream partition
    let table_source = StreamingTable::try_new(schema, vec![partition]).unwrap();

    ctx.deregister_table(&table_name).unwrap();
    ctx.register_table(&table_name, Arc::new(table_source))
        .unwrap();
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
    } else if path.ends_with(".cram") {
        InputFormat::Cram
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
                    Some(fastq_read_options) => fastq_read_options,
                    _ => FastqReadOptions::default(),
                },
                _ => FastqReadOptions::default(),
            };
            info!(
                "Registering FASTQ table {} with options: {:?}",
                table_name, fastq_read_options
            );

            if fastq_read_options.parallel {
                if path.ends_with(".bgz") {
                    let table_provider = BgzfParallelFastqTableProvider::try_new(path).unwrap();
                    ctx.register_table(table_name, Arc::new(table_provider))
                        .expect("Failed to register parallel FASTQ table");
                } else if path.ends_with(".gz") {
                    match BgzfParallelFastqTableProvider::try_new(path) {
                        Ok(table_provider) => {
                            ctx.register_table(table_name, Arc::new(table_provider))
                                .expect("Failed to register parallel FASTQ table");
                        },
                        Err(_) => {
                            let table_provider = FastqTableProvider::new(
                                path.to_string(),
                                None,
                                fastq_read_options.object_storage_options.clone(),
                            )
                            .unwrap();
                            ctx.register_table(table_name, Arc::new(table_provider))
                                .expect("Failed to register FASTQ table");
                        },
                    }
                } else {
                    let table_provider = FastqTableProvider::new(
                        path.to_string(),
                        None,
                        fastq_read_options.object_storage_options.clone(),
                    )
                    .unwrap();
                    ctx.register_table(table_name, Arc::new(table_provider))
                        .expect("Failed to register FASTQ table");
                }
            } else {
                let table_provider = FastqTableProvider::new(
                    path.to_string(),
                    None,
                    fastq_read_options.object_storage_options.clone(),
                )
                .unwrap();
                ctx.register_table(table_name, Arc::new(table_provider))
                    .expect("Failed to register FASTQ table");
            }
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
            let table_provider = VcfTableProvider::new(
                path.to_string(),
                vcf_read_options.info_fields,
                vcf_read_options.format_fields,
                vcf_read_options.thread_num,
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
            if gff_read_options.parallel {
                if path.ends_with(".bgz") {
                    // Explicit BGZF extension: use the parallel reader
                    let table_provider = BgzfParallelGffTableProvider::try_new(
                        path,
                        gff_read_options.attr_fields.clone(),
                        gff_read_options.zero_based,
                    )
                    .unwrap();
                    ctx.register_table(table_name, Arc::new(table_provider))
                        .expect("Failed to register parallel GFF table");
                    return table_name.to_string();
                } else if path.ends_with(".gz") {
                    // Heuristically try BGZF even with .gz; fall back if not BGZF
                    match BgzfParallelGffTableProvider::try_new(
                        path,
                        gff_read_options.attr_fields.clone(),
                        gff_read_options.zero_based,
                    ) {
                        Ok(table_provider) => {
                            ctx.register_table(table_name, Arc::new(table_provider))
                                .expect("Failed to register parallel GFF table");
                            return table_name.to_string();
                        },
                        Err(_) => {
                            // Not BGZF or unsupported; fall through to standard provider
                        },
                    }
                }
            }
            {
                let table_provider = GffTableProvider::new(
                    path.to_string(),
                    gff_read_options.attr_fields,
                    gff_read_options.thread_num,
                    gff_read_options.object_storage_options.clone(),
                    gff_read_options.zero_based,
                )
                .unwrap();
                ctx.register_table(table_name, Arc::new(table_provider))
                    .expect("Failed to register GFF table");
            }
        },
        InputFormat::Bam => {
            let bam_read_options = match &read_options {
                Some(options) => match options.clone().bam_read_options {
                    Some(bam_read_options) => bam_read_options,
                    _ => BamReadOptions::default(),
                },
                _ => BamReadOptions::default(),
            };
            info!(
                "Registering BAM table {} with options: {:?}",
                table_name, bam_read_options
            );
            let table_provider = BamTableProvider::new(
                path.to_string(),
                bam_read_options.thread_num,
                bam_read_options.object_storage_options.clone(),
                bam_read_options.zero_based,
                bam_read_options.tag_fields.clone(),
            )
            .unwrap();
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register BAM table");
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
                bed_read_options.thread_num,
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
                None,
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
            )
            .unwrap();
            // TODO: Add tag_fields support for CRAM once implemented in datafusion-bio-formats
            if cram_read_options.tag_fields.is_some() {
                log::warn!("CRAM tag_fields parameter is not yet supported and will be ignored");
            }
            ctx.register_table(table_name, Arc::new(table_provider))
                .expect("Failed to register CRAM table");
        },
        InputFormat::IndexedVcf | InputFormat::IndexedBam => {
            todo!("Indexed formats are not supported")
        },
        InputFormat::Gtf => {
            todo!("Gtf format is not supported")
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
