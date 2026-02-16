mod context;
mod operation;
mod option;
mod pileup;
mod query;
mod scan;
mod udtf;
mod utils;
mod write;

use std::string::ToString;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatchReader;
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion_bio_format_core::object_storage::ObjectStorageOptions;
use datafusion_bio_format_vcf::storage::VcfReader;
use datafusion_python::dataframe::PyDataFrame;
use log::{debug, error, info};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::context::PyBioSessionContext;
use crate::operation::do_range_operation;
use crate::option::{
    pyobject_storage_options_to_object_storage_options, BamReadOptions, BamWriteOptions,
    BedReadOptions, BioTable, CramReadOptions, CramWriteOptions, FastaReadOptions,
    FastqReadOptions, FastqWriteOptions, FilterOp, GffReadOptions, InputFormat, OutputFormat,
    PairsReadOptions, PileupOptions, PyObjectStorageOptions, RangeOp, RangeOptions, ReadOptions,
    VcfReadOptions, VcfWriteOptions, WriteOptions,
};
use crate::scan::{
    maybe_register_table, register_frame, register_frame_from_arrow_stream,
    register_frame_from_batches, register_table,
};

const LEFT_TABLE: &str = "s1";
const RIGHT_TABLE: &str = "s2";
const DEFAULT_COLUMN_NAMES: [&str; 3] = ["contig", "start", "end"];

#[pyfunction]
#[pyo3(signature = (py_ctx, df1, df2, range_options, limit=None))]
fn range_operation_frame(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    df1: PyArrowType<ArrowArrayStreamReader>,
    df2: PyArrowType<ArrowArrayStreamReader>,
    range_options: RangeOptions,
    limit: Option<usize>,
) -> PyResult<PyDataFrame> {
    // Consume Arrow streams WITH GIL held to avoid segfault.
    // Arrow FFI streams exported from Python may require GIL access for callbacks.
    let schema1 = df1.0.schema();
    let batches1 = df1
        .0
        .collect::<Result<Vec<datafusion::arrow::array::RecordBatch>, datafusion::arrow::error::ArrowError>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let schema2 = df2.0.schema();
    let batches2 = df2
        .0
        .collect::<Result<Vec<datafusion::arrow::array::RecordBatch>, datafusion::arrow::error::ArrowError>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    // Now release GIL for the actual computation (registration and join)
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        register_frame_from_batches(py_ctx, batches1, schema1, LEFT_TABLE.to_string());
        register_frame_from_batches(py_ctx, batches2, schema2, RIGHT_TABLE.to_string());
        match limit {
            Some(l) => Ok(PyDataFrame::new(
                do_range_operation(
                    ctx,
                    &rt,
                    range_options,
                    LEFT_TABLE.to_string(),
                    RIGHT_TABLE.to_string(),
                )
                .limit(0, Some(l))
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
            )),
            _ => {
                let df = do_range_operation(
                    ctx,
                    &rt,
                    range_options,
                    LEFT_TABLE.to_string(),
                    RIGHT_TABLE.to_string(),
                );
                let py_df = PyDataFrame::new(df);
                Ok(py_df)
            },
        }
    })
}

/// Execute a range operation with Arrow C Stream inputs from LazyFrames.
/// Uses ArrowStreamExportable (Polars >= 1.37.1) for GIL-free streaming.
///
/// This function accepts Arrow C Streams directly, which are extracted from
/// Polars LazyFrames via their `__arrow_c_stream__()` method. The streams
/// are consumed with GIL held (required for Arrow FFI export), but all
/// subsequent batch processing happens in pure Rust without GIL.
#[pyfunction]
#[pyo3(signature = (py_ctx, stream1, stream2, schema1, schema2, range_options, limit=None))]
fn range_operation_lazy(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    stream1: PyArrowType<ArrowArrayStreamReader>,
    stream2: PyArrowType<ArrowArrayStreamReader>,
    schema1: PyArrowType<arrow::datatypes::Schema>,
    schema2: PyArrowType<arrow::datatypes::Schema>,
    range_options: RangeOptions,
    limit: Option<usize>,
) -> PyResult<PyDataFrame> {
    let schema1 = Arc::new(schema1.0);
    let schema2 = Arc::new(schema2.0);

    // Extract the stream readers (this consumes them)
    let reader1 = stream1.0;
    let reader2 = stream2.0;

    // Release GIL for the actual computation (registration and join)
    // The Arrow C Streams have been extracted - no more Python interaction needed
    py.allow_threads(|| {
        let rt = Runtime::new().map_err(|e| PyValueError::new_err(e.to_string()))?;
        let ctx = &py_ctx.ctx;

        register_frame_from_arrow_stream(py_ctx, reader1, schema1, LEFT_TABLE.to_string());
        register_frame_from_arrow_stream(py_ctx, reader2, schema2, RIGHT_TABLE.to_string());

        match limit {
            Some(l) => Ok(PyDataFrame::new(
                do_range_operation(
                    ctx,
                    &rt,
                    range_options,
                    LEFT_TABLE.to_string(),
                    RIGHT_TABLE.to_string(),
                )
                .limit(0, Some(l))
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
            )),
            _ => {
                let df = do_range_operation(
                    ctx,
                    &rt,
                    range_options,
                    LEFT_TABLE.to_string(),
                    RIGHT_TABLE.to_string(),
                );
                Ok(PyDataFrame::new(df))
            },
        }
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, df_path_or_table1, df_path_or_table2, range_options, read_options1=None, read_options2=None, limit=None))]
fn range_operation_scan(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    df_path_or_table1: String,
    df_path_or_table2: String,
    range_options: RangeOptions,
    read_options1: Option<ReadOptions>,
    read_options2: Option<ReadOptions>,
    limit: Option<usize>,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let left_table = maybe_register_table(
            df_path_or_table1,
            &LEFT_TABLE.to_string(),
            read_options1,
            ctx,
            &rt,
        );
        let right_table = maybe_register_table(
            df_path_or_table2,
            &RIGHT_TABLE.to_string(),
            read_options2,
            ctx,
            &rt,
        );
        match limit {
            Some(l) => Ok(PyDataFrame::new(
                do_range_operation(ctx, &rt, range_options, left_table, right_table)
                    .limit(0, Some(l))
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            )),
            _ => Ok(PyDataFrame::new(do_range_operation(
                ctx,
                &rt,
                range_options,
                left_table,
                right_table,
            ))),
        }
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, path, name, input_format, read_options=None))]
fn py_register_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    path: String,
    name: Option<String>,
    input_format: InputFormat,
    read_options: Option<ReadOptions>,
) -> PyResult<BioTable> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;

        let table_name = match name {
            Some(name) => name,
            None => path
                .to_lowercase()
                .split('/')
                .last()
                .unwrap()
                .to_string()
                .replace(&format!(".{}", input_format).to_string().to_lowercase(), "")
                .replace(".", "_")
                .replace("-", "_"),
        };
        rt.block_on(register_table(
            ctx,
            &path,
            &table_name,
            input_format.clone(),
            read_options,
        ));
        match rt.block_on(ctx.table(&table_name)) {
            Ok(table) => {
                let schema = table.schema().as_arrow();
                info!("Table: {} registered for path: {}", table_name, path);
                let bio_table = BioTable {
                    name: table_name,
                    format: input_format,
                    path,
                };
                debug!("Schema: {:?}", schema);
                Ok(bio_table)
            },
            Err(e) => {
                error!("Failed to register table for path {}: {:?}", path, e);
                Err(PyValueError::new_err(format!(
                    "Failed to register table: {}",
                    e
                )))
            },
        }
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, sql_text))]
fn py_read_sql(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    sql_text: String,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let df = rt
            .block_on(ctx.sql(&sql_text))
            .map_err(|e| PyValueError::new_err(format!("SQL query failed: {}", e)))?;
        Ok(PyDataFrame::new(df))
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, table_name))]
fn py_read_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    table_name: String,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let df = rt
            .block_on(ctx.sql(&format!("SELECT * FROM {}", table_name)))
            .map_err(|e| {
                PyValueError::new_err(format!("Failed to read table '{}': {}", table_name, e))
            })?;
        Ok(PyDataFrame::new(df))
    })
}

/// Get the schema of a registered table without materializing data.
///
/// This function extracts the Arrow schema from a table registered in DataFusion
/// without executing any queries or reading data. It enables metadata extraction
/// from file-backed tables (VCF, FASTQ, etc.) without loading the entire file
/// into memory, which is critical for large genomics files.
///
/// # Arguments
/// * `py_ctx` - The PyBioSessionContext containing the DataFusion context
/// * `table_name` - Name of the registered table
///
/// # Returns
/// PyArrow schema with all field metadata preserved
///
/// # Example
/// ```python
/// from polars_bio.polars_bio import py_register_table, py_get_table_schema
/// from polars_bio.context import ctx
///
/// # Register VCF table (no data read yet)
/// table = py_register_table(ctx, "large.vcf", None, InputFormat.Vcf, None)
///
/// # Get schema without materializing (lightweight operation)
/// schema = py_get_table_schema(ctx, table.name)
///
/// # Extract metadata from schema
/// vcf_metadata = extract_vcf_metadata_from_schema(schema)
/// ```
#[pyfunction]
#[pyo3(signature = (py_ctx, table_name))]
fn py_get_table_schema(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    table_name: String,
) -> PyResult<PyArrowType<datafusion::arrow::datatypes::Schema>> {
    py.allow_threads(|| {
        let rt = Runtime::new()
            .map_err(|e| PyValueError::new_err(format!("Failed to create runtime: {}", e)))?;
        let ctx = &py_ctx.ctx;

        // Get table from context
        let table = rt.block_on(ctx.table(&table_name)).map_err(|e| {
            PyValueError::new_err(format!("Failed to get table '{}': {}", table_name, e))
        })?;

        // Extract schema without reading data
        let schema = table.schema();
        let arrow_schema = schema.as_arrow();

        info!(
            "Extracted schema for table '{}' without materializing data",
            table_name
        );
        debug!("Schema: {:?}", arrow_schema);

        Ok(PyArrowType((*arrow_schema).clone()))
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, path, object_storage_options=None))]
fn py_describe_vcf(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    path: String,
    object_storage_options: Option<PyObjectStorageOptions>,
) -> PyResult<PyDataFrame> {
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let base_options =
            pyobject_storage_options_to_object_storage_options(object_storage_options)
                .unwrap_or_default();

        // Set specific options for describe, overriding base options
        let desc_object_storage_options = ObjectStorageOptions {
            chunk_size: Some(8),
            concurrent_fetches: Some(1),
            ..base_options
        };
        info!("{}", desc_object_storage_options);

        let df = rt
            .block_on(async {
                let mut reader = VcfReader::new(path, Some(desc_object_storage_options)).await;
                let rb = reader
                    .describe()
                    .await
                    .map_err(|e| format!("Failed to describe VCF: {}", e))?;
                let mem_table = MemTable::try_new(rb.schema().clone(), vec![vec![rb]])
                    .map_err(|e| format!("Failed to create memory table: {}", e))?;
                let random_table_name = format!("vcf_schema_{}", rand::random::<u32>());
                ctx.register_table(random_table_name.clone(), Arc::new(mem_table))
                    .map_err(|e| format!("Failed to register table: {}", e))?;
                let df = ctx
                    .table(random_table_name)
                    .await
                    .map_err(|e| format!("Failed to get table: {}", e))?;
                Ok::<DataFrame, String>(df)
            })
            .map_err(|e| PyRuntimeError::new_err(format!("VCF schema extraction failed: {}", e)))?;
        Ok(PyDataFrame::new(df))
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, name, query))]
fn py_register_view(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    name: String,
    query: String,
) -> PyResult<()> {
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        rt.block_on(ctx.sql(&format!("CREATE OR REPLACE VIEW {} AS {}", name, query)))
            .map_err(|e| {
                PyValueError::new_err(format!("Failed to create view '{}': {}", name, e))
            })?;
        Ok(())
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, name, df))]
fn py_from_polars(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    name: String,
    df: PyArrowType<ArrowArrayStreamReader>,
) {
    py.allow_threads(|| {
        register_frame(py_ctx, df, name);
    })
}

/// Write a DataFrame to a file in the specified format.
///
/// # Arguments
/// * `py_ctx` - The PyBioSessionContext
/// * `df` - Arrow stream reader containing the DataFrame data
/// * `path` - Output file path
/// * `output_format` - Output format (Vcf or Fastq)
/// * `write_options` - Optional write options
///
/// # Returns
/// The number of rows written
#[pyfunction]
#[pyo3(signature = (py_ctx, sql, path, output_format, write_options=None))]
fn py_write_from_sql(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    sql: String,
    path: String,
    output_format: OutputFormat,
    write_options: Option<WriteOptions>,
) -> PyResult<u64> {
    py.allow_threads(|| {
        let rt = Runtime::new().map_err(|e| PyValueError::new_err(e.to_string()))?;
        let ctx = &py_ctx.ctx;

        rt.block_on(async {
            // Execute SQL to get DataFrame
            let df = ctx
                .sql(&sql)
                .await
                .map_err(|e| PyValueError::new_err(format!("SQL execution failed: {}", e)))?;

            // Write directly from DataFusion DataFrame
            let row_count = crate::write::write_table(ctx, df, &path, output_format, write_options)
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

            Ok(row_count)
        })
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, df, path, output_format, write_options=None))]
fn py_write_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    df: PyArrowType<ArrowArrayStreamReader>,
    path: String,
    output_format: OutputFormat,
    write_options: Option<WriteOptions>,
) -> PyResult<u64> {
    let PyArrowType(stream_reader) = df;
    let schema = stream_reader.schema();
    let temp_table_name = format!("_write_stream_{}", rand::random::<u32>());

    py.allow_threads(move || {
        let rt = Runtime::new().map_err(|e| PyValueError::new_err(e.to_string()))?;
        let ctx = &py_ctx.ctx;

        // Register a streaming table backed directly by the Arrow C Stream.
        // This avoids materializing all batches in Python and lets DataFusion
        // pull data on demand without the GIL.
        register_frame_from_arrow_stream(
            py_ctx,
            stream_reader,
            schema.clone(),
            temp_table_name.clone(),
        );

        rt.block_on(async {
            let mut df = ctx
                .table(&temp_table_name)
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

            // Convert string columns for bio format compatibility
            // - BAM/CRAM formats expect Utf8 (not LargeUtf8)
            // - VCF/FASTQ formats expect LargeUtf8
            // - Utf8View is not supported by any format yet
            use datafusion::arrow::datatypes::DataType;
            use datafusion::logical_expr::{Cast, Expr};
            use datafusion::prelude::*;

            let schema = df.schema().inner();
            let mut select_exprs = Vec::new();

            // Determine target string type based on output format
            let target_string_type = match output_format {
                OutputFormat::Bam | OutputFormat::Sam | OutputFormat::Cram => DataType::Utf8,
                OutputFormat::Vcf | OutputFormat::Fastq => DataType::LargeUtf8,
            };

            for field in schema.fields().iter() {
                // Use qualified column name with proper quoting for special characters
                // Format: table."column" to handle uppercase/special chars
                let qualified_name = format!("{}.\"{}\"", temp_table_name, field.name());

                match field.data_type() {
                    DataType::Utf8View | DataType::LargeUtf8 | DataType::Utf8 => {
                        // Cast all string types to the target type for this format
                        let expr = Expr::Cast(Cast::new(
                            Box::new(col(qualified_name)),
                            target_string_type.clone(),
                        ))
                        .alias(field.name());
                        select_exprs.push(expr);
                    },
                    _ => {
                        // Keep as-is but remove table prefix
                        select_exprs.push(col(qualified_name).alias(field.name()));
                    },
                }
            }

            // Always apply select to remove table prefix and cast types
            df = df
                .select(select_exprs)
                .map_err(|e| PyValueError::new_err(format!("Failed to cast Utf8View: {}", e)))?;

            let row_count = crate::write::write_table(ctx, df, &path, output_format, write_options)
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

            ctx.deregister_table(&temp_table_name)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

            Ok(row_count)
        })
    })
}

/// Register a pileup depth table without executing any query.
///
/// Creates a BAM/CRAM provider wrapped in a DepthTableProvider and registers
/// it in the DataFusion context. Returns the table name so the Python side
/// can query it lazily via `register_io_source`.
#[pyfunction]
#[pyo3(signature = (py_ctx, path, pileup_options=None))]
fn py_register_pileup_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    path: String,
    pileup_options: Option<PileupOptions>,
) -> PyResult<String> {
    py.allow_threads(|| {
        let rt = Runtime::new()?;
        let ctx = &py_ctx.ctx;
        let config = pileup::pileup_options_to_config(pileup_options);
        let binary_cigar = config.binary_cigar;
        let zero_based = config.zero_based;

        let table_name = rt
            .block_on(async {
                let path_lower = path.to_lowercase();
                let provider: Arc<dyn datafusion::datasource::TableProvider> =
                    if path_lower.ends_with(".cram") {
                        let p = datafusion_bio_format_cram::table_provider::CramTableProvider::new(
                            path.clone(),
                            None,
                            None,
                            zero_based,
                            None,
                            binary_cigar,
                        )
                        .await
                        .map_err(|e| format!("Failed to create CRAM provider: {}", e))?;
                        Arc::new(p)
                    } else {
                        // BAM or SAM
                        let p = datafusion_bio_format_bam::table_provider::BamTableProvider::new(
                            path.clone(),
                            None,
                            zero_based,
                            None,
                            binary_cigar,
                        )
                        .await
                        .map_err(|e| format!("Failed to create BAM provider: {}", e))?;
                        Arc::new(p)
                    };

                let depth_provider = pileup::DepthTableProvider::new(provider, config);
                let table_name = format!("_pileup_{}", rand::random::<u32>());
                ctx.register_table(&table_name, Arc::new(depth_provider))
                    .map_err(|e| format!("Failed to register depth table: {}", e))?;
                // No execution — just register and return the name
                Ok::<String, String>(table_name)
            })
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Pileup table registration failed: {}", e))
            })?;

        info!(
            "Registered pileup depth table '{}' for path: {}",
            table_name, path
        );
        Ok(table_name)
    })
}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(range_operation_frame, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_lazy, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_sql, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_table_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_describe_vcf, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_view, m)?)?;
    m.add_function(wrap_pyfunction!(py_from_polars, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_from_sql, m)?)?;
    m.add_function(wrap_pyfunction!(scan::py_describe_bam, m)?)?;
    m.add_function(wrap_pyfunction!(scan::py_describe_cram, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_pileup_table, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<FilterOp>()?;
    m.add_class::<RangeOp>()?;
    m.add_class::<RangeOptions>()?;
    m.add_class::<InputFormat>()?;
    m.add_class::<OutputFormat>()?;
    m.add_class::<ReadOptions>()?;
    m.add_class::<WriteOptions>()?;
    m.add_class::<GffReadOptions>()?;
    m.add_class::<VcfReadOptions>()?;
    m.add_class::<VcfWriteOptions>()?;
    m.add_class::<FastqReadOptions>()?;
    m.add_class::<FastqWriteOptions>()?;
    m.add_class::<BamReadOptions>()?;
    m.add_class::<BamWriteOptions>()?;
    m.add_class::<CramReadOptions>()?;
    m.add_class::<CramWriteOptions>()?;
    m.add_class::<BedReadOptions>()?;
    m.add_class::<FastaReadOptions>()?;
    m.add_class::<PairsReadOptions>()?;
    m.add_class::<PileupOptions>()?;
    m.add_class::<PyObjectStorageOptions>()?;
    Ok(())
}
