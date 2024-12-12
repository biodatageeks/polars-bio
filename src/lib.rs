use std::sync::Arc;
use datafusion::arrow::array::{RecordBatch};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::config::{ConfigOptions, CsvOptions};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::datafusion::prelude::SessionContext;
use pyo3::prelude::*;
use pyo3::pyclass;
use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};
use tokio::runtime::Runtime;

#[pyclass(eq, eq_int)]
#[derive(Clone,PartialEq)]
pub enum OverlapFilter {
    Weak = 0,
    Strict = 1,
}

pub enum InputFormat {
    Parquet,
    Csv,
}


#[pyclass(name = "BioSessionContext")]
#[derive(Clone)]
pub struct PyBioSessionContext {
    pub ctx: SessionContext,
}

#[pymethods]
impl PyBioSessionContext {
    #[pyo3(signature = ())]
    #[new]
    pub fn new() -> PyResult<Self> {
        let ctx = create_context(Algorithm::Coitrees);
        Ok(PyBioSessionContext { ctx })
    }
    #[pyo3(signature = (key, value))]
    pub fn set_option(&mut self, key: &str, value: &str) {
        let state = self.ctx.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .set(key, value)
            .unwrap();
    }
}

fn create_context(algorithm: Algorithm) -> SessionContext {
    let mut options = ConfigOptions::new();
    let tuning_options = vec![
        ("datafusion.optimizer.repartition_joins", "false"),
        ("datafusion.execution.coalesce_batches", "false"),
    ];

    for o in tuning_options {
        options.set(o.0, o.1).expect("TODO: panic message");
    }

    let mut sequila_config = SequilaConfig::default();
    sequila_config.prefer_interval_join = true;
    sequila_config.interval_join_algorithm = algorithm;

    let config = SessionConfig::from(options)
        .with_option_extension(sequila_config)
        .with_information_schema(true);

    SessionContext::new_with_sequila(config)
}

fn register_frame(
    ctx: &SessionContext,
    df: PyArrowType<ArrowArrayStreamReader>,
    table_name: String,
) {
    let batches =
        df.0.collect::<Result<Vec<RecordBatch>, ArrowError>>()
            .unwrap();
    let schema = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches]).unwrap();
    ctx.deregister_table(&table_name).unwrap();
    ctx.register_table(&table_name, Arc::new(table)).unwrap();
}

fn get_input_format(path: &str) -> InputFormat {
    if path.ends_with(".parquet") {
        InputFormat::Parquet
    } else if path.ends_with(".csv") {
        InputFormat::Csv
    }
    else { panic!("Unsupported format") }
}

async fn register_table(ctx: &SessionContext, path: &str, table_name: &str, format: InputFormat) {
    ctx.deregister_table(table_name).unwrap();
    match format {
        InputFormat::Parquet => {
            ctx.register_parquet(table_name, path, ParquetReadOptions::new())
                .await
                .unwrap()
        }
        InputFormat::Csv => {
            let csv_read_options = CsvReadOptions::new() //FIXME: expose
                .delimiter(b',')
                .has_header(true);
            ctx.register_csv(table_name, path, csv_read_options)
                .await
                .unwrap()
        }
    }
}

async fn do_overlap(ctx: &SessionContext, filter: OverlapFilter) -> datafusion::dataframe::DataFrame {

    let sign = match filter {
        OverlapFilter::Weak => "=".to_string(),
        _ => "".to_string(),
    };
        let query = format!(r#"
            SELECT
                a.contig as contig_1,
                a.pos_start as pos_start_1,
                a.pos_end as pos_end_1,
                b.contig as contig_2,
                b.pos_start as pos_start_2,
                b.pos_end as pos_end_2
            FROM
                s1 a, s2 b
            WHERE
                a.contig=b.contig
            AND
                a.pos_end >{} b.pos_start
            AND
                a.pos_start <{} b.pos_end
        "#, sign, sign);
    ctx.sql(&query).await.unwrap()
}

#[pyfunction]
fn overlap_frame(
    py_ctx: &PyBioSessionContext,
    df1: PyArrowType<ArrowArrayStreamReader>,
    df2: PyArrowType<ArrowArrayStreamReader>,
    overlap_filter: OverlapFilter
) -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    register_frame(&ctx, df1, "s1".to_string());
    register_frame(&ctx, df2, "s2".to_string());
    let df = rt.block_on(do_overlap(&ctx, overlap_filter));
    Ok(PyDataFrame::new(df))
}

#[pyfunction]
fn overlap_scan(
    py_ctx: &PyBioSessionContext,
    df_path1: String,
    df_path2: String,
    overlap_filter: OverlapFilter
) -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    println!(
        "Running overlap with {} threads",
        ctx.state().config().options().execution.target_partitions
    );
    let s1_path = &df_path1;
    let s2_path = &df_path2;
    rt.block_on(register_table(&ctx, s1_path, "s1", get_input_format(s1_path)));
    rt.block_on(register_table(&ctx, s2_path, "s2", get_input_format(s2_path)));

    let df = rt.block_on(do_overlap(&ctx, overlap_filter));
    Ok(PyDataFrame::new(df))
}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(overlap_frame, m)?)?;
    m.add_function(wrap_pyfunction!(overlap_scan, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<OverlapFilter>()?;
    Ok(())
}
