use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::datafusion::prelude::SessionContext;
use log::info;
use pyo3::prelude::*;
use pyo3::pyclass;
use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};
use tokio::runtime::Runtime;

const LEFT_TABLE: &str = "s1";
const RIGHT_TABLE: &str = "s2";

#[pyclass(name = "RangeOptions")]
#[derive(Clone)]
pub struct RangeOptions {
    pub range_op: RangeOp,
    pub filter_op: Option<FilterOp>,
    pub suffixes: Option<Vec<String>>,
    pub columns_1: Option<Vec<String>>,
    pub columns_2: Option<Vec<String>>,
    pub on_cols: Option<Vec<String>>,
    pub overlap_alg: Option<String>,
}

#[pymethods]
impl RangeOptions {
    #[new]
    #[pyo3(signature = (range_op, filter_op=None, suffixes=None, columns_1=None, columns_2=None, on_cols=None, overlap_alg=None))]
    pub fn new(
        range_op: RangeOp,
        filter_op: Option<FilterOp>,
        suffixes: Option<Vec<String>>,
        columns_1: Option<Vec<String>>,
        columns_2: Option<Vec<String>>,
        on_cols: Option<Vec<String>>,
        overlap_alg: Option<String>,
    ) -> Self {
        RangeOptions {
            range_op,
            filter_op,
            suffixes,
            columns_1,
            columns_2,
            on_cols,
            overlap_alg,
        }
    }
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum FilterOp {
    Weak = 0,
    Strict = 1,
}

#[pyclass(eq, eq_int)]
#[derive(Clone, PartialEq)]
pub enum RangeOp {
    Overlap = 0,
    Complement = 1,
    Cluster = 2,
    Nearest = 3,
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
        let ctx = create_context();
        Ok(PyBioSessionContext { ctx })
    }
    #[pyo3(signature = (key, value))]
    pub fn set_option(&mut self, key: &str, value: &str) {
        set_option_internal(&self.ctx, key, value);
    }
}

pub fn set_option_internal(ctx: &SessionContext, key: &str, value: &str) {
    let state = ctx.state_ref();
    state
        .write()
        .config_mut()
        .options_mut()
        .set(key, value)
        .unwrap();
}

fn create_context() -> SessionContext {
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
    } else {
        panic!("Unsupported format")
    }
}

async fn register_table(ctx: &SessionContext, path: &str, table_name: &str, format: InputFormat) {
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
    }
}

async fn do_nearest(ctx: &SessionContext, filter: FilterOp) -> datafusion::dataframe::DataFrame {
    info!(
        "Running nearest: algorithm {} with {} thread(s)",
        ctx.state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.state().config().options().execution.target_partitions
    );
    let sign = match filter {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let query = format!(
        r#"
        SELECT
            a.contig AS contig_1,
            a.pos_start AS pos_start_1,
            a.pos_end AS pos_end_1,
            b.contig AS contig_2,
            b.pos_start AS pos_start_2,
            b.pos_end AS pos_end_2,
       CAST(
       CASE WHEN b.pos_start >= a.pos_end
            THEN
                abs(b.pos_start-a.pos_end)
        WHEN b.pos_end <= a.pos_start
            THEN
            abs(a.pos_start-b.pos_end)
            ELSE 0
       END AS BIGINT) AS distance

       FROM {} AS b, {} AS a
        WHERE  b.contig = a.contig
            AND cast(b.pos_end AS INT) >{} cast(a.pos_start AS INT )
            AND cast(b.pos_start AS INT) <{} cast(a.pos_end AS INT)
        "#,
        RIGHT_TABLE, LEFT_TABLE, sign, sign
    );
    ctx.sql(&query).await.unwrap()
}
async fn do_overlap(ctx: &SessionContext, filter: FilterOp) -> datafusion::dataframe::DataFrame {
    let sign = match filter {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    info!(
        "Running overlap: algorithm {} with {} thread(s)",
        ctx.state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.state().config().options().execution.target_partitions
    );
    let query = format!(
        r#"
            SELECT
                a.contig as contig_1,
                a.pos_start as pos_start_1,
                a.pos_end as pos_end_1,
                b.contig as contig_2,
                b.pos_start as pos_start_2,
                b.pos_end as pos_end_2
            FROM
                {} a, {} b
            WHERE
                a.contig=b.contig
            AND
                cast(a.pos_end AS INT) >{} cast(b.pos_start AS INT)
            AND
                cast(a.pos_start AS INT) <{} cast(b.pos_end AS INT)
        "#,
        LEFT_TABLE, RIGHT_TABLE, sign, sign,
    );
    ctx.sql(&query).await.unwrap()
}

#[pyfunction]
fn range_operation_frame(
    py_ctx: &PyBioSessionContext,
    df1: PyArrowType<ArrowArrayStreamReader>,
    df2: PyArrowType<ArrowArrayStreamReader>,
    range_options: RangeOptions,
) -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    register_frame(ctx, df1, LEFT_TABLE.to_string());
    register_frame(ctx, df2, RIGHT_TABLE.to_string());
    Ok(PyDataFrame::new(do_range_operation(
        ctx,
        &rt,
        range_options,
    )))
}

#[pyfunction]
fn range_operation_scan(
    py_ctx: &PyBioSessionContext,
    df_path1: String,
    df_path2: String,
    range_options: RangeOptions,
) -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    let s1_path = &df_path1;
    let s2_path = &df_path2;
    rt.block_on(register_table(
        ctx,
        s1_path,
        LEFT_TABLE,
        get_input_format(s1_path),
    ));
    rt.block_on(register_table(
        ctx,
        s2_path,
        RIGHT_TABLE,
        get_input_format(s2_path),
    ));
    Ok(PyDataFrame::new(do_range_operation(
        ctx,
        &rt,
        range_options,
    )))
}

fn do_range_operation(
    ctx: &SessionContext,
    rt: &Runtime,
    range_options: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    // defaults
    match range_options.overlap_alg {
        Some(alg) if alg == "coitreesnearest" => {
            panic!("CoitreesNearest is an internal algorithm for nearest operation. Can't be set explicitly.");
        },
        Some(alg) => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", &alg);
        },
        _ => {
            set_option_internal(
                ctx,
                "sequila.interval_join_algorithm",
                &Algorithm::Coitrees.to_string(),
            );
        },
    }
    match range_options.range_op {
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options.filter_op.unwrap())),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(ctx, range_options.filter_op.unwrap()))
        },
        _ => panic!("Unsupported operation"),
    }
}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(range_operation_frame, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_scan, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<FilterOp>()?;
    m.add_class::<RangeOp>()?;
    m.add_class::<RangeOptions>()?;
    Ok(())
}
