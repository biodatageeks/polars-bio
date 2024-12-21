use std::fmt;
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
    #[pyo3(get, set)]
    range_op: RangeOp,
    #[pyo3(get, set)]
    filter_op: Option<FilterOp>,
    #[pyo3(get, set)]
    suffixes: Option<(String, String)>,
    #[pyo3(get, set)]
    columns_1: Option<Vec<String>>,
    #[pyo3(get, set)]
    columns_2: Option<Vec<String>>,
    #[pyo3(get, set)]
    on_cols: Option<Vec<String>>,
    #[pyo3(get, set)]
    overlap_alg: Option<String>,
}

#[pymethods]
impl RangeOptions {
    #[new]
    #[pyo3(signature = (range_op, filter_op=None, suffixes=None, columns_1=None, columns_2=None, on_cols=None, overlap_alg=None))]
    pub fn new(
        range_op: RangeOp,
        filter_op: Option<FilterOp>,
        suffixes: Option<(String, String)>,
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
    Coverage = 4,
}

impl fmt::Display for RangeOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeOp::Overlap => write!(f, "Overlap"),
            RangeOp::Nearest => write!(f, "Nearest"),
            RangeOp::Complement => write!(f, "Complement"),
            RangeOp::Cluster => write!(f, "Cluster"),
            RangeOp::Coverage => write!(f, "Coverage"),
        }
    }
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

async fn do_nearest(
    ctx: &SessionContext,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    let sign = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let columns_1 = match range_opts.columns_1 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };

    let query = format!(
        r#"
        SELECT
            a.{} AS {}{}, -- contig
            a.{} AS {}{}, -- pos_start
            a.{} AS {}{}, -- pos_end
            b.{} AS {}{}, -- contig
            b.{} AS {}{}, -- pos_start
            b.{} AS {}{},  -- pos_end
            a.* except({}, {}, {}), -- all join columns from left table
            b.* except({}, {}, {}), -- all join columns from right table
       CAST(
       CASE WHEN b.{} >= a.{}
            THEN
                abs(b.{}-a.{})
        WHEN b.{} <= a.{}
            THEN
            abs(a.{}-b.{})
            ELSE 0
       END AS BIGINT) AS distance

       FROM {} AS b, {} AS a
        WHERE  b.{} = a.{}
            AND cast(b.{} AS INT) >{} cast(a.{} AS INT )
            AND cast(b.{} AS INT) <{} cast(a.{} AS INT)
        "#,
        columns_1[0],
        columns_1[0],
        suffixes.0, // contig
        columns_1[1],
        columns_1[1],
        suffixes.0, // pos_start
        columns_1[2],
        columns_1[2],
        suffixes.0, // pos_end
        columns_2[0],
        columns_2[0],
        suffixes.1, // contig
        columns_2[1],
        columns_2[1],
        suffixes.1, // pos_start
        columns_2[2],
        columns_2[2],
        suffixes.1, // pos_end
        columns_1[0],
        columns_1[1],
        columns_1[2], // all join columns from right table
        columns_2[0],
        columns_2[1],
        columns_2[2], // all join columns from left table
        columns_2[1],
        columns_1[2], //  b.pos_start >= a.pos_end
        columns_2[1],
        columns_1[2], // b.pos_start-a.pos_end
        columns_2[2],
        columns_1[1], // b.pos_end <= a.pos_start
        columns_2[2],
        columns_1[1], // a.pos_start-b.pos_end
        RIGHT_TABLE,
        LEFT_TABLE,
        columns_1[0],
        columns_2[0], // contig
        columns_1[2],
        sign,
        columns_2[1], // pos_start
        columns_1[1],
        sign,
        columns_2[2], // pos_end
    );
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &SessionContext,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    let sign = match range_opts.clone().filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let columns_1 = match range_opts.columns_1 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let query = format!(
        r#"
            SELECT
                a.{} as {}{}, -- contig
                a.{} as {}{}, -- pos_start
                a.{} as {}{}, -- pos_end
                b.{} as {}{}, -- contig
                b.{} as {}{}, -- pos_start
                b.{} as {}{}, -- pos_end
                a.* except({}, {}, {}), -- all join columns from left table
                b.* except({}, {}, {}) -- all join columns from right table
            FROM
                {} a, {} b
            WHERE
                a.{}=b.{}
            AND
                cast(a.{} AS INT) >{} cast(b.{} AS INT)
            AND
                cast(a.{} AS INT) <{} cast(b.{} AS INT)
        "#,
        columns_1[0],
        columns_1[0],
        suffixes.0, // contig
        columns_1[1],
        columns_1[1],
        suffixes.0, // pos_start
        columns_1[2],
        columns_1[2],
        suffixes.0, // pos_end
        columns_2[0],
        columns_2[0],
        suffixes.1, // contig
        columns_2[1],
        columns_2[1],
        suffixes.1, // pos_start
        columns_2[2],
        columns_2[2],
        suffixes.1, // pos_end
        columns_1[0],
        columns_1[1],
        columns_1[2], // all join columns from right table
        columns_2[0],
        columns_2[1],
        columns_2[2], // all join columns from left table
        LEFT_TABLE,
        RIGHT_TABLE,
        columns_1[0],
        columns_2[0], // contig
        columns_1[2],
        sign,
        columns_2[1], // pos_start
        columns_1[1],
        sign,
        columns_2[2], // pos_end
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
    match &range_options.overlap_alg {
        Some(alg) if alg == "coitreesnearest" => {
            panic!("CoitreesNearest is an internal algorithm for nearest operation. Can't be set explicitly.");
        },
        Some(alg) => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", alg);
        },
        _ => {
            set_option_internal(
                ctx,
                "sequila.interval_join_algorithm",
                &Algorithm::Coitrees.to_string(),
            );
        },
    }
    info!(
        "Running {} operation with algorithm {} and {} thread(s)...",
        range_options.range_op,
        ctx.state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.state().config().options().execution.target_partitions
    );
    match range_options.range_op {
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options)),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(ctx, range_options))
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
