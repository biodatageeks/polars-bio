mod context;
mod operation;
mod option;
mod query;
mod scan;
mod streaming;
mod utils;

use std::string::ToString;
use std::sync::{Arc, Mutex};

use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::prelude::CsvReadOptions;
use datafusion_python::dataframe::PyDataFrame;
use log::{debug, error, info};
use polars_lazy::prelude::{LazyFrame, ScanArgsAnonymous};
use polars_python::error::PyPolarsErr;
use polars_python::lazyframe::PyLazyFrame;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::context::PyBioSessionContext;
use crate::operation::do_range_operation;
use crate::option::{BioTable, FilterOp, InputFormat, RangeOp, RangeOptions};
use crate::scan::{get_input_format, register_frame, register_table};
use crate::streaming::RangeOperationScan;
use crate::utils::convert_arrow_rb_schema_to_polars_df_schema;

const LEFT_TABLE: &str = "s1";
const RIGHT_TABLE: &str = "s2";
const DEFAULT_COLUMN_NAMES: [&str; 3] = ["contig", "start", "end"];

#[pyfunction]
fn range_operation_frame(
    py_ctx: &PyBioSessionContext,
    df1: PyArrowType<ArrowArrayStreamReader>,
    df2: PyArrowType<ArrowArrayStreamReader>,
    range_options: RangeOptions,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    register_frame(py_ctx, df1, LEFT_TABLE.to_string());
    register_frame(py_ctx, df2, RIGHT_TABLE.to_string());
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
    #[allow(clippy::useless_conversion)]
    let rt = Runtime::new()?;
    let ctx = &py_ctx.ctx;
    rt.block_on(register_table(
        ctx,
        &df_path1,
        LEFT_TABLE,
        get_input_format(&df_path1),
    ));
    rt.block_on(register_table(
        ctx,
        &df_path2,
        RIGHT_TABLE,
        get_input_format(&df_path2),
    ));
    Ok(PyDataFrame::new(do_range_operation(
        ctx,
        &rt,
        range_options,
    )))
}

#[pyfunction]
fn stream_range_operation_scan(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    df_path1: String,
    df_path2: String,
    range_options: RangeOptions,
) -> PyResult<PyLazyFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;

        rt.block_on(register_table(
            ctx,
            &df_path1,
            LEFT_TABLE,
            get_input_format(&df_path1),
        ));
        rt.block_on(register_table(
            ctx,
            &df_path2,
            RIGHT_TABLE,
            get_input_format(&df_path2),
        ));

        let df = do_range_operation(ctx, &rt, range_options);
        let schema = df.schema().as_arrow();
        let polars_schema = convert_arrow_rb_schema_to_polars_df_schema(schema).unwrap();
        debug!("Schema: {:?}", polars_schema);
        let args = ScanArgsAnonymous {
            schema: Some(Arc::new(polars_schema)),
            name: "SCAN polars-bio",
            ..ScanArgsAnonymous::default()
        };
        debug!(
            "{}",
            ctx.session
                .state()
                .config()
                .options()
                .execution
                .target_partitions
        );
        let stream = rt.block_on(df.execute_stream()).unwrap();
        let scan = RangeOperationScan {
            df_iter: Arc::new(Mutex::new(stream)),
        };
        let function = Arc::new(scan);
        let lf = LazyFrame::anonymous_scan(function, args).map_err(PyPolarsErr::from)?;
        Ok(lf.into())
    })
}

#[pyfunction]
fn py_register_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    path: String,
    input_format: InputFormat,
) -> PyResult<Option<BioTable>> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        let table_name = path
            .to_lowercase()
            .split('/')
            .last()
            .unwrap()
            .to_string()
            .replace(&format!(".{}", input_format).to_string().to_lowercase(), "")
            .replace(".", "_");
        rt.block_on(register_table(
            ctx,
            &path,
            &table_name,
            input_format.clone(),
        ));
        match rt.block_on(ctx.session.table(&table_name)) {
            Ok(table) => {
                let schema = table.schema().as_arrow();
                info!("Table: {} registered for path: {}", table_name, path);
                let bio_table = BioTable {
                    name: table_name,
                    format: input_format,
                    path,
                };
                debug!("Schema: {:?}", schema);
                Ok(Some(bio_table))
            },
            Err(e) => {
                error!("{:?}", e);
                Ok(None)
            },
        }
    })
}

#[pyfunction]
fn py_scan_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    table_name: String,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        let df = rt
            .block_on(ctx.sql(&format!("SELECT * FROM {}", table_name)))
            .unwrap();
        Ok(PyDataFrame::new(df))
    })
}

//TODO: not exposed Polars used for now
#[pyfunction]
fn py_read_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    path: String,
) -> PyResult<PyDataFrame> {
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        let options = CsvReadOptions::default()
            .delimiter(b'\t')
            .file_extension("bed")
            .has_header(false);
        let df = rt.block_on(ctx.session.read_csv(&path, options))?;
        Ok(PyDataFrame::new(df))
    })
}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(range_operation_frame, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(stream_range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_table, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<FilterOp>()?;
    m.add_class::<RangeOp>()?;
    m.add_class::<RangeOptions>()?;
    m.add_class::<InputFormat>()?;
    Ok(())
}
