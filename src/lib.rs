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
use datafusion_python::dataframe::PyDataFrame;
use log::debug;
use polars_lazy::prelude::{LazyFrame, ScanArgsAnonymous};
use polars_python::error::PyPolarsErr;
use polars_python::lazyframe::PyLazyFrame;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::context::PyBioSessionContext;
use crate::operation::do_range_operation;
use crate::option::{FilterOp, RangeOp, RangeOptions};
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
            ctx.state().config().options().execution.target_partitions
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

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(range_operation_frame, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(stream_range_operation_scan, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<FilterOp>()?;
    m.add_class::<RangeOp>()?;
    m.add_class::<RangeOptions>()?;
    Ok(())
}
