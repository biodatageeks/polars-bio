mod context;
mod operation;
mod option;
mod scan;
mod utils;

use std::string::ToString;

use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_python::dataframe::PyDataFrame;
// use polars::prelude::LazyFrame;
// use polars_lazy::prelude::LazyFrame;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::context::PyBioSessionContext;
use crate::operation::do_range_operation;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::scan::{get_input_format, register_frame, register_table};

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

// #[pyfunction]
// fn lazy_range_operation_scan(
//     py_ctx: &PyBioSessionContext,
//     df_path1: String,
//     df_path2: String,
//     range_options: RangeOptions,
// ) -> PyResult<LazyFrame> {
//     let rt = Runtime::new().unwrap();
//     let ctx = &py_ctx.ctx;
//     let s1_path = &df_path1;
//     let s2_path = &df_path2;
//     rt.block_on(register_table(
//         ctx,
//         s1_path,
//         LEFT_TABLE,
//         get_input_format(s1_path),
//     ));
//     rt.block_on(register_table(
//         ctx,
//         s2_path,
//         RIGHT_TABLE,
//         get_input_format(s2_path),
//     ));
//     Ok(LazyFrame::anonymous_scan(do_range_operation(
//         ctx,
//         &rt,
//         range_options,
//     )))
// }

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
