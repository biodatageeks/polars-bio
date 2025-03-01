mod context;
mod operation;
mod option;
mod query;
mod scan;
mod streaming;
mod udtf;
mod utils;

use std::string::ToString;
use std::sync::{Arc, Mutex};

use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_python::dataframe::PyDataFrame;
use log::{debug, error, info};
use polars_lazy::prelude::{LazyFrame, ScanArgsAnonymous};
use polars_python::error::PyPolarsErr;
use polars_python::lazyframe::PyLazyFrame;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::context::PyBioSessionContext;
use crate::operation::do_range_operation;
use crate::option::{
    BioTable, FilterOp, InputFormat, RangeOp, RangeOptions, ReadOptions, VcfReadOptions,
};
use crate::scan::{maybe_register_table, register_frame, register_table};
use crate::streaming::RangeOperationScan;
use crate::utils::convert_arrow_rb_schema_to_polars_df_schema;

const LEFT_TABLE: &str = "s1";
const RIGHT_TABLE: &str = "s2";
const DEFAULT_COLUMN_NAMES: [&str; 3] = ["contig", "start", "end"];

#[pyfunction]
#[pyo3(signature = (py_ctx, df1, df2, range_options, limit=None))]
fn range_operation_frame(
    py_ctx: &PyBioSessionContext,
    df1: PyArrowType<ArrowArrayStreamReader>,
    df2: PyArrowType<ArrowArrayStreamReader>,
    range_options: RangeOptions,
    limit: Option<usize>,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    let rt = Runtime::new().unwrap();
    let ctx = &py_ctx.ctx;
    register_frame(py_ctx, df1, LEFT_TABLE.to_string());
    register_frame(py_ctx, df2, RIGHT_TABLE.to_string());
    match limit {
        Some(l) => Ok(PyDataFrame::new(
            do_range_operation(
                ctx,
                &rt,
                range_options,
                LEFT_TABLE.to_string(),
                RIGHT_TABLE.to_string(),
            )
            .limit(0, Some(l))?,
        )),
        _ => Ok(PyDataFrame::new(do_range_operation(
            ctx,
            &rt,
            range_options,
            LEFT_TABLE.to_string(),
            RIGHT_TABLE.to_string(),
        ))),
    }
}

#[pyfunction]
#[pyo3(signature = (py_ctx, df_path_or_table1, df_path_or_table2, range_options, read_options1=None, read_options2=None, limit=None))]
fn range_operation_scan(
    py_ctx: &PyBioSessionContext,
    df_path_or_table1: String,
    df_path_or_table2: String,
    range_options: RangeOptions,
    read_options1: Option<ReadOptions>,
    read_options2: Option<ReadOptions>,
    limit: Option<usize>,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
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
                .limit(0, Some(l))?,
        )),
        _ => Ok(PyDataFrame::new(do_range_operation(
            ctx,
            &rt,
            range_options,
            left_table,
            right_table,
        ))),
    }
}

#[pyfunction]
#[pyo3(signature = (py_ctx, df_path_or_table1, df_path_or_table2, range_options, read_options1=None, read_options2=None))]
fn stream_range_operation_scan(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    df_path_or_table1: String,
    df_path_or_table2: String,
    range_options: RangeOptions,
    read_options1: Option<ReadOptions>,
    read_options2: Option<ReadOptions>,
) -> PyResult<PyLazyFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        // check if the input has an extension

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

        let df = do_range_operation(ctx, &rt, range_options, left_table, right_table);
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
            rt: Runtime::new().unwrap(),
        };
        let function = Arc::new(scan);
        let lf = LazyFrame::anonymous_scan(function, args).map_err(PyPolarsErr::from)?;
        Ok(lf.into())
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
) -> PyResult<Option<BioTable>> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
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
#[pyo3(signature = (py_ctx, sql_text))]
fn py_read_sql(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    sql_text: String,
) -> PyResult<PyDataFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        let df = rt.block_on(ctx.sql(&sql_text)).unwrap();
        Ok(PyDataFrame::new(df))
    })
}

#[pyfunction]
#[pyo3(signature = (py_ctx, table_name))]
fn py_stream_scan_table(
    py: Python<'_>,
    py_ctx: &PyBioSessionContext,
    table_name: String,
) -> PyResult<PyLazyFrame> {
    #[allow(clippy::useless_conversion)]
    py.allow_threads(|| {
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;

        let df = rt.block_on(ctx.session.table(&table_name))?;
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
            rt: Runtime::new().unwrap(),
        };
        let function = Arc::new(scan);
        let lf = LazyFrame::anonymous_scan(function, args).map_err(PyPolarsErr::from)?;
        Ok(lf.into())
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
        let rt = Runtime::new().unwrap();
        let ctx = &py_ctx.ctx;
        let df = rt
            .block_on(ctx.sql(&format!("SELECT * FROM {}", table_name)))
            .unwrap();
        Ok(PyDataFrame::new(df))
    })
}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_function(wrap_pyfunction!(range_operation_frame, m)?)?;
    m.add_function(wrap_pyfunction!(range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(stream_range_operation_scan, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_register_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_table, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_sql, m)?)?;
    m.add_function(wrap_pyfunction!(py_stream_scan_table, m)?)?;
    // m.add_function(wrap_pyfunction!(unary_operation_scan, m)?)?;
    m.add_class::<PyBioSessionContext>()?;
    m.add_class::<FilterOp>()?;
    m.add_class::<RangeOp>()?;
    m.add_class::<RangeOptions>()?;
    m.add_class::<InputFormat>()?;
    m.add_class::<ReadOptions>()?;
    m.add_class::<VcfReadOptions>()?;
    Ok(())
}
