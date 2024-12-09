mod context;

use std::sync::Arc;
use std::time::Instant;
use datafusion::arrow::array::{ArrayData, RecordBatch};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;

use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::datafusion::prelude::SessionContext;
use polars_core::export::arrow::array::TryExtend;
use pyo3::prelude::*;

use sequila_core::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};

use tokio::runtime::Runtime;

fn create_context(algorithm: Algorithm) -> SessionContext {
    let mut options = ConfigOptions::new();
    let tuning_options = vec![
        ("datafusion.execution.target_partitions", "1"),
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
        .with_information_schema(true)
        .with_target_partitions(1);

    SessionContext::new_with_sequila(config)
}

fn register_frame(ctx: &SessionContext,df: PyArrowType<ArrowArrayStreamReader>, table_name: String) {
    let batches = df.0.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();
    let schema = batches[0].schema();
    let table = MemTable::try_new(schema, vec![batches]).unwrap();
    ctx.register_table(&table_name, Arc::new(table)).unwrap();
}

async fn do_overlap(ctx: &SessionContext) -> datafusion::dataframe::DataFrame {
    const QUERY: &str = r#"
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
                a.pos_end>=b.pos_start
            AND
                a.pos_start<=b.pos_end
        "#;
   ctx.sql(QUERY).await.unwrap()
}


#[pyfunction]
fn overlap_frame(df1: PyArrowType<ArrowArrayStreamReader>, df2: PyArrowType<ArrowArrayStreamReader>) -> PyResult<PyDataFrame> {

    let start = Instant::now();
    let rt = Runtime::new().unwrap();
    let ctx = create_context(Algorithm::Coitrees);
    register_frame(&ctx, df1, "s1".to_string());
    register_frame(&ctx, df2, "s2".to_string());
    let df = rt.block_on(do_overlap(&ctx));
    Ok(PyDataFrame::new(df))
}

#[pyfunction]
fn overlap_scan(df_path1: String, df_path2: String) -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = create_context(Algorithm::Coitrees);
    let s1_path = &df_path1;
    let s2_path = &df_path2;
    rt.block_on(ctx.register_parquet("s1", s1_path, ParquetReadOptions::new())).expect("TODO: panic message");
    rt.block_on(ctx.register_parquet("s2", s2_path, ParquetReadOptions::new())).expect("TODO: panic message");

    let df = rt.block_on(do_overlap(&ctx));
    Ok(PyDataFrame::new(df))

}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(overlap_frame, m)?)?;
    m.add_function(wrap_pyfunction!(overlap_scan, m)?)?;
    Ok(())
}

