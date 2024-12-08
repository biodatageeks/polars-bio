mod context;

use std::sync::Arc;
use std::time::Instant;
use datafusion::arrow::array::{ArrayData, RecordBatch};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ffi_stream::ArrowArrayStreamReader;
use datafusion::arrow::pyarrow::PyArrowType;

use datafusion::config::ConfigOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::{ParquetReadOptions, SessionConfig};
use datafusion_python::dataframe::PyDataFrame;
use datafusion_python::datafusion::prelude::SessionContext;
use polars_core::df;
use polars_core::error::PolarsResult;
use polars_core::export::arrow::array::TryExtend;

use polars_core::prelude::{Column, DataFrame};
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

async fn do_overlap(ctx: &SessionContext, df_1: Vec<RecordBatch>, df_2: Vec<RecordBatch>) -> datafusion::dataframe::DataFrame {


    let schema_1 = df_1[0].schema();
    let schema_2 = df_2[0].schema();

    let table_1 = MemTable::try_new(schema_1, vec![df_1]).unwrap();
    let table_2 = MemTable::try_new(schema_2, vec![df_2]).unwrap();
    ctx.register_table("s1", Arc::new(table_1)).unwrap();
    ctx.register_table("s2", Arc::new(table_2)).unwrap();
    const QUERY: &str = r#"
            SELECT
                *
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
fn overlap_internal(df1: PyArrowType<ArrowArrayStreamReader>, df2: PyArrowType<ArrowArrayStreamReader>) -> PyResult<PyDataFrame> {

    let start = Instant::now();
    let rt = Runtime::new().unwrap();
    let ctx = create_context(Algorithm::Coitrees);
    let batches_df1 = df1.0.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();
    let batches_df2 = df2.0.collect::<Result<Vec<RecordBatch>, ArrowError>>().unwrap();
    let df = rt.block_on(do_overlap(&ctx, batches_df1, batches_df2));
    let duration = start.elapsed(); // Calculate the elapsed time
    println!("Execution time: {:?}", duration);
    // df.clone().execute_stream();
    Ok(PyDataFrame::new(df))
}

#[pyfunction]
fn test_data_exchange() -> PyResult<PyDataFrame> {
    let rt = Runtime::new().unwrap();
    let ctx = create_context(Algorithm::Coitrees);
    // let s1_path = "/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/ex-anno//*.parquet";
    // let s2_path = "/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/ex-rna/*parquet";
    let s1_path = "/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/chainRn4/*parquet";
    let s2_path = "/Users/mwiewior/research/git/openstack-bdg-runners/ansible/roles/gha_runner/files/databio/chainOrnAna1/*parquet";
    rt.block_on(ctx.register_parquet("s1", s1_path.clone(), ParquetReadOptions::new())).expect("TODO: panic message");
    rt.block_on(ctx.register_parquet("s2", s2_path.clone(), ParquetReadOptions::new())).expect("TODO: panic message");
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
    let df = rt.block_on(ctx.sql(QUERY))?;
    Ok(PyDataFrame::new(df))

}

#[pymodule]
fn polars_bio(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(overlap_internal, m)?)?;
    m.add_function(wrap_pyfunction!(test_data_exchange, m)?)?;
    Ok(())
}

