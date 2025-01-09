use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};

use crate::option::InputFormat;

pub(crate) fn register_frame(
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

pub(crate) fn get_input_format(path: &str) -> InputFormat {
    if path.ends_with(".parquet") {
        InputFormat::Parquet
    } else if path.ends_with(".csv") {
        InputFormat::Csv
    } else {
        panic!("Unsupported format")
    }
}

pub(crate) async fn register_table(
    ctx: &SessionContext,
    path: &str,
    table_name: &str,
    format: InputFormat,
) {
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
