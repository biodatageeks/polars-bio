use std::sync::{Arc, Mutex};
use std::thread;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::PyArrowType;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use futures_util::stream::StreamExt;
use polars::prelude::PolarsResult;
use polars_plan::prelude::{AnonymousScan, AnonymousScanArgs};
use polars_python::exceptions::PolarsError;
use pyo3::create_exception;
use tokio::runtime::Runtime;

use crate::option::InputFormat;
use crate::utils::{convert_arrow_rb_schema_to_polars_df_schema, convert_arrow_rb_to_polars_df};

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

pub struct RangeOperationScan {
    pub(crate) df_iter: Arc<Mutex<SendableRecordBatchStream>>,
}

impl AnonymousScan for RangeOperationScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<polars::prelude::DataFrame> {
        !todo!("Only streaming is supported")
    }

    fn next_batch(
        &self,
        scan_opts: AnonymousScanArgs,
    ) -> PolarsResult<Option<polars::prelude::DataFrame>> {
        let mutex = Arc::clone(&self.df_iter);
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let result = rt.block_on(mutex.lock().unwrap().next());
            match result {
                Some(batch) => {
                    let rb = batch.unwrap();
                    let schema_polars = convert_arrow_rb_schema_to_polars_df_schema(&rb.schema())?;
                    let df = convert_arrow_rb_to_polars_df(&rb, &schema_polars)?;
                    Ok(Some(df))
                },
                None => Ok(None),
            }
        })
        .join()
        .unwrap()

        // let rt = Runtime::new()?;
        // let a = &self.df_iter;
        // let stream = a.lock();
        // // let mut c = rt.block_on(stream);
        // let mut record_batch_stream = &mut *stream.unwrap();
        // let mut result = rt.block_on(record_batch_stream.next());
        //
        // match result {
        //     Some(batch) => {
        //         let rb = batch.unwrap();
        //         let schema_polars = convert_arrow_rb_schema_to_polars_df_schema(&rb.schema())?;
        //         let df = convert_arrow_rb_to_polars_df(&rb, &schema_polars)?;
        //
        //         Ok(Some(df))
        //     }
        //     None => Ok(None),
        // }
    }
    fn allows_projection_pushdown(&self) -> bool {
        false //FIXME: implement
    }
}
