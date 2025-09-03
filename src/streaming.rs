use std::sync::{Arc, Mutex};

use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use polars::prelude::{DataFrame, PolarsResult, SchemaRef};
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};
use tokio::runtime::Runtime;
use tracing::debug;

use crate::utils::convert_arrow_rb_to_polars_df;

pub struct RangeOperationScan {
    pub(crate) df_iter: Arc<Mutex<SendableRecordBatchStream>>,
    pub(crate) rt: Runtime,
    pub(crate) schema: SchemaRef,
}

impl AnonymousScan for RangeOperationScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        debug!("SCAN called - returning empty DataFrame for streaming compatibility");
        // Return empty DataFrame - when streaming is used, only next_batch should provide data
        // This ensures the stream isn't consumed during non-streaming operations
        Ok(DataFrame::empty())
    }

    fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
        debug!("NEXT_BATCH called - streaming mode");

        let mutex = Arc::clone(&self.df_iter);
        let result = self.rt.block_on(mutex.lock().unwrap().next());
        match result {
            Some(batch) => {
                let rb = batch.map_err(|e| {
                    polars::prelude::PolarsError::ComputeError(e.to_string().into())
                })?;
                let df = convert_arrow_rb_to_polars_df(&rb, self.schema.as_ref())?;
                eprintln!("Next batch returned {} rows", df.height());
                Ok(Some(df))
            },
            None => {
                eprintln!("No more batches");
                Ok(None)
            },
        }
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }
}
