use std::sync::{Arc, Mutex};

use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use polars::prelude::PolarsResult;
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};
use tokio::runtime::Runtime;

use crate::utils::{convert_arrow_rb_schema_to_polars_df_schema, convert_arrow_rb_to_polars_df};

pub struct RangeOperationScan {
    pub(crate) df_iter: Arc<Mutex<SendableRecordBatchStream>>,
    pub(crate) rt: Runtime,
}

impl AnonymousScan for RangeOperationScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[allow(unused)]
    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<polars::prelude::DataFrame> {
        unimplemented!()
    }
    #[allow(unused)]
    fn next_batch(
        &self,
        scan_opts: AnonymousScanArgs,
    ) -> PolarsResult<Option<polars::prelude::DataFrame>> {
        let mutex = Arc::clone(&self.df_iter);
        let result = self.rt.block_on(mutex.lock().unwrap().next());
        match result {
            Some(batch) => {
                let rb = batch.unwrap();
                let schema_polars = convert_arrow_rb_schema_to_polars_df_schema(&rb.schema())?;
                let df = convert_arrow_rb_to_polars_df(&rb, &schema_polars)?;
                Ok(Some(df))
            },
            None => Ok(None),
        }
    }
    fn allows_projection_pushdown(&self) -> bool {
        false //TODO: implement
    }
}
