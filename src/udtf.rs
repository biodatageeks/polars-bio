use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{GenericStringArray, Int32Array, StringArray};
use datafusion::arrow::array::Int64Array;
use datafusion::catalog::TableProvider;
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::Expr;
use polars_arrow::array::Utf8Array;

#[derive(Debug)]
pub struct CountOverlapsFunction {}

impl TableFunctionImpl for CountOverlapsFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Int64(Some(value)))) = exprs.get(0) else {
            return plan_err!("First argument must be an table name");
        };

        // Create the schema for the table
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        // Create a single RecordBatch with the value as a single column
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![*value]))],
        )?;
        // Create a MemTable plan that returns the RecordBatch
        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        Ok(Arc::new(provider))
    }
}
