use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, StringArray};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

pub struct BaseSequenceQualityProvider {
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    schema: SchemaRef,
}

impl BaseSequenceQualityProvider {
    pub fn new(session: Arc<SessionContext>, table_name: String, column_name: String) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pos", DataType::Int64, false),
            Field::new("score", DataType::Int8, false),
        ]));
        Self {
            session,
            table_name,
            column_name,
            schema,
        }
    }
}

impl Debug for BaseSequenceQualityProvider {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[async_trait]
impl TableProvider for BaseSequenceQualityProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = self.session.state().config().target_partitions();

        Ok(Arc::new(BaseSequenceQualityExec {
            schema: self.schema.clone(),
            session: Arc::clone(&self.session),
            table_name: self.table_name.clone(),
            column_name: self.column_name.clone(),
            cache: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(target_partitions),
                ExecutionMode::Bounded,
            ),
        }))
    }
}

pub struct BaseSequenceQualityExec {
    schema: SchemaRef,
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    cache: PlanProperties,
}

impl Debug for BaseSequenceQualityExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for BaseSequenceQualityExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for BaseSequenceQualityExec {
    fn name(&self) -> &str {
        "BaseSequenceQualityExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fut = get_stream(
            Arc::clone(&self.session),
            self.table_name.clone(),
            self.column_name.clone(),
            self.cache.partitioning.partition_count(),
            partition,
            context,
            self.schema.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn decode_score(c: char) -> Option<u8> {
    let ascii = c as u8;
    if ascii >= 33 {
        Some(ascii - 33)
    } else {
        None
    }
}

fn calc_stats(values: &mut Vec<u8>) -> (f64, f64, f64, f64, f64, f64) {
    values.sort_unstable();
    let n = values.len();
    let average = values.iter().map(|&v| v as f64).sum::<f64>() / n as f64;
    let median = if n % 2 == 0 {
        (values[n / 2 - 1] as f64 + values[n / 2] as f64) / 2.0
    } else {
        values[n / 2] as f64
    };
    let q1 = values[n / 4] as f64;
    let q3 = values[(3 * n) / 4] as f64;
    let iqr = q3 - q1;
    let lower = q1 - 1.5 * iqr;
    let upper = q3 + 1.5 * iqr;
    (average, median, q1, q3, lower, upper)
}

async fn get_stream(
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    target_partitions: usize,
    partition: usize,
    context: Arc<TaskContext>,
    new_schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let table_stream = session.table(table_name).await?;
    let plan = table_stream.create_physical_plan().await?;
    let repartition_stream =
        RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(target_partitions))?;
    let partition_stream = repartition_stream.execute(partition, context)?;
    let new_schema_out = new_schema.clone();
    let iter = partition_stream.map(move |batch| match batch {
        Ok(batch) => {
            let index = match batch.schema().index_of(&column_name) {
                Ok(idx) => idx,
                Err(_) => {
                    return Err(DataFusionError::Internal(format!(
                        "Column '{}' not found in schema",
                        column_name
                    )))
                },
            };
            let col = batch.column(index);

            // Try to cast to StringArray if possible
            let col = arrow::compute::cast(col, &DataType::Utf8)
                .map_err(|e| DataFusionError::Internal(format!("Cast error: {e}")))?;

            let col = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;

            let mut positions = Vec::new();
            let mut scores = Vec::new();

            for row in 0..col.len() {
                if col.is_null(row) {
                    continue;
                }
                let s = col.value(row);
                for (pos, byte) in s.bytes().enumerate() {
                    if let Some(score) = decode_score(byte as char) {
                        positions.push(pos as i64);
                        scores.push(score as i8);
                    }
                }
            }

            let pos_array = Arc::new(arrow_array::Int64Array::from(positions));
            let score_array = Arc::new(arrow_array::Int8Array::from(scores));
            let new_batch =
                RecordBatch::try_new(new_schema.clone(), vec![pos_array, score_array]).unwrap();

            Ok(new_batch)
        },
        Err(e) => Err(e),
    });

    let adapted_stream =
        RecordBatchStreamAdapter::new(new_schema_out, Box::pin(iter) as BoxStream<_>);
    Ok(Box::pin(adapted_stream))
}
