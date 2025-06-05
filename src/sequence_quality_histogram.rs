use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, StringArray};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{col, SessionContext};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
pub struct SequenceQualityHistogramProvider {
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    schema: SchemaRef,
}

impl SequenceQualityHistogramProvider {
    pub fn new(session: Arc<SessionContext>, table_name: String, column_name: String) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pos", DataType::UInt64, false),
            Field::new("score", DataType::UInt8, false),
            Field::new("count", DataType::UInt64, false),
        ]));
        Self {
            session,
            table_name,
            column_name,
            schema,
        }
    }
}

impl Debug for SequenceQualityHistogramProvider {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[async_trait]
impl TableProvider for SequenceQualityHistogramProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = self.session.state().config().target_partitions();
        Ok(Arc::new(SequenceQualityHistogramExec {
            schema: self.schema.clone(),
            session: self.session.clone(),
            table_name: self.table_name.clone(),
            column_name: self.column_name.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(target_partitions),
                ExecutionMode::Bounded,
            ),
        }))
    }
}

pub struct SequenceQualityHistogramExec {
    schema: SchemaRef,
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    properties: PlanProperties,
}

impl Debug for SequenceQualityHistogramExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for SequenceQualityHistogramExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl ExecutionPlan for SequenceQualityHistogramExec {
    fn name(&self) -> &str {
        "BaseSequenceQualityExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
            self.session.clone(),
            self.table_name.clone(),
            self.column_name.clone(),
            self.properties.partitioning.partition_count(),
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

async fn get_stream(
    session: Arc<SessionContext>,
    table_name: String,
    column_name: String,
    target_partitions: usize,
    partition: usize,
    context: Arc<TaskContext>,
    new_schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let df = session
        .table(table_name.clone())
        .await?
        .select(vec![col(&column_name)])?;

    let plan = df.create_physical_plan().await?;

    let repartition_stream =
        RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(target_partitions))?;

    let mut partition_stream = repartition_stream.execute(partition, context)?;

    let mut pos_map: HashMap<usize, Vec<u64>> = HashMap::new();

    while let Some(batch_result) = partition_stream.next().await {
        let batch = batch_result?;
        let col = batch.column(0); // tylko jedna kolumna

        let col = arrow::compute::cast(col, &DataType::Utf8)
            .map_err(|e| DataFusionError::Internal(format!("Cast error: {e}")))?;

        let col = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;

        for row in 0..col.len() {
            if col.is_null(row) {
                continue;
            }
            let s = col.value(row);
            for (pos, byte) in s.bytes().enumerate() {
                if let Some(score) = decode_score(byte as char) {
                    let entry = pos_map.entry(pos).or_insert_with(|| vec![0u64; 94]);
                    if (score as usize) < entry.len() {
                        entry[score as usize] += 1;
                    }
                }
            }
        }
    }

    let mut positions = Vec::new();
    let mut scores = Vec::new();
    let mut counts = Vec::new();

    for (pos, counts_vec) in pos_map {
        for (score, &count) in counts_vec.iter().enumerate() {
            if count > 0 {
                positions.push(pos as u64);
                scores.push(score as u8);
                counts.push(count as u64);
            }
        }
    }
    let pos_array = Arc::new(arrow_array::UInt64Array::from(positions));
    let score_array = Arc::new(arrow_array::UInt8Array::from(scores));
    let count_array = Arc::new(arrow_array::UInt64Array::from(counts));
    let new_batch = RecordBatch::try_new(
        new_schema.clone(),
        vec![pos_array, score_array, count_array],
    )
    .unwrap();

    let iter = futures::stream::once(async move { Ok(new_batch) });

    let adapted_stream =
        RecordBatchStreamAdapter::new(new_schema.clone(), Box::pin(iter) as BoxStream<_>);

    Ok(Box::pin(adapted_stream))
}
