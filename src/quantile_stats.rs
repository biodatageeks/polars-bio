use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{Array, Float64Builder, UInt64Array, UInt64Builder, UInt8Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};

pub struct QuantileStatsTableProvider {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl QuantileStatsTableProvider {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pos", DataType::UInt64, false),
            Field::new("avg", DataType::Float64, true),
            Field::new("q1", DataType::Float64, true),
            Field::new("median", DataType::Float64, true),
            Field::new("q3", DataType::Float64, true),
            Field::new("lower", DataType::Float64, true),
            Field::new("upper", DataType::Float64, true),
        ]));
        Self {
            input,
            schema,
        }
    }
}

impl Debug for QuantileStatsTableProvider {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[async_trait]
impl TableProvider for QuantileStatsTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(QuantileStatsExec::new(self.input.clone())))
    }
}

#[derive(Debug)]
pub struct QuantileStatsExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl QuantileStatsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pos", DataType::UInt64, false),
            Field::new("avg", DataType::Float64, true),
            Field::new("q1", DataType::Float64, true),
            Field::new("median", DataType::Float64, true),
            Field::new("q3", DataType::Float64, true),
            Field::new("lower", DataType::Float64, true),
            Field::new("upper", DataType::Float64, true),
        ]));

        let schema_clone = schema.clone();

        Self {
            input,
            schema,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema_clone),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl ExecutionPlan for QuantileStatsExec {
    fn name(&self) -> &str {
        "QuantileAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.clone();
        let schema = self.schema.clone();

        let batches = futures::executor::block_on(collect(input, context.clone()))?;
        let combined = concat_batches(&self.input.schema(), &batches)?;
        let pos_array = combined
            .column_by_name("pos")
            .expect("Column 'pos' not found")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Expected UInt64 for pos");

        let score_array = combined
            .column_by_name("score")
            .expect("Column 'score' not found")
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("Expected UInt8 for score");

        let count_array = combined
            .column_by_name("count")
            .expect("Column 'count' not found")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Expected UInt64 for count");

        let mut groups: HashMap<u64, Vec<u64>> = HashMap::new();
        for i in 0..combined.num_rows() {
            if pos_array.is_valid(i) && score_array.is_valid(i) && count_array.is_valid(i) {
                let pos = pos_array.value(i);
                let score = score_array.value(i);
                let count = count_array.value(i);
                let entry = groups.entry(pos).or_insert_with(|| vec![0; 256]);
                entry[score as usize] += count;
            }
        }

        let mut pos_builder = UInt64Builder::with_capacity(groups.len());
        let mut avg_builder = Float64Builder::with_capacity(groups.len());
        let mut q1_builder = Float64Builder::with_capacity(groups.len());
        let mut median_builder = Float64Builder::with_capacity(groups.len());
        let mut q3_builder = Float64Builder::with_capacity(groups.len());
        let mut lower_builder = Float64Builder::with_capacity(groups.len());
        let mut upper_builder = Float64Builder::with_capacity(groups.len());

        for (pos, hist) in groups {
            if let Some((average, q1, median, q3, lower, upper)) = calculate_histogram_stats(&hist)
            {
                pos_builder.append_value(pos);
                avg_builder.append_value(average);
                q1_builder.append_value(q1);
                median_builder.append_value(median);
                q3_builder.append_value(q3);
                lower_builder.append_value(lower);
                upper_builder.append_value(upper);
            }
        }

        let result_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(pos_builder.finish()),
                Arc::new(avg_builder.finish()),
                Arc::new(q1_builder.finish()),
                Arc::new(median_builder.finish()),
                Arc::new(q3_builder.finish()),
                Arc::new(lower_builder.finish()),
                Arc::new(upper_builder.finish()),
            ],
        )?;
        let mem_stream = MemoryStream::try_new(vec![result_batch], schema, None)?;
        Ok(Box::pin(mem_stream))
    }
}

impl DisplayAs for QuantileStatsExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

fn calculate_histogram_stats(hist: &[u64]) -> Option<(f64, f64, f64, f64, f64, f64)> {
    let total_count: u64 = hist.iter().sum();
    if total_count == 0 {
        return None;
    }

    let weighted_sum: u64 = hist
        .iter()
        .enumerate()
        .map(|(score, &count)| score as u64 * count)
        .sum();
    let average = weighted_sum as f64 / total_count as f64;

    fn quantile(hist: &[u64], quantile: f64, total: u64) -> f64 {
        let target = quantile * (total - 1) as f64;
        let target_ = target.floor();
        let delta = target - target_;
        let n = target_ as u64 + 1;
        let mut lo = None;
        let mut acc = 0u64;
        for (hi, &count) in hist.iter().enumerate().filter(|(_, &count)| count > 0) {
            if acc == n && lo.is_some() {
                let lo = lo.unwrap() as f64;
                return  (lo + (hi as f64 - lo) * delta) as f64;
            } else if acc + count > n {
                return  hi as f64;
            }
            acc += count;
            lo = Some(hi);
        }

        hist.iter().enumerate().fold(
            0_usize,
            |acc, (value, &count)| if count > 0 { value } else { acc },
        ) as f64
    }

    let q1 = quantile(hist, 0.25, total_count);
    let median = quantile(hist, 0.5, total_count);
    let q3 = quantile(hist, 0.75, total_count);
    let iqr = q3 - q1;
    let lower = q1 - 1.5 * iqr;
    let upper = q3 + 1.5 * iqr;

    Some((average, q1, median, q3, lower, upper))
}
