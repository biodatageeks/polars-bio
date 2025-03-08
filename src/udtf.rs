use std::any::Any;
use std::cmp::{max, min};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{
    Array, GenericStringArray, Int32Array, Int64Array, RecordBatch, StringViewArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use coitrees::{COITree, Interval, IntervalTree};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::{Expr, SessionContext};
use fnv::FnvHashMap;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

use crate::option::FilterOp;

pub struct CountOverlapsProvider {
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    coverage: bool,
    schema: SchemaRef,
}

impl CountOverlapsProvider {
    pub fn new(
        session: Arc<SessionContext>,
        left_table: String,
        right_table: String,
        right_table_schema: Schema,
        columns_1: Vec<String>,
        columns_2: Vec<String>,
        filter_op: FilterOp,
        coverage: bool,
    ) -> Self {
        Self {
            session,
            left_table,
            right_table,
            schema: {
                let mut fields = right_table_schema.fields().to_vec();
                let name = if coverage { "coverage" } else { "count" };
                let new_field = Field::new(name, DataType::Int64, false);
                fields.push(FieldRef::new(new_field));
                let new_schema = Arc::new(Schema::new(fields).clone());
                SchemaRef::from(new_schema.clone())
            },
            columns_1: (
                columns_1[0].clone(),
                columns_1[1].clone(),
                columns_1[2].clone(),
            ),
            columns_2: (
                columns_2[0].clone(),
                columns_2[1].clone(),
                columns_2[2].clone(),
            ),
            filter_op,
            coverage,
        }
    }
}

impl Debug for CountOverlapsProvider {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[async_trait]
impl TableProvider for CountOverlapsProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_partitions = self
            .session
            .state()
            .config()
            .options()
            .execution
            .target_partitions;
        let left_table = self
            .session
            .table(self.left_table.clone())
            .await?
            .collect()
            .await?;
        let trees = Arc::new(build_coitree_from_batches(
            left_table,
            self.columns_1.clone(),
            self.coverage,
        ));
        Ok(Arc::new(CountOverlapsExec {
            schema: self.schema().clone(),
            session: Arc::clone(&self.session),
            trees,
            right_table: self.right_table.clone(),
            columns_1: self.columns_1.clone(),
            columns_2: self.columns_2.clone(),
            filter_op: self.filter_op.clone(),
            coverage: self.coverage.clone(),
            cache: PlanProperties::new(
                EquivalenceProperties::new(self.schema().clone()),
                Partitioning::UnknownPartitioning(target_partitions),
                ExecutionMode::Bounded,
            ),
        }))
    }
}

struct CountOverlapsExec {
    schema: SchemaRef,
    session: Arc<SessionContext>,
    trees: Arc<FnvHashMap<String, COITree<(), u32>>>,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    coverage: bool,
    cache: PlanProperties,
}

impl Debug for CountOverlapsExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for CountOverlapsExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        Ok(())
    }
}

impl CountOverlapsExec {}

impl ExecutionPlan for CountOverlapsExec {
    fn name(&self) -> &str {
        "CountOverlapsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
            self.trees.clone(),
            self.right_table.clone(),
            self.schema.clone(),
            self.columns_1.clone(),
            self.columns_2.clone(),
            self.filter_op.clone(),
            self.coverage.clone(),
            self.cache.partitioning.partition_count(),
            partition,
            context,
        );
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

type IntervalHashMap = FnvHashMap<String, Vec<Interval<()>>>;

fn merge_intervals(mut intervals: Vec<Interval<()>>) -> Vec<Interval<()>> {
    // Return early if there are no intervals.
    if intervals.is_empty() {
        return vec![];
    }

    // Sort intervals by their start time.
    intervals.sort_by(|a, b| a.first.cmp(&b.first));

    // Initialize merged intervals with the first interval.
    let mut merged = Vec::new();
    let mut current = intervals[0];

    // Iterate over the rest of the intervals.
    for interval in intervals.into_iter().skip(1) {
        if interval.first <= current.last {
            // Overlapping intervals; merge them by extending the current interval.
            current.last = current.last.max(interval.last);
        } else {
            // No overlap: push the current interval and update it.
            merged.push(current);
            current = interval;
        }
    }
    // Push the last interval.
    merged.push(current);

    merged
}

fn build_coitree_from_batches(
    batches: Vec<RecordBatch>,
    columns: (String, String, String),
    coverage: bool,
) -> FnvHashMap<String, COITree<(), u32>> {
    let mut nodes = IntervalHashMap::default();

    for batch in batches {
        let (contig_arr, start_arr, end_arr) = get_join_col_arrays(&batch, columns.clone());

        for i in 0..batch.num_rows() {
            let contig = contig_arr.value(i).to_string();
            let pos_start = start_arr.value(i) as i32;
            let pos_end = end_arr.value(i) as i32;
            let node_arr = if let Some(node_arr) = nodes.get_mut(&contig) {
                node_arr
            } else {
                nodes.entry(contig).or_insert(Vec::new())
            };
            node_arr.push(Interval::new(pos_start, pos_end, ()));
        }
    }
    let mut trees = FnvHashMap::<String, COITree<(), u32>>::default();
    for (seqname, seqname_nodes) in nodes {
        if !coverage {
            trees.insert(seqname, COITree::new(&seqname_nodes));
        } else {
            trees.insert(seqname, COITree::new(&merge_intervals(seqname_nodes)));
        }
    }
    trees
}

enum ContigArray<'a> {
    GenericString(&'a GenericStringArray<i64>),
    Utf8View(&'a StringViewArray),
    Utf8(&'a GenericStringArray<i32>),
}

impl ContigArray<'_> {
    fn value(&self, i: usize) -> &str {
        match self {
            ContigArray::GenericString(arr) => arr.value(i),
            ContigArray::Utf8View(arr) => arr.value(i),
            ContigArray::Utf8(arr) => arr.value(i),
        }
    }
}

enum PosArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl PosArray<'_> {
    fn value(&self, i: usize) -> i32 {
        match self {
            PosArray::Int32(arr) => arr.value(i),
            PosArray::Int64(arr) => arr.value(i) as i32,
        }
    }
}

fn get_join_col_arrays(
    batch: &RecordBatch,
    columns: (String, String, String),
) -> (ContigArray, PosArray, PosArray) {
    let contig_arr = match batch.column_by_name(&columns.0).unwrap().data_type() {
        DataType::LargeUtf8 => {
            let contig_arr = batch
                .column_by_name(&columns.0)
                .unwrap()
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .unwrap();
            ContigArray::GenericString(contig_arr)
        },
        DataType::Utf8View => {
            let contig_arr = batch
                .column_by_name(&columns.0)
                .unwrap()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            ContigArray::Utf8View(contig_arr)
        },
        DataType::Utf8 => {
            let contig_arr = batch
                .column_by_name(&columns.0)
                .unwrap()
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap();
            ContigArray::Utf8(contig_arr)
        },
        _ => todo!(),
    };

    let start_arr = match batch.column_by_name(&columns.1).unwrap().data_type() {
        DataType::Int32 => {
            let start_arr = batch
                .column_by_name(&columns.1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            PosArray::Int32(start_arr)
        },
        DataType::Int64 => {
            let start_arr = batch
                .column_by_name(&columns.1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            PosArray::Int64(start_arr)
        },
        _ => todo!(),
    };

    let end_arr = match batch.column_by_name(&columns.2).unwrap().data_type() {
        DataType::Int32 => {
            let end_arr = batch
                .column_by_name(&columns.2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            PosArray::Int32(end_arr)
        },
        DataType::Int64 => {
            let end_arr = batch
                .column_by_name(&columns.2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            PosArray::Int64(end_arr)
        },
        _ => todo!(),
    };

    (contig_arr, start_arr, end_arr)
}

fn get_coverage(tree: &COITree<(), u32>, start: i32, end: i32) -> i32 {
    let mut coverage = 0;
    tree.query(start, end, |node| {
        let overlap = max(1, min(end + 1, node.last) - max(start - 1, node.first));
        coverage += overlap;
    });
    coverage
}

async fn get_stream(
    session: Arc<SessionContext>,
    trees: Arc<FnvHashMap<String, COITree<(), u32>>>,
    right_table: String,
    new_schema: SchemaRef,
    _columns_1: (String, String, String),
    columns_2: (String, String, String),
    filter_op: FilterOp,
    coverage: bool,
    target_partitions: usize,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let right_table = session.table(right_table);
    let table_stream = right_table.await?;
    let plan = table_stream.create_physical_plan().await?;
    let repartition_stream =
        RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(target_partitions))?;

    let partition_stream = repartition_stream.execute(partition, context)?;
    let new_schema_out = new_schema.clone();

    let iter = partition_stream.map(move |rb| match rb {
        Ok(rb) => {
            let (contig, pos_start, pos_end) = get_join_col_arrays(&rb, columns_2.clone());
            let mut count_arr = Vec::with_capacity(rb.num_rows());
            let num_rows = rb.num_rows();
            for i in 0..num_rows {
                let contig = contig.value(i).to_string();
                let pos_start = pos_start.value(i);
                let pos_end = pos_end.value(i);
                let tree = trees.get(&contig);
                if tree.is_none() {
                    count_arr.push(0);
                    continue;
                }
                let count = match coverage {
                    true => {
                        if filter_op == FilterOp::Strict {
                            get_coverage(tree.unwrap(), pos_start + 1, pos_end - 1)
                        } else {
                            get_coverage(tree.unwrap(), pos_start, pos_end)
                        }
                    },
                    false => {
                        if filter_op == FilterOp::Strict {
                            tree.unwrap().query_count(pos_start + 1, pos_end - 1) as i32
                        } else {
                            tree.unwrap().query_count(pos_start, pos_end) as i32
                        }
                    },
                };
                count_arr.push(count as i64);
            }
            let count_arr = Arc::new(Int64Array::from(count_arr));
            let mut columns = rb.columns().to_vec();
            columns.push(count_arr);
            let new_rb = RecordBatch::try_new(new_schema.clone(), columns).unwrap();
            Ok(new_rb)
        },
        Err(e) => Err(e),
    });

    let adapted_stream =
        RecordBatchStreamAdapter::new(new_schema_out, Box::pin(iter) as BoxStream<_>);
    Ok(Box::pin(adapted_stream))
}
