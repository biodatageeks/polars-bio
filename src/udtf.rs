use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use coitrees::{COITree, Interval, IntervalTree};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::{Expr, SessionContext};
use fnv::FnvHashMap;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

pub struct CountOverlapsFunction {
    session: Arc<SessionContext>,
}

impl CountOverlapsFunction {
    pub fn new(session: SessionContext) -> Self {
        Self {
            session: Arc::new(session),
        }
    }
}

impl Debug for CountOverlapsFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountOverlapsFunction")
            .field("session", &"<SessionContext>")
            .finish()
    }
}

impl TableFunctionImpl for CountOverlapsFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(left_table)))) = exprs.get(0) else {
            return plan_err!("1. argument must be an table name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(right_table)))) = exprs.get(1) else {
            return plan_err!("2. argument must be an table name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(contig_col_1)))) = exprs.get(2) else {
            return plan_err!("3. argument must be an a column name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(start_col_1)))) = exprs.get(3) else {
            return plan_err!("4. argument must be an a column name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(end_col_1)))) = exprs.get(4) else {
            return plan_err!("5. argument must be an a column name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(contig_col_2)))) = exprs.get(5) else {
            return plan_err!("6. argument must be an a column name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(start_col_2)))) = exprs.get(6) else {
            return plan_err!("7. argument must be an a column name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(end_col_2)))) = exprs.get(7) else {
            return plan_err!("8. argument must be an a column name");
        };

        let provider = CountOverlapsProvider {
            session: self.session.clone(),
            left_table: left_table.clone(),
            right_table: right_table.clone(),
            columns_1: (contig_col_1.clone(), start_col_1.clone(), end_col_1.clone()),
            columns_2: (contig_col_2.clone(), start_col_2.clone(), end_col_2.clone()),
        };

        Ok(Arc::new(provider))
    }
}

struct CountOverlapsProvider {
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
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
        SchemaRef::from(Schema::empty())
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
        Ok(Arc::new(CountOverlapsExec {
            schema: self.schema().clone(),
            session: self.session.clone(),
            left_table: self.left_table.clone(),
            right_table: self.right_table.clone(),
            columns_1: self.columns_1.clone(),
            columns_2: self.columns_2.clone(),
            cache: PlanProperties::new(
                EquivalenceProperties::new(self.schema().clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }))
    }
}

struct CountOverlapsExec {
    schema: SchemaRef,
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fut = get_stream(
            self.session.clone(),
            self.left_table.clone(),
            self.right_table.clone(),
            self.columns_1.clone(),
            self.columns_2.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

type IntervalHashMap = FnvHashMap<String, Vec<Interval<()>>>;

fn build_coitree_from_batches(
    batches: Vec<RecordBatch>,
    columns: (String, String, String),
) -> FnvHashMap<String, COITree<(), u32>> {
    let mut nodes = IntervalHashMap::default();

    for batch in batches {
        let (contig_arr, start_arr, end_arr) = get_join_col_arrays(&batch, columns.clone());

        for i in 0..batch.num_rows() {
            let contig = contig_arr.value(i).to_string();
            let pos_start = start_arr.value(i);
            let pos_end = end_arr.value(i);
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
        trees.insert(seqname, COITree::new(&seqname_nodes));
    }
    trees
}

fn get_join_col_arrays(
    batch: &RecordBatch,
    columns: (String, String, String),
) -> (&StringViewArray, &Int32Array, &Int32Array) {
    let contig_arr = batch
        .column_by_name(&columns.0)
        .unwrap()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    let start_arr = batch
        .column_by_name(&columns.1)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let end_arr = batch
        .column_by_name(&columns.2)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (contig_arr, start_arr, end_arr)
}

async fn get_stream(
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
    columns_1: (String, String, String),
    columns_2: (String, String, String),
) -> Result<SendableRecordBatchStream> {
    let left_table = session.table(left_table).await?.collect().await?;
    let trees = build_coitree_from_batches(left_table, columns_1.clone());
    let right_table = session.table(right_table);
    let stream = right_table.await?.execute_stream().await?;
    let mut fields = stream.schema().fields().to_vec();
    let new_field = Field::new("count", DataType::Int64, false);
    fields.push(FieldRef::new(new_field));
    let new_schema = Arc::new(Schema::new(fields).clone());
    let new_schema_out = SchemaRef::from(new_schema.clone());

    let iter = stream.map(move |rb| match rb {
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
                let count = tree.unwrap().query_count(pos_start + 1, pos_end - 1);
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
    // Wrap it in the adapter to get a SendableRecordBatchStream.
}
