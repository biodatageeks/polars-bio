use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::catalog_common::TableReference;
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
use futures_util::{StreamExt, TryStreamExt};

use crate::{LEFT_TABLE, RIGHT_TABLE};

pub struct CountOverlapsFunction {
    session: Arc<SessionContext>,
    rt: tokio::runtime::Runtime,
}

impl CountOverlapsFunction {
    pub fn new(session: SessionContext) -> Self {
        Self {
            session: Arc::new(session),
            rt: tokio::runtime::Runtime::new().unwrap(),
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
            return plan_err!("First argument must be an table name");
        };
        let Some(Expr::Literal(ScalarValue::Utf8(Some(right_table)))) = exprs.get(1) else {
            return plan_err!("Second argument must be an table name");
        };
        let provider = CountOverlapsProvider {
            session: self.session.clone(),
            left_table: left_table.clone(),
            right_table: right_table.clone(),
        };

        Ok(Arc::new(provider))
    }
}

struct CountOverlapsProvider {
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
}

impl CountOverlapsProvider {
    pub fn new(
        session: Arc<SessionContext>,
        left_table: String,
        right_table: String,
        schema: SchemaRef,
    ) -> Self {
        Self {
            session,
            left_table,
            right_table,
        }
    }
}

impl Debug for CountOverlapsProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[async_trait]
impl TableProvider for CountOverlapsProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("pos_start", DataType::Int32, false),
            Field::new("pos_end", DataType::Int32, false),
        ]));
        SchemaRef::from(schema)
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CountOverlapsExec {
            schema: self.schema().clone(),
            session: self.session.clone(),
            left_table: self.left_table.clone(),
            right_table: self.right_table.clone(),
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
    cache: PlanProperties,
}

impl Debug for CountOverlapsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl DisplayAs for CountOverlapsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
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
            self.left_table.clone(),
            self.right_table.clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        // todo!()
    }
}

async fn get_stream(
    session: Arc<SessionContext>,
    left_table: String,
    right_table: String,
) -> Result<SendableRecordBatchStream> {
    let left_table = session.table(left_table).await?;
    let right_table = session.table(right_table).await?;
    let iter = std::iter::from_fn(move || Some(1));
    Ok(left_table.execute_stream().await?)
}
