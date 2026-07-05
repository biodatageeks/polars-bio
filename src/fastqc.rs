use std::any::Any;
use std::sync::Arc;

use arrow_array::Array;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use datafusion_bio_function_fastqc::{tidy_schema, FastqcExec, ModuleSet};
use log::info;

/// A TableProvider that wraps polars-bio's FASTQ provider with FastqcExec to
/// compute FastQC quality-control modules as a tidy Arrow stream.
pub struct FastqcTableProvider {
    input: Arc<dyn TableProvider>,
    selection: Option<Vec<String>>,
}

impl FastqcTableProvider {
    pub fn new(input: Arc<dyn TableProvider>, selection: Option<Vec<String>>) -> Self {
        Self { input, selection }
    }
}

impl std::fmt::Debug for FastqcTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastqcTableProvider")
            .field("selection", &self.selection)
            .finish()
    }
}

#[async_trait]
impl TableProvider for FastqcTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        tidy_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // sequence + quality_scores feed all modules; name/description feed
        // header-aware modules (per_tile_quality). filter_map below drops any
        // the input schema lacks.
        let input_schema = self.input.schema();
        let needed = ["sequence", "quality_scores", "name", "description"];
        let projection: Vec<usize> = needed
            .iter()
            .filter_map(|n| input_schema.index_of(n).ok())
            .collect();
        let input_plan = self.input.scan(state, Some(&projection), &[], None).await?;
        Ok(Arc::new(FastqcExec::new(
            input_plan,
            self.selection.clone(),
        )))
    }
}

/// `SELECT * FROM fastqc('reads.fastq'[, ['per_base_quality', ...]])`.
#[derive(Debug, Default)]
pub struct FastqcFunction;

impl TableFunctionImpl for FastqcFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            _ => {
                return Err(DataFusionError::Plan(
                    "fastqc() requires a string literal path as first argument".to_string(),
                ))
            },
        };

        // Optional second arg: a list of module names.
        let selection: Option<Vec<String>> = match args.get(1) {
            None => None,
            Some(Expr::Literal(ScalarValue::List(arr), _)) => {
                let values = arr.values();
                let strs = values
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("fastqc() modules list must be strings".to_string())
                    })?;
                let mut out = Vec::new();
                for i in 0..strs.len() {
                    if !strs.is_null(i) {
                        out.push(strs.value(i).to_string());
                    }
                }
                Some(out)
            },
            Some(_) => {
                return Err(DataFusionError::Plan(
                    "fastqc() second argument must be a list of module names".to_string(),
                ))
            },
        };

        info!("fastqc() UDTF: path={path}, modules={selection:?}");

        // Validate selection early (surfaces bad module names at plan time).
        ModuleSet::build(selection.as_deref())?;

        // FastqTableProvider::new is synchronous (see py_register_fastqc_table in
        // lib.rs, which calls it the same way), so no runtime bridging is needed.
        let provider: Arc<dyn TableProvider> = FastqTableProvider::new(path.clone(), None)
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>)
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create FASTQ provider: {e}"))
            })?;

        Ok(Arc::new(FastqcTableProvider::new(provider, selection)))
    }
}
