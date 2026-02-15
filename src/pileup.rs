use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_bio_format_bam::table_provider::BamTableProvider;
use datafusion_bio_format_cram::table_provider::CramTableProvider;
use datafusion_bio_function_pileup::physical_exec::PileupConfig;
use datafusion_bio_function_pileup::{DenseMode, PileupExec, ReadFilter};
use log::info;

use crate::option::PileupOptions;

/// Convert PileupOptions (PyO3) to PileupConfig (Rust).
pub fn pileup_options_to_config(opts: Option<PileupOptions>) -> PileupConfig {
    match opts {
        Some(opts) => {
            let dense_mode = match opts.dense_mode.to_lowercase().as_str() {
                "force" => DenseMode::Force,
                "disable" => DenseMode::Disable,
                _ => DenseMode::Auto,
            };
            PileupConfig {
                filter: ReadFilter {
                    filter_flag: opts.filter_flag,
                    min_mapping_quality: opts.min_mapping_quality,
                },
                dense_mode,
                binary_cigar: opts.binary_cigar,
                zero_based: opts.zero_based,
            }
        },
        None => PileupConfig::default(),
    }
}

/// A TableProvider that wraps an alignment provider (BAM/SAM/CRAM) with
/// PileupExec to compute per-base read depth.
pub struct DepthTableProvider {
    input: Arc<dyn TableProvider>,
    config: PileupConfig,
}

impl DepthTableProvider {
    pub fn new(input: Arc<dyn TableProvider>, config: PileupConfig) -> Self {
        Self { input, config }
    }
}

impl std::fmt::Debug for DepthTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DepthTableProvider")
            .field("config", &self.config)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DepthTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        datafusion_bio_function_pileup::schema::coverage_output_schema(self.config.zero_based)
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
        // Request only the columns needed for pileup: chrom, start, flags, cigar, mapping_quality
        // These correspond to indices in the BAM/CRAM provider schema
        let input_schema = self.input.schema();
        let needed_cols = ["chrom", "start", "flags", "cigar", "mapping_quality"];
        let projection: Vec<usize> = needed_cols
            .iter()
            .filter_map(|name| input_schema.index_of(name).ok())
            .collect();

        let input_plan = self.input.scan(state, Some(&projection), &[], None).await?;

        Ok(Arc::new(PileupExec::new(input_plan, self.config.clone())))
    }
}

/// A DataFusion table function that implements `SELECT * FROM depth('file.bam')`.
///
/// This is polars-bio's own implementation that reuses its existing BAM/SAM/CRAM
/// table providers rather than depending on the pileup crate's `bam` feature.
#[derive(Debug, Default)]
pub struct DepthFunction;

impl TableFunctionImpl for DepthFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        // Extract path from first argument (string literal)
        let path = match &args[0] {
            Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "depth() requires a string literal path as first argument".to_string(),
                ))
            },
        };

        // Optional second argument: zero_based (boolean), default false (1-based)
        let zero_based = if args.len() > 1 {
            match &args[1] {
                Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(b)), _) => *b,
                _ => false,
            }
        } else {
            false
        };

        info!(
            "depth() UDTF called with path: {}, zero_based: {}",
            path, zero_based
        );

        let path_lower = path.to_lowercase();

        // Use block_in_place + Handle::current() to avoid nested runtime panic
        // when called from within the SQL planning pipeline (which is already async).
        let provider: Arc<dyn TableProvider> = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            if path_lower.ends_with(".cram") {
                let provider = handle
                    .block_on(CramTableProvider::new(
                        path, None, // reference_path
                        None, // object_storage_options
                        zero_based, None, // tag_fields
                        true, // binary_cigar
                    ))
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to create CRAM provider: {}",
                            e
                        ))
                    })?;
                Ok::<_, datafusion::error::DataFusionError>(
                    Arc::new(provider) as Arc<dyn TableProvider>
                )
            } else {
                let provider = handle
                    .block_on(BamTableProvider::new(
                        path, None, // object_storage_options
                        zero_based, None, // tag_fields
                        true, // binary_cigar
                    ))
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to create BAM provider: {}",
                            e
                        ))
                    })?;
                Ok::<_, datafusion::error::DataFusionError>(
                    Arc::new(provider) as Arc<dyn TableProvider>
                )
            }
        })?;

        let config = PileupConfig {
            binary_cigar: true,
            zero_based,
            ..PileupConfig::default()
        };

        Ok(Arc::new(DepthTableProvider::new(provider, config)))
    }
}
