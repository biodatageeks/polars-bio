use std::sync::Arc;

use datafusion::catalog_common::TableReference;
use exon::ExonSession;
use log::{debug, info};
use sequila_core::session_context::{Algorithm, SequilaConfig};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::query::{count_overlaps_query, nearest_query, overlap_query};
use crate::udtf::CountOverlapsProvider;
use crate::utils::default_cols_to_string;
use crate::DEFAULT_COLUMN_NAMES;

use datafusion::dataframe::DataFrame;

use arrow_array::{Array, ArrayRef, Float64Array, StringArray};
use arrow_array::builder::ListBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::ArrayBuilder;
use arrow::array::ListArray;
use arrow_schema::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Signature, Volatility, ScalarUDF};
use datafusion::logical_expr::ColumnarValue;
use std::any::Any;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::logical_expr::AggregateUDFImpl;
use arrow::array::Int64Array;
use std::collections::HashMap;
use arrow::record_batch::RecordBatch;
use crate::MemTable;
use arrow::datatypes::Schema;

pub(crate) struct QueryParams {
    pub sign: String,
    pub suffixes: (String, String),
    pub columns_1: Vec<String>,
    pub columns_2: Vec<String>,
    pub other_columns_1: Vec<String>,
    pub other_columns_2: Vec<String>,
    pub left_table: String,
    pub right_table: String,
}
pub(crate) fn do_range_operation(
    ctx: &ExonSession,
    rt: &Runtime,
    range_options: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    // defaults
    match &range_options.overlap_alg {
        Some(alg) if alg == "coitreesnearest" => {
            panic!("CoitreesNearest is an internal algorithm for nearest operation. Can't be set explicitly.");
        },
        Some(alg) => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", alg);
        },
        _ => {
            set_option_internal(
                ctx,
                "sequila.interval_join_algorithm",
                &Algorithm::Coitrees.to_string(),
            );
        },
    }
    let streaming = range_options.streaming.unwrap_or(false);
    if streaming {
        info!("Running in streaming mode...");
    }
    info!(
        "Running {} operation with algorithm {} and {} thread(s)...",
        range_options.range_op,
        ctx.session
            .state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );
    match range_options.range_op {
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options, left_table, right_table)),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(ctx, range_options, left_table, right_table))
        },
        RangeOp::CountOverlaps => rt.block_on(do_count_overlaps(
            ctx,
            range_options,
            left_table,
            right_table,
        )),
        RangeOp::CountOverlapsNaive => rt.block_on(do_count_overlaps_coverage_naive(
            ctx,
            range_options,
            left_table,
            right_table,
            false,
        )),
        RangeOp::Coverage => rt.block_on(do_count_overlaps_coverage_naive(
            ctx,
            range_options,
            left_table,
            right_table,
            true,
        )),

        _ => panic!("Unsupported operation"),
    }
}

async fn do_nearest(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(nearest_query, range_opts, ctx, left_table, right_table)
        .await
        .to_string();
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(overlap_query, range_opts, ctx, left_table, right_table)
        .await
        .to_string();
    debug!("Query: {}", query);
    debug!(
        "{}",
        ctx.session
            .state()
            .config()
            .options()
            .execution
            .target_partitions
    );
    ctx.sql(&query).await.unwrap()
}

async fn do_count_overlaps(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(
        count_overlaps_query,
        range_opts,
        ctx,
        left_table,
        right_table,
    )
    .await
    .to_string();
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_count_overlaps_coverage_naive(
    ctx: &ExonSession,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
    coverage: bool,
) -> datafusion::dataframe::DataFrame {
    let columns_1 = range_opts.columns_1.unwrap();
    let columns_2 = range_opts.columns_2.unwrap();
    let session = &ctx.session;
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = session
        .table(right_table_ref.clone())
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let count_overlaps_provider = CountOverlapsProvider::new(
        Arc::new(session.clone()),
        left_table,
        right_table,
        right_schema,
        columns_1,
        columns_2,
        range_opts.filter_op.unwrap(),
        coverage,
    );
    let table_name = "count_overlaps_coverage".to_string();
    session.deregister_table(table_name.clone()).unwrap();
    session
        .register_table(table_name.clone(), Arc::new(count_overlaps_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn get_non_join_columns(
    table_name: String,
    join_columns: Vec<String>,
    ctx: &ExonSession,
) -> Vec<String> {
    let table_ref = TableReference::from(table_name);
    let table = ctx.session.table(table_ref).await.unwrap();
    table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .filter(|f| !join_columns.contains(f))
        .collect::<Vec<String>>()
}

pub(crate) fn format_non_join_tables(
    columns: Vec<String>,
    table_alias: String,
    suffix: String,
) -> String {
    if columns.is_empty() {
        return "".to_string();
    }
    columns
        .iter()
        .map(|c| format!("{}.{} as {}{}", table_alias, c, c, suffix))
        .collect::<Vec<String>>()
        .join(", ")
}
//==============
fn decode_phred_scores(quality_string: &str, offset: i32) -> Vec<f64> {
    quality_string
        .chars()
        .map(|c| (c as i32 - offset) as f64)
        .collect()
}

fn calculate_quantile(mut scores: Vec<f64>, quantile: f64) -> f64 {
    if scores.is_empty() {
        return 0.0;
    }
    
    scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let len = scores.len();
    
    if quantile <= 0.0 {
        return scores[0];
    }
    if quantile >= 1.0 {
        return scores[len - 1];
    }
    
    let index = (len as f64 - 1.0) * quantile;
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    
    if lower == upper {
        scores[lower]
    } else {
        let weight = index - lower as f64;
        scores[lower] * (1.0 - weight) + scores[upper] * weight
    }
}

#[derive(Debug)]
struct DecodePhredUDF {
    signature: Signature,
    return_type: DataType,
}

impl DecodePhredUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
            return_type: DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        }
    }
}

impl ScalarUDFImpl for DecodePhredUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "decode_phred"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let quality_strings = match &args[0] {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected string array".to_string()))?
            },
            _ => return Err(DataFusionError::Internal("Expected string array".to_string())),
        };
        
        let offset = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(val))) => *val,
            _ => 33,
        };

        let mut list_builder = ListBuilder::new(Float64Builder::new());
        
        for i in 0..quality_strings.len() {
            if quality_strings.is_null(i) {
                list_builder.append_null();
            } else {
                let quality_str = quality_strings.value(i);
                let scores = decode_phred_scores(quality_str, offset);
                
                for score in scores {
                    list_builder.values().append_value(score);
                }
                list_builder.append(true);
            }
        }
        
        Ok(ColumnarValue::Array(Arc::new(list_builder.finish())))
    }
}

#[derive(Debug)]
struct QuantileUDF {
    signature: Signature,
    return_type: DataType,
}

impl QuantileUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
            return_type: DataType::Float64,
        }
    }
}

impl ScalarUDFImpl for QuantileUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "quantile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let arrays = match &args[0] {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected list array".to_string()))?
            },
            _ => return Err(DataFusionError::Internal("Expected list array".to_string())),
        };
        
        let quantile_value = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) => *val,
            _ => return Err(DataFusionError::Internal("Expected float64 quantile".to_string())),
        };

        let mut result_builder = Float64Builder::new();
        
        for i in 0..arrays.len() {
            if arrays.is_null(i) {
                result_builder.append_null();
            } else {
                let scores_array = arrays.value(i);
                if let Some(float_array) = scores_array.as_any().downcast_ref::<Float64Array>() {
                    let scores: Vec<f64> = float_array.iter()
                        .filter_map(|v| v)
                        .collect();
                    
                    let quantile_result = calculate_quantile(scores, quantile_value);
                    result_builder.append_value(quantile_result);
                } else {
                    result_builder.append_null();
                }
            }
        }
        
        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

#[derive(Debug)]
struct ArrayLengthUDF {
    signature: Signature,
    return_type: DataType,
}

impl ArrayLengthUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))],
                Volatility::Immutable,
            ),
            return_type: DataType::Int64,
        }
    }
}

impl ScalarUDFImpl for ArrayLengthUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let arrays = match &args[0] {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected list array".to_string()))?
            },
            _ => return Err(DataFusionError::Internal("Expected list array".to_string())),
        };

        let mut result_builder = Int64Builder::new();
        
        for i in 0..arrays.len() {
            if arrays.is_null(i) {
                result_builder.append_null();
            } else {
                let scores_array = arrays.value(i);
                result_builder.append_value(scores_array.len() as i64);
            }
        }
        
        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

#[derive(Debug)]
struct ArrayElementUDF {
    signature: Signature,
    return_type: DataType,
}

impl ArrayElementUDF {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
            return_type: DataType::Float64,
        }
    }
}

impl ScalarUDFImpl for ArrayElementUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_element"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let arrays = match &args[0] {
            ColumnarValue::Array(array) => {
                array.as_any().downcast_ref::<ListArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected list array".to_string()))?
            },
            _ => return Err(DataFusionError::Internal("Expected list array".to_string())),
        };
        
        let position = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(val))) => *val as usize,
            _ => return Err(DataFusionError::Internal("Expected int64 position".to_string())),
        };

        let mut result_builder = Float64Builder::new();
        
        for i in 0..arrays.len() {
            if arrays.is_null(i) {
                result_builder.append_null();
            } else {
                let scores_array = arrays.value(i);
                if let Some(float_array) = scores_array.as_any().downcast_ref::<Float64Array>() {
                    if position < float_array.len() && !float_array.is_null(position) {
                        result_builder.append_value(float_array.value(position));
                    } else {
                        result_builder.append_null();
                    }
                } else {
                    result_builder.append_null();
                }
            }
        }
        
        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }
}

fn create_decode_phred_udf() -> ScalarUDF {
    ScalarUDF::from(DecodePhredUDF::new())
}

fn create_quantile_udf() -> ScalarUDF {
    ScalarUDF::from(QuantileUDF::new())
}

fn create_array_length_udf() -> ScalarUDF {
    ScalarUDF::from(ArrayLengthUDF::new())
}

fn create_array_element_udf() -> ScalarUDF {
    ScalarUDF::from(ArrayElementUDF::new())
}

pub(crate) async fn calc_base_sequence_quality(
    ctx: &ExonSession,
    table: String,
    column: String
) -> Result<DataFrame> {
    // Register all UDFs
    ctx.session.register_udf(create_decode_phred_udf());
    ctx.session.register_udf(create_quantile_udf());
    ctx.session.register_udf(create_array_length_udf());
    ctx.session.register_udf(create_array_element_udf());
    
    let max_length_df = ctx
        .sql(&format!(
            r#"
            SELECT 
                MAX(array_length(decode_phred({}, 33))) as max_length
            FROM {}
            "#,
            column, table
        ))
        .await?;
    
    let max_length_batches = max_length_df.collect().await?;
    let max_length = max_length_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0) as usize;
    
    let mut position_queries = Vec::new();
    
    for pos in 0..max_length {
        position_queries.push(format!(
            r#"
            SELECT 
                {} as position,
                quantile(ARRAY_AGG(array_element(phred_scores, {})), 0.0) as min,
                quantile(ARRAY_AGG(array_element(phred_scores, {})), 1.0) as max,
                quantile(ARRAY_AGG(array_element(phred_scores, {})), 0.5) as median,
                quantile(ARRAY_AGG(array_element(phred_scores, {})), 0.25) as q1,
                quantile(ARRAY_AGG(array_element(phred_scores, {})), 0.75) as q3,
                COUNT(array_element(phred_scores, {})) as sample_count
            FROM (
                SELECT decode_phred({}, 33) as phred_scores
                FROM {}
                WHERE array_length(decode_phred({}, 33)) > {}
            )
            "#,
            pos, pos, pos, pos, pos, pos, pos, column, table, column, pos
        ));
    }
    
    let union_query = position_queries.join(" UNION ALL ");
    
    let result_df = ctx
        .sql(&format!(
            r#"
            SELECT 
                position,
                min,
                max,
                median,
                q1,
                q3,
                sample_count
            FROM (
                {}
            )
            ORDER BY position
            "#,
            union_query
        ))
        .await?;
    
    Ok(result_df)
}
//----
pub(crate) async fn prepare_query(
    query: fn(QueryParams) -> String,
    range_opts: RangeOptions,
    ctx: &ExonSession,
    left_table: String,
    right_table: String,
) -> String {
    let sign = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let columns_1 = match range_opts.columns_1 {
        Some(cols) => cols,
        _ => default_cols_to_string(&DEFAULT_COLUMN_NAMES),
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => default_cols_to_string(&DEFAULT_COLUMN_NAMES),
    };

    let left_table_columns =
        get_non_join_columns(left_table.to_string(), columns_1.clone(), ctx).await;
    let right_table_columns =
        get_non_join_columns(right_table.to_string(), columns_2.clone(), ctx).await;

    let query_params = QueryParams {
        sign,
        suffixes,
        columns_1,
        columns_2,
        other_columns_1: left_table_columns,
        other_columns_2: right_table_columns,
        left_table,
        right_table,
    };

    query(query_params)
}
