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

#[derive(Debug, Clone)]
struct QualityStats {
    min: f64,
    max: f64,
    median: f64,
    q1: f64,
    q3: f64,
}

fn calculate_stats(mut scores: Vec<f64>) -> QualityStats {
    if scores.is_empty() {
        return QualityStats {
            min: 0.0,
            max: 0.0,
            median: 0.0,
            q1: 0.0,
            q3: 0.0,
        };
    }
    
    scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let len = scores.len();
    
    let min = scores[0];
    let max = scores[len - 1];
    
    let median = if len % 2 == 0 {
        (scores[len / 2 - 1] + scores[len / 2]) / 2.0
    } else {
        scores[len / 2]
    };
    
    let q1_idx = len / 4;
    let q3_idx = 3 * len / 4;
    
    let q1 = if q1_idx > 0 { scores[q1_idx] } else { min };
    let q3 = if q3_idx < len { scores[q3_idx] } else { max };
    
    QualityStats { min, max, median, q1, q3 }
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
                
                // Append scores to the list builder
                for score in scores {
                    list_builder.values().append_value(score);
                }
                list_builder.append(true);
            }
        }
        
        Ok(ColumnarValue::Array(Arc::new(list_builder.finish())))
    }
}

fn create_decode_phred_udf() -> ScalarUDF {
    ScalarUDF::from(DecodePhredUDF::new())
}

async fn calculate_position_statistics(
    ctx: &ExonSession,
    decoded_scores_df: DataFrame,
) -> Result<DataFrame> {
    let batches = decoded_scores_df.collect().await?;
    
    let mut position_scores: HashMap<usize, Vec<f64>> = HashMap::new();
    let mut max_length = 0;
    
    for batch in batches {
        let phred_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected list array".to_string()))?;
        
        for i in 0..phred_column.len() {
            if !phred_column.is_null(i) {
                let scores_array = phred_column.value(i);
                if let Some(float_array) = scores_array.as_any().downcast_ref::<Float64Array>() {
                    max_length = max_length.max(float_array.len());
                    for (pos, score) in float_array.iter().enumerate() {
                        if let Some(score_val) = score {
                            position_scores
                                .entry(pos)
                                .or_insert_with(Vec::new)
                                .push(score_val);
                        }
                    }
                }
            }
        }
    }

    let mut positions = Vec::with_capacity(max_length);
    let mut min_values = Vec::with_capacity(max_length);
    let mut max_values = Vec::with_capacity(max_length);
    let mut median_values = Vec::with_capacity(max_length);
    let mut q1_values = Vec::with_capacity(max_length);
    let mut q3_values = Vec::with_capacity(max_length);
    let mut sample_counts = Vec::with_capacity(max_length);
    
    for pos in 0..max_length {
        positions.push(pos as i64);
        
        if let Some(scores) = position_scores.get(&pos) {
            let stats = calculate_stats(scores.clone());
            min_values.push(Some(stats.min));
            max_values.push(Some(stats.max));
            median_values.push(Some(stats.median));
            q1_values.push(Some(stats.q1));
            q3_values.push(Some(stats.q3));
            sample_counts.push(scores.len() as i64);
        } else {
            min_values.push(None);
            max_values.push(None);
            median_values.push(None);
            q1_values.push(None);
            q3_values.push(None);
            sample_counts.push(0);
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int64, false),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("median", DataType::Float64, true),
        Field::new("q1", DataType::Float64, true),
        Field::new("q3", DataType::Float64, true),
        Field::new("sample_count", DataType::Int64, false),
    ]));
    
    let position_array = Arc::new(Int64Array::from(positions)) as ArrayRef;
    let min_array = Arc::new(Float64Array::from(min_values)) as ArrayRef;
    let max_array = Arc::new(Float64Array::from(max_values)) as ArrayRef;
    let median_array = Arc::new(Float64Array::from(median_values)) as ArrayRef;
    let q1_array = Arc::new(Float64Array::from(q1_values)) as ArrayRef;
    let q3_array = Arc::new(Float64Array::from(q3_values)) as ArrayRef;
    let count_array = Arc::new(Int64Array::from(sample_counts)) as ArrayRef;
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![position_array, min_array, max_array, median_array, q1_array, q3_array, count_array],
    )?;
    
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let df = ctx.session.read_table(Arc::new(table))?;
    
    Ok(df)
}

pub(crate) async fn calc_base_sequence_quality(
    ctx: &ExonSession,
    table: String,
    column: String
) -> Result<DataFrame> {
    ctx.session.register_udf(create_decode_phred_udf());
    
    let decoded_scores_df = ctx
        .sql(&format!(
            r#"
            SELECT 
                decode_phred({}, 33) as phred_scores
            FROM {}
            "#,
            column, table
        ))
        .await?;
    
    let result_df = calculate_position_statistics(&ctx, decoded_scores_df).await?;
    
    Ok(result_df)
}

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
