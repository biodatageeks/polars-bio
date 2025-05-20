use std::sync::Arc;

use datafusion::catalog_common::TableReference;
use exon::ExonSession;
use log::{debug, info};
use sequila_core::session_context::{Algorithm, SequilaConfig};
use serde_json::json;
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::query::{count_overlaps_query, nearest_query, overlap_query};
use crate::udtf::CountOverlapsProvider;
use crate::utils::default_cols_to_string;
use crate::DEFAULT_COLUMN_NAMES;

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

pub(crate) async fn do_base_sequence_quality(
    ctx: &ExonSession,
    table: String,
) -> serde_json::Value {
    let query = format!("SELECT quality_scores FROM {}", table);
    debug!("Query: {}", query);
    let batches = ctx.sql(&query).await.unwrap().collect().await.unwrap();

    let mut base_quality_count: std::collections::HashMap<usize, Vec<usize>> =
        std::collections::HashMap::new();

    use std::any::type_name_of_val;

    for batch in batches {
        let col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "quality_scores")
            .expect("Column 'quality_scores' not found");
        let array = batch.column(col_idx);
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let quality_str = if let Some(string_array) =
                array.as_any().downcast_ref::<arrow_array::StringArray>()
            {
                string_array.value(i)
            } else if let Some(large_string_array) = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
            {
                large_string_array.value(i)
            } else if let Some(generic_string_array_i64) = array
                .as_any()
                .downcast_ref::<arrow_array::GenericStringArray<i64>>(
            ) {
                generic_string_array_i64.value(i)
            } else if let Some(generic_string_array_i32) = array
                .as_any()
                .downcast_ref::<arrow_array::GenericStringArray<i32>>(
            ) {
                generic_string_array_i32.value(i)
            } else if let Some(string_view_array) = array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
            {
                string_view_array.value(i)
            } else {
                panic!(
                    "Column 'quality_scores' has unsupported array type: {:?} (concrete Rust type: {})",
                    array.data_type(),
                    type_name_of_val(array)
                );
            };

            for (pos, qchar) in quality_str.chars().enumerate() {
                let qscore = qchar as usize - 33;
                let rec = base_quality_count
                    .entry(pos)
                    .or_insert_with(|| vec![0_usize; 94]);
                if qscore < 94 {
                    rec[qscore] += 1;
                }
            }
        }
    }

    fn quartiles(counts: &[usize]) -> Vec<f32> {
        let mut expanded = Vec::new();
        for (q, &c) in counts.iter().enumerate() {
            for _ in 0..c {
                expanded.push(q as f32 + 33.0);
            }
        }
        if expanded.is_empty() {
            return vec![0.0; 5];
        }
        expanded.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = expanded.len();
        let q = |p: f32| {
            let idx = (p * (n - 1) as f32).round() as usize;
            expanded[idx]
        };
        vec![
            q(0.0),  // min
            q(0.25), // Q1
            q(0.5),  // median
            q(0.75), // Q3
            q(1.0),  // max
        ]
    }

    let mut base_quality_warn = "pass";
    let mut base_per_pos_data = Vec::new();
    for (position, qualities) in base_quality_count.iter() {
        let (sum, len) = qualities
            .iter()
            .enumerate()
            .fold((0_usize, 0_usize), |(s, l), (q, c)| {
                (s + (q + 33) * c, l + c)
            });
        let avg = if len > 0 {
            sum as f64 / len as f64
        } else {
            0.0
        };
        let values = quartiles(qualities);
        let median = values[2];
        if median <= 20.0 {
            base_quality_warn = "fail";
        } else if median <= 25.0 && base_quality_warn != "fail" {
            base_quality_warn = "warn";
        }
        base_per_pos_data.push(json!({
            "pos": position,
            "average": avg,
            "upper": values[4],
            "lower": values[0],
            "q1": values[1],
            "q3": values[3],
            "median": values[2],
        }));
    }

    json!({
        "base_quality_warn": base_quality_warn,
        "base_per_pos_data": base_per_pos_data
    })
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
