use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion_bio_function_ranges::nearest::NearestProvider;
use datafusion_bio_function_ranges::{
    Algorithm, BioConfig, CountOverlapsProvider, FilterOp as RangesFilterOp,
};
use log::{debug, info};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::query::overlap_query;
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
    pub projection_columns: Option<Vec<String>>,
}
pub(crate) fn do_range_operation(
    ctx: &SessionContext,
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
            set_option_internal(ctx, "bio.interval_join_algorithm", alg);
        },
        _ => {
            set_option_internal(
                ctx,
                "bio.interval_join_algorithm",
                &Algorithm::Coitrees.to_string(),
            );
        },
    }
    // Optional low-memory toggle for interval join
    if let Some(low_mem) = range_options.overlap_low_memory {
        let v = if low_mem { "true" } else { "false" };
        set_option_internal(ctx, "bio.interval_join_low_memory", v);
        info!("IntervalJoin low_memory requested: {}", v);
    }
    info!(
        "Running {} operation with algorithm {} and {} thread(s)...",
        range_options.range_op,
        ctx.state()
            .config()
            .options()
            .extensions
            .get::<BioConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.state().config().options().execution.target_partitions
    );
    match range_options.range_op {
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options, left_table, right_table)),
        RangeOp::Nearest => rt.block_on(do_nearest(ctx, range_options, left_table, right_table)),
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
    ctx: &SessionContext,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let columns_1 = range_opts.columns_1.unwrap();
    let columns_2 = range_opts.columns_2.unwrap();
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let k = range_opts.nearest_k.unwrap_or(1);
    let include_overlaps = range_opts.include_overlaps.unwrap_or(true);
    let compute_distance = range_opts.compute_distance.unwrap_or(true);

    let session = ctx.clone();

    // Get schemas from both tables
    let left_table_ref = TableReference::from(left_table.clone());
    let left_schema = ctx
        .table(left_table_ref)
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();

    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = ctx
        .table(right_table_ref)
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();

    // Convert FilterOp
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };

    // NearestProvider indexes its `left_table` and iterates its `right_table`.
    // We want one result per df1 row (our left_table), so df1 must be the
    // provider's RIGHT (iterated) and df2 must be the provider's LEFT (indexed).
    let nearest_provider = NearestProvider::new(
        Arc::new(session),
        right_table,
        left_table,
        right_schema.clone(),
        left_schema.clone(),
        columns_2.clone(),
        columns_1.clone(),
        upstream_filter_op,
        include_overlaps,
        k,
    );

    let table_name = "nearest_result";
    ctx.deregister_table(table_name).unwrap();
    ctx.register_table(table_name, Arc::new(nearest_provider))
        .unwrap();

    // Build SELECT with column renaming.
    // Provider output: left_* (df2), right_* (df1).
    // User expects: df1 columns (suffix_1) first, df2 columns (suffix_2) second.
    let mut select_parts = Vec::new();

    // df1 columns first (provider's right_*)
    for field in left_schema.fields() {
        select_parts.push(format!(
            "\"right_{}\" AS \"{}{}\"",
            field.name(),
            field.name(),
            suffixes.0
        ));
    }

    // df2 columns second (provider's left_*)
    for field in right_schema.fields() {
        select_parts.push(format!(
            "\"left_{}\" AS \"{}{}\"",
            field.name(),
            field.name(),
            suffixes.1
        ));
    }

    // Optional distance computation
    if compute_distance {
        let start_1 = &columns_1[1];
        let end_1 = &columns_1[2];
        let start_2 = &columns_2[1];
        let end_2 = &columns_2[2];
        select_parts.push(format!(
            r#"CASE
                WHEN "left_{}" IS NULL THEN NULL
                WHEN "left_{}" < "right_{}" THEN CAST("right_{}" - "left_{}" AS BIGINT)
                WHEN "right_{}" < "left_{}" THEN CAST("left_{}" - "right_{}" AS BIGINT)
                ELSE CAST(0 AS BIGINT)
            END AS distance"#,
            start_2, end_2, start_1, start_1, end_2, end_1, start_2, start_2, end_1,
        ));
    }

    let query = format!("SELECT {} FROM {}", select_parts.join(", "), table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &SessionContext,
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
        ctx.state().config().options().execution.target_partitions
    );
    ctx.sql(&query).await.unwrap()
}

async fn do_count_overlaps_coverage_naive(
    ctx: &SessionContext,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
    coverage: bool,
) -> datafusion::dataframe::DataFrame {
    let columns_1 = range_opts.columns_1.unwrap();
    let columns_2 = range_opts.columns_2.unwrap();
    let session = ctx.clone();
    // Get schema from right_table since the Rust code iterates over right_table
    // and returns its rows with count/coverage appended
    let right_table_ref = TableReference::from(right_table.clone());
    let right_schema = ctx
        .table(right_table_ref.clone())
        .await
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    // Convert polars-bio FilterOp to upstream FilterOp
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };
    let count_overlaps_provider = CountOverlapsProvider::new(
        Arc::new(session),
        left_table,
        right_table,
        right_schema,
        columns_1,
        columns_2,
        upstream_filter_op,
        coverage,
    );
    let table_name = "count_overlaps_coverage".to_string();
    ctx.deregister_table(table_name.clone()).unwrap();
    ctx.register_table(table_name.clone(), Arc::new(count_overlaps_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn get_non_join_columns(
    table_name: String,
    join_columns: Vec<String>,
    ctx: &SessionContext,
) -> Vec<String> {
    let table_ref = TableReference::from(table_name);
    let table = ctx.table(table_ref).await.unwrap();
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
        .map(|c| format!("{}.`{}` as `{}{}`", table_alias, c, c, suffix))
        .collect::<Vec<String>>()
        .join(", ")
}

pub(crate) async fn prepare_query(
    query: fn(QueryParams) -> String,
    range_opts: RangeOptions,
    ctx: &SessionContext,
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
        projection_columns: None, // For now, no projection pushdown in SQL generation
    };

    query(query_params)
}
