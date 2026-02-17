use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion_bio_function_ranges::{
    Algorithm, BioConfig, ClusterProvider, ComplementProvider, CountOverlapsProvider,
    FilterOp as RangesFilterOp, MergeProvider, NearestProvider, OverlapProvider, SubtractProvider,
};
use log::{debug, info};
use tokio::runtime::Runtime;

static NEAREST_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static OVERLAP_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static MERGE_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static COUNT_OVERLAPS_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static CLUSTER_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static COMPLEMENT_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);
static SUBTRACT_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::utils::default_cols_to_string;
use crate::DEFAULT_COLUMN_NAMES;

pub(crate) fn do_range_operation(
    ctx: &SessionContext,
    rt: &Runtime,
    range_options: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    // Configure interval join algorithm for operations that use it
    if !matches!(
        range_options.range_op,
        RangeOp::Merge | RangeOp::Cluster | RangeOp::Complement | RangeOp::Subtract
    ) {
        match &range_options.overlap_alg {
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
    } else {
        info!(
            "Running {} operation with {} thread(s)...",
            range_options.range_op,
            ctx.state().config().options().execution.target_partitions
        );
    }
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
        RangeOp::Merge => rt.block_on(do_merge(ctx, range_options, left_table)),
        RangeOp::Cluster => rt.block_on(do_cluster(ctx, range_options, left_table)),
        RangeOp::Complement => rt.block_on(do_complement(ctx, range_options, left_table)),
        RangeOp::Subtract => rt.block_on(do_subtract(ctx, range_options, left_table, right_table)),
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
        compute_distance,
    );

    let table_name = format!(
        "nearest_result_{}",
        NEAREST_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(nearest_provider))
        .unwrap();

    // Build SELECT with column renaming.
    // Provider output: left_* (df2), right_* (df1), [distance].
    // User expects: df1 columns (suffix_1) first, df2 columns (suffix_2) second, [distance].
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

    // Distance column is computed natively by NearestProvider when enabled
    if compute_distance {
        select_parts.push("\"distance\"".to_string());
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
    let columns_1 = range_opts
        .columns_1
        .clone()
        .unwrap_or_else(|| default_cols_to_string(&DEFAULT_COLUMN_NAMES));
    let columns_2 = range_opts
        .columns_2
        .clone()
        .unwrap_or_else(|| default_cols_to_string(&DEFAULT_COLUMN_NAMES));
    let suffixes = range_opts
        .suffixes
        .clone()
        .unwrap_or_else(|| ("_1".to_string(), "_2".to_string()));
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };

    let session = ctx.clone();
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

    let overlap_provider = OverlapProvider::new(
        Arc::new(session),
        left_table,
        right_table,
        left_schema.clone(),
        right_schema.clone(),
        columns_1,
        columns_2,
        upstream_filter_op,
    );

    let table_name = format!(
        "overlap_result_{}",
        OVERLAP_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(overlap_provider))
        .unwrap();

    // Build SELECT clause to rename left_* → *{suffix_1}, right_* → *{suffix_2}
    let mut select_parts = Vec::new();
    for field in left_schema.fields() {
        select_parts.push(format!(
            "`left_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.0
        ));
    }
    for field in right_schema.fields() {
        select_parts.push(format!(
            "`right_{}` AS `{}{}`",
            field.name(),
            field.name(),
            suffixes.1
        ));
    }
    let query = format!("SELECT {} FROM {}", select_parts.join(", "), table_name);
    debug!("Query: {}", query);
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
    let table_name = format!(
        "count_overlaps_coverage_{}",
        COUNT_OVERLAPS_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(count_overlaps_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_merge(
    ctx: &SessionContext,
    range_opts: RangeOptions,
    table: String,
) -> datafusion::dataframe::DataFrame {
    let columns = range_opts.columns_1.unwrap();
    let min_dist = range_opts.min_dist.unwrap_or(0);
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };
    let session = ctx.clone();
    let merge_provider = MergeProvider::new(
        Arc::new(session),
        table,
        (columns[0].clone(), columns[1].clone(), columns[2].clone()),
        min_dist,
        upstream_filter_op,
    );
    let table_name = format!(
        "merge_result_{}",
        MERGE_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(merge_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_cluster(
    ctx: &SessionContext,
    range_opts: RangeOptions,
    table: String,
) -> datafusion::dataframe::DataFrame {
    let columns = range_opts.columns_1.unwrap();
    let min_dist = range_opts.min_dist.unwrap_or(0);
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };
    let session = ctx.clone();
    let cluster_provider = ClusterProvider::new(
        Arc::new(session),
        table,
        (columns[0].clone(), columns[1].clone(), columns[2].clone()),
        min_dist,
        upstream_filter_op,
    );
    let table_name = format!(
        "cluster_result_{}",
        CLUSTER_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(cluster_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_complement(
    ctx: &SessionContext,
    range_opts: RangeOptions,
    table: String,
) -> datafusion::dataframe::DataFrame {
    let columns = range_opts.columns_1.unwrap();
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };
    let view_table = range_opts.view_table;
    let view_columns = range_opts.view_columns.unwrap_or_else(|| columns.clone());
    let session = ctx.clone();
    let complement_provider = ComplementProvider::new(
        Arc::new(session),
        table,
        view_table,
        (columns[0].clone(), columns[1].clone(), columns[2].clone()),
        (
            view_columns[0].clone(),
            view_columns[1].clone(),
            view_columns[2].clone(),
        ),
        upstream_filter_op,
    );
    let table_name = format!(
        "complement_result_{}",
        COMPLEMENT_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(complement_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_subtract(
    ctx: &SessionContext,
    range_opts: RangeOptions,
    left_table: String,
    right_table: String,
) -> datafusion::dataframe::DataFrame {
    let columns_1 = range_opts.columns_1.unwrap();
    let columns_2 = range_opts.columns_2.unwrap();
    let upstream_filter_op = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => RangesFilterOp::Weak,
        FilterOp::Strict => RangesFilterOp::Strict,
    };
    let session = ctx.clone();
    let subtract_provider = SubtractProvider::new(
        Arc::new(session),
        left_table,
        right_table,
        (
            columns_1[0].clone(),
            columns_1[1].clone(),
            columns_1[2].clone(),
        ),
        (
            columns_2[0].clone(),
            columns_2[1].clone(),
            columns_2[2].clone(),
        ),
        upstream_filter_op,
    );
    let table_name = format!(
        "subtract_result_{}",
        SUBTRACT_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    ctx.register_table(table_name.as_str(), Arc::new(subtract_provider))
        .unwrap();
    let query = format!("SELECT * FROM {}", table_name);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}
