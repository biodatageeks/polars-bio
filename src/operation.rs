use std::sync::Arc;

use datafusion::catalog_common::TableReference;
use exon::ExonSession;
use log::{debug, info};
use sequila_core::session_context::{Algorithm, SequilaConfig};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::query::count_overlaps_query;
use crate::udtf::CountOverlapsProvider;
use crate::nearest::do_nearest;
use crate::overlap::do_overlap;

pub(crate) struct QueryParams {
    pub sign: String,
    pub suffixes: (String, String),
    pub columns_1: Vec<String>,
    pub columns_2: Vec<String>,
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
    let streaming = range_options.streaming;
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
        RangeOp::Overlap => rt.block_on(do_overlap(
            ctx,
            left_table,
            right_table,
            range_options.filter_op,
            range_options.suffixes,
            range_options.columns_1,
            range_options.columns_2,
        )),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(
                ctx,
                left_table,
                right_table,
                range_options.filter_op,
                range_options.suffixes,
                range_options.columns_1,
                range_options.columns_2,
            ))
        },
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


pub(crate) async fn get_non_join_columns(
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
