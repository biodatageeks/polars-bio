use exon::ExonSession;
use log::{debug, info};
use sequila_core::session_context::{Algorithm, SequilaConfig};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::query::{nearest_query, overlap_query};
use crate::utils::default_cols_to_string;
use crate::DEFAULT_COLUMN_NAMES;

pub(crate) struct QueryParams {
    pub sign: String,
    pub suffixes: (String, String),
    pub columns_1: Vec<String>,
    pub columns_2: Vec<String>,
}
pub(crate) fn do_range_operation(
    ctx: &ExonSession,
    rt: &Runtime,
    range_options: RangeOptions,
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
        RangeOp::Overlap => rt.block_on(do_overlap(ctx, range_options)),
        RangeOp::Nearest => {
            set_option_internal(ctx, "sequila.interval_join_algorithm", "coitreesnearest");
            rt.block_on(do_nearest(ctx, range_options))
        },
        _ => panic!("Unsupported operation"),
    }
}

async fn do_nearest(
    ctx: &ExonSession,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(nearest_query, range_opts);
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &ExonSession,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    let query = prepare_query(overlap_query, range_opts);
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

pub(crate) fn prepare_query(query: fn(QueryParams) -> String, range_opts: RangeOptions) -> String {
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
    let query_params = QueryParams {
        sign,
        suffixes,
        columns_1,
        columns_2,
    };

    query(query_params)
}
