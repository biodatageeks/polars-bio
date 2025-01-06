use datafusion::prelude::SessionContext;
use log::info;
use sequila_core::session_context::{Algorithm, SequilaConfig};
use tokio::runtime::Runtime;

use crate::context::set_option_internal;
use crate::option::{FilterOp, RangeOp, RangeOptions};
use crate::{LEFT_TABLE, RIGHT_TABLE};

pub(crate) fn do_range_operation(
    ctx: &SessionContext,
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
    info!(
        "Running {} operation with algorithm {} and {} thread(s)...",
        range_options.range_op,
        ctx.state()
            .config()
            .options()
            .extensions
            .get::<SequilaConfig>()
            .unwrap()
            .interval_join_algorithm,
        ctx.state().config().options().execution.target_partitions
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
    ctx: &SessionContext,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
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
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };

    let query = format!(
        r#"
        SELECT
            a.{} AS {}{}, -- contig
            a.{} AS {}{}, -- pos_start
            a.{} AS {}{}, -- pos_end
            b.{} AS {}{}, -- contig
            b.{} AS {}{}, -- pos_start
            b.{} AS {}{},  -- pos_end
            a.* except({}, {}, {}), -- all join columns from left table
            b.* except({}, {}, {}), -- all join columns from right table
       CAST(
       CASE WHEN b.{} >= a.{}
            THEN
                abs(b.{}-a.{})
        WHEN b.{} <= a.{}
            THEN
            abs(a.{}-b.{})
            ELSE 0
       END AS BIGINT) AS distance

       FROM {} AS b, {} AS a
        WHERE  b.{} = a.{}
            AND cast(b.{} AS INT) >{} cast(a.{} AS INT )
            AND cast(b.{} AS INT) <{} cast(a.{} AS INT)
        "#,
        columns_1[0],
        columns_1[0],
        suffixes.0, // contig
        columns_1[1],
        columns_1[1],
        suffixes.0, // pos_start
        columns_1[2],
        columns_1[2],
        suffixes.0, // pos_end
        columns_2[0],
        columns_2[0],
        suffixes.1, // contig
        columns_2[1],
        columns_2[1],
        suffixes.1, // pos_start
        columns_2[2],
        columns_2[2],
        suffixes.1, // pos_end
        columns_1[0],
        columns_1[1],
        columns_1[2], // all join columns from right table
        columns_2[0],
        columns_2[1],
        columns_2[2], // all join columns from left table
        columns_2[1],
        columns_1[2], //  b.pos_start >= a.pos_end
        columns_2[1],
        columns_1[2], // b.pos_start-a.pos_end
        columns_2[2],
        columns_1[1], // b.pos_end <= a.pos_start
        columns_2[2],
        columns_1[1], // a.pos_start-b.pos_end
        RIGHT_TABLE,
        LEFT_TABLE,
        columns_1[0],
        columns_2[0], // contig
        columns_1[2],
        sign,
        columns_2[1], // pos_start
        columns_1[1],
        sign,
        columns_2[2], // pos_end
    );
    ctx.sql(&query).await.unwrap()
}

async fn do_overlap(
    ctx: &SessionContext,
    range_opts: RangeOptions,
) -> datafusion::dataframe::DataFrame {
    let sign = match range_opts.clone().filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let suffixes = match range_opts.suffixes {
        Some((s1, s2)) => (s1, s2),
        _ => ("_1".to_string(), "_2".to_string()),
    };
    let columns_1 = match range_opts.columns_1 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let columns_2 = match range_opts.columns_2 {
        Some(cols) => cols,
        _ => vec![
            "contig".to_string(),
            "pos_start".to_string(),
            "pos_end".to_string(),
        ],
    };
    let query = format!(
        r#"
            SELECT
                a.{} as {}{}, -- contig
                a.{} as {}{}, -- pos_start
                a.{} as {}{}, -- pos_end
                b.{} as {}{}, -- contig
                b.{} as {}{}, -- pos_start
                b.{} as {}{}, -- pos_end
                a.* except({}, {}, {}), -- all join columns from left table
                b.* except({}, {}, {}) -- all join columns from right table
            FROM
                {} a, {} b
            WHERE
                a.{}=b.{}
            AND
                cast(a.{} AS INT) >{} cast(b.{} AS INT)
            AND
                cast(a.{} AS INT) <{} cast(b.{} AS INT)
        "#,
        columns_1[0],
        columns_1[0],
        suffixes.0, // contig
        columns_1[1],
        columns_1[1],
        suffixes.0, // pos_start
        columns_1[2],
        columns_1[2],
        suffixes.0, // pos_end
        columns_2[0],
        columns_2[0],
        suffixes.1, // contig
        columns_2[1],
        columns_2[1],
        suffixes.1, // pos_start
        columns_2[2],
        columns_2[2],
        suffixes.1, // pos_end
        columns_1[0],
        columns_1[1],
        columns_1[2], // all join columns from right table
        columns_2[0],
        columns_2[1],
        columns_2[2], // all join columns from left table
        LEFT_TABLE,
        RIGHT_TABLE,
        columns_1[0],
        columns_2[0], // contig
        columns_1[2],
        sign,
        columns_2[1], // pos_start
        columns_1[1],
        sign,
        columns_2[2], // pos_end
    );
    ctx.sql(&query).await.unwrap()
}
