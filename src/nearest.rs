use exon::ExonSession;
use log::info;
use crate::option::FilterOp;
use crate::operation::format_non_join_tables;


pub(crate) async fn do_nearest(
    ctx: &ExonSession,
    left_table: String,
    right_table: String,
    overlap_filter: FilterOp,
    suffixes: (String, String),
    columns_1: Vec<String>,
    columns_2: Vec<String>,
) -> datafusion::dataframe::DataFrame {
    let sign = match range_opts.filter_op.unwrap() {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let left_table_columns =
        get_non_join_columns(left_table.to_string(), columns_1.clone(), ctx).await;
    let right_table_columns =
        get_non_join_columns(right_table.to_string(), columns_2.clone(), ctx).await;
    let query = format!(
        r#"
        SELECT
            a.{} AS {}{}, -- contig
            a.{} AS {}{}, -- pos_start
            a.{} AS {}{}, -- pos_end
            b.{} AS {}{}, -- contig
            b.{} AS {}{}, -- pos_start
            b.{} AS {}{}  -- pos_end
            {}
            {},
       CAST(
       CASE WHEN b.{} >= a.{}
            THEN
                abs(b.{}-a.{})
        WHEN b.{} <= a.{}
            THEN
            abs(b.{}-a.{})
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
        if !other_columns_1.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    other_columns_1.clone(),
                    "a".to_string(),
                    suffixes.0.clone(),
                )
        } else {
            "".to_string()
        },
        if !other_columns_2.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    other_columns_2.clone(),
                    "b".to_string(),
                    suffixes.1.clone(),
                )
        } else {
            "".to_string()
        },
        columns_2[1],
        columns_1[2], //  b.pos_start >= a.pos_end
        columns_2[1],
        columns_1[2], // b.pos_start-a.pos_end
        columns_2[2],
        columns_1[1], // b.pos_end <= a.pos_start
        columns_2[2],
        columns_1[1], // a.pos_start-b.pos_end
        right_table,
        left_table,
        columns_1[0],
        columns_2[0], // contig
        columns_1[2],
        sign,
        columns_2[1], // pos_start
        columns_1[1],
        sign,
        columns_2[2], // pos_end
    );
    debug!("Query: {}", query);
    ctx.sql(&query).await.unwrap()
}
