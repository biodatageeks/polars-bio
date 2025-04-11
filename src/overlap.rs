use exon::ExonSession;
use log::debug;
use crate::option::FilterOp;
use crate::operation::{format_non_join_tables, get_non_join_columns};


pub(crate) async fn do_overlap(
    ctx: &ExonSession,
    left_table: String,
    right_table: String,
    overlap_filter: FilterOp,
    suffixes: (String, String),
    columns_1: Vec<String>,
    columns_2: Vec<String>,
) -> datafusion::dataframe::DataFrame {
    let sign = match overlap_filter {
        FilterOp::Weak => "=".to_string(),
        _ => "".to_string(),
    };
    let other_columns_1 =
        get_non_join_columns(left_table.to_string(), columns_1.clone(), ctx).await;
    let other_columns_2 =
        get_non_join_columns(right_table.to_string(), columns_2.clone(), ctx).await;
    
    let query = format!(
        r#"
            SELECT
                b.{} as {}{}, -- contig
                b.{} as {}{}, -- pos_start
                b.{} as {}{}, -- pos_end
                a.{} as {}{}, -- contig
                a.{} as {}{}, -- pos_start
                a.{} as {}{} -- pos_end
                {}
                {}
            FROM
                {} AS a, {} AS b
            WHERE
                a.{}=b.{}
            AND
                cast(a.{} AS INT) >{} cast(b.{} AS INT)
            AND
                cast(a.{} AS INT) <{} cast(b.{} AS INT)
        "#,
        columns_2[0],
        columns_2[0],
        suffixes.0, // contig
        columns_2[1],
        columns_2[1],
        suffixes.0, // pos_start
        columns_2[2],
        columns_2[2],
        suffixes.0, // pos_end
        columns_1[0],
        columns_1[0],
        suffixes.1, // contig
        columns_1[1],
        columns_1[1],
        suffixes.1, // pos_start
        columns_1[2],
        columns_1[2],
        suffixes.1, // pos_end
        if !other_columns_2.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    other_columns_2.clone(),
                    "a".to_string(),
                    suffixes.0.clone(),
                )
        } else {
            "".to_string()
        },
        if !other_columns_1.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    other_columns_1.clone(),
                    "b".to_string(),
                    suffixes.1.clone(),
                )
        } else {
            "".to_string()
        },
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
