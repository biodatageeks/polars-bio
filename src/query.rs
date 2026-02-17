use crate::operation::{format_non_join_tables, QueryParams};

pub(crate) fn overlap_query(query_params: QueryParams) -> String {
    // Build SELECT clause based on projected columns if available
    let select_clause = if let Some(ref projected_cols) = query_params.projection_columns {
        // Filter the columns to only include projected ones
        let mut selected_columns = Vec::new();

        // Always include the required coordinate columns
        selected_columns.push(format!(
            "a.`{}` as `{}{}`",
            query_params.columns_1[0], query_params.columns_1[0], query_params.suffixes.0
        ));
        selected_columns.push(format!(
            "a.`{}` as `{}{}`",
            query_params.columns_1[1], query_params.columns_1[1], query_params.suffixes.0
        ));
        selected_columns.push(format!(
            "a.`{}` as `{}{}`",
            query_params.columns_1[2], query_params.columns_1[2], query_params.suffixes.0
        ));
        selected_columns.push(format!(
            "b.`{}` as `{}{}`",
            query_params.columns_2[0], query_params.columns_2[0], query_params.suffixes.1
        ));
        selected_columns.push(format!(
            "b.`{}` as `{}{}`",
            query_params.columns_2[1], query_params.columns_2[1], query_params.suffixes.1
        ));
        selected_columns.push(format!(
            "b.`{}` as `{}{}`",
            query_params.columns_2[2], query_params.columns_2[2], query_params.suffixes.1
        ));

        // Add other columns only if they are in the projected columns
        for col in &query_params.other_columns_1 {
            if projected_cols.iter().any(|pc| pc.contains(col)) {
                selected_columns.push(format!(
                    "a.`{}` as `{}{}`",
                    col, col, query_params.suffixes.0
                ));
            }
        }
        for col in &query_params.other_columns_2 {
            if projected_cols.iter().any(|pc| pc.contains(col)) {
                selected_columns.push(format!(
                    "b.`{}` as `{}{}`",
                    col, col, query_params.suffixes.1
                ));
            }
        }

        selected_columns.join(",\n                ")
    } else {
        // Use original logic when no projection is specified
        format!(
            "a.`{}` as `{}{}`, -- contig
                a.`{}` as `{}{}`, -- pos_start
                a.`{}` as `{}{}`, -- pos_end
                b.`{}` as `{}{}`, -- contig
                b.`{}` as `{}{}`, -- pos_start
                b.`{}` as `{}{}` -- pos_end
                {}
                {}",
            query_params.columns_1[0],
            query_params.columns_1[0],
            query_params.suffixes.0,
            query_params.columns_1[1],
            query_params.columns_1[1],
            query_params.suffixes.0,
            query_params.columns_1[2],
            query_params.columns_1[2],
            query_params.suffixes.0,
            query_params.columns_2[0],
            query_params.columns_2[0],
            query_params.suffixes.1,
            query_params.columns_2[1],
            query_params.columns_2[1],
            query_params.suffixes.1,
            query_params.columns_2[2],
            query_params.columns_2[2],
            query_params.suffixes.1,
            if !query_params.other_columns_2.is_empty() {
                ",".to_string()
                    + &format_non_join_tables(
                        query_params.other_columns_2.clone(),
                        "b".to_string(),
                        query_params.suffixes.1.clone(),
                    )
            } else {
                "".to_string()
            },
            if !query_params.other_columns_1.is_empty() {
                ",".to_string()
                    + &format_non_join_tables(
                        query_params.other_columns_1.clone(),
                        "a".to_string(),
                        query_params.suffixes.0.clone(),
                    )
            } else {
                "".to_string()
            }
        )
    };

    let query = format!(
        r#"
            SELECT
                {}
            FROM
                {} AS b, {} AS a
            WHERE
                a.`{}`=b.`{}`
            AND
                cast(a.`{}` AS INT) >{} cast(b.`{}` AS INT)
            AND
                cast(a.`{}` AS INT) <{} cast(b.`{}` AS INT)
        "#,
        select_clause,
        query_params.right_table,
        query_params.left_table,
        query_params.columns_1[0],
        query_params.columns_2[0], // contig
        query_params.columns_1[2],
        query_params.sign,
        query_params.columns_2[1], // pos_start
        query_params.columns_1[1],
        query_params.sign,
        query_params.columns_2[2] // pos_end
    );
    query
}
