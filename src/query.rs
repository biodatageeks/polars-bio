use crate::operation::{format_non_join_tables, QueryParams};

pub(crate) fn nearest_query(query_params: QueryParams) -> String {
    let a_contig = &query_params.columns_1[0];
    let a_start = &query_params.columns_1[1];
    let a_end = &query_params.columns_1[2];
    let b_contig = &query_params.columns_2[0];
    let b_start = &query_params.columns_2[1];
    let b_end = &query_params.columns_2[2];
    let s1 = &query_params.suffixes.0;
    let s2 = &query_params.suffixes.1;

    let extra_a = if !query_params.other_columns_1.is_empty() {
        ",".to_string()
            + &format_non_join_tables(
                query_params.other_columns_1.clone(),
                "a".to_string(),
                query_params.suffixes.0.clone(),
            )
    } else {
        "".to_string()
    };
    let extra_b = if !query_params.other_columns_2.is_empty() {
        ",".to_string()
            + &format_non_join_tables(
                query_params.other_columns_2.clone(),
                "b".to_string(),
                query_params.suffixes.1.clone(),
            )
    } else {
        "".to_string()
    };

    let query = format!(
        r#"
        SELECT
            a.`{}` AS `{}{}`, -- contig
            a.`{}` AS `{}{}`, -- pos_start
            a.`{}` AS `{}{}`, -- pos_end
            b.`{}` AS `{}{}`, -- contig
            b.`{}` AS `{}{}`, -- pos_start
            b.`{}` AS `{}{}`  -- pos_end
            {}
            {},
       CAST(
       CASE WHEN b.`{}` >= a.`{}`
            THEN
                abs(b.`{}`-a.`{}`)
        WHEN b.`{}` <= a.`{}`
            THEN
            abs(b.`{}`-a.`{}`)
            ELSE 0
       END AS BIGINT) AS distance

       FROM {} AS b, {} AS a
        WHERE  a.`{}` = b.`{}`
            AND cast(b.`{}` AS INT) >{} cast(a.`{}` AS INT )
            AND cast(b.`{}` AS INT) <{} cast(a.`{}` AS INT)
        "#,
        a_contig,
        a_contig,
        s1,
        a_start,
        a_start,
        s1,
        a_end,
        a_end,
        s1,
        b_contig,
        b_contig,
        s2,
        b_start,
        b_start,
        s2,
        b_end,
        b_end,
        s2,
        extra_a,
        extra_b,
        b_start,
        a_end,
        b_start,
        a_end,
        b_end,
        a_start,
        b_end,
        a_start,
        query_params.right_table,
        query_params.left_table,
        a_contig,
        b_contig,
        b_end,
        query_params.sign,
        a_start,
        b_start,
        query_params.sign,
        a_end,
    );
    query
}

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
