use crate::operation::{format_non_join_tables, QueryParams};

pub(crate) fn nearest_query(query_params: QueryParams) -> String {
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
        query_params.columns_1[0],
        query_params.columns_1[0],
        query_params.suffixes.0, // contig
        query_params.columns_1[1],
        query_params.columns_1[1],
        query_params.suffixes.0, // pos_start
        query_params.columns_1[2],
        query_params.columns_1[2],
        query_params.suffixes.0, // pos_end
        query_params.columns_2[0],
        query_params.columns_2[0],
        query_params.suffixes.1, // contig
        query_params.columns_2[1],
        query_params.columns_2[1],
        query_params.suffixes.1, // pos_start
        query_params.columns_2[2],
        query_params.columns_2[2],
        query_params.suffixes.1, // pos_end
        if !query_params.other_columns_1.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    query_params.other_columns_1.clone(),
                    "a".to_string(),
                    query_params.suffixes.0.clone(),
                )
        } else {
            "".to_string()
        },
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
        query_params.columns_2[1],
        query_params.columns_1[2], //  b.pos_start >= a.pos_end
        query_params.columns_2[1],
        query_params.columns_1[2], // b.pos_start-a.pos_end
        query_params.columns_2[2],
        query_params.columns_1[1], // b.pos_end <= a.pos_start
        query_params.columns_2[2],
        query_params.columns_1[1], // a.pos_start-b.pos_end
        query_params.right_table,
        query_params.left_table,
        query_params.columns_1[0],
        query_params.columns_2[0], // contig
        query_params.columns_1[2],
        query_params.sign,
        query_params.columns_2[1], // pos_start
        query_params.columns_1[1],
        query_params.sign,
        query_params.columns_2[2], // pos_end
    );
    query
}

pub(crate) fn overlap_query(query_params: QueryParams) -> String {
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
        query_params.columns_2[0],
        query_params.columns_2[0],
        query_params.suffixes.0, // contig
        query_params.columns_2[1],
        query_params.columns_2[1],
        query_params.suffixes.0, // pos_start
        query_params.columns_2[2],
        query_params.columns_2[2],
        query_params.suffixes.0, // pos_end
        query_params.columns_1[0],
        query_params.columns_1[0],
        query_params.suffixes.1, // contig
        query_params.columns_1[1],
        query_params.columns_1[1],
        query_params.suffixes.1, // pos_start
        query_params.columns_1[2],
        query_params.columns_1[2],
        query_params.suffixes.1, // pos_end
        if !query_params.other_columns_2.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    query_params.other_columns_2.clone(),
                    "a".to_string(),
                    query_params.suffixes.0.clone(),
                )
        } else {
            "".to_string()
        },
        if !query_params.other_columns_1.is_empty() {
            ",".to_string()
                + &format_non_join_tables(
                    query_params.other_columns_1.clone(),
                    "b".to_string(),
                    query_params.suffixes.1.clone(),
                )
        } else {
            "".to_string()
        },
        query_params.right_table,
        query_params.left_table,
        query_params.columns_1[0],
        query_params.columns_2[0], // contig
        query_params.columns_1[2],
        query_params.sign,
        query_params.columns_2[1], // pos_start
        query_params.columns_1[1],
        query_params.sign,
        query_params.columns_2[2], // pos_end
    );
    query
}

pub(crate) fn count_overlaps_query(query_params: QueryParams) -> String {
    let query = format!(
        r#"
            SELECT
                chr AS {}{},           -- contig
                s1ends2start AS {}{},  -- pos_start
                s1starts2end AS {}{},  -- pos_end
                st - ed AS count
            FROM (
                SELECT
                    chr,
                    SUM(iss1) OVER (
                        PARTITION BY chr ORDER BY s1starts2end ASC, iss1 {}
                    ) st,
                    SUM(iss1) OVER (
                        PARTITION BY chr ORDER BY s1ends2start ASC, iss1 {}
                    ) ed,
                    iss1,
                    s1starts2end,
                    s1ends2start
                FROM (
                    (SELECT
                        a.{} AS chr, -- contig
                        a.{} AS s1starts2end, -- pos_start
                        a.{} AS s1ends2start, -- pos_end
                        1 AS iss1
                    FROM {} AS a)
                    UNION ALL
                    (SELECT
                        b.{} AS chr, -- contig
                        b.{} AS s1starts2end, -- pos_end
                        b.{} AS s1ends2start, -- pos_start
                        0 AS iss1
                    FROM {} AS b)
                )
            )
            WHERE
                iss1 = 0
        "#,
        query_params.columns_1[0],
        query_params.suffixes.0, // contig
        query_params.columns_1[1],
        query_params.suffixes.0, // pos_start
        query_params.columns_1[2],
        query_params.suffixes.0, // pos_end
        if query_params.sign == "=" {
            "DESC"
        } else {
            "ASC"
        },
        if query_params.sign == "=" {
            "ASC"
        } else {
            "DESC"
        },
        query_params.columns_2[0],
        query_params.columns_2[1],
        query_params.columns_2[2],
        query_params.right_table,
        query_params.columns_1[0],
        query_params.columns_1[2],
        query_params.columns_1[1],
        query_params.left_table,
    );
    query
}
