use crate::operation::QueryParams;
use crate::{LEFT_TABLE, RIGHT_TABLE};

pub(crate) fn nearest_query(query_params: QueryParams) -> String {
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
        query_params.columns_1[0],
        query_params.columns_1[1],
        query_params.columns_1[2], // all join columns from right table
        query_params.columns_2[0],
        query_params.columns_2[1],
        query_params.columns_2[2], // all join columns from left table
        query_params.columns_2[1],
        query_params.columns_1[2], //  b.pos_start >= a.pos_end
        query_params.columns_2[1],
        query_params.columns_1[2], // b.pos_start-a.pos_end
        query_params.columns_2[2],
        query_params.columns_1[1], // b.pos_end <= a.pos_start
        query_params.columns_2[2],
        query_params.columns_1[1], // a.pos_start-b.pos_end
        RIGHT_TABLE,
        LEFT_TABLE,
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
        query_params.columns_1[0],
        query_params.columns_1[1],
        query_params.columns_1[2], // all join columns from right table
        query_params.columns_2[0],
        query_params.columns_2[1],
        query_params.columns_2[2], // all join columns from left table
        LEFT_TABLE,
        RIGHT_TABLE,
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
