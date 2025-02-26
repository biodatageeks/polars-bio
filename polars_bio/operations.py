from polars_bio.polars_bio import FilterOp, RangeOp

LEFT_TABLE = "s1"
RIGHT_TABLE = "s2"


def do_range_operation(ctx, range_options):
    if range_options.range_op == RangeOp.CountOverlaps:
        return do_count_overlaps(ctx, range_options)


def do_count_overlaps(ctx, range_options):
    contig1 = range_options.columns_1[0]
    pos_start1 = range_options.columns_1[1]
    pos_end1 = range_options.columns_1[2]
    contig2 = range_options.columns_2[0]
    pos_start2 = range_options.columns_2[1]
    pos_end2 = range_options.columns_2[2]
    suffix1, suffix2 = range_options.suffixes

    order1 = "DESC" if range_options.filter_op == FilterOp.Weak else "ASC"
    order2 = "ASC" if range_options.filter_op == FilterOp.Weak else "DESC"
    query = f"""
            SELECT
                chr AS {contig1}{suffix1},           -- contig
                s1ends2start AS {pos_start1}{suffix1},  -- pos_start
                s1starts2end AS {pos_end1}{suffix1},  -- pos_end
                st - ed AS count
            FROM (
                SELECT
                    chr,
                    SUM(iss1) OVER (
                        PARTITION BY chr ORDER BY s1starts2end ASC, iss1 {order1}
                    ) st,
                    SUM(iss1) OVER (
                        PARTITION BY chr ORDER BY s1ends2start ASC, iss1 {order2}
                    ) ed,
                    iss1,
                    s1starts2end,
                    s1ends2start
                FROM (
                    (SELECT
                        a.{contig1} AS chr, -- contig
                        a.{pos_start1} AS s1starts2end, -- pos_start
                        a.{pos_end1} AS s1ends2start, -- pos_end
                        1 AS iss1
                    FROM {LEFT_TABLE} AS a)
                    UNION ALL
                    (SELECT
                        b.{contig2} AS chr, -- contig
                        b.{pos_end2} AS s1starts2end, -- pos_end
                        b.{pos_start2} AS s1ends2start, -- pos_start
                        0 AS iss1
                    FROM {RIGHT_TABLE} AS b)
                )
            )
            WHERE
                iss1 = 0
        """
    return ctx.sql(query)
