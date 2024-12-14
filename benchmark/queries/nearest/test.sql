SET sequila.prefer_interval_join TO false;

CREATE EXTERNAL TABLE table_A
STORED AS CSV
LOCATION '/Users/mwiewior/research/git/polars-bio/benchmark/queries/nearest/table_1.csv'
OPTIONS ('has_header' 'true');

CREATE EXTERNAL TABLE table_B
STORED AS CSV
LOCATION '/Users/mwiewior/research/git/polars-bio/benchmark/queries/nearest/table_2.csv'
OPTIONS ('has_header' 'true');


explain WITH Distances AS (
    SELECT
        A.chrom AS chrom_A,
        A.start AS start_A,
        A.end AS end_A,
        B.chrom AS chrom_B,
        B.start AS start_B,
        B.end AS end_B,
        CASE
            WHEN A.end < B.start THEN B.start - A.end
            WHEN B.end < A.start THEN A.start - B.end
            ELSE 0  -- Overlapping ranges
            END AS distance
    FROM
        table_A A
            JOIN
        table_B B
        ON
            A.chrom = B.chrom
),
     Nearest AS (
         SELECT
             chrom_A,
             start_A,
             end_A,
             chrom_B,
             start_B,
             end_B,
             distance,
             ROW_NUMBER() OVER (PARTITION BY chrom_A, start_A, end_A ORDER BY distance ASC) AS rank
         FROM
             Distances
     )
SELECT
    chrom_A,
    start_A,
    end_A,
    chrom_B,
    start_B,
    end_B,
    distance
FROM
    Nearest
WHERE
    rank = 2;
