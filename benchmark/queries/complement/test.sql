CREATE EXTERNAL TABLE table_1
STORED AS CSV
LOCATION '/Users/mwiewior/research/git/polars-bio/benchmark/queries/complement/table_1.csv'
OPTIONS ('has_header' 'true');

CREATE EXTERNAL TABLE chrom_sizes
STORED AS CSV
LOCATION '/Users/mwiewior/research/git/polars-bio/benchmark/queries/complement/table_2.csv'
OPTIONS ('has_header' 'true');


WITH sorted_intervals AS (
    SELECT chrom, start, end
FROM table_1
ORDER BY chrom, start
    ),
    gaps AS (
SELECT
    si1.chrom,
    si1.end AS gap_start,
    si2.start AS gap_end
FROM sorted_intervals si1
    JOIN sorted_intervals si2
ON si1.chrom = si2.chrom AND si1.end < si2.start
WHERE NOT EXISTS (
    SELECT 1
    FROM sorted_intervals si3
    WHERE si3.chrom = si1.chrom
  AND si3.start < si2.start
  AND si3.start > si1.end
    )
    ),
    boundary_gaps AS (
SELECT
    cs.chrom,
    0 AS start,
    MIN(si.start) AS end
FROM chrom_sizes cs
    LEFT JOIN sorted_intervals si ON cs.chrom = si.chrom
GROUP BY cs.chrom

UNION ALL

SELECT
    cs.chrom,
    MAX(si.end) AS start,
    cs.size AS end
FROM chrom_sizes cs
    LEFT JOIN sorted_intervals si ON cs.chrom = si.chrom
GROUP BY cs.chrom, cs.size
    ),
    all_gaps AS (
SELECT chrom, gap_start AS start, gap_end AS end
FROM gaps

UNION ALL

SELECT chrom, start, end
FROM boundary_gaps
    )
SELECT chrom, start, end, chrom AS view_region
FROM all_gaps
ORDER BY chrom, start;
