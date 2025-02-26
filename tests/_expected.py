from pathlib import Path

import mdpd
import pandas as pd
import polars as pl

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"
EXPECTED_OVERLAP = """
        "+--------+-----------+---------+--------+-----------+---------+",
        "| contig_1 | pos_start_1 | pos_end_1 | contig_2 | pos_start_2 | pos_end_2 |",
        "+--------+-----------+---------+--------+-----------+---------+",
        "| chr1   | 150       | 250     | chr1   | 100       | 190     |",
        "| chr1   | 150       | 250     | chr1   | 200       | 290     |",
        "| chr1   | 190       | 300     | chr1   | 100       | 190     |",
        "| chr1   | 190       | 300     | chr1   | 200       | 290     |",
        "| chr1   | 300       | 501     | chr1   | 400       | 600     |",
        "| chr1   | 500       | 700     | chr1   | 400       | 600     |",
        "| chr1   | 15000     | 15000   | chr1   | 10000     | 20000   |",
        "| chr1   | 22000     | 22300   | chr1   | 22100     | 22100   |",
        "| chr2   | 150       | 250     | chr2   | 100       | 190     |",
        "| chr2   | 150       | 250     | chr2   | 200       | 290     |",
        "| chr2   | 190       | 300     | chr2   | 100       | 190     |",
        "| chr2   | 190       | 300     | chr2   | 200       | 290     |",
        "| chr2   | 300       | 500     | chr2   | 400       | 600     |",
        "| chr2   | 500       | 700     | chr2   | 400       | 600     |",
        "| chr2   | 15000     | 15000   | chr2   | 10000     | 20000   |",
        "| chr2   | 22000     | 22300   | chr2   | 22100     | 22100   |",
        "+--------+-----------+---------+--------+-----------+---------+",
"""

EXPECTED_NEAREST = """
        +--------+-----------+---------+--------+-----------+---------+----------+
        | contig_1 | pos_start_1 | pos_end_1 | contig_2 | pos_start_2 | pos_end_2 | distance |
        +--------+-----------+---------+--------+-----------+---------+----------+
        | chr2   | 100       | 190     | chr2   | 150       | 250     | 0        |
        | chr2   | 200       | 290     | chr2   | 150       | 250     | 0        |
        | chr2   | 400       | 600     | chr2   | 300       | 500     | 0        |
        | chr2   | 10000     | 20000   | chr2   | 15000     | 15000   | 0        |
        | chr2   | 22100     | 22100   | chr2   | 22000     | 22300   | 0        |
        | chr1   | 100       | 190     | chr1   | 150       | 250     | 0        |
        | chr1   | 200       | 290     | chr1   | 150       | 250     | 0        |
        | chr1   | 400       | 600     | chr1   | 300       | 501     | 0        |
        | chr1   | 10000     | 20000   | chr1   | 15000     | 15000   | 0        |
        | chr1   | 22100     | 22100   | chr1   | 22000     | 22300   | 0        |
        | chr3   | 100       | 200     | chr3   | 234       | 300     | 34       |
        +--------+-----------+---------+--------+-----------+---------+----------+
"""

EXPECTED_MERGE = """
| contig   |   pos_start |   pos_end |   n_intervals |
|:---------|------------:|----------:|--------------:|
| chr1     |         100 |       300 |             4 |
| chr1     |         300 |       700 |             3 |
| chr1     |       10000 |     20000 |             2 |
| chr1     |       22000 |     22300 |             2 |
| chr2     |         100 |       300 |             4 |
| chr2     |         300 |       700 |             3 |
| chr2     |       10000 |     20000 |             2 |
| chr2     |       22000 |     22300 |             2 |
"""

EXPECTED_COUNT_OVERLAPS = """
        +--------+-----------+---------+-------+
        | contig | pos_start | pos_end | count |
        +--------+-----------+---------+-------+
        | chr1   | 100       | 190     | 2     |
        | chr1   | 200       | 290     | 2     |
        | chr1   | 400       | 600     | 2     |
        | chr1   | 10000     | 20000   | 1     |
        | chr1   | 22100     | 22100   | 1     |
        | chr2   | 100       | 190     | 2     |
        | chr2   | 200       | 290     | 2     |
        | chr2   | 400       | 600     | 2     |
        | chr2   | 10000     | 20000   | 1     |
        | chr2   | 22100     | 22100   | 1     |
        | chr3   | 100       | 200     | 0     |
        +--------+-----------+---------+-------+
"""

# Pandas
PD_DF_OVERLAP = (
    mdpd.from_md(EXPECTED_OVERLAP)
    .astype({"pos_start_1": "int64"})
    .astype({"pos_end_1": "int64"})
    .astype({"pos_start_2": "int64"})
    .astype({"pos_end_2": "int64"})
)
PD_DF_NEAREST = (
    mdpd.from_md(EXPECTED_NEAREST)
    .astype({"pos_start_1": "int64"})
    .astype({"pos_end_1": "int64"})
    .astype({"pos_start_2": "int64"})
    .astype({"pos_end_2": "int64"})
    .astype({"distance": "int64"})
)
PD_DF_MERGE = (
    mdpd.from_md(EXPECTED_MERGE)
    .astype({"pos_start": "int64"})
    .astype({"pos_end": "int64"})
    .astype({"n_intervals": "int64"})
)
PD_DF_COUNT_OVERLAPS = (
    mdpd.from_md(EXPECTED_COUNT_OVERLAPS)
    .astype({"pos_start": "int64"})
    .astype({"pos_end": "int64"})
    .astype({"count": "int64"})
)


PD_DF_OVERLAP = PD_DF_OVERLAP.sort_values(by=list(PD_DF_OVERLAP.columns)).reset_index(
    drop=True
)
PD_DF_NEAREST = PD_DF_NEAREST.sort_values(by=list(PD_DF_NEAREST.columns)).reset_index(
    drop=True
)
PD_DF_MERGE = PD_DF_MERGE.sort_values(by=list(PD_DF_MERGE.columns)).reset_index(
    drop=True
)
PD_DF_COUNT_OVERLAPS = PD_DF_COUNT_OVERLAPS.sort_values(
    by=list(PD_DF_COUNT_OVERLAPS.columns)
).reset_index(drop=True)

DF_OVER_PATH1 = f"{DATA_DIR}/overlap/reads.csv"
DF_OVER_PATH2 = f"{DATA_DIR}/overlap/targets.csv"
PD_OVERLAP_DF1 = pd.read_csv(DF_OVER_PATH1)
PD_OVERLAP_DF2 = pd.read_csv(DF_OVER_PATH2)

DF_NEAREST_PATH1 = f"{DATA_DIR}/nearest/targets.csv"
DF_NEAREST_PATH2 = f"{DATA_DIR}/nearest/reads.csv"
PD_NEAREST_DF1 = pd.read_csv(DF_NEAREST_PATH1)
PD_NEAREST_DF2 = pd.read_csv(DF_NEAREST_PATH2)

DF_MERGE_PATH = f"{DATA_DIR}/merge/input.csv"
PD_MERGE_DF = pd.read_csv(DF_MERGE_PATH)
DF_COUNT_OVERLAPS_PATH1 = f"{DATA_DIR}/count_overlaps/targets.csv"
DF_COUNT_OVERLAPS_PATH2 = f"{DATA_DIR}/count_overlaps/reads.csv"
PD_COUNT_OVERLAPS_DF1 = pd.read_csv(DF_COUNT_OVERLAPS_PATH1)
PD_COUNT_OVERLAPS_DF2 = pd.read_csv(DF_COUNT_OVERLAPS_PATH2)


BIO_DF_PATH1 = f"{DATA_DIR}/exons/*.parquet"
BIO_DF_PATH2 = f"{DATA_DIR}/fBrain-DS14718/*.parquet"

BIO_PD_DF1 = pd.read_parquet(f"{DATA_DIR}/exons/")
BIO_PD_DF2 = pd.read_parquet(f"{DATA_DIR}/fBrain-DS14718/")


# Polars
PL_DF_OVERLAP = pl.DataFrame(PD_DF_OVERLAP)
PL_DF1 = pl.DataFrame(PD_OVERLAP_DF1)
PL_DF2 = pl.DataFrame(PD_OVERLAP_DF2)

PL_DF_NEAREST = pl.DataFrame(PD_DF_NEAREST)
PL_NEAREST_DF1 = pl.DataFrame(PD_NEAREST_DF1)
PL_NEAREST_DF2 = pl.DataFrame(PD_NEAREST_DF2)

PL_DF_MERGE = pl.DataFrame(PD_DF_MERGE)
PL_MERGE_DF = pl.DataFrame(PD_MERGE_DF)

PL_DF_COUNT_OVERLAPS = pl.DataFrame(PD_DF_COUNT_OVERLAPS)
PL_COUNT_OVERLAPS_DF1 = pl.DataFrame(PD_COUNT_OVERLAPS_DF1)
PL_COUNT_OVERLAPS_DF2 = pl.DataFrame(PD_COUNT_OVERLAPS_DF2)
