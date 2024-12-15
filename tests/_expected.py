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


PD_DF_OVERLAP = PD_DF_OVERLAP.sort_values(by=list(PD_DF_OVERLAP.columns)).reset_index(
    drop=True
)
PD_DF_NEAREST = PD_DF_NEAREST.sort_values(by=list(PD_DF_NEAREST.columns)).reset_index(
    drop=True
)

DF_OVER_PATH1 = f"{DATA_DIR}/overlap/reads.csv"
DF_OVER_PATH2 = f"{DATA_DIR}/overlap/targets.csv"
PD_OVERLAP_DF1 = pd.read_csv(DF_OVER_PATH1)
PD_OVERLAP_DF2 = pd.read_csv(DF_OVER_PATH2)

DF_NEAREST_PATH1 = f"{DATA_DIR}/nearest/targets.csv"
DF_NEAREST_PATH2 = f"{DATA_DIR}/nearest/reads.csv"
PD_NEAREST_DF1 = pd.read_csv(DF_NEAREST_PATH1)
PD_NEAREST_DF2 = pd.read_csv(DF_NEAREST_PATH2)


BIO_PD_DF1 = pd.read_parquet(f"{DATA_DIR}/exons/")
BIO_PD_DF2 = pd.read_parquet(f"{DATA_DIR}/fBrain-DS14718/")


# Polars
PL_DF_OVERLAP = pl.DataFrame(PD_DF_OVERLAP)
PL_DF1 = pl.DataFrame(PD_OVERLAP_DF1)
PL_DF2 = pl.DataFrame(PD_OVERLAP_DF2)

PL_DF_NEAREST = pl.DataFrame(PD_DF_NEAREST)
PL_NEAREST_DF1 = pl.DataFrame(PD_NEAREST_DF1)
PL_NEAREST_DF2 = pl.DataFrame(PD_NEAREST_DF2)
