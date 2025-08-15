from pathlib import Path

import pandas as pd
import polars as pl

TEST_DIR = Path(__file__).parent
DATA_DIR = TEST_DIR / "data"

# Pandas
PD_DF_OVERLAP = pd.DataFrame(
    {
        "contig_1": [
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
        ],
        "pos_start_1": [
            150,
            150,
            190,
            190,
            300,
            500,
            15000,
            22000,
            150,
            150,
            190,
            190,
            300,
            500,
            15000,
            22000,
        ],
        "pos_end_1": [
            250,
            250,
            300,
            300,
            501,
            700,
            15000,
            22300,
            250,
            250,
            300,
            300,
            500,
            700,
            15000,
            22300,
        ],
        "contig_2": [
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
        ],
        "pos_start_2": [
            100,
            200,
            100,
            200,
            400,
            400,
            10000,
            22100,
            100,
            200,
            100,
            200,
            400,
            400,
            10000,
            22100,
        ],
        "pos_end_2": [
            190,
            290,
            190,
            290,
            600,
            600,
            20000,
            22100,
            190,
            290,
            190,
            290,
            600,
            600,
            20000,
            22100,
        ],
    }
).astype(
    {
        "pos_start_1": "int64",
        "pos_end_1": "int64",
        "pos_start_2": "int64",
        "pos_end_2": "int64",
    }
)

PD_DF_NEAREST = pd.DataFrame(
    {
        "contig_1": [
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr3",
        ],
        "pos_start_1": [100, 200, 400, 10000, 22100, 100, 200, 400, 10000, 22100, 100],
        "pos_end_1": [190, 290, 600, 20000, 22100, 190, 290, 600, 20000, 22100, 200],
        "contig_2": [
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr3",
        ],
        "pos_start_2": [150, 150, 300, 15000, 22000, 150, 150, 300, 15000, 22000, 234],
        "pos_end_2": [250, 250, 500, 15000, 22300, 250, 250, 501, 15000, 22300, 300],
        "distance": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 34],
    }
).astype(
    {
        "pos_start_1": "int64",
        "pos_end_1": "int64",
        "pos_start_2": "int64",
        "pos_end_2": "int64",
        "distance": "int64",
    }
)

PD_DF_MERGE = pd.DataFrame(
    {
        "contig": ["chr1", "chr1", "chr1", "chr1", "chr2", "chr2", "chr2", "chr2"],
        "pos_start": [100, 300, 10000, 22000, 100, 300, 10000, 22000],
        "pos_end": [300, 700, 20000, 22300, 300, 700, 20000, 22300],
        "n_intervals": [4, 3, 2, 2, 4, 3, 2, 2],
    }
).astype({"pos_start": "int64", "pos_end": "int64", "n_intervals": "int64"})

PD_DF_COUNT_OVERLAPS = pd.DataFrame(
    {
        "contig": [
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr1",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr2",
            "chr3",
        ],
        "pos_start": [100, 200, 400, 10000, 22100, 100, 200, 400, 10000, 22100, 100],
        "pos_end": [190, 290, 600, 20000, 22100, 190, 290, 600, 20000, 22100, 200],
        "count": [2, 2, 2, 1, 1, 2, 2, 2, 1, 1, 0],
    }
).astype({"pos_start": "int64", "pos_end": "int64", "count": "int64"})

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

DF_COVERAGE_PATH1 = f"{DATA_DIR}/coverage/targets.csv"
DF_COVERAGE_PATH2 = f"{DATA_DIR}/coverage/reads.csv"
PD_COVERAGE_DF1 = pd.read_csv(DF_COVERAGE_PATH1)
PD_COVERAGE_DF2 = pd.read_csv(DF_COVERAGE_PATH2)


BIO_DF_PATH1 = f"{DATA_DIR}/exons/*.parquet"
BIO_DF_PATH2 = f"{DATA_DIR}/fBrain-DS14718/*.parquet"

BIO_PD_DF1 = pd.read_parquet(f"{DATA_DIR}/exons/")
BIO_PD_DF2 = pd.read_parquet(f"{DATA_DIR}/fBrain-DS14718/")


# Polars
PL_DF_OVERLAP = pl.from_pandas(PD_DF_OVERLAP)
PL_DF1 = pl.from_pandas(PD_OVERLAP_DF1)
PL_DF2 = pl.from_pandas(PD_OVERLAP_DF2)

PL_DF_NEAREST = pl.from_pandas(PD_DF_NEAREST)
PL_NEAREST_DF1 = pl.from_pandas(PD_NEAREST_DF1)
PL_NEAREST_DF2 = pl.from_pandas(PD_NEAREST_DF2)

PL_DF_MERGE = pl.from_pandas(PD_DF_MERGE)
PL_MERGE_DF = pl.from_pandas(PD_MERGE_DF)

PL_DF_COUNT_OVERLAPS = pl.from_pandas(PD_DF_COUNT_OVERLAPS)
PL_COUNT_OVERLAPS_DF1 = pl.from_pandas(PD_COUNT_OVERLAPS_DF1)
PL_COUNT_OVERLAPS_DF2 = pl.from_pandas(PD_COUNT_OVERLAPS_DF2)
