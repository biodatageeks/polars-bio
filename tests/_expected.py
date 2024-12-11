import mdpd
from pathlib import Path

import pandas as pd

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

DF_OVERLAP = (mdpd.from_md(EXPECTED_OVERLAP)
              .astype({'pos_start_1': 'int64'})
              .astype({'pos_end_1': 'int64'})
              .astype({'pos_start_2': 'int64'})
              .astype({'pos_end_2': 'int64'}))

DF_OVERLAP = DF_OVERLAP.sort_values(by=list(DF_OVERLAP.columns)).reset_index(drop=True)



DF1 = pd.read_csv(f"{DATA_DIR}/reads.csv")
DF2 = pd.read_csv(f"{DATA_DIR}/targets.csv")