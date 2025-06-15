from typing import Union

import pandas as pd
import polars as pl
from matplotlib import pyplot as plt

def visualize_base_sequence_quality(df: Union[pd.DataFrame, pl.DataFrame]) -> None:
    """
    Visualize base sequence quality metrics across positions.

    Parameters:
        df (Union[pd.DataFrame, pl.DataFrame]): A DataFrame containing base quality statistics
            for each position. Must include columns such as 'pos', 'avg', 'lower', 'q1', 'median', 'q3', and 'upper'.

    Returns:
        None. Displays a plot showing quality scores across base positions.
    """
    assert isinstance(
        df, (pd.DataFrame, pl.DataFrame)
    ), "df must be a Pandas or Polars DataFrame"

    df = df if isinstance(df, pd.DataFrame) else df.to_pandas()

    positions = df["pos"].tolist()
    boxes = []

    for _, row in df.iterrows():
        box = {
            'med': row["median"],
            'q1': row["q1"],
            'q3': row["q3"],
            'whislo': row["lower"],
            'whishi': row["upper"],
            'fliers': []
        }
        boxes.append(box)


    fig, ax = plt.subplots(figsize=(6, len(positions) // 5))

    ax.bxp(boxes, positions=positions, showfliers=False, vert=False, label='Median')
    ax.plot(df["avg"], df["pos"], 'r-', label="Average")

    ax.set_title("Base Sequence Quality by Position")
    ax.set_ylabel("Position in read (bp)")
    ax.set_xlabel("Phred Score")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="upper right")

    ax.tick_params(axis='y', labelsize=8)
    ax.invert_yaxis()

    plt.tight_layout()
    plt.show()
