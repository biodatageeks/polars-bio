from typing import Union

import pandas as pd
import polars as pl
from matplotlib import pyplot as plt


def visualize_base_sequence_quality(df: Union[pd.DataFrame, pl.DataFrame]) -> None:
    """
    Visualize the overlapping intervals.

    Parameters:
        df: Pandas DataFrame or Polars DataFrame. The DataFrame containing the base sequence quality results
    """
    assert isinstance(
        df, (pd.DataFrame, pl.DataFrame)
    ), "df must be a Pandas or Polars DataFrame"
    df = df if isinstance(df, pd.DataFrame) else df.to_pandas()
    df = df.sort_values(by="pos")

    boxes = [
        {
            "label": int(row["pos"]),
            "whislo": row["lower"],
            "q1": row["q1"],
            "med": row["median"],
            "q3": row["q3"],
            "whishi": row["upper"],
        }
        for _, row in df.iterrows()
    ]

    fig, ax = plt.subplots()
    fig.set_size_inches(15, 5)

    plot = ax.plot(df["pos"] + 1, df["avg"])
    box_plot = ax.bxp(boxes, showfliers=False)

    ax.set_title("base sequence quality")
    ax.set_ylabel("Phred score")
    ax.set_xlabel("Position in read (bp)")

    ax.legend(
        [plot[0], box_plot["medians"][0]],
        ["Average of phred score", "Median of phred score"],
    )

    for label in ax.get_xticklabels():
        label.set_fontsize(6)

    plt.show()
