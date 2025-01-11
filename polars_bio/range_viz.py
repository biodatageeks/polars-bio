from typing import Union

import bioframe as bf
import pandas as pd
import polars as pl
from matplotlib import pyplot as plt


def visualize_intervals(
    df: Union[pd.DataFrame, pl.DataFrame], label: str = "overlapping pair"
) -> None:
    """
    Visualize the overlapping intervals.

    Parameters:
        df: Pandas DataFrame or Polars DataFrame. The DataFrame containing the overlapping intervals
        label: TBD

    """
    assert isinstance(
        df, (pd.DataFrame, pl.DataFrame)
    ), "df must be a Pandas or Polars DataFrame"
    df = df if isinstance(df, pd.DataFrame) else df.to_pandas()
    for i, reg_pair in df.iterrows():
        bf.vis.plot_intervals_arr(
            starts=[reg_pair.start_1, reg_pair.start_2],
            ends=[reg_pair.end_1, reg_pair.end_2],
            colors=["skyblue", "lightpink"],
            levels=[2, 1],
            xlim=(0, 16),
            show_coords=True,
        )
        plt.title(f"{label} #{i}")
