from typing import Union

import bioframe as bf
import pandas as pd
import polars as pl
from matplotlib import pyplot as plt


class QCUtils:
    @staticmethod
    def visualize_mean_quality(
        df: Union[pd.DataFrame, pl.DataFrame], label: str = "mean quality scores"
    ) -> None:
        """
        Visualize mean quality scores

        Parameters:
            df: Pandas DataFrame or Polars DataFrame. The DataFrame containing mean quality scores
            label: TBD

        """
        assert isinstance(
            df, (pd.DataFrame, pl.DataFrame)
        ), "df must be a Pandas or Polars DataFrame"
        df = df if isinstance(df, pd.DataFrame) else df.to_pandas()
        if "mean_c" not in df.columns:
            raise ValueError("DataFrame must contain a 'mean_c' column")

        plt.figure(figsize=(10, 4))
        plt.plot(df["mean_c"], label=label)
        plt.title(label)
        plt.xlabel("Read index")
        plt.ylabel("Mean quality score")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()

    @staticmethod
    def visualize_mean_quality_histogram(
        df: Union[pd.DataFrame, pl.DataFrame], label: str = "mean quality scores histogram"
    ) -> None:
        """
        Visualize mean quality scores histogram

        Parameters:
            df: Pandas DataFrame or Polars DataFrame. The DataFrame containing mean quality scores histogram
            label: TBD

        """
        assert isinstance(
            df, (pd.DataFrame, pl.DataFrame)
        ), "df must be a Pandas or Polars DataFrame"
        df = df if isinstance(df, pd.DataFrame) else df.to_pandas()
        
        if not {"bin_start", "count"}.issubset(df.columns):
            raise ValueError("DataFrame must contain 'bin_start' and 'count' columns")

        plt.figure(figsize=(10, 4))
        plt.bar(df["bin_start"], df["count"], width=1.0, align='center')
        plt.title(label)
        plt.xlabel("Mean quality score (binned)")
        plt.ylabel("Count")
        plt.grid(axis="y")
        plt.tight_layout()
        plt.show()