from typing import Union
import pandas as pd
import polars as pl
from matplotlib import pyplot as plt

def plot_base_quality(
    metrics_df: Union[pd.DataFrame, pl.DataFrame], 
    label: str = "Base Quality per Position"
) -> None:
    """
    Visualize base quality metrics as box plots per position.

    Parameters:
        metrics_df: Pandas DataFrame or Polars DataFrame. The DataFrame containing 
                    quality metrics with columns ['position', 'min', 'q1', 'median', 'q3', 'max']
        label: Title for the plot (default: "Base Quality per Position")
    """
    assert isinstance(
        metrics_df, (pd.DataFrame, pl.DataFrame)
    ), "metrics_df must be a Pandas or Polars DataFrame"
    
    # Convert to pandas if polars DataFrame
    df = metrics_df if isinstance(metrics_df, pd.DataFrame) else metrics_df.to_pandas()

    positions = df['position']
    min_vals = df['min']
    q1_vals = df['q1']
    median_vals = df['median']
    q3_vals = df['q3']
    max_vals = df['max']

    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot median line
    ax.plot(positions, median_vals, color='blue', label='Median')
    # Fill between Q1 and Q3
    ax.fill_between(positions, q1_vals, q3_vals, color='lightblue', alpha=0.5, label='IQR (Q1-Q3)')
    # Plot min and max as whiskers
    ax.vlines(positions, min_vals, max_vals, color='gray', alpha=0.7, label='Min-Max')

    ax.set_xlabel('Position in Read')
    ax.set_ylabel('Phred Quality Score')
    ax.set_title(label)
    ax.legend()
    ax.grid(True)
    plt.show()
