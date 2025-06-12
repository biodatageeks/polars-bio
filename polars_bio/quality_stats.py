from pathlib import Path
from typing import Union
import datafusion
import polars as pl
import pandas as pd
import pyarrow as pa
from .context import ctx
from polars_bio.polars_bio import (
    base_sequance_quality_scan,
    base_sequance_quality_frame,
)


def base_sequence_quality(
    df: Union[str, Path, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    quality_scores_column: str = "quality_scores",
    output_type: str = "polars.DataFrame",
    target_partitions: int = 8,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """
    Compute base sequence quality statistics from various dataframe/file types.

    Args:
        df: Input data as a file path or dataframe.
        quality_scores_column: Name of the column with quality scores.
        output_type: Output type, either "polars.DataFrame" or "pandas.DataFrame".

    Returns:
        DataFrame with base sequence quality statistics.
    """
    ctx.set_option(
        "datafusion.execution.target_partitions", str(target_partitions), False
    )

    if isinstance(df, (str, Path)):
        df = str(df)
        supported_exts = {".parquet", ".csv", ".bed", ".vcf", ".fastq"}
        ext = set(Path(df).suffixes)
        if not (supported_exts & ext or not ext):
            raise ValueError(
                "Input file must be a Parquet, CSV, BED, VCF, or FASTQ file."
            )
        result: datafusion.DataFrame = base_sequance_quality_scan(
            ctx, df, quality_scores_column
        )
    else:
        if isinstance(df, pl.LazyFrame):
            arrow_table = df.collect().to_arrow()
        elif isinstance(df, pl.DataFrame):
            arrow_table = df.to_arrow()
        elif isinstance(df, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(df)
        else:
            raise TypeError("Unsupported dataframe type.")
        result: datafusion.DataFrame = base_sequance_quality_frame(
            ctx, arrow_table, quality_scores_column
        )

    if output_type == "polars.DataFrame":
        return result.to_polars()
    elif output_type == "pandas.DataFrame":
        return result.to_pandas()
    else:
        raise ValueError("output_type must be 'polars.DataFrame' or 'pandas.DataFrame'")
