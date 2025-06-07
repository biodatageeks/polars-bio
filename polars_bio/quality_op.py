from __future__ import annotations

import datafusion
import pandas as pd
import polars as pl
import pyarrow as pa

from pathlib import Path
from typing import Union
from .context import ctx

from polars_bio.polars_bio import (
    calc_base_sequance_quality_from_file,
    calc_base_sequance_quality_from_frame
)

def cacl_base_seq_quality(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    quality_scores_column: str = "quality_scores",
    output_type: str = "polars.DataFrame",
    target_partitions: int = 8,
) -> Union[pl.DataFrame, pd.DataFrame]:
    
    ctx.set_option("datafusion.execution.target_partitions", str(target_partitions))

    if isinstance(df, str):
        supported_exts = {".parquet", ".csv", ".bed", ".vcf", ".fastq"}
        ext = set(Path(df).suffixes)
        assert (
            len(supported_exts.intersection(ext)) > 0 or len(ext) == 0
        ), "Dataframe1 must be a Parquet, a BED or CSV or VCF or Fastq file"

        result: datafusion.DataFrame = calc_base_sequance_quality_from_file(
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
        
        result: datafusion.DataFrame = calc_base_sequance_quality_from_frame(
            ctx, arrow_table, quality_scores_column
        )

    if output_type == "polars.DataFrame":
        return result.to_polars()
    elif output_type == "pandas.DataFrame":
        return result.to_pandas()
    else:
        raise ValueError("output_type must be 'polars.DataFrame' or 'pandas.DataFrame'")