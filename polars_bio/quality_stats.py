from pathlib import Path
from typing import Union
import polars as pl
import pandas as pd
import pyarrow as pa
from .context import ctx
from polars_bio.polars_bio import my_scan, my_frame


def base_sequence_quality(df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame]):
    if isinstance(df, str):
        supported_exts = set([".parquet", ".csv", ".bed", ".vcf"])
        ext = set(Path(df).suffixes)
        assert (
            len(supported_exts.intersection(ext)) > 0 or len(ext) == 0
        ), "Dataframe1 must be a Parquet, a BED or CSV or VCF file"
        return my_scan(ctx, df)
    else:
        if isinstance(df, pl.DataFrame):
            df = df.to_arrow().to_reader()
        elif isinstance(df, pd.DataFrame):
            df = pa.Table.from_pandas(df)
        elif isinstance(df, pl.LazyFrame):
            df = df.collect().to_arrow().to_reader()
        return my_frame(ctx, df)
