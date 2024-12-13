from pathlib import Path
from typing import Union

import pandas as pd
import polars as pl

from .polars_bio import (
    BioSessionContext,
    RangeOptions,
    range_operation_frame,
    range_operation_scan,
)
from .range_op_io import _df_to_arrow, _get_schema, _rename_columns, range_lazy_scan


def range_operation(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    suffixes: tuple[str, str],
    range_options: RangeOptions,
    col1: list[str],
    col2: list[str],
    output_type: str,
    ctx: BioSessionContext,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    if isinstance(df1, str) and isinstance(df2, str):
        ext1 = Path(df1).suffix
        assert (
            ext1 == ".parquet" or ext1 == ".csv"
        ), "Dataframe1 must be a Parquet or CSV file"
        ext2 = Path(df2).suffix
        assert (
            ext2 == ".parquet" or ext2 == ".csv"
        ), "Dataframe1 must be a Parquet or CSV file"
        # use suffixes to avoid column name conflicts
        df_schema1 = _get_schema(df2, suffixes[0])
        df_schema2 = _get_schema(df2, suffixes[1])
        merged_schema = pl.Schema({**df_schema1, **df_schema2})
        if output_type == "polars.LazyFrame":
            return range_lazy_scan(
                df1, df2, merged_schema, range_options=range_options, ctx=ctx
            )
        elif output_type == "polars.DataFrame":
            return range_operation_scan(ctx, df1, df2, range_options).to_polars()
        elif output_type == "pandas.DataFrame":
            return range_operation_scan(ctx, df1, df2, range_options).to_pandas()
        else:
            raise ValueError(
                "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"
            )
    elif (
        isinstance(df1, pl.DataFrame)
        and isinstance(df2, pl.DataFrame)
        or isinstance(df1, pl.LazyFrame)
        and isinstance(df2, pl.LazyFrame)
        or isinstance(df1, pd.DataFrame)
        and isinstance(df2, pd.DataFrame)
    ):
        if output_type == "polars.LazyFrame":
            merged_schema = pl.Schema(
                {
                    **_rename_columns(df1, suffixes[0]).schema,
                    **_rename_columns(df2, suffixes[1]).schema,
                }
            )
            return range_lazy_scan(
                df1, df2, merged_schema, col1, col2, range_options, ctx
            )
        elif output_type == "polars.DataFrame":
            if isinstance(df1, pl.DataFrame) and isinstance(df2, pl.DataFrame):
                df1 = df1.to_arrow().to_reader()
                df2 = df2.to_arrow().to_reader()
            else:
                raise ValueError(
                    "Input and output dataframes must be of the same type: either polars or pandas"
                )
            return range_operation_frame(ctx, df1, df2, range_options).to_polars()
        elif output_type == "pandas.DataFrame":
            if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
                df1 = _df_to_arrow(df1, col1[0]).to_reader()
                df2 = _df_to_arrow(df2, col2[0]).to_reader()
            else:
                raise ValueError(
                    "Input and output dataframes must be of the same type: either polars or pandas"
                )
            return range_operation_frame(ctx, df1, df2, range_options).to_pandas()
    else:
        raise ValueError(
            "Both dataframes must be of the same type: either polars or pandas or a path to a file"
        )


def singleton(cls):
    """Decorator to make a class a singleton."""
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class Context:
    def __init__(self):
        self.ctx = BioSessionContext()
        self.ctx.set_option("datafusion.execution.target_partitions", "1")
