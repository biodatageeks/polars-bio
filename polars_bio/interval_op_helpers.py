from pathlib import Path

import datafusion
import pandas as pd
import polars as pl
import pyarrow as pa
from typing_extensions import Union

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import Context


def get_py_ctx() -> datafusion.context.SessionContext:
    return datafusion.context.SessionContext(Context().config)


def read_df_to_datafusion(
    py_ctx: datafusion.context.SessionContext,
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
) -> datafusion.dataframe:
    if isinstance(df, pl.DataFrame):
        return py_ctx.from_polars(df)
    elif isinstance(df, pd.DataFrame):
        return py_ctx.from_pandas(df)
    elif isinstance(df, pl.LazyFrame):
        return py_ctx.from_polars(df.collect())
    elif isinstance(df, str):
        ext = Path(df).suffix
        if ext == ".csv":
            return py_ctx.read_csv(df)
        elif ext == ".bed":
            return py_ctx.read_csv(
                df,
                has_header=False,
                delimited="\t",
                file_extension=".bed",
                schema=pa.schema(
                    [
                        (DEFAULT_INTERVAL_COLUMNS[0], pa.string()),
                        (DEFAULT_INTERVAL_COLUMNS[1], pa.int64()),
                        (DEFAULT_INTERVAL_COLUMNS[2], pa.int64()),
                    ]
                ),
            )
        else:
            return py_ctx.read_parquet(df)
    raise ValueError("Invalid `df` argument.")


def df_to_lazyframe(df: datafusion.DataFrame) -> pl.LazyFrame:
    # TODO: make it actually lazy
    """
    def _get_lazy(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None
    ) -> Iterator[pl.DataFrame]:

    return register_io_source(_overlap_source, schema=schema)
    """
    return df.to_polars().lazy()


def convert_result(
    df: datafusion.DataFrame, output_type: str, streaming: bool
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    # TODO: implement streaming
    if streaming:
        # raise NotImplementedError("streaming is not implemented")
        return df.to_polars().lazy()
    if output_type == "polars.DataFrame":
        return df.to_polars()
    elif output_type == "pandas.DataFrame":
        return df.to_pandas()
    elif output_type == "polars.LazyFrame":
        return df_to_lazyframe(df)
    raise ValueError("Invalid `output_type` argument")
