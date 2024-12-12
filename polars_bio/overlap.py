from __future__ import annotations

from pathlib import Path

import datafusion.dataframe
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from polars.io.plugins import register_io_source
from typing_extensions import TYPE_CHECKING, Union

from .polars_bio import BioSessionContext, OverlapFilter, overlap_frame, overlap_scan


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


if TYPE_CHECKING:
    from collections.abc import Iterator

DEFAULT_INTERVAL_COLUMNS = ["contig", "pos_start", "pos_end"]

ctx = Context().ctx


def overlap(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    how="inner",
    overlap_filter: OverlapFilter = OverlapFilter.Weak,
    suffixes=("_1", "_2"),
    on_cols=None,
    col1: Union[list[str] | None] = None,
    col2: Union[list[str] | None] = None,
    output_type: str = "polars.LazyFrame",
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters
    ----------
    :param df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
    :param df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
    :param how: How to handle the overlaps on the two dataframes. inner: use intersection of the set of intervals from df1 and df2, optional.
    :param overlap_filter: OverlapFilter, optional. The type of overlap to consider(Weak or Strict). default is OverlapFilter.Weak.
    :param col1: The names of columns containing the chromosome, start and end of the
    genomic intervals, provided separately for each set. The default
    values are 'contig', 'pos_start', 'pos_end'.
    :param col2:  The names of columns containing the chromosome, start and end of the
        genomic intervals, provided separately for each set. The default
        values are 'contig', 'pos_start', 'pos_end'.
    :param suffixes: (str, str), optional The suffixes for the columns of the two overlapped sets.
    :param on_cols: list[str], optional The list of additional column names to join on. default is None.
    :param output_type: str, optional The type of the output. default is "polars.LazyFrame".
    :return: **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.
    """
    # TODO: Add support for col1 and col2
    assert col1 is None, "col1 is not supported yet"
    assert col2 is None, "col2 is not supported yet"
    col1 = ["contig", "pos_start", "pos_end"] if col1 is None else col1
    col2 = ["contig", "pos_start", "pos_end"] if col2 is None else col2

    # TODO: Add support for on_cols ()
    assert on_cols is None, "on_cols is not supported yet"

    assert suffixes == ("_1", "_2"), "Only default suffixes are supported"
    assert output_type in [
        "polars.LazyFrame",
        "polars.DataFrame",
        "pandas.DataFrame",
    ], "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"

    assert how in ["inner"], "Only inner join is supported"
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
            return overlap_lazy_scan(
                df1, df2, merged_schema, overlap_filter=overlap_filter
            )
        elif output_type == "polars.DataFrame":
            return overlap_scan(ctx, df1, df2, overlap_filter).to_polars()
        elif output_type == "pandas.DataFrame":
            return overlap_scan(ctx, df1, df2, overlap_filter).to_pandas()
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
            return overlap_lazy_scan(
                df1, df2, merged_schema, col1, col2, overlap_filter
            )
        elif output_type == "polars.DataFrame":
            if isinstance(df1, pl.DataFrame) and isinstance(df2, pl.DataFrame):
                df1 = df1.to_arrow().to_reader()
                df2 = df2.to_arrow().to_reader()
            else:
                raise ValueError(
                    "Input and output dataframes must be of the same type: either polars or pandas"
                )
            return overlap_frame(ctx, df1, df2, overlap_filter).to_polars()
        elif output_type == "pandas.DataFrame":
            if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
                df1 = _df_to_arrow(df1, col1[0]).to_reader()
                df2 = _df_to_arrow(df2, col2[0]).to_reader()
            else:
                raise ValueError(
                    "Input and output dataframes must be of the same type: either polars or pandas"
                )
            return overlap_frame(ctx, df1, df2, overlap_filter).to_pandas()
    else:
        raise ValueError(
            "Both dataframes must be of the same type: either polars or pandas or a path to a file"
        )


def _rename_columns_pl(df: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return df.rename({col: f"{col}{suffix}" for col in df.columns})


def _rename_columns(
    df: Union[pl.DataFrame, pd.DataFrame], suffix: str
) -> Union[pl.DataFrame, pd.DataFrame]:
    if isinstance(df, pl.DataFrame):
        df = pl.DataFrame(schema=df.schema)
        return _rename_columns_pl(df, suffix)
    elif isinstance(df, pd.DataFrame):
        df = pl.from_pandas(pd.DataFrame(columns=df.columns))
        return _rename_columns_pl(df, suffix)
    else:
        raise ValueError("Only polars and pandas dataframes are supported")


def _get_schema(path: str, suffix=None) -> pl.Schema:
    ext = Path(path).suffix
    if ext == ".parquet":
        df = pl.read_parquet(path)
    elif ext == ".csv":
        df = pl.read_csv(path)
    else:
        raise ValueError("Only CSV and Parquet files are supported")
    if suffix is not None:
        df = _rename_columns(df, suffix)
    return df.schema


# since there is an error when Pandas DF are converted to Arrow, we need to use the following function
# to change the type of the columns to largestring (the problem is with the string type for
# larger datasets)
def _string_to_largestring(table: pa.Table, column_name: str) -> pa.Table:
    index = _get_column_index(table, column_name)
    return table.set_column(
        index,  # Index of the column to replace
        table.schema.field(index).name,  # Name of the column
        pc.cast(table.column(index), pa.large_string()),  # Cast to `largestring`
    )


def _get_column_index(table, column_name):
    try:
        return table.schema.names.index(column_name)
    except ValueError:
        raise KeyError(f"Column '{column_name}' not found in the table.")


def _df_to_arrow(df: pd.DataFrame, col: str) -> pa.Table:
    table_1 = pa.Table.from_pandas(df)
    return _string_to_largestring(table_1, col)


def overlap_lazy_scan(
    df_1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df_2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    schema: pl.Schema,
    col1: list[str] = None,
    col2: list[str] = None,
    overlap_filter: OverlapFilter = OverlapFilter.Weak,
) -> pl.LazyFrame:
    overlap_function = None
    if isinstance(df_1, str) and isinstance(df_2, str):
        overlap_function = overlap_scan
    elif isinstance(df_1, pl.DataFrame) and isinstance(df_2, pl.DataFrame):
        overlap_function = overlap_frame
        df_1 = df_1.to_arrow().to_reader()
        df_2 = df_2.to_arrow().to_reader()
    elif isinstance(df_1, pd.DataFrame) and isinstance(df_2, pd.DataFrame):
        overlap_function = overlap_frame
        df_1 = _df_to_arrow(df_1, col1[0]).to_reader()
        df_2 = _df_to_arrow(df_2, col2[0]).to_reader()
    else:
        raise ValueError("Only polars and pandas dataframes are supported")

    def _overlap_source(
        with_columns: pl.Expr | None,
        predicate: pl.Expr | None,
        _n_rows: int | None,
        _batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        df_lazy: datafusion.DataFrame = overlap_function(
            ctx, df_1, df_2, overlap_filter
        )
        df_stream = df_lazy.execute_stream()
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # TODO: We can push predicates down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if predicate is not None:
                df = df.filter(predicate)
            # TODO: We can push columns down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if with_columns is not None:
                df = df.select(with_columns)
            yield df

    return register_io_source(_overlap_source, schema=schema)
