from pathlib import Path
from typing import Iterator, Union

import datafusion
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    BioSessionContext,
    InputFormat,
    RangeOptions,
    ReadOptions,
    py_read_table,
    py_register_table,
)

from .range_wrappers import range_operation_frame_wrapper, range_operation_scan_wrapper


def range_lazy_scan(
    df_1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df_2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    schema: pl.Schema,
    range_options: RangeOptions,
    ctx: BioSessionContext,
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
) -> pl.LazyFrame:
    range_function = None
    if isinstance(df_1, str) and isinstance(df_2, str):
        range_function = range_operation_scan_wrapper
    elif isinstance(df_1, pl.DataFrame) and isinstance(df_2, pl.DataFrame):
        range_function = range_operation_frame_wrapper
        df_1 = df_1.to_arrow().to_reader()
        df_2 = df_2.to_arrow().to_reader()
    elif isinstance(df_1, pd.DataFrame) and isinstance(df_2, pd.DataFrame):
        range_function = range_operation_frame_wrapper
        df_1 = _df_to_arrow(df_1, range_options.columns_1[0]).to_reader()
        df_2 = _df_to_arrow(df_2, range_options.columns_2[0]).to_reader()
    elif isinstance(df_1, pl.LazyFrame) and isinstance(df_2, pl.LazyFrame):
        range_function = range_operation_frame_wrapper
        df_1 = df_1.collect().to_arrow().to_reader()
        df_2 = df_2.collect().to_arrow().to_reader()
    else:
        raise ValueError("Only polars and pandas dataframes are supported")

    def _range_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        _n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        df_lazy: datafusion.DataFrame = (
            range_function(
                ctx, df_1, df_2, range_options, read_options1, read_options2, _n_rows
            )
            if isinstance(df_1, str) and isinstance(df_2, str)
            else range_function(ctx, df_1, df_2, range_options, _n_rows)
        )
        df_lazy.schema()
        df_stream = df_lazy.execute_stream()
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # # TODO: We can push predicates down to the DataFusion plan in the future,
            # #  but for now we'll do it here.
            # if predicate is not None:
            #     df = df.filter(predicate)
            # # TODO: We can push columns down to the DataFusion plan in the future,
            # #  but for now we'll do it here.
            # if with_columns is not None:
            #     df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

    return register_io_source(_range_source, schema=schema)


def _rename_columns_pl(df: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return df.rename({col: f"{col}{suffix}" for col in df.columns})


def _rename_columns(
    df: Union[pl.DataFrame, pd.DataFrame, pl.LazyFrame], suffix: str
) -> Union[pl.DataFrame, pd.DataFrame]:
    if isinstance(df, pl.DataFrame) or isinstance(df, pl.LazyFrame):
        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        df = pl.DataFrame(schema=schema)
        return _rename_columns_pl(df, suffix)
    elif isinstance(df, pd.DataFrame):
        df = pl.from_pandas(pd.DataFrame(columns=df.columns))
        return _rename_columns_pl(df, suffix)
    else:
        raise ValueError("Only polars and pandas dataframes are supported")


def _get_schema(
    path: str,
    ctx: BioSessionContext,
    suffix=None,
    read_options: Union[ReadOptions, None] = None,
) -> pl.Schema:
    ext = Path(path).suffixes
    if len(ext) == 0:
        df: DataFrame = py_read_table(ctx, path)
        arrow_schema = df.schema()
        empty_table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in arrow_schema],
            schema=arrow_schema,
        )
        df = pl.from_arrow(empty_table)

    elif ext[-1] == ".parquet":
        df = pl.read_parquet(path)
    elif ".csv" in ext:
        df = pl.read_csv(path)
    elif ".vcf" in ext:
        table = py_register_table(ctx, path, None, InputFormat.Vcf, read_options)
        df: DataFrame = py_read_table(ctx, table.name)
        arrow_schema = df.schema()
        empty_table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in arrow_schema],
            schema=arrow_schema,
        )
        df = pl.from_arrow(empty_table)
    else:
        raise ValueError("Only CSV and Parquet files are supported")
    if suffix is not None:
        df = _rename_columns(df, suffix)
    return df.schema


def _df_to_arrow(df: pd.DataFrame, col: str) -> pa.Table:
    table_1 = pa.Table.from_pandas(df)
    return _string_to_largestring(table_1, col)


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
