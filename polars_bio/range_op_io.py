import logging
from pathlib import Path
from typing import Iterator, Union

import datafusion
import polars as pl
import pyarrow as pa
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
    range_operation_frame,
    range_operation_scan,
)

try:
    import pandas as pd
except ImportError:
    pd = None


def range_lazy_scan(
    df_1: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    df_2: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    schema: pl.Schema,
    range_options: RangeOptions,
    ctx: BioSessionContext,
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
    projection_pushdown: bool = False,
) -> pl.LazyFrame:
    range_function = None
    if isinstance(df_1, str) and isinstance(df_2, str):
        range_function = range_operation_scan
    else:
        range_function = range_operation_frame
        df_1 = _df_to_reader(df_1, range_options.columns_1[0])
        df_2 = _df_to_reader(df_2, range_options.columns_2[0])

    def _range_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        _n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # Extract projected columns if projection pushdown is enabled
        projected_columns = None
        if projection_pushdown and with_columns is not None:
            from .io import _extract_column_names_from_expr

            projected_columns = _extract_column_names_from_expr(with_columns)

        # Apply projection pushdown to range options if enabled
        modified_range_options = range_options
        if projection_pushdown and projected_columns:
            # Create a copy of range options with projection information
            # This is where we would modify the SQL generation in a full implementation
            modified_range_options = range_options

        # Announce chosen algorithm for overlap at execution time
        try:
            alg = getattr(modified_range_options, "overlap_alg", None)
            if alg is not None:
                logging.info(
                    "Optimizing into IntervalJoinExec using %s algorithm",
                    alg,
                )
        except Exception:
            pass

        df_lazy: datafusion.DataFrame = (
            range_function(
                ctx,
                df_1,
                df_2,
                modified_range_options,
                read_options1,
                read_options2,
                _n_rows,
            )
            if isinstance(df_1, str) and isinstance(df_2, str)
            else range_function(
                ctx,
                df_1,
                df_2,
                modified_range_options,
                _n_rows,
            )
        )

        # Apply DataFusion-level projection if enabled
        datafusion_projection_applied = False
        if projection_pushdown and projected_columns:
            try:
                # Try to select only the requested columns at the DataFusion level
                df_lazy = df_lazy.select(projected_columns)
                datafusion_projection_applied = True
            except Exception:
                # Fallback to Python-level selection if DataFusion selection fails
                datafusion_projection_applied = False

        df_lazy.schema()
        df_stream = df_lazy.execute_stream()
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # Handle predicate and column projection
            if predicate is not None:
                df = df.filter(predicate)
            # Apply Python-level projection if DataFusion projection failed or projection pushdown is disabled
            if with_columns is not None and (
                not projection_pushdown or not datafusion_projection_applied
            ):
                df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

    return register_io_source(_range_source, schema=schema)


def _rename_columns_pl(df: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return df.rename({col: f"{col}{suffix}" for col in df.columns})


def _rename_columns(
    df: Union[pl.DataFrame, "pd.DataFrame", pl.LazyFrame], suffix: str
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    if isinstance(df, pl.DataFrame) or isinstance(df, pl.LazyFrame):
        schema = df.collect_schema() if isinstance(df, pl.LazyFrame) else df.schema
        df = pl.DataFrame(schema=schema)
        return _rename_columns_pl(df, suffix)
    elif pd and isinstance(df, pd.DataFrame):
        # Convert to polars while preserving dtypes, then create empty DataFrame with correct schema
        polars_df = pl.from_pandas(df)
        df = pl.DataFrame(schema=polars_df.schema)
        return _rename_columns_pl(df, suffix)
    elif hasattr(df, "_base_lf") and hasattr(df, "collect_schema"):
        # Handle GffLazyFrameWrapper or similar wrapper classes
        schema = df.collect_schema()
        df = pl.DataFrame(schema=schema)
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


# since there is an error when Pandas DF are converted to Arrow, we need to use
# the following function to change the type of the columns to largestring (the
# problem is with the string type for larger datasets)


def _string_to_largestring(table: pa.Table, column_name: str) -> pa.Table:
    index = _get_column_index(table, column_name)
    return table.set_column(
        index,
        table.schema.field(index).name,
        pa.compute.cast(table.column(index), pa.large_string()),
    )


def _get_column_index(table: pa.Table, column_name: str) -> int:
    try:
        return table.schema.names.index(column_name)
    except ValueError as exc:
        raise KeyError(f"Column '{column_name}' not found in the table.") from exc


def _df_to_reader(
    df: Union[pl.DataFrame, "pd.DataFrame", pl.LazyFrame],
    col: str,
) -> pa.RecordBatchReader:
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if isinstance(df, pl.DataFrame):
        arrow_tbl = df.to_arrow()
    elif pd and isinstance(df, pd.DataFrame):
        arrow_tbl = pa.Table.from_pandas(df)
        arrow_tbl = _string_to_largestring(arrow_tbl, col)
    else:
        raise ValueError("Only polars and pandas are supported")
    return arrow_tbl.to_reader()
