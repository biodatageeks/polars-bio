import logging
from pathlib import Path
from typing import Callable, Iterator, Union

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
    range_operation_lazy,
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
    use_file_paths = isinstance(df_1, str) and isinstance(df_2, str)
    use_lazy_sources = _is_lazyframe_like(df_1) or _is_lazyframe_like(df_2)

    if use_file_paths:
        range_function = range_operation_scan
        stored_df1, stored_df2 = df_1, df_2
        stored_arrow_tbl1 = stored_arrow_tbl2 = None
        lazy_sources = None
    elif use_lazy_sources:
        range_function = range_operation_lazy
        col1, col2 = range_options.columns_1[0], range_options.columns_2[0]
        lazy_sources = (
            _prepare_lazy_stream_input(df_1, col1),
            _prepare_lazy_stream_input(df_2, col2),
        )
        stored_df1 = stored_df2 = None
        stored_arrow_tbl1 = stored_arrow_tbl2 = None
    else:
        range_function = range_operation_frame
        col1, col2 = range_options.columns_1[0], range_options.columns_2[0]

        if isinstance(df_1, pl.DataFrame):
            stored_arrow_tbl1 = df_1.to_arrow()
            stored_df1 = df_1
        elif pd is not None and isinstance(df_1, pd.DataFrame):
            stored_arrow_tbl1 = pa.Table.from_pandas(df_1)
            stored_arrow_tbl1 = _string_to_largestring(stored_arrow_tbl1, col1)
            stored_df1 = df_1
        else:
            raise ValueError("df_1 must be a Polars DataFrame or Pandas DataFrame")

        if isinstance(df_2, pl.DataFrame):
            stored_arrow_tbl2 = df_2.to_arrow()
            stored_df2 = df_2
        elif pd is not None and isinstance(df_2, pd.DataFrame):
            stored_arrow_tbl2 = pa.Table.from_pandas(df_2)
            stored_arrow_tbl2 = _string_to_largestring(stored_arrow_tbl2, col2)
            stored_df2 = df_2
        else:
            raise ValueError("df_2 must be a Polars DataFrame or Pandas DataFrame")
        lazy_sources = None

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

        # For file paths, use stored paths directly.
        # For LazyFrames, create fresh iterators from collect_batches().
        # For DataFrames, create fresh Arrow readers from stored tables.
        if use_file_paths:
            df_lazy: datafusion.DataFrame = range_function(
                ctx,
                stored_df1,
                stored_df2,
                modified_range_options,
                read_options1,
                read_options2,
                _n_rows,
            )
        elif use_lazy_sources:
            assert lazy_sources is not None
            left_schema, left_stream_factory = lazy_sources[0]
            right_schema, right_stream_factory = lazy_sources[1]
            # Call factories to get fresh streams - allows LazyFrame to be collected multiple times
            # Rust extracts Arrow C Stream via __arrow_c_stream__ protocol
            df_lazy = range_function(
                ctx,
                left_stream_factory(),
                right_stream_factory(),
                left_schema,
                right_schema,
                modified_range_options,
                _n_rows,
            )
        else:
            # Create fresh readers from pre-converted Arrow tables.
            # Arrow tables were converted in the main thread (thread-safe).
            # Creating readers from tables is thread-safe.
            reader1 = stored_arrow_tbl1.to_reader()
            reader2 = stored_arrow_tbl2.to_reader()
            df_lazy: datafusion.DataFrame = range_function(
                ctx,
                reader1,
                reader2,
                modified_range_options,
                _n_rows,
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


def _is_lazyframe_like(df: object) -> bool:
    """Return True for Polars LazyFrames or wrappers exposing collect_batches."""

    if isinstance(df, pl.LazyFrame):
        return True
    return hasattr(df, "collect_batches") and hasattr(df, "collect_schema")


def _prepare_lazy_stream_input(
    df: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    contig_col: str,
) -> tuple[pa.Schema, Callable[[], object]]:
    """Prepare schema + factory for Arrow C Stream exportable objects.

    Returns (arrow_schema, stream_factory) where stream_factory() returns an object
    that implements the Arrow C Stream protocol (__arrow_c_stream__). Rust receives
    the stream via PyO3's PyArrowType which automatically extracts the Arrow C Stream.

    A factory is returned (instead of the stream directly) because Arrow C Streams
    can only be consumed once. This allows the returned LazyFrame to be collected
    multiple times - each collect() will create a fresh stream.

    For LazyFrames, this uses Polars' ArrowStreamExportable feature (>= 1.37.0)
    via collect_batches(lazy=True)._inner, enabling GIL-free streaming:
    - Single GIL acquisition when exporting the stream to Rust
    - All subsequent batch processing happens in pure Rust without GIL
    - True streaming execution - batches are computed on-demand

    For DataFrames, this exports via to_arrow().to_reader() which also supports
    the Arrow C Stream protocol for zero-copy FFI transfer.

    Note: The schema is extracted from the actual stream (not from Polars schema)
    to ensure type compatibility (e.g., Utf8View vs LargeUtf8).
    """
    if isinstance(df, str):
        raise ValueError(
            "File path inputs must be provided for both arguments to use scan-based streaming."
        )

    if isinstance(df, pl.LazyFrame) or _is_lazyframe_like(df):
        # Get schema from a temporary stream to ensure type compatibility
        # (Polars may use Utf8View which differs from LargeUtf8 in empty DataFrame)
        temp_batches = df.collect_batches(lazy=True, engine="streaming")
        arrow_schema = pa.RecordBatchReader.from_stream(temp_batches._inner).schema

        # Return a factory that creates a fresh stream each time
        # This allows the LazyFrame result to be collected multiple times
        def stream_factory():
            batches = df.collect_batches(lazy=True, engine="streaming")
            return batches._inner

        return arrow_schema, stream_factory

    if isinstance(df, pl.DataFrame):
        # DataFrame -> Arrow table (stored for reuse)
        arrow_table = df.to_arrow()
        arrow_schema = arrow_table.schema

        # Factory creates a fresh reader each time
        def stream_factory():
            return arrow_table.to_reader()

        return arrow_schema, stream_factory

    if pd is not None and isinstance(df, pd.DataFrame):
        polars_df = pl.from_pandas(df)
        polars_df = polars_df.with_columns(
            [pl.col(contig_col).cast(pl.Utf8)]
            if contig_col in polars_df.columns
            else []
        )
        arrow_table = polars_df.to_arrow()
        arrow_schema = arrow_table.schema

        # Factory creates a fresh reader each time
        def stream_factory():
            return arrow_table.to_reader()

        return arrow_schema, stream_factory

    raise ValueError(
        "Inputs must be Polars LazyFrame/DataFrame or Pandas DataFrame for streaming operations"
    )


def _schema_to_arrow(schema: pl.Schema) -> pa.Schema:
    """Convert a Polars schema to a PyArrow schema without materializing data."""

    empty_df = pl.DataFrame(schema=schema)
    return empty_df.to_arrow().schema


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
