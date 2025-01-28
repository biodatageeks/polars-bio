from __future__ import annotations

import pandas as pd
import polars as pl
from typing_extensions import TYPE_CHECKING, Union

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import ctx
from .range_op_helpers import _validate_overlap_input, range_operation
from .interval_op_helpers import read_df_to_datafusion, convert_result, get_py_ctx

import datafusion
from datafusion import col, literal

__all__ = ["overlap", "nearest", "merge"]


if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import FilterOp, RangeOp, RangeOptions


def overlap(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    how: str = "inner",
    overlap_filter: FilterOp = FilterOp.Strict,
    suffixes: tuple[str, str] = ("_1", "_2"),
    on_cols: Union[list[str], None] = None,
    cols1: Union[list[str], None] = ["chrom", "start", "end"],
    cols2: Union[list[str], None] = ["chrom", "start", "end"],
    algorithm: str = "Coitrees",
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        how: How to handle the overlaps on the two dataframes. inner: use intersection of the set of intervals from df1 and df2, optional.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        algorithm: The algorithm to use for the overlap operation.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming-out-of-core-processing) engine.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Note:
        1. The default output format, i.e.  [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html), is recommended for large datasets as it supports output streaming and lazy evaluation.
        This enables efficient processing of large datasets without loading the entire output dataset into memory.
        2. Streaming is only supported for polars.LazyFrame output.

    Example:
        ```python
        import polars_bio as pb
        import pandas as pd

        df1 = pd.DataFrame([
            ['chr1', 1, 5],
            ['chr1', 3, 8],
            ['chr1', 8, 10],
            ['chr1', 12, 14]],
        columns=['chrom', 'start', 'end']
        )

        df2 = pd.DataFrame(
        [['chr1', 4, 8],
         ['chr1', 10, 11]],
        columns=['chrom', 'start', 'end' ]
        )
        overlapping_intervals = pb.overlap(df1, df2, output_type="pandas.DataFrame")

        overlapping_intervals
            chrom_1         start_1     end_1 chrom_2       start_2  end_2
        0     chr1            1          5     chr1            4          8
        1     chr1            3          8     chr1            4          8

        ```

    Todo:
         Support for on_cols.
    """

    _validate_overlap_input(cols1, cols2, on_cols, suffixes, output_type, how)

    cols1 = DEFAULT_INTERVAL_COLUMNS if cols1 is None else cols1
    cols2 = DEFAULT_INTERVAL_COLUMNS if cols2 is None else cols2
    range_options = RangeOptions(
        range_op=RangeOp.Overlap,
        filter_op=overlap_filter,
        suffixes=suffixes,
        columns_1=cols1,
        columns_2=cols2,
        overlap_alg=algorithm,
        streaming=streaming,
    )
    return range_operation(df1, df2, range_options, output_type, ctx)


def nearest(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    suffixes: tuple[str, str] = ("_1", "_2"),
    on_cols: Union[list[str], None] = None,
    cols1: Union[list[str], None] = ["chrom", "start", "end"],
    cols2: Union[list[str], None] = ["chrom", "start", "end"],
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming-out-of-core-processing) engine.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Note:
        The default output format, i.e. [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html), is recommended for large datasets as it supports output streaming and lazy evaluation.
        This enables efficient processing of large datasets without loading the entire output dataset into memory.

    Example:

    Todo:
        Support for on_cols.
    """

    _validate_overlap_input(cols1, cols2, on_cols, suffixes, output_type, how="inner")

    cols1 = DEFAULT_INTERVAL_COLUMNS if cols1 is None else cols1
    cols2 = DEFAULT_INTERVAL_COLUMNS if cols2 is None else cols2
    range_options = RangeOptions(
        range_op=RangeOp.Nearest,
        filter_op=overlap_filter,
        suffixes=suffixes,
        columns_1=cols1,
        columns_2=cols2,
        streaming=streaming,
    )
    return range_operation(df1, df2, range_options, output_type, ctx)

def merge(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    min_dist: float = 0,
    cols: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Merge overlapping intervals. It is assumed that start < end.


    Parameters:
        df: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
        cols: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        on_cols: List of additional column names for clustering. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming-out-of-core-processing) engine.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Example:

    Todo:
        Support for on_cols.
    """

    _validate_overlap_input(cols, cols, on_cols, suffixes, output_type, how="inner")
    my_ctx = get_py_ctx()
    cols = DEFAULT_INTERVAL_COLUMNS if cols is None else cols
    contig = cols[0]
    start = cols[1]
    end = cols[2]
    
    on_cols = [] if on_cols is None else on_cols
    on_cols = [contig] + on_cols
    
    df = read_df_to_datafusion(my_ctx, df)

    # TODO: make sure to avoid conflicting column names
    start_end = "start_end"
    is_start_end = "is_start_or_end"
    current_intervals = "current_intervals"
    
    start_positions = df.select(*([col(start).alias(start_end), literal(1).alias(is_start_end)] + on_cols))
    end_positions = df.select(*([(col(end) + min_dist).alias(start_end), literal(-1).alias(is_start_end)] + on_cols))

    all_positions = start_positions.union(end_positions)
    sorting = [col(start_end).sort(), col(is_start_end).sort(ascending=(overlap_filter == FilterOp.Strict))]
    all_positions = all_positions.sort(*sorting)
    all_positions = all_positions.select(*([start_end, is_start_end,
        datafusion.functions.sum(col(is_start_end)).over(
            datafusion.expr.Window(
                partition_by=[col(c) for c in on_cols],
                order_by=sorting,
            )
        ).alias(current_intervals)] + on_cols))
    all_positions = all_positions.filter(
        ((col(current_intervals) == 0) & (col(is_start_end) == -1)) | ((col(current_intervals) == 1) & (col(is_start_end) == 1))
    )
    result = all_positions.select(*([(col(start_end) - min_dist).alias(end), is_start_end,
        datafusion.functions.lag(col(start_end), partition_by=[col(c) for c in on_cols]).alias(start)] + on_cols))
    result = result.filter(col(is_start_end) == -1)
    result = result.select(*([start, end] + on_cols))
    
    return convert_result(result, output_type, streaming)

