from __future__ import annotations

import pandas as pd
import polars as pl
from typing_extensions import TYPE_CHECKING, Union

from .range_op_helpers import Context, _validate_overlap_input, range_operation

if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import FilterOp, RangeOp, RangeOptions

DEFAULT_INTERVAL_COLUMNS = ["chrom", "start", "end"]

ctx = Context().ctx


def overlap(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    how: str = "inner",
    overlap_filter: FilterOp = FilterOp.Strict,
    suffixes: tuple[str, str] = ("_1", "_2"),
    on_cols=None,
    col1: Union[list[str], None] = None,
    col2: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
        how: How to handle the overlaps on the two dataframes. inner: use intersection of the set of intervals from df1 and df2, optional.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). default is FilterOp.Weak.
        col1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set. The default
            values are 'chrom', 'start', 'end'.
        col2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set. The default
            values are 'chrom', 'start', 'end'.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Note:
        The default output format, i.e.  [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html), is recommended for large datasets as it supports output streaming and lazy evaluation.
        This enables efficient processing of large datasets without loading the entire output dataset into memory.

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
         Support for col1, col2, and on_cols and  suffixes parameters.
    """

    _validate_overlap_input(col1, col2, on_cols, suffixes, output_type, how)

    col1 = DEFAULT_INTERVAL_COLUMNS if col1 is None else col1
    col2 = DEFAULT_INTERVAL_COLUMNS if col2 is None else col2
    range_options = RangeOptions(
        range_op=RangeOp.Overlap,
        filter_op=overlap_filter,
        suffixes=suffixes,
        columns_1=col1,
        columns_2=col2,
    )
    return range_operation(df1, df2, range_options, output_type, ctx)


def nearest(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    suffixes: tuple[str, str] = ("_1", "_2"),
    on_cols: Union[list[str], None] = None,
    col1: Union[list[str], None] = None,
    col2: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). default is FilterOp.Weak.
        col1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set. The default
            values are 'chrom', 'start', 'end'.
        col2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set. The default
            values are 'chrom', 'start', 'end'.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Note:
        The default output format, i.e. [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html), is recommended for large datasets as it supports output streaming and lazy evaluation.
        This enables efficient processing of large datasets without loading the entire output dataset into memory.

    Example:

    Todo:
        Support for col1, col2, and on_cols and  suffixes parameters.
    """

    _validate_overlap_input(col1, col2, on_cols, suffixes, output_type, how="inner")

    col1 = DEFAULT_INTERVAL_COLUMNS if col1 is None else col1
    col2 = DEFAULT_INTERVAL_COLUMNS if col2 is None else col2
    range_options = RangeOptions(
        range_op=RangeOp.Nearest,
        filter_op=overlap_filter,
        suffixes=suffixes,
        columns_1=col1,
        columns_2=col2,
    )
    return range_operation(df1, df2, range_options, output_type, ctx)
