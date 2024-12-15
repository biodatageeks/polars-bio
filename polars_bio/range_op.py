from __future__ import annotations

import pandas as pd
import polars as pl
from typing_extensions import TYPE_CHECKING, Union

from .polars_bio import FilterOp, RangeOp, RangeOptions
from .range_op_helpers import Context, _validate_overlap_input, range_operation

if TYPE_CHECKING:
    pass

DEFAULT_INTERVAL_COLUMNS = ["contig", "pos_start", "pos_end"]

ctx = Context().ctx


def overlap(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    how="inner",
    overlap_filter: FilterOp = FilterOp.Weak,
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
    :param overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). default is FilterOp.Weak.
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

    _validate_overlap_input(col1, col2, on_cols, suffixes, output_type, how)

    col1 = ["contig", "pos_start", "pos_end"] if col1 is None else col1
    col2 = ["contig", "pos_start", "pos_end"] if col2 is None else col2
    range_options = RangeOptions(range_op=RangeOp.Overlap, filter_op=overlap_filter)
    return range_operation(
        df1, df2, suffixes, range_options, col1, col2, output_type, ctx
    )


def nearest(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Weak,
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
    :param overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). default is FilterOp.Weak.
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

    _validate_overlap_input(col1, col2, on_cols, suffixes, output_type, how="inner")

    col1 = ["contig", "pos_start", "pos_end"] if col1 is None else col1
    col2 = ["contig", "pos_start", "pos_end"] if col2 is None else col2
    range_options = RangeOptions(range_op=RangeOp.Nearest, filter_op=overlap_filter)
    return range_operation(
        df1, df2, suffixes, range_options, col1, col2, output_type, ctx
    )
