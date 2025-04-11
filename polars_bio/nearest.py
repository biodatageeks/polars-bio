from __future__ import annotations

import datafusion
import pandas as pd
import polars as pl
from typing_extensions import TYPE_CHECKING, Union

from polars_bio.polars_bio import ReadOptions

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import ctx
from .range_op_helpers import _validate_overlap_input, range_operation

__all__ = ["nearest"]


if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import FilterOp, RangeOp, RangeOptions

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
    read_options: Union[ReadOptions, None] = None,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
    """
    Find pairs of closest genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table (see [register_vcf](api.md#polars_bio.register_vcf)). CSV with a header, BED and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table. CSV with a header, BED  and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). Strict for **0-based**, Weak for **1-based** coordinate systems.
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.
        read_options: Additional options for reading the input files.


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
    return range_operation(df1, df2, range_options, output_type, ctx, read_options)
