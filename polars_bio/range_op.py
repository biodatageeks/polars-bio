from __future__ import annotations

import datafusion
import pandas as pd
import polars as pl
from datafusion import col, literal
from typing_extensions import TYPE_CHECKING, Union

from polars_bio.polars_bio import ReadOptions

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import ctx
from .interval_op_helpers import convert_result, get_py_ctx, read_df_to_datafusion
from .range_op_helpers import _validate_overlap_input, range_operation

__all__ = ["overlap", "nearest", "count_overlaps", "merge"]


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
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table (see [register_vcf](api.md#polars_bio.register_vcf)). CSV with a header, BED and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table. CSV with a header, BED  and Parquet are supported.
        how: How to handle the overlaps on the two dataframes. inner: use intersection of the set of intervals from df1 and df2, optional.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). Strict for **0-based**, Weak for **1-based** coordinate systems.
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        suffixes: Suffixes for the columns of the two overlapped sets.
        on_cols: List of additional column names to join on. default is None.
        algorithm: The algorithm to use for the overlap operation.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.
        read_options1: Additional options for reading the input files.
        read_options2: Additional options for reading the input files.

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
    return range_operation(
        df1, df2, range_options, output_type, ctx, read_options1, read_options2
    )


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


def coverage(
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
    Calculate intervals coverage.
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
        range_op=RangeOp.Coverage,
        filter_op=overlap_filter,
        suffixes=suffixes,
        columns_1=cols1,
        columns_2=cols2,
        streaming=streaming,
    )
    return range_operation(df2, df1, range_options, output_type, ctx, read_options)


def count_overlaps(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    suffixes: tuple[str, str] = ("", "_"),
    cols1: Union[list[str], None] = ["chrom", "start", "end"],
    cols2: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
    naive_query: bool = True,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
    """
    Count pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table (see [register_vcf](api.md#polars_bio.register_vcf)). CSV with a header, BED and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table. CSV with a header, BED  and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). Strict for **0-based**, Weak for **1-based** coordinate systems.
        suffixes: Suffixes for the columns of the two overlapped sets.
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
        naive_query: If True, use naive query for counting overlaps based on overlaps.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.
    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

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
        counts = pb.count_overlaps(df1, df2, output_type="pandas.DataFrame")

        counts

        chrom  start  end  count
        0  chr1      1    5      1
        1  chr1      3    8      1
        2  chr1      8   10      0
        3  chr1     12   14      0
        ```

    Todo:
         Support return_input.
    """
    _validate_overlap_input(cols1, cols2, on_cols, suffixes, output_type, how="inner")
    my_ctx = get_py_ctx()
    on_cols = [] if on_cols is None else on_cols
    cols1 = DEFAULT_INTERVAL_COLUMNS if cols1 is None else cols1
    cols2 = DEFAULT_INTERVAL_COLUMNS if cols2 is None else cols2
    if naive_query:
        range_options = RangeOptions(
            range_op=RangeOp.CountOverlapsNaive,
            filter_op=overlap_filter,
            suffixes=suffixes,
            columns_1=cols1,
            columns_2=cols2,
            streaming=streaming,
        )
        return range_operation(df2, df1, range_options, output_type, ctx)
    df1 = read_df_to_datafusion(my_ctx, df1)
    df2 = read_df_to_datafusion(my_ctx, df2)

    # TODO: guarantee no collisions
    s1start_s2end = "s1starts2end"
    s1end_s2start = "s1ends2start"
    contig = "contig"
    count = "count"
    starts = "starts"
    ends = "ends"
    is_s1 = "is_s1"
    suff, _ = suffixes
    df1, df2 = df2, df1
    df1 = df1.select(
        *(
            [
                literal(1).alias(is_s1),
                col(cols1[1]).alias(s1start_s2end),
                col(cols1[2]).alias(s1end_s2start),
                col(cols1[0]).alias(contig),
            ]
            + on_cols
        )
    )
    df2 = df2.select(
        *(
            [
                literal(0).alias(is_s1),
                col(cols2[2]).alias(s1end_s2start),
                col(cols2[1]).alias(s1start_s2end),
                col(cols2[0]).alias(contig),
            ]
            + on_cols
        )
    )

    df = df1.union(df2)

    partitioning = [col(contig)] + [col(c) for c in on_cols]
    df = df.select(
        *(
            [
                s1start_s2end,
                s1end_s2start,
                contig,
                is_s1,
                datafusion.functions.sum(col(is_s1))
                .over(
                    datafusion.expr.Window(
                        partition_by=partitioning,
                        order_by=[
                            col(s1start_s2end).sort(),
                            col(is_s1).sort(
                                ascending=(overlap_filter == FilterOp.Strict)
                            ),
                        ],
                    )
                )
                .alias(starts),
                datafusion.functions.sum(col(is_s1))
                .over(
                    datafusion.expr.Window(
                        partition_by=partitioning,
                        order_by=[
                            col(s1end_s2start).sort(),
                            col(is_s1).sort(
                                ascending=(overlap_filter == FilterOp.Weak)
                            ),
                        ],
                    )
                )
                .alias(ends),
            ]
            + on_cols
        )
    )
    df = df.filter(col(is_s1) == 0)
    df = df.select(
        *(
            [
                col(contig).alias(cols1[0] + suff),
                col(s1end_s2start).alias(cols1[1] + suff),
                col(s1start_s2end).alias(cols1[2] + suff),
            ]
            + on_cols
            + [(col(starts) - col(ends)).alias(count)]
        )
    )

    return convert_result(df, output_type, streaming)


def merge(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    min_dist: float = 0,
    cols: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
    """
    Merge overlapping intervals. It is assumed that start < end.


    Parameters:
        df: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict). Strict for **0-based**, Weak for **1-based** coordinate systems.
        cols: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        on_cols: List of additional column names for clustering. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Example:

    Todo:
        Support for on_cols.
    """
    suffixes = ("_1", "_2")
    _validate_overlap_input(cols, cols, on_cols, suffixes, output_type, how="inner")

    my_ctx = get_py_ctx()
    cols = DEFAULT_INTERVAL_COLUMNS if cols is None else cols
    contig = cols[0]
    start = cols[1]
    end = cols[2]

    on_cols = [] if on_cols is None else on_cols
    on_cols = [contig] + on_cols

    df = read_df_to_datafusion(my_ctx, df)
    df_schema = df.schema()
    start_type = df_schema.field(start).type
    end_type = df_schema.field(end).type
    # TODO: make sure to avoid conflicting column names
    start_end = "start_end"
    is_start_end = "is_start_or_end"
    current_intervals = "current_intervals"
    n_intervals = "n_intervals"

    end_positions = df.select(
        *(
            [(col(end) + min_dist).alias(start_end), literal(-1).alias(is_start_end)]
            + on_cols
        )
    )
    start_positions = df.select(
        *([col(start).alias(start_end), literal(1).alias(is_start_end)] + on_cols)
    )
    all_positions = start_positions.union(end_positions)
    start_end_type = all_positions.schema().field(start_end).type
    all_positions = all_positions.select(
        *([col(start_end).cast(start_end_type), col(is_start_end)] + on_cols)
    )

    sorting = [
        col(start_end).sort(),
        col(is_start_end).sort(ascending=(overlap_filter == FilterOp.Strict)),
    ]
    all_positions = all_positions.sort(*sorting)

    on_cols_expr = [col(c) for c in on_cols]

    win = datafusion.expr.Window(
        partition_by=on_cols_expr,
        order_by=sorting,
    )
    all_positions = all_positions.select(
        *(
            [
                start_end,
                is_start_end,
                datafusion.functions.sum(col(is_start_end))
                .over(win)
                .alias(current_intervals),
            ]
            + on_cols
            + [
                datafusion.functions.row_number(
                    partition_by=on_cols_expr, order_by=sorting
                ).alias(n_intervals)
            ]
        )
    )
    all_positions = all_positions.filter(
        ((col(current_intervals) == 0) & (col(is_start_end) == -1))
        | ((col(current_intervals) == 1) & (col(is_start_end) == 1))
    )
    all_positions = all_positions.select(
        *(
            [start_end, is_start_end]
            + on_cols
            + [
                (
                    (
                        col(n_intervals)
                        - datafusion.functions.lag(
                            col(n_intervals), partition_by=on_cols_expr
                        )
                        + 1
                    )
                    / 2
                ).alias(n_intervals)
            ]
        )
    )
    result = all_positions.select(
        *(
            [
                (col(start_end) - min_dist).alias(end),
                is_start_end,
                datafusion.functions.lag(
                    col(start_end), partition_by=on_cols_expr
                ).alias(start),
            ]
            + on_cols
            + [n_intervals]
        )
    )
    result = result.filter(col(is_start_end) == -1)
    result = result.select(
        *(
            [contig, col(start).cast(start_type), col(end).cast(end_type)]
            + on_cols[1:]
            + [n_intervals]
        )
    )

    return convert_result(result, output_type, streaming)
