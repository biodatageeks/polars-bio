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

import datafusion
from datafusion import col, literal
import pyarrow

__all__ = ["overlap", "nearest", "merge", "cluster", "coverage", "count_overlaps"]


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
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
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
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
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


def merge(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame, datafusion.dataframe.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    min_dist: float = 0,
    cols: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.dataframe.DataFrame]:
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
    
    end_positions = df.select(*([(col(end) + min_dist).alias(start_end), literal(-1).alias(is_start_end)] + on_cols))
    start_positions = df.select(*([col(start).alias(start_end), literal(1).alias(is_start_end)] + on_cols))
    all_positions = start_positions.union(end_positions)
    start_end_type = all_positions.schema().field(start_end).type
    all_positions = all_positions.select(*([col(start_end).cast(start_end_type), col(is_start_end)] + on_cols)) 
    
    sorting = [col(start_end).sort(), col(is_start_end).sort(ascending=(overlap_filter == FilterOp.Strict))]
    all_positions = all_positions.sort(*sorting)

    on_cols_expr = [col(c) for c in on_cols]

    win = datafusion.expr.Window(
        partition_by=on_cols_expr,
        order_by=sorting,
    )
    all_positions = all_positions.select(*([start_end, is_start_end,
        datafusion.functions.sum(col(is_start_end)).over(win).alias(current_intervals)] + on_cols +
        [datafusion.functions.row_number(partition_by = on_cols_expr, order_by=sorting).alias(n_intervals)]))
    all_positions = all_positions.filter(
        ((col(current_intervals) == 0) & (col(is_start_end) == -1)) | ((col(current_intervals) == 1) & (col(is_start_end) == 1))
    )
    all_positions = all_positions.select(*([start_end, is_start_end] + on_cols + [((col(n_intervals) - datafusion.functions.lag(col(n_intervals), partition_by=on_cols_expr) + 1) / 2).alias(n_intervals)]))
    result = all_positions.select(*([(col(start_end) - min_dist).alias(end), is_start_end,
        datafusion.functions.lag(col(start_end), partition_by=on_cols_expr).alias(start)] + on_cols + [n_intervals]))
    result = result.filter(col(is_start_end) == -1)
    result = result.select(*([contig, col(start).cast(start_type), col(end).cast(end_type)] + on_cols[1:] + [n_intervals]))
    
    return convert_result(result, output_type, streaming)

def cluster(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame, datafusion.dataframe.DataFrame],
    overlap_filter: FilterOp = FilterOp.Strict,
    min_dist: float = 0,
    cols: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.dataframe.DataFrame]:
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
    print(df_schema)
    print(start)
    start_type = df_schema.field(start).type
    end_type = df_schema.field(end).type
    # TODO: make sure to avoid conflicting column names
    start_end = "start_end"
    is_start_end = "is_start_or_end"
    current_intervals = "current_intervals"
    n_intervals = "n_intervals"
    row_no = "row_no"
    cluster_start = "cluster_start"
    cluster_end = "cluster_end"
    does_cluster_start = "does_cluster_start"
    does_cluster_end = "does_cluster_end"
    cluster_id = "cluster"
    
    end_positions = df.select(*([(col(end) + min_dist).alias(start_end), literal(-1).alias(is_start_end), start, end,
        literal(0).alias(row_no)] + on_cols))
    start_positions = df.select(*([col(start).alias(start_end), literal(1).alias(is_start_end), start, end,
        datafusion.functions.row_number().alias(row_no)] + on_cols))
    all_positions = start_positions.union(end_positions)
    start_end_type = all_positions.schema().field(start_end).type
    all_positions = all_positions.select(*([col(start_end).cast(start_end_type), col(is_start_end), start, end, row_no] + on_cols)) 
    
    sorting = [col(start_end).sort(), col(is_start_end).sort(ascending=(overlap_filter == FilterOp.Strict))]

    on_cols_expr = [col(c) for c in on_cols]
    win = datafusion.expr.Window(
        partition_by=on_cols_expr,
        order_by=sorting,
    )
    all_positions = all_positions.select(*([start_end, is_start_end, start, end, row_no,
        datafusion.functions.sum(col(is_start_end)).over(win).alias(current_intervals)] + on_cols))

    all_positions = all_positions.select(*([
        start,
        end,
        start_end,
        is_start_end,
        current_intervals,
        row_no,
        ((col(current_intervals) == 1) & (col(is_start_end) == 1)).cast(pyarrow.int64()).alias(does_cluster_start)] + on_cols))

    all_positions = all_positions.select(*([
        row_no,
        start,
        end,
        start_end,
        is_start_end,
        does_cluster_start,
        current_intervals,
        datafusion.functions.sum(col(does_cluster_start))
            .over(datafusion.expr.Window(
                order_by = [c.sort() for c in on_cols_expr] + sorting
            ))
            .alias(cluster_id),
        ] + on_cols))
    all_positions = all_positions.filter(col(is_start_end) == 1)
    cluster_window = datafusion.expr.Window(
        partition_by=[col(cluster_id)],
        window_frame=datafusion.expr.WindowFrame(
            units='rows',
            start_bound=None,
            end_bound=None,
        )
    )

    all_positions = all_positions.select(*([
        row_no,
        start,
        end,
        cluster_id,
        datafusion.functions.min(col(start)).over(cluster_window).alias(cluster_start),
        datafusion.functions.max(col(end)).over(cluster_window).alias(cluster_end)] + on_cols))
    all_positions = all_positions.sort(col(row_no).sort())

    all_positions = all_positions.select(*(on_cols + [
        start,
        end,
        (col(cluster_id) - 1).alias(cluster_id),
        cluster_start,
        cluster_end]))
    return convert_result(all_positions, output_type, streaming)

def coverage(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame, datafusion.dataframe.DataFrame],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame, datafusion.dataframe.DataFrame],
    suffixes: tuple[str, str] = ("", "_"),
    return_input: bool = True,
    cols1: Union[list[str], None] = ["chrom", "start", "end"],
    cols2: Union[list[str], None] = ["chrom", "start", "end"],
    on_cols: Union[list[str], None] = None,
    output_type: str = "polars.LazyFrame",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.dataframe.DataFrame]:
    """
    Count coverage of intervals.
    Bioframe inspired API.

    Parameters:
        df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED and Parquet are supported.
        df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header, BED  and Parquet are supported.
        suffixes: Suffixes for the columns of the two overlapped sets.
        return_input: If true, return input.
        cols1: The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        cols2:  The names of columns containing the chromosome, start and end of the
            genomic intervals, provided separately for each set.
        on_cols: List of additional column names to join on. default is None.
        output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" are also supported.
        streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming-out-of-core-processing) engine.

    Returns:
        **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

    Example:

    Todo:
         Support return_input.
     """
    _validate_overlap_input(cols1, cols2, on_cols, suffixes, output_type, how="inner")
    my_ctx = get_py_ctx()
    
    df1 = read_df_to_datafusion(my_ctx, df1)
    df2 = read_df_to_datafusion(my_ctx, df2)

    df2 = merge(df2, output_type="datafusion.DataFrame", cols=cols2, on_cols=on_cols)
    
    on_cols = [] if on_cols is None else on_cols
    cols1 = DEFAULT_INTERVAL_COLUMNS if cols1 is None else cols1
    cols2 = DEFAULT_INTERVAL_COLUMNS if cols2 is None else cols2
    cols1 = list(cols1)
    cols2 = list(cols2)

    # TODO: guarantee no collisions
    contig = "contig"
    row_id = "row_id"
    interval_counter = "interval_counter"
    interval_sum = "interval_sum"
    position = "position"
    coverage = "coverage"

    suff, _ = suffixes

    df1 = df1.select(*([(literal(2) * datafusion.functions.row_number()).alias(row_id)] + cols1 + on_cols))

    df1_starts = df1.select(*([
        row_id,
        col(cols1[0]).alias(contig),
        col(cols1[1]).alias(position),
        literal(0).alias(interval_counter),
        literal(0).alias(interval_sum)] + on_cols))
    df1_ends = df1.select(*([
        (col(row_id) + 1).alias(row_id),
        col(cols1[0]).alias(contig),
        col(cols1[2]).alias(position),
        literal(0).alias(interval_counter),
        literal(0).alias(interval_sum)] + on_cols))

    df2_starts = df2.select(*([
        literal(0).alias(row_id),
        col(cols2[0]).alias(contig),
        col(cols2[1]).alias(position),
        literal(1).alias(interval_counter),
        (literal(0) - col(cols2[1])).alias(interval_sum)] + on_cols))
    df2_ends = df2.select(*([
        literal(0).alias(row_id),
        col(cols2[0]).alias(contig),
        col(cols2[2]).alias(position),
        literal(-1).alias(interval_counter),
        col(cols2[2]).alias(interval_sum)] + on_cols))

    df = df1_starts.union(df1_ends).union(df2_starts).union(df2_ends)

    on_cols = [contig] + on_cols
    on_cols_expr = [col(c) for c in on_cols]

    win = datafusion.expr.Window(
        partition_by=on_cols_expr,
        order_by=[col(position).sort()]
    )

    df = df.select(*([
        row_id,
        position,
        datafusion.functions.sum(col(interval_counter)).over(win).alias(interval_counter),
        datafusion.functions.sum(col(interval_sum)).over(win).alias(interval_sum),
        ] + on_cols))
    df = df.select(*([
        row_id,
        position,
        ((col(interval_counter) * col(position)) + col(interval_sum)).alias(interval_sum),
        ] + on_cols))
    df = df.filter(col(row_id) > 0)
    df = df.sort(col(row_id))

    start_result = cols1[1] + suff
    end_result = cols1[2] + suff

    df = df.select(*([
        row_id,
        col(position).alias(start_result),
        datafusion.functions.lead(col(position)).alias(end_result),
        (datafusion.functions.lead(col(interval_sum)) - col(interval_sum)).alias(coverage)
        ] + on_cols))
    df = df.filter((col(row_id) % 2) == 0)
    df = df.select(*([
        col(contig).alias(cols1[0] + suff),
        start_result,
        end_result] + on_cols[1:] + [
        coverage]))

    return convert_result(df, output_type, streaming)
  
  
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
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
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
        overlap_filter: FilterOp, optional. The type of overlap to consider(Weak or Strict).
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
