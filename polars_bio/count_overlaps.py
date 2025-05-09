from __future__ import annotations

import datafusion
import pandas as pd
import polars as pl
from datafusion import col, literal
from typing_extensions import TYPE_CHECKING, Union

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import ctx
from .interval_op_helpers import convert_result, get_py_ctx, read_df_to_datafusion
from .range_op_helpers import _validate_overlap_input, range_operation

__all__ = ["count_overlaps"]


if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import FilterOp, RangeOp, RangeOptions


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


