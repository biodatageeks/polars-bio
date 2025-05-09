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

__all__ = ["merge"]


if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import FilterOp, RangeOp, RangeOptions

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
