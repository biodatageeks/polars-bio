from pathlib import Path
from typing import Union

import polars as pl

from polars_bio.polars_bio import (
    BioSessionContext,
    FilterOp,
    RangeOp,
    RangeOptions,
    ReadOptions,
    range_operation_frame,
    range_operation_scan,
)

from ._metadata import set_coordinate_system
from .logging import logger
from .range_op_io import _df_to_reader, _get_schema, _rename_columns, range_lazy_scan

try:
    import pandas as pd
except ImportError:
    pd = None


def _get_zero_based_from_filter_op(filter_op: FilterOp) -> bool:
    """Derive zero_based value from FilterOp.

    FilterOp.Strict means 0-based (half-open) coordinates.
    FilterOp.Weak means 1-based (closed) coordinates.
    """
    return filter_op == FilterOp.Strict


def _set_result_metadata(
    result: Union[pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    zero_based: bool,
) -> Union[pl.DataFrame, pl.LazyFrame, "pd.DataFrame"]:
    """Set coordinate system metadata on a range operation result.

    Args:
        result: The DataFrame result from a range operation.
        zero_based: True for 0-based, False for 1-based coordinates.

    Returns:
        The same result with metadata set.
    """
    if isinstance(result, (pl.DataFrame, pl.LazyFrame)):
        set_coordinate_system(result, zero_based)
    elif pd is not None and isinstance(result, pd.DataFrame):
        set_coordinate_system(result, zero_based)
    return result


def _generate_overlap_schema(
    df1_schema: pl.Schema,
    df2_schema: pl.Schema,
    range_options: RangeOptions,
) -> pl.Schema:
    """Generate schema for overlap operations with correct suffix handling."""
    coord_cols = set(range_options.columns_1 + range_options.columns_2)
    merged_schema_dict = {}

    # Add df1 columns with appropriate suffixes
    for col_name, col_type in df1_schema.items():
        # All df1 columns get suffix _1
        merged_schema_dict[f"{col_name}{range_options.suffixes[0]}"] = col_type

    # Add df2 columns with appropriate suffixes
    for col_name, col_type in df2_schema.items():
        # All df2 columns get suffix _2
        merged_schema_dict[f"{col_name}{range_options.suffixes[1]}"] = col_type

    return pl.Schema(merged_schema_dict)


def _generate_merge_schema(columns: list[str]) -> pl.Schema:
    """Generate schema for merge operations.

    MergeProvider outputs: (contig: Utf8, start: Int64, end: Int64, n_intervals: Int64).
    """
    return pl.Schema(
        {
            columns[0]: pl.Utf8,
            columns[1]: pl.Int64,
            columns[2]: pl.Int64,
            "n_intervals": pl.Int64,
        }
    )


def _lazyframe_to_dataframe(
    df: Union[pl.LazyFrame, "GffLazyFrameWrapper"],
) -> pl.DataFrame:
    """Convert LazyFrame or GffLazyFrameWrapper to DataFrame via collection.

    This is more efficient than writing to parquet now that GIL is released
    during Arrow operations via py.allow_threads().
    """
    if hasattr(df, "_base_lf"):
        # GffLazyFrameWrapper or similar - collect the underlying LazyFrame
        return df.collect()
    else:
        # Regular LazyFrame
        return df.collect()


def range_operation(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame", "GffLazyFrameWrapper"],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame", "GffLazyFrameWrapper"],
    range_options: RangeOptions,
    output_type: str,
    ctx: BioSessionContext,
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
    projection_pushdown: bool = True,
) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
    ctx.sync_options()

    # For non-LazyFrame outputs, convert LazyFrames to DataFrames directly.
    # This is efficient now that GIL is released during Arrow operations via py.allow_threads().
    # For LazyFrame outputs, we need to collect first since Arrow readers can only be consumed once.
    if output_type != "polars.LazyFrame":
        if isinstance(df1, pl.LazyFrame) or hasattr(df1, "_base_lf"):
            df1 = _lazyframe_to_dataframe(df1)
        if isinstance(df2, pl.LazyFrame) or hasattr(df2, "_base_lf"):
            df2 = _lazyframe_to_dataframe(df2)

    if isinstance(df1, str) and isinstance(df2, str):
        supported_exts = set([".parquet", ".csv", ".bed", ".vcf"])
        ext1 = set(Path(df1).suffixes)
        assert (
            len(supported_exts.intersection(ext1)) > 0 or len(ext1) == 0
        ), "Dataframe1 must be a Parquet, BED, CSV or VCF file"
        ext2 = set(Path(df2).suffixes)
        assert (
            len(supported_exts.intersection(ext2)) > 0 or len(ext2) == 0
        ), "Dataframe2 must be a Parquet, BED, CSV or VCF file"
        # use suffixes to avoid column name conflicts

        if range_options.range_op == RangeOp.CountOverlapsNaive:
            # add count column to the schema (Int64 to match engine)
            # Note: Use df2's schema because range_op.py swaps df1/df2 for these operations,
            # and the Rust code iterates over right_table (which is df2 here) and returns its rows
            merged_schema = pl.Schema(
                {
                    **_get_schema(df2, ctx, None, read_options2),
                    **{"count": pl.Int64},
                }
            )
        elif range_options.range_op == RangeOp.Coverage:
            # add coverage column to the schema (Int64 to match engine)
            # Note: Use df2's schema because range_op.py swaps df1/df2 for these operations,
            # and the Rust code iterates over right_table (which is df2 here) and returns its rows
            merged_schema = pl.Schema(
                {
                    **_get_schema(df2, ctx, None, read_options2),
                    **{"coverage": pl.Int64},
                }
            )
        elif range_options.range_op == RangeOp.Merge:
            merged_schema = _generate_merge_schema(range_options.columns_1)
        else:
            # Get the base schemas without suffixes first
            df_schema1_base = _get_schema(df1, ctx, None, read_options1)
            df_schema2_base = _get_schema(df2, ctx, None, read_options2)

            # Generate the correct schema using common function
            merged_schema = _generate_overlap_schema(
                df_schema1_base, df_schema2_base, range_options
            )
            # Nearest adds an extra computed column when distance is enabled
            if range_options.range_op == RangeOp.Nearest and (
                range_options.compute_distance is None or range_options.compute_distance
            ):
                merged_schema = pl.Schema({**merged_schema, **{"distance": pl.Int64}})
        # Derive coordinate system from filter_op for metadata propagation
        zero_based = _get_zero_based_from_filter_op(range_options.filter_op)

        if output_type == "polars.LazyFrame":
            result = range_lazy_scan(
                df1,
                df2,
                merged_schema,
                range_options=range_options,
                ctx=ctx,
                read_options1=read_options1,
                read_options2=read_options2,
                projection_pushdown=projection_pushdown,
            )
            return _set_result_metadata(result, zero_based)
        elif output_type == "polars.DataFrame":
            result = range_operation_scan(
                ctx,
                df1,
                df2,
                range_options,
                read_options1,
                read_options2,
            ).to_polars()
            return _set_result_metadata(result, zero_based)
        elif output_type == "pandas.DataFrame":
            if pd is None:
                raise ImportError(
                    "pandas is not installed. Install pandas or "
                    "use `polars-bio[pandas]`."
                )
            result = range_operation_scan(
                ctx,
                df1,
                df2,
                range_options,
                read_options1,
                read_options2,
            ).to_pandas()
            return _set_result_metadata(result, zero_based)
        elif output_type == "datafusion.DataFrame":
            return range_operation_scan(
                ctx,
                df1,
                df2,
                range_options,
                read_options1,
                read_options2,
            )
        else:
            raise ValueError(
                "Only polars.LazyFrame, polars.DataFrame and pandas.DataFrame "
                "are supported"
            )
    else:
        # Derive coordinate system from filter_op for metadata propagation
        zero_based = _get_zero_based_from_filter_op(range_options.filter_op)

        if output_type == "polars.LazyFrame":
            # Get base schemas from collected DataFrames
            df1_base_schema = _rename_columns(df1, "").schema
            df2_base_schema = _rename_columns(df2, "").schema

            # Generate schema based on operation type
            if range_options.range_op == RangeOp.CountOverlapsNaive:
                merged_schema = pl.Schema({**df2_base_schema, **{"count": pl.Int64}})
            elif range_options.range_op == RangeOp.Coverage:
                merged_schema = pl.Schema({**df2_base_schema, **{"coverage": pl.Int64}})
            elif range_options.range_op == RangeOp.Merge:
                merged_schema = _generate_merge_schema(range_options.columns_1)
            else:
                merged_schema = _generate_overlap_schema(
                    df1_base_schema, df2_base_schema, range_options
                )
                if range_options.range_op == RangeOp.Nearest and (
                    range_options.compute_distance is None
                    or range_options.compute_distance
                ):
                    merged_schema = pl.Schema(
                        {**merged_schema, **{"distance": pl.Int64}}
                    )

            # Use range_lazy_scan which handles LazyFrame collection and
            # creates fresh Arrow readers per streaming call
            result = range_lazy_scan(
                df1,
                df2,
                merged_schema,
                range_options,
                ctx,
                projection_pushdown=projection_pushdown,
            )
            return _set_result_metadata(result, zero_based)
        else:
            # Note: LazyFrames and GffLazyFrameWrapper are already collected
            # at the start of range_operation() when output_type != "polars.LazyFrame"
            df1_reader = _df_to_reader(df1, range_options.columns_1[0])
            df2_reader = _df_to_reader(df2, range_options.columns_2[0])
            result = range_operation_frame(ctx, df1_reader, df2_reader, range_options)

            if output_type == "polars.DataFrame":
                result_df = result.to_polars()
                return _set_result_metadata(result_df, zero_based)
            elif output_type == "pandas.DataFrame":
                if pd is None:
                    raise ImportError(
                        "pandas is not installed. Install pandas or "
                        "use `polars-bio[pandas]`."
                    )
                result_df = result.to_pandas()
                return _set_result_metadata(result_df, zero_based)
            else:
                raise ValueError(
                    "Only polars.LazyFrame, polars.DataFrame and pandas "
                    "DataFrame are supported"
                )


def _validate_overlap_input(
    col1,
    col2,
    on_cols,
    suffixes,
    output_type,
):
    """Validate input parameters for range operations.

    Note: Coordinate system is now determined from DataFrame metadata,
    not from an explicit parameter.
    """
    assert on_cols is None, "on_cols is not supported yet"
    assert output_type in [
        "polars.LazyFrame",
        "polars.DataFrame",
        "pandas.DataFrame",
        "datafusion.DataFrame",
    ], (
        "Only polars.LazyFrame, polars.DataFrame and pandas DataFrame are " "supported"
    )
