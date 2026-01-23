import tempfile
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
from .constants import TMP_CATALOG_DIR
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


def _lazyframe_to_parquet(
    df: Union[pl.LazyFrame, "GffLazyFrameWrapper"], ctx: BioSessionContext
) -> str:
    """Convert LazyFrame or GffLazyFrameWrapper to temporary parquet file and return the path."""
    # Create temporary parquet file in the session catalog path
    # Use a timestamped directory under TMP_CATALOG_DIR
    import datetime

    timestamp = str(datetime.datetime.now().timestamp())
    catalog_path = Path(f"{TMP_CATALOG_DIR}/{timestamp}")
    catalog_path.mkdir(parents=True, exist_ok=True)

    # Create a unique temporary file
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, suffix=".parquet", dir=catalog_path
    )
    temp_path = temp_file.name
    temp_file.close()

    # Sink LazyFrame to parquet (works for both LazyFrame and GffLazyFrameWrapper)
    df.sink_parquet(temp_path)
    logger.info(f"LazyFrame sunk to temporary parquet: {temp_path}")

    return temp_path


def range_operation(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame", "GffLazyFrameWrapper"],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame", "GffLazyFrameWrapper"],
    range_options: RangeOptions,
    output_type: str,
    ctx: BioSessionContext,
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
    projection_pushdown: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
    ctx.sync_options()

    # Handle LazyFrames and GffLazyFrameWrapper by converting them to temporary parquet files
    original_df1_is_lazy = isinstance(df1, pl.LazyFrame) or hasattr(df1, "_base_lf")
    original_df2_is_lazy = isinstance(df2, pl.LazyFrame) or hasattr(df2, "_base_lf")

    if original_df1_is_lazy:
        df1 = _lazyframe_to_parquet(df1, ctx)
    if original_df2_is_lazy:
        df2 = _lazyframe_to_parquet(df2, ctx)

    # If we have mixed case (one LazyFrame converted to parquet, one DataFrame),
    # convert the DataFrame to parquet as well for consistency
    if (original_df1_is_lazy or original_df2_is_lazy) and not (
        isinstance(df1, str) and isinstance(df2, str)
    ):
        if not isinstance(df1, str) and isinstance(
            df1, (pl.DataFrame, pd.DataFrame if pd else type(None))
        ):
            # Convert DataFrame to temporary parquet
            temp_df1_lazy = (
                df1.lazy()
                if isinstance(df1, pl.DataFrame)
                else pl.from_pandas(df1).lazy()
            )
            df1 = _lazyframe_to_parquet(temp_df1_lazy, ctx)
        if not isinstance(df2, str) and isinstance(
            df2, (pl.DataFrame, pd.DataFrame if pd else type(None))
        ):
            # Convert DataFrame to temporary parquet
            temp_df2_lazy = (
                df2.lazy()
                if isinstance(df2, pl.DataFrame)
                else pl.from_pandas(df2).lazy()
            )
            df2 = _lazyframe_to_parquet(temp_df2_lazy, ctx)

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
            merged_schema = pl.Schema(
                {
                    **_get_schema(df1, ctx, None, read_options1),
                    **{"count": pl.Int64},
                }
            )
        elif range_options.range_op == RangeOp.Coverage:
            # add coverage column to the schema (Int64 to match engine)
            merged_schema = pl.Schema(
                {
                    **_get_schema(df1, ctx, None, read_options1),
                    **{"coverage": pl.Int64},
                }
            )
        else:
            # Get the base schemas without suffixes first
            df_schema1_base = _get_schema(df1, ctx, None, read_options1)
            df_schema2_base = _get_schema(df2, ctx, None, read_options2)

            # Generate the correct schema using common function
            merged_schema = _generate_overlap_schema(
                df_schema1_base, df_schema2_base, range_options
            )
            # Nearest adds an extra computed column
            if range_options.range_op == RangeOp.Nearest:
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
            # Get base schemas without suffixes
            df1_base_schema = _rename_columns(df1, "").schema
            df2_base_schema = _rename_columns(df2, "").schema

            # Generate the correct schema using common function
            merged_schema = _generate_overlap_schema(
                df1_base_schema, df2_base_schema, range_options
            )
            # Add computed columns for streaming outputs
            if range_options.range_op == RangeOp.Nearest:
                merged_schema = pl.Schema({**merged_schema, **{"distance": pl.Int64}})
            elif range_options.range_op == RangeOp.CountOverlapsNaive:
                merged_schema = pl.Schema({**merged_schema, **{"count": pl.Int64}})
            elif range_options.range_op == RangeOp.Coverage:
                merged_schema = pl.Schema({**merged_schema, **{"coverage": pl.Int64}})
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
            df1 = _df_to_reader(
                df1,
                range_options.columns_1[0],
            )
            df2 = _df_to_reader(
                df2,
                range_options.columns_2[0],
            )
            result = range_operation_frame(
                ctx,
                df1,
                df2,
                range_options,
            )
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


def tmp_cleanup(session_catalog_path: str):
    # remove temp parquet files
    logger.info(f"Cleaning up temp files for catalog path: '{session_catalog_path}'")
    path = Path(session_catalog_path)
    for path in path.glob("*.parquet"):
        path.unlink(missing_ok=True)
    path.rmdir()
