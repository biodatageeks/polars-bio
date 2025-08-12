from pathlib import Path
from typing import Union

import polars as pl

from polars_bio.polars_bio import (
    BioSessionContext,
    RangeOp,
    RangeOptions,
    ReadOptions,
    range_operation_frame,
    range_operation_scan,
    stream_range_operation_scan,
)

from .constants import TMP_CATALOG_DIR
from .logging import logger
from .range_op_io import _df_to_reader, _get_schema, _rename_columns, range_lazy_scan

try:
    import pandas as pd
except ImportError:
    pd = None


def range_operation(
    df1: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    df2: Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"],
    range_options: RangeOptions,
    output_type: str,
    ctx: BioSessionContext,
    read_options1: Union[ReadOptions, None] = None,
    read_options2: Union[ReadOptions, None] = None,
) -> Union[pl.LazyFrame, pl.DataFrame, "pd.DataFrame"]:
    ctx.sync_options()
    if isinstance(df1, str) and isinstance(df2, str):
        supported_exts = set([".parquet", ".csv", ".bed", ".vcf"])
        ext1 = set(Path(df1).suffixes)
        assert (
            len(supported_exts.intersection(ext1)) > 0 or len(ext1) == 0
        ), "Dataframe1 must be a Parquet, a BED or CSV or VCF file"
        ext2 = set(Path(df2).suffixes)
        assert (
            len(supported_exts.intersection(ext2)) > 0 or len(ext2) == 0
        ), "Dataframe2 must be a Parquet, a BED or CSV or VCF file"
        # use suffixes to avoid column name conflicts
        if range_options.streaming:
            # FIXME: Parallelism is not supported
            # FIXME: StringViews not supported yet see: https://datafusion.apache.org/blog/2024/12/14/datafusion-python-43.1.0/

            ctx.set_option("datafusion.execution.target_partitions", "1", False)
            ctx.set_option(
                "datafusion.execution.parquet.schema_force_view_types", "false", True
            )
            return stream_wrapper(
                stream_range_operation_scan(
                    ctx, df1, df2, range_options, read_options1, read_options2
                )
            )

        if range_options.range_op == RangeOp.CountOverlapsNaive:
            ## add count column to the schema
            merged_schema = pl.Schema(
                {**_get_schema(df1, ctx, None, read_options1), **{"count": pl.Int32}}
            )
        elif range_options.range_op == RangeOp.Coverage:
            merged_schema = pl.Schema(
                {**_get_schema(df1, ctx, None, read_options1), **{"coverage": pl.Int32}}
            )
        else:
            df_schema1 = _get_schema(df1, ctx, range_options.suffixes[0], read_options1)
            df_schema2 = _get_schema(df2, ctx, range_options.suffixes[1], read_options2)
            merged_schema = pl.Schema({**df_schema1, **df_schema2})
        if output_type == "polars.LazyFrame":
            return range_lazy_scan(
                df1,
                df2,
                merged_schema,
                range_options=range_options,
                ctx=ctx,
                read_options1=read_options1,
                read_options2=read_options2,
            )
        elif output_type == "polars.DataFrame":
            return range_operation_scan(
                ctx, df1, df2, range_options, read_options1, read_options2
            ).to_polars()
        elif output_type == "pandas.DataFrame":
            if pd is None:
                raise ImportError(
                    "pandas is not installed. Please run `pip install pandas` or `pip install polars-bio[pandas]`."
                )
            result = range_operation_scan(
                ctx, df1, df2, range_options, read_options1, read_options2
            )
            return result.to_pandas()
        elif output_type == "datafusion.DataFrame":
            from datafusion._internal import SessionContext as SessionContextInternal

            a = SessionContextInternal()
            return range_operation_scan(
                ctx, df1, df2, range_options, read_options1, read_options2
            )
        else:
            raise ValueError(
                "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"
            )
    else:
        if output_type == "polars.LazyFrame":
            merged_schema = pl.Schema(
                {
                    **_rename_columns(df1, range_options.suffixes[0]).schema,
                    **_rename_columns(df2, range_options.suffixes[1]).schema,
                }
            )
            return range_lazy_scan(df1, df2, merged_schema, range_options, ctx)
        else:
            df1 = _df_to_reader(df1, range_options.columns_1[0])
            df2 = _df_to_reader(df2, range_options.columns_2[0])
            result = range_operation_frame(ctx, df1, df2, range_options)
            if output_type == "polars.DataFrame":
                return result.to_polars()
            elif output_type == "pandas.DataFrame":
                if pd is None:
                    raise ImportError(
                        "pandas is not installed. Please run `pip install pandas` or `pip install polars-bio[pandas]`."
                    )
                return result.to_pandas()
            else:
                raise ValueError(
                    "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"
                )


def _validate_overlap_input(col1, col2, on_cols, suffixes, output_type, use_zero_based):
    # TODO: Add support for on_cols ()
    _zero_based_warning(use_zero_based)
    assert on_cols is None, "on_cols is not supported yet"
    assert output_type in [
        "polars.LazyFrame",
        "polars.DataFrame",
        "pandas.DataFrame",
        "datafusion.DataFrame",
    ], "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"


def _zero_based_warning(use_zero_based: bool):
    if use_zero_based:
        logger.warning(
            "0-based coordinate system was selected. Please ensure that both datasets follow this coordinate system."
        )


def stream_wrapper(pyldf):
    return pl.LazyFrame._from_pyldf(pyldf)


def tmp_cleanup(session_catalog_path: str):
    # remove temp parquet files
    logger.info(f"Cleaning up temp files for catalog path: '{session_catalog_path}'")
    path = Path(session_catalog_path)
    for path in path.glob("*.parquet"):
        path.unlink(missing_ok=True)
    path.rmdir()
