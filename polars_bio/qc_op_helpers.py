from pathlib import Path
from typing import Union, Optional

import pandas as pd
import polars as pl

from polars_bio.polars_bio import (
    BioSessionContext,
    QCOptions,
    ReadOptions,
    qc_operation_scan,
    QCOp,
    qc_operation_frame,
)

from .constants import TMP_CATALOG_DIR
from .logging import logger
from .range_op_io import _df_to_reader, _get_schema, _rename_columns, range_lazy_scan


def _validate_sequence_quality_score_input( output_type):
    assert output_type in [
        "polars.LazyFrame",
        "polars.DataFrame",
        "pandas.DataFrame",
        "datafusion.DataFrame",
    ], "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"

def apply_mean_quality(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    qc_options: QCOptions,
    output_type: str,
    ctx: BioSessionContext,
    read_options: Optional[ReadOptions] = None
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    ctx.sync_options()
    _validate_sequence_quality_score_input(output_type=output_type)

    if isinstance(df, str):
        # Obsługa plików na dysku
        result = qc_operation_scan(ctx, df, qc_options, read_options)
    else:
        # Obsługa in-memory DataFrames
        df_reader = _df_to_reader(df, qc_options.quality_col[0])
        result = qc_operation_frame(ctx, df_reader, qc_options)

    if output_type == "polars.DataFrame":
        return result.to_polars()
    elif output_type == "pandas.DataFrame":
        return result.to_pandas()
    elif output_type == "polars.LazyFrame":
        # jeśli chcesz też wspierać LazyFrame
        return pl.LazyFrame.from_dataframe(result.to_polars())
    else:
        raise ValueError("Unsupported output_type")


# def apply_mean_quality(
#     df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
#     qc_options: QCOptions,
#     output_type: str,
#     ctx: BioSessionContext,
#     read_options: Optional[ReadOptions] = None
# ) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
#     ctx.sync_options()

#     _validate_sequence_quality_score_input(output_type=output_type)
#     if isinstance(df, str):
#         result = qc_operation_scan(ctx, df, qc_options, read_options)
#         if output_type == "polars.DataFrame":
#             return result.to_polars()
#         elif output_type == "pandas.DataFrame":
#             return result.to_pandas()
#         else:
#             raise ValueError("Unsupported output_type")
#     else:
#         raise ValueError("In-memory DataFrames not yet supported for QC ops")


def qc_operation(
    df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
    qc_options: QCOptions,
    output_type: str,
    ctx: BioSessionContext,
    read_options: Union[ReadOptions, None] = None
) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:

    if qc_options.qc_op == QCOp.MeanQuality:
        return apply_mean_quality(df, qc_options, output_type, ctx, read_options)
    else:
        raise NotImplementedError(f"Unsupported method: {qc_options.method}")

