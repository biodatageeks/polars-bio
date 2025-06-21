from __future__ import annotations

import datafusion
import pandas as pd
import polars as pl
from datafusion import col, literal
from typing_extensions import TYPE_CHECKING, Union

from polars_bio.polars_bio import QCOptions, ReadOptions

from .constants import DEFAULT_INTERVAL_COLUMNS
from .context import ctx
from .interval_op_helpers import (
    convert_result,
    get_py_ctx,
    prevent_column_collision,
    read_df_to_datafusion,
)
from .logging import logger
from .qc_op_helpers import _validate_sequence_quality_score_input, qc_operation



if TYPE_CHECKING:
    pass
from polars_bio.polars_bio import QCOp


class QCOperations:

    @staticmethod
    def sequence_quality_score(
        df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
        output_type: str = "polars.LazyFrame",
        streaming: bool = False,
        read_options: Union[ReadOptions, None] = None
    ) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
        """
        Calculate sequence quality score as mean value.

        Parameters:
            df: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table (see [register_vcf](api.md#polars_bio.register_vcf)). CSV with a header, BED and Parquet are supported.
            output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
            streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.

        Returns:
            **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

        Note:
            1. The default output format, i.e.  [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html), is recommended for large datasets as it supports output streaming and lazy evaluation.
            This enables efficient processing of large datasets without loading the entire output dataset into memory.
            2. Streaming is only supported for polars.LazyFrame output.

        Example:
            ```python

            ```
        """

        _validate_sequence_quality_score_input(
            output_type
        )

        qc_options = QCOptions(
            qc_op=QCOp.MeanQuality,
            quality_col="quality_scores",
            output_col="mean_q",
            ascii_offset=33,
            streaming=False
        )

        return qc_operation(
            df, qc_options, output_type, ctx, read_options
        )
    
    @staticmethod
    def sequence_quality_score_histogram(
        df: Union[str, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
        output_type: str = "polars.LazyFrame",
        streaming: bool = False,
        read_options: Union[ReadOptions, None] = None
    ) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame, datafusion.DataFrame]:
        """
        Calculate sequence quality score histogram.

        Parameters:
            df: Can be a path to a file, a polars DataFrame, or a pandas DataFrame or a registered table (see [register_vcf](api.md#polars_bio.register_vcf)). CSV with a header, BED and Parquet are supported.
            output_type: Type of the output. default is "polars.LazyFrame", "polars.DataFrame", or "pandas.DataFrame" or "datafusion.DataFrame" are also supported.
            streaming: **EXPERIMENTAL** If True, use Polars [streaming](features.md#streaming) engine.

        Returns:
            **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.

        """

        _validate_sequence_quality_score_input(
            output_type
        )

        qc_options = QCOptions(
            qc_op=QCOp.MeanQualityHistogram,
            quality_col="quality_scores",
            output_col="mean_q",
            ascii_offset=33,
            streaming=False
        )

        return qc_operation(
            df, qc_options, output_type, ctx, read_options
        )