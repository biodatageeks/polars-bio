from __future__ import annotations

from pathlib import Path

import datafusion.dataframe
import pandas as pd
import polars as pl
from polars.io.plugins import register_io_source
from pygments.styles.dracula import yellow
from typing_extensions import TYPE_CHECKING, Union


from .polars_bio import overlap_scan

if TYPE_CHECKING:
    from collections.abc import Iterator


DEFAULT_INTERVAL_COLUMNS = ["contig", "pos_start", "pos_end"]


def overlap(df1 : Union[str, pl.DataFrame, pd.DataFrame],
            df2 : Union[str, pl.DataFrame, pd.DataFrame],
            how="inner",
            suffixes=("_1", "_2"),
            on_cols=None,
            col1: Union[list[str], None]=None,
            col2: Union[list[str], None]=None,
            output_type: str ="polars.LazyFrame"
            ) -> Union[pl.LazyFrame, pl.DataFrame, pd.DataFrame]:
    """
    Find pairs of overlapping genomic intervals.
    Bioframe inspired API.

    Parameters
    ----------
    :param col1: The names of columns containing the chromosome, start and end of the
    genomic intervals, provided separately for each set. The default
    values are 'contig', 'pos_start', 'pos_end'.
    :param col2:  The names of columns containing the chromosome, start and end of the
        genomic intervals, provided separately for each set. The default
        values are 'contig', 'pos_start', 'pos_end'.
    :param df1: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
    :param df2: Can be a path to a file, a polars DataFrame, or a pandas DataFrame. CSV with a header and Parquet are supported.
    :param how: How to handle the overlaps on the two dataframes.
            inner: use intersection of the set of intervals from df1 and df2, optional.
    :param suffixes: (str, str), optional The suffixes for the columns of the two overlapped sets.
    :param on_cols: list[str], optional The list of additional column names to join on. default is None.
    :param output_type: str, optional The type of the output. default is "polars.LazyFrame".
    :return: **polars.LazyFrame** or polars.DataFrame or pandas.DataFrame of the overlapping intervals.
    """

    assert how in ["inner"], "Only inner join is supported"
    ext1 = Path(df1).suffix
    # TODO: Add support for CSV files
    assert ext1 == '.parquet', "Dataframe1 must be a Parquet file"
    ext2 = Path(df2).suffix
    assert ext2 == '.parquet', "Dataframe2 must be a Parquet file"

    # TODO: Add support for on_cols ()
    assert on_cols is None, "on_cols is not supported yet"
    # TODO: Add support for col1 and col2
    assert col1 is None, "col1 is not supported yet"
    assert col2 is None, "col2 is not supported yet"

    assert output_type in ["polars.LazyFrame", "polars.DataFrame", "pandas.DataFrame"], "Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported"
    # use suffixes to avoid column name conflicts
    df_schema1 = _get_schema(df2, suffixes[0])
    df_schema2 = _get_schema(df2, suffixes[1])
    merged_schema = pl.Schema({**df_schema1, **df_schema2})
    if output_type == "polars.LazyFrame":
        return scan_overlap(df1, df2, merged_schema)
    elif output_type == "polars.DataFrame":
        return overlap_scan(df1, df2).to_polars()
    elif output_type == "pandas.DataFrame":
        return overlap_scan(df1, df2).to_pandas()
    else:
        raise ValueError("Only polars.LazyFrame, polars.DataFrame, and pandas.DataFrame are supported")




def _get_schema(path: str, suffix = None ) -> pl.Schema:
    ext = Path(path).suffix
    if ext == '.parquet':
        df = pl.read_parquet(path)
    elif ext == '.csv':
        df = pl.read_csv(path)
    else:
        raise ValueError("Only CSV and Parquet files are supported")
    if suffix is not None:
        df = df.rename({col: f"{col}{suffix}" for col in df.columns})
    return df.schema




def scan_overlap(df_1:str, df_2: str, schema: pl.Schema ) -> pl.LazyFrame:
    def _overlap_source(
            with_columns: pl.Expr | None,
            predicate: pl.Expr | None,
            _n_rows: int | None,
            _batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        df_lazy: datafusion.DataFrame = overlap_scan(df_1, df_2)
        df_stream = df_lazy.execute_stream()
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # TODO: We can push predicates down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if predicate is not None:
                df = df.filter(predicate)
            # TODO: We can push columns down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if with_columns is not None:
                df = df.select(with_columns)
            yield df
    return register_io_source(_overlap_source, schema=schema)


def test_exchange_lazy() -> pl.LazyFrame:
    return scan_overlap()

def test_exchange() -> datafusion.dataframe.DataFrame:
    return test_data_exchange()