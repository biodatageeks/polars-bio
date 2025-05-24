import polars as pl
from polars_bio.polars_bio import py_base_quality

def base_quality(
    input: pl.DataFrame,
    streaming: bool = True,
    target_partitions: int = None
) -> pl.DataFrame:
    """
    Calculate base sequence quality metrics from FASTQ data.

    Parameters
    ----------
    input : pl.DataFrame
        DataFrame with 'position' and 'quality' columns.
    streaming : bool
        Enable out-of-core processing if True.
    target_partitions : int, optional
        Level of parallelism for processing.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns:
        - position: u32
        - average: f64
        - q1: f64
        - median: f64
        - q3: f64
        - min: f64
        - max: f64
        - warning_status: str
    """
    return py_base_quality(input, streaming, target_partitions)
