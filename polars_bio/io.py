from typing import Iterator, Union

import polars as pl
from datafusion import DataFrame
from polars.io.plugins import register_io_source

from polars_bio.polars_bio import InputFormat, py_register_table, py_scan_table

from .context import ctx


def read_bam(path: str) -> pl.LazyFrame:
    """
    Read a BAM file into a LazyFrame.

    Parameters:
        path: The path to the BAM file.
    """
    return file_lazy_scan(path, InputFormat.Bam)


# TODO handling reference
# def read_cram(path: str) -> pl.LazyFrame:
#     """
#     Read a CRAM file into a LazyFrame.
#
#     Parameters:
#         path: The path to the CRAM file.
#     """
#     return file_lazy_scan(path, InputFormat.Cram)


# TODO passing of bam_region_filter
# def read_indexed_bam(path: str) -> pl.LazyFrame:
#     """
#     Read an indexed BAM file into a LazyFrame.
#
#     Parameters:
#         path: The path to the BAM file.
#
#     !!! warning
#         Predicate pushdown is not supported yet. So no real benefit from using an indexed BAM file.
#     """
#     return file_lazy_scan(path, InputFormat.IndexedBam)


def read_vcf(path: str) -> pl.LazyFrame:
    """
    Read a VCF file into a LazyFrame.

    Parameters:
        Parameters:
        path: The path to the VCF file.
    """
    return file_lazy_scan(path, InputFormat.Vcf)


def read_bed(path: str) -> pl.LazyFrame:
    """
    Read a BED file into a LazyFrame.

    Parameters:
        Parameters:
        path: The path to the BED file.
    """
    return file_lazy_scan(path, InputFormat.Bed)


def read_fasta(path: str) -> pl.LazyFrame:
    """
    Read a FASTA file into a LazyFrame.

    Parameters:
        Parameters:
        path: The path to the FASTA file.
    """
    return file_lazy_scan(path, InputFormat.Fasta)


def read_fastq(path: str) -> pl.LazyFrame:
    """
    Read a FASTQ file into a LazyFrame.

    Parameters:
        Parameters:
        path: The path to the FASTQ file.
    """
    return file_lazy_scan(path, InputFormat.Fastq)


# def read_indexed_vcf(path: str) -> pl.LazyFrame:
#     """
#     Read an indexed VCF file into a LazyFrame.
#
#     Parameters:
#         Parameters:
#         path: The path to the VCF file.
#
#     !!! warning
#         Predicate pushdown is not supported yet. So no real benefit from using an indexed VCF file.
#     """
#     return file_lazy_scan(path, InputFormat.Vcf)


def file_lazy_scan(path: str, input_format: InputFormat) -> pl.LazyFrame:
    df_lazy: DataFrame = read_file(path, input_format)
    arrow_schema = df_lazy.schema()

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        if n_rows and n_rows < 8192:  # 8192 is the default batch size in datafusion
            df = df_lazy.execute_stream().next().to_pyarrow()
            df = pl.DataFrame(df).limit(n_rows)
            if predicate is not None:
                df = df.filter(predicate)
            # TODO: We can push columns down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if with_columns is not None:
                df = df.select(with_columns)
            yield df
            return
        df_stream = df_lazy.execute_stream()
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            if predicate is not None:
                df = df.filter(predicate)
            # TODO: We can push columns down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if with_columns is not None:
                df = df.select(with_columns)
            yield df

    return register_io_source(_overlap_source, schema=arrow_schema)


def read_file(path: str, input_format: InputFormat) -> pl.DataFrame:
    """
    Read a file into a DataFrame.

    Parameters
    ----------
    path : str
        The path to the file.
    input_format : InputFormat
        The input format of the file.

    Returns
    -------
    pl.DataFrame
        The DataFrame.
    """
    table = py_register_table(ctx, path, input_format)
    return py_scan_table(ctx, table.name)
