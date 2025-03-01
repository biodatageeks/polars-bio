from typing import Dict, Iterator, Union

import polars as pl
from bioframe import SCHEMAS
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    InputFormat,
    ReadOptions,
    VcfReadOptions,
    py_read_sql,
    py_read_table,
    py_register_table,
    py_scan_sql,
    py_scan_table,
)

from .context import ctx
from .range_op_helpers import stream_wrapper


def read_bam(path: str) -> pl.LazyFrame:
    """
    Read a BAM file into a LazyFrame.

    Parameters:
        path: The path to the BAM file.
    """
    df = read_file(path, InputFormat.Bam, None)
    return lazy_scan(df)


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


def read_vcf(
    path: str,
    info_fields: Union[list[str], None] = None,
    thread_num: int = 1,
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame]:
    """
    Read a VCF file into a LazyFrame.

    Parameters:
        path: The path to the VCF file.
        info_fields: The fields to read from the INFO column.
        thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
        streaming: Whether to read the VCF file in streaming mode.
    """
    vcf_read_options = VcfReadOptions(info_fields=info_fields, thread_num=thread_num)
    read_options = ReadOptions(vcf_read_options=vcf_read_options)
    if streaming:
        return read_file(path, InputFormat.Vcf, read_options, streaming)
    else:
        df = read_file(path, InputFormat.Vcf, read_options)
        return lazy_scan(df)


def read_fasta(path: str) -> pl.LazyFrame:
    """
    Read a FASTA file into a LazyFrame.

    Parameters:
        path: The path to the FASTA file.
    """
    df = read_file(path, InputFormat.Fasta, None)
    return lazy_scan(df)


def read_fastq(path: str) -> pl.LazyFrame:
    """
    Read a FASTQ file into a LazyFrame.

    Parameters:
        path: The path to the FASTQ file.
    """
    df = read_file(path, InputFormat.Fastq, None)
    return lazy_scan(df)


def lazy_scan(df: Union[pl.DataFrame, pl.LazyFrame]) -> pl.LazyFrame:
    df_lazy: DataFrame = df
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
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            if predicate is not None:
                df = df.filter(predicate)
            # TODO: We can push columns down to the DataFusion plan in the future,
            #  but for now we'll do it here.
            if with_columns is not None:
                df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

    return register_io_source(_overlap_source, schema=arrow_schema)


def read_file(
    path: str,
    input_format: InputFormat,
    read_options: ReadOptions,
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame]:
    """
    Read a file into a DataFrame.

    Parameters
    ----------
    path : str
        The path to the file.
    input_format : InputFormat
        The input format of the file.
    read_options : ReadOptions, e.g. VcfReadOptions
    streaming: Whether to read the file in streaming mode.

    Returns
    -------
    pl.DataFrame
        The DataFrame.
    """
    table = py_register_table(ctx, path, None, input_format, read_options)
    if streaming:
        return stream_wrapper(py_scan_table(ctx, table.name))
    else:
        return py_read_table(ctx, table.name)


def read_table(path: str, schema: Dict = None, **kwargs) -> pl.LazyFrame:
    """
     Read a tab-delimited (i.e. BED) file into a Polars LazyFrame.
     Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
     but faster and lazy. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

    Parameters:
        path: The path to the file.
        schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).


    """
    df = pl.scan_csv(path, separator="\t", has_header=False, **kwargs)
    if schema is not None:
        columns = SCHEMAS[schema]
        if len(columns) != len(df.collect_schema()):
            raise ValueError(
                f"Schema incompatible with the input. Expected {len(columns)} columns in a schema, got {len(df.collect_schema())} in the input data file. Please provide a valid schema."
            )
        for i, c in enumerate(columns):
            df = df.rename({f"column_{i+1}": c})
    return df


def register_vcf(
    path: str,
    name: Union[str, None] = None,
    info_fields: Union[list[str], None] = None,
    thread_num: int = 1,
) -> None:
    """
    Register a VCF file as a Datafusion table.

    Parameters:
        path: The path to the VCF file.
        name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
        info_fields: The fields to read from the INFO column.
        thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.

    !!! Example
          ```python
          import polars_bio as pb
          pb.register_vcf("/tmp/gnomad.v4.1.sv.sites.vcf.gz")
          ```
         ```shell
            INFO:polars_bio:Table: gnomad_v4_1_sv_sites_gz registered for path: /tmp/gnomad.v4.1.sv.sites.vcf.gz
         ```
    """
    vcf_read_options = VcfReadOptions(info_fields=info_fields, thread_num=thread_num)
    read_options = ReadOptions(vcf_read_options=vcf_read_options)
    py_register_table(ctx, path, name, InputFormat.Vcf, read_options)


def sql(query: str, streaming: bool = False) -> pl.LazyFrame:
    """
    Execute a SQL query on the registered tables.

    Parameters:
        query: The SQL query.
        streaming: Whether to execute the query in streaming mode.

    !!! Example
          ```python
          import polars_bio as pb
          pb.register_vcf("/tmp/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_v4_1_sv")
          pb.sql("SELECT * FROM gnomad_v4_1_sv LIMIT 5").collect()
          ```
    """
    if streaming:
        return stream_wrapper(py_scan_sql(ctx, query))
    else:
        df = py_read_sql(ctx, query)
        return lazy_scan(df)
