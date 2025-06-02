from typing import Dict, Iterator, Union

import polars as pl
from bioframe import SCHEMAS
from datafusion import DataFrame, SessionContext
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    InputFormat,
    PyObjectStorageOptions,
    ReadOptions,
    VcfReadOptions,
    py_describe_vcf,
    py_from_polars,
    py_read_sql,
    py_read_table,
    py_register_table,
    py_register_view,
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
    chunk_size: int = 8,
    concurrent_fetches: int = 1,
    allow_anonymous: bool = False,
    enable_request_payer: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    compression_type: str = "auto",
    streaming: bool = False,
) -> Union[pl.LazyFrame, pl.DataFrame]:
    """
    Read a VCF file into a LazyFrame.

    Parameters:
        path: The path to the VCF file.
        info_fields: The fields to read from the INFO column.
        thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
        chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
        concurrent_fetches: The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
        allow_anonymous: Whether to allow anonymous access to object storage.
        enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
        max_retries:  The maximum number of retries for reading the file from object storage.
        timeout: The timeout in seconds for reading the file from object storage.
        compression_type: The compression type of the VCF file. If not specified, it will be detected automatically. For presigned URLs, you need to specify the compression type explicitly, e.g. `compression="bgz"`.
        streaming: Whether to read the VCF file in streaming mode.

    !!! note
        VCF reader uses **1-based** coordinate system for the `start` and `end` columns.
    """
    object_storage_options = PyObjectStorageOptions(
        allow_anonymous=allow_anonymous,
        enable_request_payer=enable_request_payer,
        chunk_size=chunk_size,
        concurrent_fetches=concurrent_fetches,
        max_retries=max_retries,
        timeout=timeout,
        compression_type=compression_type,
    )

    vcf_read_options = VcfReadOptions(
        info_fields=_cleanse_infos(info_fields),
        thread_num=thread_num,
        object_storage_options=object_storage_options,
    )
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


def describe_vcf(
    path: str,
    allow_anonymous: bool = False,
    enable_request_payer: bool = False,
    compression_type: str = "auto",
) -> pl.DataFrame:
    """
    Describe VCF INFO schema.

    Parameters:
        path: The path to the VCF file.
        allow_anonymous: Whether to allow anonymous access to object storage (GCS and S3 supported).
        enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
        compression_type: The compression type of the VCF file. If not specified, it will be detected automatically. For presigned URLs, you need to specify the compression type explicitly, e.g. `compression="bgz"`.

    !!! Example
        ```python
        import polars_bio as pb
        vcf_1 = "gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz"
        pb.describe_vcf(vcf_1, allow_anonymous=True).sort("name").limit(5)
        ```

        ```shell
            shape: (5, 3)
        ┌───────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────┐
        │ name      ┆ type    ┆ description                                                                          │
        │ ---       ┆ ---     ┆ ---                                                                                  │
        │ str       ┆ str     ┆ str                                                                                  │
        ╞═══════════╪═════════╪══════════════════════════════════════════════════════════════════════════════════════╡
        │ AC        ┆ Integer ┆ Number of non-reference alleles observed (biallelic sites only).                     │
        │ AC_XX     ┆ Integer ┆ Number of non-reference XX alleles observed (biallelic sites only).                  │
        │ AC_XY     ┆ Integer ┆ Number of non-reference XY alleles observed (biallelic sites only).                  │
        │ AC_afr    ┆ Integer ┆ Number of non-reference African-American alleles observed (biallelic sites only).    │
        │ AC_afr_XX ┆ Integer ┆ Number of non-reference African-American XX alleles observed (biallelic sites only). │
        └───────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────┘


        ```
    """
    object_storage_options = PyObjectStorageOptions(
        allow_anonymous=allow_anonymous,
        enable_request_payer=enable_request_payer,
        chunk_size=8,
        concurrent_fetches=1,
        max_retries=1,
        timeout=10,
        compression_type=compression_type,
    )
    return py_describe_vcf(ctx, path, object_storage_options).to_polars()


def register_vcf(
    path: str,
    name: Union[str, None] = None,
    info_fields: Union[list[str], None] = None,
    thread_num: int = 1,
    chunk_size: int = 64,
    concurrent_fetches: int = 8,
    allow_anonymous: bool = False,
    max_retries: int = 5,
    timeout: int = 300,
    enable_request_payer: bool = False,
    compression_type: str = "auto",
) -> None:
    """
    Register a VCF file as a Datafusion table.

    Parameters:
        path: The path to the VCF file.
        name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
        info_fields: The fields to read from the INFO column.
        thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
        chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
        concurrent_fetches: The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
        allow_anonymous: Whether to allow anonymous access to object storage.
        enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
        compression_type: The compression type of the VCF file. If not specified, it will be detected automatically. For presigned URLs, you need to specify the compression type explicitly, e.g. `compression="bgz"`.
        max_retries:  The maximum number of retries for reading the file from object storage.
        timeout: The timeout in seconds for reading the file from object storage.
    !!! note
        VCF reader uses **1-based** coordinate system for the `start` and `end` columns.

    !!! Example
          ```python
          import polars_bio as pb
          pb.register_vcf("/tmp/gnomad.v4.1.sv.sites.vcf.gz")
          ```
         ```shell
         INFO:polars_bio:Table: gnomad_v4_1_sv_sites_gz registered for path: /tmp/gnomad.v4.1.sv.sites.vcf.gz
         ```
    !!! tip
        `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the VCF file. As a rule of thumb for large scale operations (reading a whole VCF), it is recommended to the default values.
    """

    object_storage_options = PyObjectStorageOptions(
        allow_anonymous=allow_anonymous,
        enable_request_payer=enable_request_payer,
        chunk_size=chunk_size,
        concurrent_fetches=concurrent_fetches,
        max_retries=max_retries,
        timeout=timeout,
        compression_type=compression_type,
    )

    vcf_read_options = VcfReadOptions(
        info_fields=_cleanse_infos(info_fields),
        thread_num=thread_num,
        object_storage_options=object_storage_options,
    )
    read_options = ReadOptions(vcf_read_options=vcf_read_options)
    py_register_table(ctx, path, name, InputFormat.Vcf, read_options)


def register_view(name: str, query: str) -> None:
    """
    Register a query as a Datafusion view. This view can be used in genomic ranges operations,
    such as overlap, nearest, and count_overlaps. It is useful for filtering, transforming, and aggregating data
    prior to the range operation. When combined with the range operation, it can be used to perform complex in a streaming fashion end-to-end.

    Parameters:
        name: The name of the table.
        query: The SQL query.

    !!! Example
          ```python
          import polars_bio as pb
          pb.register_vcf("gs://gcp-public-data--gnomad/release/4.1/vcf/exomes/gnomad.exomes.v4.1.sites.chr21.vcf.bgz", "gnomad_sv")
          pb.register_view("v_gnomad_sv", "SELECT replace(chrom,'chr', '') AS chrom, start, end FROM gnomad_sv")
          pb.sql("SELECT * FROM v_gnomad_sv").limit(5).collect()
          ```
          ```shell
            shape: (5, 3)
            ┌───────┬─────────┬─────────┐
            │ chrom ┆ start   ┆ end     │
            │ ---   ┆ ---     ┆ ---     │
            │ str   ┆ u32     ┆ u32     │
            ╞═══════╪═════════╪═════════╡
            │ 21    ┆ 5031905 ┆ 5031905 │
            │ 21    ┆ 5031905 ┆ 5031905 │
            │ 21    ┆ 5031909 ┆ 5031909 │
            │ 21    ┆ 5031911 ┆ 5031911 │
            │ 21    ┆ 5031911 ┆ 5031911 │
            └───────┴─────────┴─────────┘
          ```
    """
    py_register_view(ctx, name, query)


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


def from_polars(name: str, df: Union[pl.DataFrame, pl.LazyFrame]) -> None:
    """
    Register a Polars DataFrame as a DataFusion table.

    Parameters:
        name: The name of the table.
        df: The Polars DataFrame.
    !!! Example
        ```python
        import polars as pl
        import polars_bio as pb
        df = pl.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 6]
        })
        pb.from_polars("test_df", df)
        pb.sql("SELECT * FROM test_df").collect()
        ```
        ```shell
        3rows [00:00, 2978.91rows/s]
        shape: (3, 2)
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 1   ┆ 4   │
        │ 2   ┆ 5   │
        │ 3   ┆ 6   │
        └─────┴─────┘
        ```
    """
    reader = (
        df.to_arrow()
        if isinstance(df, pl.DataFrame)
        else df.collect().to_arrow().to_reader()
    )
    py_from_polars(ctx, name, reader)


def _cleanse_infos(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]
