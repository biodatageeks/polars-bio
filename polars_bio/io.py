from typing import Dict, Iterator, Optional, Union

import polars as pl
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    BamReadOptions,
    BedReadOptions,
    CramReadOptions,
    FastaReadOptions,
    FastqReadOptions,
    FastqWriteOptions,
    GffReadOptions,
    InputFormat,
    OutputFormat,
    PyObjectStorageOptions,
    ReadOptions,
    VcfReadOptions,
    VcfWriteOptions,
    WriteOptions,
    py_describe_vcf,
    py_from_polars,
    py_get_table_schema,
    py_read_sql,
    py_read_table,
    py_register_table,
    py_write_table,
)

from ._metadata import get_vcf_metadata, set_coordinate_system, set_vcf_metadata
from .context import _resolve_zero_based, ctx

SCHEMAS = {
    "bed3": ["chrom", "start", "end"],
    "bed4": ["chrom", "start", "end", "name"],
    "bed5": ["chrom", "start", "end", "name", "score"],
    "bed6": ["chrom", "start", "end", "name", "score", "strand"],
    "bed7": ["chrom", "start", "end", "name", "score", "strand", "thickStart"],
    "bed8": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
    ],
    "bed9": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
        "itemRgb",
    ],
    "bed12": [
        "chrom",
        "start",
        "end",
        "name",
        "score",
        "strand",
        "thickStart",
        "thickEnd",
        "itemRgb",
        "blockCount",
        "blockSizes",
        "blockStarts",
    ],
}


class IOOperations:
    @staticmethod
    def read_fasta(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
    ) -> pl.DataFrame:
        """

        Read a FASTA file into a DataFrame.

        Parameters:
            path: The path to the FASTA file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.read_fasta("/tmp/test.fasta").limit(1)
            ```
            ```shell
             shape: (1, 3)
            ┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name                    ┆ description                     ┆ sequence                        │
            │ ---                     ┆ ---                             ┆ ---                             │
            │ str                     ┆ str                             ┆ str                             │
            ╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ ENA|BK006935|BK006935.2 ┆ TPA_inf: Saccharomyces cerevis… ┆ CCACACCACACCCACACACCCACACACCAC… │
            └─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
            ```
        """
        return IOOperations.scan_fasta(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
        ).collect()

    @staticmethod
    def scan_fasta(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
    ) -> pl.LazyFrame:
        """

        Lazily read a FASTA file into a LazyFrame.

        Parameters:
            path: The path to the FASTA file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.scan_fasta("/tmp/test.fasta").limit(1).collect()
            ```
            ```shell
             shape: (1, 3)
            ┌─────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name                    ┆ description                     ┆ sequence                        │
            │ ---                     ┆ ---                             ┆ ---                             │
            │ str                     ┆ str                             ┆ str                             │
            ╞═════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ ENA|BK006935|BK006935.2 ┆ TPA_inf: Saccharomyces cerevis… ┆ CCACACCACACCCACACACCCACACACCAC… │
            └─────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘
            ```
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
        fasta_read_options = FastaReadOptions(
            object_storage_options=object_storage_options
        )
        read_options = ReadOptions(fasta_read_options=fasta_read_options)
        return _read_file(path, InputFormat.Fasta, read_options, projection_pushdown)

    @staticmethod
    def read_vcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a VCF file into a DataFrame.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields will be detected automatically from the VCF header. Use this to limit fields for better performance.
            format_fields: List of FORMAT field names to include (per-sample genotype data). If *None*, all FORMAT fields will be automatically detected from the VCF header. Column naming depends on the number of samples: for **single-sample** VCFs, columns are named directly by the FORMAT field (e.g., `GT`, `DP`); for **multi-sample** VCFs, columns are named `{sample_name}_{format_field}` (e.g., `NA12878_GT`, `NA12879_DP`). The GT field is always converted to string with `/` (unphased) or `|` (phased) separator.
            thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! Example "Reading VCF with INFO and FORMAT fields"
            ```python
            import polars_bio as pb

            # Read VCF with both INFO and FORMAT fields
            df = pb.read_vcf(
                "sample.vcf.gz",
                info_fields=["END"],              # INFO field
                format_fields=["GT", "DP", "GQ"]  # FORMAT fields
            )

            # Single-sample VCF: FORMAT columns named directly (GT, DP, GQ)
            print(df.select(["chrom", "start", "ref", "alt", "END", "GT", "DP", "GQ"]))
            # Output:
            # shape: (10, 8)
            # ┌───────┬───────┬─────┬─────┬──────┬─────┬─────┬─────┐
            # │ chrom ┆ start ┆ ref ┆ alt ┆ END  ┆ GT  ┆ DP  ┆ GQ  │
            # │ str   ┆ u32   ┆ str ┆ str ┆ i32  ┆ str ┆ i32 ┆ i32 │
            # ╞═══════╪═══════╪═════╪═════╪══════╪═════╪═════╪═════╡
            # │ 1     ┆ 10009 ┆ A   ┆ .   ┆ null ┆ 0/0 ┆ 10  ┆ 27  │
            # │ 1     ┆ 10015 ┆ A   ┆ .   ┆ null ┆ 0/0 ┆ 17  ┆ 35  │
            # └───────┴───────┴─────┴─────┴──────┴─────┴─────┴─────┘

            # Multi-sample VCF: FORMAT columns named {sample}_{field}
            df = pb.read_vcf("multisample.vcf", format_fields=["GT", "DP"])
            print(df.select(["chrom", "start", "NA12878_GT", "NA12878_DP", "NA12879_GT"]))
            ```
        """
        lf = IOOperations.scan_vcf(
            path,
            info_fields,
            format_fields,
            thread_num,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_vcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a VCF file into a LazyFrame.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields will be detected automatically from the VCF header. Use this to limit fields for better performance.
            format_fields: List of FORMAT field names to include (per-sample genotype data). If *None*, all FORMAT fields will be automatically detected from the VCF header. Column naming depends on the number of samples: for **single-sample** VCFs, columns are named directly by the FORMAT field (e.g., `GT`, `DP`); for **multi-sample** VCFs, columns are named `{sample_name}_{format_field}` (e.g., `NA12878_GT`, `NA12879_DP`). The GT field is always converted to string with `/` (unphased) or `|` (phased) separator.
            thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! Example "Lazy scanning VCF with INFO and FORMAT fields"
            ```python
            import polars_bio as pb

            # Lazily scan VCF with both INFO and FORMAT fields
            lf = pb.scan_vcf(
                "sample.vcf.gz",
                info_fields=["END"],              # INFO field
                format_fields=["GT", "DP", "GQ"]  # FORMAT fields
            )

            # Apply filters and collect only what's needed
            df = lf.filter(pl.col("DP") > 20).select(
                ["chrom", "start", "ref", "alt", "GT", "DP", "GQ"]
            ).collect()

            # Single-sample VCF: FORMAT columns named directly (GT, DP, GQ)
            # Multi-sample VCF: FORMAT columns named {sample}_{field}
            ```
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

        # Use provided info_fields or autodetect from VCF header
        if info_fields is not None:
            initial_info_fields = info_fields
        else:
            # Get all info fields from VCF header for proper projection pushdown
            all_info_fields = None
            try:
                vcf_schema_df = IOOperations.describe_vcf(
                    path,
                    allow_anonymous=allow_anonymous,
                    enable_request_payer=enable_request_payer,
                    compression_type=compression_type,
                )
                # Use column name 'name' not 'id' based on the schema output
                all_info_fields = vcf_schema_df.select("name").to_series().to_list()
            except Exception:
                # Fallback to None if unable to get info fields
                all_info_fields = None

            # Always start with all info fields to establish full schema
            # The callback will re-register with only requested info fields for optimization
            initial_info_fields = all_info_fields

        zero_based = _resolve_zero_based(use_zero_based)
        vcf_read_options = VcfReadOptions(
            info_fields=initial_info_fields,
            format_fields=format_fields,
            thread_num=thread_num,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        return _read_file(
            path,
            InputFormat.Vcf,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_gff(
        path: str,
        attr_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        predicate_pushdown: bool = False,
        parallel: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a GFF file into a DataFrame.

        Parameters:
            path: The path to the GFF file.
            attr_fields: List of attribute field names to extract as separate columns. If *None*, attributes will be kept as a nested structure. Use this to extract specific attributes like 'ID', 'gene_name', 'gene_type', etc. as direct columns for easier access.
            thread_num: The number of threads to use for reading the GFF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown optimization to push filter conditions down to the DataFusion table provider level, reducing data processing and I/O.
            parallel: Whether to use the parallel reader for BGZF-compressed local files (uses BGZF chunk-level parallelism similar to FASTQ).
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_gff(
            path,
            attr_fields,
            thread_num,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            predicate_pushdown,
            parallel,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_gff(
        path: str,
        attr_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        predicate_pushdown: bool = False,
        parallel: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a GFF file into a LazyFrame.

        Parameters:
            path: The path to the GFF file.
            attr_fields: List of attribute field names to extract as separate columns. If *None*, attributes will be kept as a nested structure. Use this to extract specific attributes like 'ID', 'gene_name', 'gene_type', etc. as direct columns for easier access.
            thread_num: The number of threads to use for reading the GFF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            predicate_pushdown: Enable predicate pushdown optimization to push filter conditions down to the DataFusion table provider level, reducing data processing and I/O.
            parallel: Whether to use the parallel reader for BGZF-compressed local files (use BGZF chunk-level parallelism similar to FASTQ).
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
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

        zero_based = _resolve_zero_based(use_zero_based)
        gff_read_options = GffReadOptions(
            attr_fields=attr_fields,
            thread_num=thread_num,
            object_storage_options=object_storage_options,
            parallel=parallel,
            zero_based=zero_based,
        )
        read_options = ReadOptions(gff_read_options=gff_read_options)
        return _read_file(
            path,
            InputFormat.Gff,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_bam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a BAM file into a DataFrame.

        Parameters:
            path: The path to the BAM file.
            tag_fields: List of BAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large-scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_bam(
            path,
            tag_fields,
            thread_num,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            projection_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bam(
        path: str,
        tag_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BAM file into a LazyFrame.

        Parameters:
            path: The path to the BAM file.
            tag_fields: List of BAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type="auto",
        )

        zero_based = _resolve_zero_based(use_zero_based)
        bam_read_options = BamReadOptions(
            thread_num=thread_num,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            tag_fields=tag_fields,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        return _read_file(
            path,
            InputFormat.Bam,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_cram(
        path: str,
        reference_path: str = None,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a CRAM file into a DataFrame.

        Parameters:
            path: The path to the CRAM file (local or cloud storage: S3, GCS, Azure Blob).
            reference_path: Optional path to external FASTA reference file (**local path only**, cloud storage not supported). If not provided, the CRAM file must contain embedded reference sequences. The FASTA file must have an accompanying index file (.fai) in the same directory. Create the index using: `samtools faidx reference.fasta`
            tag_fields: List of CRAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! example "Using External Reference"
            ```python
            import polars_bio as pb

            # Read CRAM with external reference
            df = pb.read_cram(
                "/path/to/file.cram",
                reference_path="/path/to/reference.fasta"
            )
            ```

        !!! example "Public CRAM File Example"
            Download and read a public CRAM file from 42basepairs:
            ```bash
            # Download the CRAM file and reference
            wget https://42basepairs.com/download/s3/gatk-test-data/wgs_cram/NA12878_20k_hg38/NA12878.cram
            wget https://storage.googleapis.com/genomics-public-data/resources/broad/hg38/v0/Homo_sapiens_assembly38.fasta

            # Create FASTA index (required)
            samtools faidx Homo_sapiens_assembly38.fasta
            ```

            ```python
            import polars_bio as pb

            # Read first 5 reads from the CRAM file
            df = pb.scan_cram(
                "NA12878.cram",
                reference_path="Homo_sapiens_assembly38.fasta"
            ).limit(5).collect()

            print(df.select(["name", "chrom", "start", "end", "cigar"]))
            ```

        !!! example "Creating CRAM with Embedded Reference"
            To create a CRAM file with embedded reference using samtools:
            ```bash
            samtools view -C -o output.cram --output-fmt-option embed_ref=1 input.bam
            ```

        Returns:
            A Polars DataFrame with the following schema:
                - name: Read name (String)
                - chrom: Chromosome/contig name (String)
                - start: Alignment start position, 1-based (UInt32)
                - end: Alignment end position, 1-based (UInt32)
                - flags: SAM flags (UInt32)
                - cigar: CIGAR string (String)
                - mapping_quality: Mapping quality (UInt32)
                - mate_chrom: Mate chromosome/contig name (String)
                - mate_start: Mate alignment start position, 1-based (UInt32)
                - sequence: Read sequence (String)
                - quality_scores: Base quality scores (String)
        """
        lf = IOOperations.scan_cram(
            path,
            reference_path,
            tag_fields,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            projection_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_cram(
        path: str,
        reference_path: str = None,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a CRAM file into a LazyFrame.

        Parameters:
            path: The path to the CRAM file (local or cloud storage: S3, GCS, Azure Blob).
            reference_path: Optional path to external FASTA reference file (**local path only**, cloud storage not supported). If not provided, the CRAM file must contain embedded reference sequences. The FASTA file must have an accompanying index file (.fai) in the same directory. Create the index using: `samtools faidx reference.fasta`
            tag_fields: List of CRAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown optimization. When True, only requested columns are processed at the DataFusion execution level, improving performance and reducing memory usage.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.

        !!! example "Using External Reference"
            ```python
            import polars_bio as pb

            # Lazy scan CRAM with external reference
            lf = pb.scan_cram(
                "/path/to/file.cram",
                reference_path="/path/to/reference.fasta"
            )

            # Apply transformations and collect
            df = lf.filter(pl.col("chrom") == "chr1").collect()
            ```

        !!! example "Public CRAM File Example"
            Download and read a public CRAM file from 42basepairs:
            ```bash
            # Download the CRAM file and reference
            wget https://42basepairs.com/download/s3/gatk-test-data/wgs_cram/NA12878_20k_hg38/NA12878.cram
            wget https://storage.googleapis.com/genomics-public-data/resources/broad/hg38/v0/Homo_sapiens_assembly38.fasta

            # Create FASTA index (required)
            samtools faidx Homo_sapiens_assembly38.fasta
            ```

            ```python
            import polars_bio as pb
            import polars as pl

            # Lazy scan and filter for chromosome 20 reads
            df = pb.scan_cram(
                "NA12878.cram",
                reference_path="Homo_sapiens_assembly38.fasta"
            ).filter(
                pl.col("chrom") == "chr20"
            ).select(
                ["name", "chrom", "start", "end", "mapping_quality"]
            ).limit(10).collect()

            print(df)
            ```

        !!! example "Creating CRAM with Embedded Reference"
            To create a CRAM file with embedded reference using samtools:
            ```bash
            samtools view -C -o output.cram --output-fmt-option embed_ref=1 input.bam
            ```

        Returns:
            A Polars LazyFrame with the following schema:
                - name: Read name (String)
                - chrom: Chromosome/contig name (String)
                - start: Alignment start position, 1-based (UInt32)
                - end: Alignment end position, 1-based (UInt32)
                - flags: SAM flags (UInt32)
                - cigar: CIGAR string (String)
                - mapping_quality: Mapping quality (UInt32)
                - mate_chrom: Mate chromosome/contig name (String)
                - mate_start: Mate alignment start position, 1-based (UInt32)
                - sequence: Read sequence (String)
                - quality_scores: Base quality scores (String)
        """
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type="auto",
        )

        zero_based = _resolve_zero_based(use_zero_based)
        cram_read_options = CramReadOptions(
            reference_path=reference_path,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
            tag_fields=tag_fields,
        )
        read_options = ReadOptions(cram_read_options=cram_read_options)
        return _read_file(
            path,
            InputFormat.Cram,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_fastq(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        parallel: bool = False,
        projection_pushdown: bool = False,
    ) -> pl.DataFrame:
        """
        Read a FASTQ file into a DataFrame.

        Parameters:
            path: The path to the FASTQ file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            parallel: Whether to use the parallel reader for BGZF compressed files stored **locally**. GZI index is **required**.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
        """
        return IOOperations.scan_fastq(
            path,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            parallel,
            projection_pushdown,
        ).collect()

    @staticmethod
    def scan_fastq(
        path: str,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        parallel: bool = False,
        projection_pushdown: bool = False,
    ) -> pl.LazyFrame:
        """
        Lazily read a FASTQ file into a LazyFrame.

        Parameters:
            path: The path to the FASTQ file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            parallel: Whether to use the parallel reader for BGZF compressed files stored **locally**. GZI index is **required**.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
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

        fastq_read_options = FastqReadOptions(
            object_storage_options=object_storage_options, parallel=parallel
        )
        read_options = ReadOptions(fastq_read_options=fastq_read_options)
        return _read_file(path, InputFormat.Fastq, read_options, projection_pushdown)

    @staticmethod
    def read_bed(
        path: str,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a BED file into a DataFrame.

        Parameters:
            path: The path to the BED file.
            thread_num: The number of threads to use for reading the BED file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically based on the file extension. BGZF compressions is supported ('bgz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
        """
        lf = IOOperations.scan_bed(
            path,
            thread_num,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            compression_type,
            projection_pushdown,
            use_zero_based,
        )
        # Get metadata before collecting (polars-config-meta doesn't preserve through collect)
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        # Set metadata on the collected DataFrame
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_bed(
        path: str,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = False,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a BED file into a LazyFrame.

        Parameters:
            path: The path to the BED file.
            thread_num: The number of threads to use for reading the BED file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically based on the file extension. BGZF compressions is supported ('bgz').
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration `datafusion.bio.coordinate_system_zero_based`.

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            By default, coordinates are output in **1-based closed** format. Use `use_zero_based=True` or set `pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, True)` for 0-based half-open coordinates.
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

        zero_based = _resolve_zero_based(use_zero_based)
        bed_read_options = BedReadOptions(
            thread_num=thread_num,
            object_storage_options=object_storage_options,
            zero_based=zero_based,
        )
        read_options = ReadOptions(bed_read_options=bed_read_options)
        return _read_file(
            path,
            InputFormat.Bed,
            read_options,
            projection_pushdown,
            zero_based=zero_based,
        )

    @staticmethod
    def read_table(path: str, schema: Dict = None, **kwargs) -> pl.DataFrame:
        """
         Read a tab-delimited (i.e. BED) file into a Polars DataFrame.
         Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
         but faster. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

        Parameters:
            path: The path to the file.
            schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).
        """
        return IOOperations.scan_table(path, schema, **kwargs).collect()

    @staticmethod
    def scan_table(path: str, schema: Dict = None, **kwargs) -> pl.LazyFrame:
        """
         Lazily read a tab-delimited (i.e. BED) file into a Polars LazyFrame.
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

    @staticmethod
    def describe_vcf(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> pl.DataFrame:
        """
        Describe VCF INFO schema.

        Parameters:
            path: The path to the VCF file.
            allow_anonymous: Whether to allow anonymous access to object storage (GCS and S3 supported).
            enable_request_payer: Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
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

    @staticmethod
    def from_polars(name: str, df: Union[pl.DataFrame, pl.LazyFrame]) -> None:
        """
        Register a Polars DataFrame as a DataFusion table.

        Parameters:
            name: The name of the table.
            df: The Polars DataFrame.
        """
        reader = (
            df.to_arrow()
            if isinstance(df, pl.DataFrame)
            else df.collect().to_arrow().to_reader()
        )
        py_from_polars(ctx, name, reader)

    @staticmethod
    def write_vcf(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
    ) -> int:
        """
        Write a DataFrame to VCF format.

        Coordinate system is automatically read from DataFrame metadata (set during
        read_vcf). Compression is auto-detected from the file extension.

        Parameters:
            df: The DataFrame or LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        Returns:
            The number of rows written.

        !!! Example "Writing VCF files"
            ```python
            import polars_bio as pb

            # Read a VCF file
            df = pb.read_vcf("input.vcf")

            # Write to uncompressed VCF
            pb.write_vcf(df, "output.vcf")

            # Write to BGZF-compressed VCF
            pb.write_vcf(df, "output.vcf.bgz")

            # Write to GZIP-compressed VCF
            pb.write_vcf(df, "output.vcf.gz")
            ```
        """
        return _write_file(df, path, OutputFormat.Vcf)

    @staticmethod
    def sink_vcf(
        lf: pl.LazyFrame,
        path: str,
    ) -> None:
        """
        Streaming write a LazyFrame to VCF format.

        This method executes the LazyFrame immediately and writes the results
        to the specified path. Unlike `write_vcf`, it doesn't return the row count.

        Coordinate system is automatically read from LazyFrame metadata (set during
        scan_vcf). Compression is auto-detected from the file extension.

        Parameters:
            lf: The LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.vcf.bgz for BGZF, .vcf.gz for GZIP, .vcf for uncompressed).

        !!! Example "Streaming write VCF"
            ```python
            import polars_bio as pb

            # Lazy read and filter, then sink to VCF
            lf = pb.scan_vcf("large_input.vcf").filter(pl.col("qual") > 30)
            pb.sink_vcf(lf, "filtered_output.vcf.bgz")
            ```
        """
        _write_file(lf, path, OutputFormat.Vcf)

    @staticmethod
    def write_fastq(
        df: Union[pl.DataFrame, pl.LazyFrame],
        path: str,
    ) -> int:
        """
        Write a DataFrame to FASTQ format.

        Compression is auto-detected from the file extension.

        Parameters:
            df: The DataFrame or LazyFrame to write. Must have columns:
                - name: Read name/identifier
                - sequence: DNA sequence
                - quality_scores: Quality scores string
                Optional: description (added after name on header line)
            path: The output file path. Compression is auto-detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        Returns:
            The number of rows written.

        !!! Example "Writing FASTQ files"
            ```python
            import polars_bio as pb

            # Read a FASTQ file
            df = pb.read_fastq("input.fastq")

            # Write to uncompressed FASTQ
            pb.write_fastq(df, "output.fastq")

            # Write to GZIP-compressed FASTQ
            pb.write_fastq(df, "output.fastq.gz")
            ```
        """
        return _write_file(df, path, OutputFormat.Fastq)

    @staticmethod
    def sink_fastq(
        lf: pl.LazyFrame,
        path: str,
    ) -> None:
        """
        Streaming write a LazyFrame to FASTQ format.

        Compression is auto-detected from the file extension.

        Parameters:
            lf: The LazyFrame to write.
            path: The output file path. Compression is auto-detected from extension
                  (.fastq.bgz for BGZF, .fastq.gz for GZIP, .fastq for uncompressed).

        !!! Example "Streaming write FASTQ"
            ```python
            import polars_bio as pb

            # Lazy read, filter by quality, then sink
            lf = pb.scan_fastq("large_input.fastq.gz")
            pb.sink_fastq(lf.limit(1000), "sample_output.fastq")
            ```
        """
        _write_file(lf, path, OutputFormat.Fastq)


def _cleanse_fields(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]


def _write_file(
    df: Union[pl.DataFrame, pl.LazyFrame],
    path: str,
    output_format: OutputFormat,
) -> int:
    """
    Internal helper to write DataFrame to a file with TRUE STREAMING.

    This function now streams data directly from LazyFrame to file without
    materializing the entire dataset in memory. This is critical for large files!

    Coordinate system is read from DataFrame/LazyFrame metadata.
    Compression is auto-detected from file extension.

    Parameters:
        df: The DataFrame or LazyFrame to write.
        path: The output file path.
        output_format: The output format (Vcf or Fastq).

    Returns:
        The number of rows written.
    """
    import json

    from ._metadata import get_coordinate_system, get_metadata

    # Get metadata WITHOUT collecting (works for both DataFrame and LazyFrame)
    source_meta = None
    vcf_header = None
    zero_based = None

    try:
        source_meta = get_metadata(df)
        if output_format == OutputFormat.Vcf:
            vcf_header = source_meta.get("header") if source_meta else None
    except (KeyError, AttributeError, TypeError):
        pass

    # Get coordinate system from metadata
    try:
        zero_based = get_coordinate_system(df)
    except (KeyError, AttributeError, TypeError):
        pass

    if zero_based is None:
        zero_based = _resolve_zero_based(None)

    # Build write options based on format
    if output_format == OutputFormat.Vcf:
        # Extract VCF metadata from source_header
        info_fields_json = None
        format_fields_json = None
        sample_names_json = None
        if vcf_header:
            if vcf_header.get("info_fields"):
                info_fields_json = json.dumps(vcf_header["info_fields"])
            if vcf_header.get("format_fields"):
                format_fields_json = json.dumps(vcf_header["format_fields"])
            if vcf_header.get("sample_names"):
                sample_names_json = json.dumps(vcf_header["sample_names"])

        vcf_opts = VcfWriteOptions(
            zero_based=zero_based,
            info_fields_metadata=info_fields_json,
            format_fields_metadata=format_fields_json,
            sample_names=sample_names_json,
        )
        write_options = WriteOptions(vcf_write_options=vcf_opts)
    elif output_format == OutputFormat.Fastq:
        fastq_opts = FastqWriteOptions()
        write_options = WriteOptions(fastq_write_options=fastq_opts)
    else:
        write_options = None

    # ✅ TRUE STREAMING: Use collect_batches pattern with Utf8View → LargeUtf8 conversion
    # This works for filtered/transformed LazyFrames
    # NOTE: Filtering currently materializes all data - predicate pushdown to DataFusion not yet implemented
    if isinstance(df, pl.LazyFrame):
        import pyarrow as pa
        import pyarrow.compute as pc

        # Get streaming batches from Polars
        batches_iter = df.collect_batches(lazy=True, engine="streaming")
        stream = batches_iter._inner

        # We need to convert Utf8View to LargeUtf8 on the Rust side
        # Pass the stream and let Rust handle the conversion
        return py_write_table(ctx, stream, path, output_format, write_options)
    else:
        # Already a DataFrame
        arrow_table = df.to_arrow()
        reader = arrow_table.to_reader()
        return py_write_table(ctx, reader, path, output_format, write_options)


def _apply_combined_pushdown_via_sql(
    ctx,
    table_name,
    original_df,
    predicate,
    projected_columns,
    predicate_pushdown,
    projection_pushdown,
):
    """Apply both predicate and projection pushdown using SQL approach."""
    from polars_bio.polars_bio import py_read_sql

    # Build SQL query with combined optimizations
    select_clause = "*"
    if projection_pushdown and projected_columns:
        select_clause = ", ".join([f'"{c}"' for c in projected_columns])

    where_clause = ""
    if predicate_pushdown and predicate is not None:
        try:
            # Use the proven regex-based predicate translation
            where_clause = _build_sql_where_from_predicate_safe(predicate)
        except Exception as e:
            where_clause = ""

    # No fallback - if we can't parse to SQL, just use projection only
    # This keeps us in pure SQL mode for maximum performance

    # Construct optimized SQL query
    if where_clause:
        sql = f"SELECT {select_clause} FROM {table_name} WHERE {where_clause}"
    else:
        sql = f"SELECT {select_clause} FROM {table_name}"

    # Execute with DataFusion - this leverages the proven 4x+ optimization
    return py_read_sql(ctx, sql)


def _build_sql_where_from_predicate_safe(predicate):
    """Build SQL WHERE clause by parsing all individual conditions and connecting with AND."""
    import re

    pred_str = str(predicate).strip("[]")

    # Find all individual conditions in the nested structure
    conditions = []

    # String equality/inequality patterns (including empty strings)
    # Accept both with and without surrounding parentheses in Polars repr
    str_eq_patterns = [
        r'\(col\("([^"]+)"\)\)\s*==\s*\("([^"]*)"\)',  # (col("x")) == ("v")
        r'col\("([^"]+)"\)\s*==\s*"([^"]*)"',  # col("x") == "v"
    ]
    for pat in str_eq_patterns:
        for column, value in re.findall(pat, pred_str):
            conditions.append(f"\"{column}\" = '{value}'")

    # Numeric comparison patterns (handle both formats: with and without "dyn int:")
    numeric_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\((?:dyn int:\s*)?(\d+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\((?:dyn int:\s*)?(\d+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\((?:dyn int:\s*)?(\d+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\((?:dyn int:\s*)?(\d+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\((?:dyn int:\s*)?(\d+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\((?:dyn int:\s*)?(\d+)\)', "="),
        (r'col\("([^"]+)"\)\s*>\s*(\d+)', ">"),
        (r'col\("([^"]+)"\)\s*<\s*(\d+)', "<"),
        (r'col\("([^"]+)"\)\s*>=\s*(\d+)', ">="),
        (r'col\("([^"]+)"\)\s*<=\s*(\d+)', "<="),
        (r'col\("([^"]+)"\)\s*!=\s*(\d+)', "!="),
        (r'col\("([^"]+)"\)\s*==\s*(\d+)', "="),
    ]

    for pattern, op in numeric_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # Float comparison patterns (handle both formats: with and without "dyn float:")
    float_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\((?:dyn float:\s*)?([\d.]+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\((?:dyn float:\s*)?([\d.]+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\((?:dyn float:\s*)?([\d.]+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\((?:dyn float:\s*)?([\d.]+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\((?:dyn float:\s*)?([\d.]+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\((?:dyn float:\s*)?([\d.]+)\)', "="),
        (r'col\("([^"]+)"\)\s*>\s*([\d.]+)', ">"),
        (r'col\("([^"]+)"\)\s*<\s*([\d.]+)', "<"),
        (r'col\("([^"]+)"\)\s*>=\s*([\d.]+)', ">="),
        (r'col\("([^"]+)"\)\s*<=\s*([\d.]+)', "<="),
        (r'col\("([^"]+)"\)\s*!=\s*([\d.]+)', "!="),
        (r'col\("([^"]+)"\)\s*==\s*([\d.]+)', "="),
    ]

    for pattern, op in float_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # IN list pattern: col("x").is_in([v1, v2, ...])
    in_matches = re.findall(r'col\("([^"]+)"\)\.is_in\(\[(.*?)\]\)', pred_str)
    for column, values_str in in_matches:
        # Tokenize values: quoted strings or numbers
        tokens = re.findall(r"'(?:[^']*)'|\"(?:[^\"]*)\"|\d+(?:\.\d+)?", values_str)
        items = []
        for t in tokens:
            if t.startswith('"') and t.endswith('"'):
                items.append("'" + t[1:-1] + "'")
            else:
                items.append(t)
        if items:
            conditions.append(f'"{column}" IN ({", ".join(items)})')

    # Join all conditions with AND
    if conditions:
        where = " AND ".join(conditions)
        # Clean up any residual bracketed list formatting from IN clause (defensive)
        where = (
            where.replace("IN ([", "IN (")
            .replace("])", ")")
            .replace("[ ", "")
            .replace(" ]", "")
        )
        # Collapse simple >= and <= pairs into BETWEEN when possible
        try:
            import re as _re

            where = _re.sub(
                r'"([^"]+)"\s*>=\s*([\d.]+)\s*AND\s*"\1"\s*<=\s*([\d.]+)',
                r'"\1" BETWEEN \2 AND \3',
                where,
            )
            where = _re.sub(
                r'"([^"]+)"\s*<=\s*([\d.]+)\s*AND\s*"\1"\s*>=\s*([\d.]+)',
                r'"\1" BETWEEN \3 AND \2',
                where,
            )
        except Exception:
            pass
        return where

    return ""


def _lazy_scan(
    schema_or_df,  # Either: PyArrow schema (from py_get_table_schema) or DataFusion DataFrame (from py_read_sql for SQL path)
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
    table_name: str = None,
    input_format: InputFormat = None,
    file_path: str = None,
    read_options: ReadOptions = None,
) -> pl.LazyFrame:

    # Handle both PyArrow schema (new streaming path) and DataFusion DataFrame (old SQL path)
    import pyarrow as pa

    df_for_stream = None  # Used for SQL path

    # Check if it's a DataFusion DataFrame by checking for schema() method
    # We use hasattr because there are multiple DataFrame classes in datafusion package
    is_datafusion_df = hasattr(schema_or_df, "schema") and hasattr(
        schema_or_df, "execute_stream"
    )

    if isinstance(schema_or_df, pa.Schema):
        # PyArrow schema (from py_get_table_schema or df.schema())
        # Convert to Polars schema dict for register_io_source
        empty_table = pa.table(
            {field.name: pa.array([], type=field.type) for field in schema_or_df}
        )
        temp_df = pl.from_arrow(empty_table)
        original_schema = dict(temp_df.schema)  # Convert to dict for register_io_source
    elif is_datafusion_df:
        # DataFusion DataFrame from py_read_sql (sql() function)
        # Extract PyArrow schema and convert to Polars schema dict
        df_for_stream = schema_or_df
        pa_schema = schema_or_df.schema()
        empty_table = pa.table(
            {field.name: pa.array([], type=field.type) for field in pa_schema}
        )
        temp_df = pl.from_arrow(empty_table)
        original_schema = dict(temp_df.schema)  # Convert to dict for register_io_source
    else:
        # Fallback: already a Polars schema
        original_schema = (
            dict(schema_or_df) if not isinstance(schema_or_df, dict) else schema_or_df
        )

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # Import here to ensure availability in all code paths
        from polars_bio.polars_bio import py_read_sql

        from .context import ctx as _ctx

        # If this is a GFF scan, perform pushdown by building a single SELECT ... WHERE ...
        if input_format == InputFormat.Gff and file_path is not None:
            from polars_bio.polars_bio import GffReadOptions, PyObjectStorageOptions
            from polars_bio.polars_bio import ReadOptions as _ReadOptions
            from polars_bio.polars_bio import (
                py_read_table,
                py_register_table,
                py_register_view,
            )

            # Extract columns requested by Polars optimizer
            requested_cols = (
                _extract_column_names_from_expr(with_columns)
                if with_columns is not None
                else []
            )

            # Compute attribute fields to request based on selected columns
            STATIC = {
                "chrom",
                "start",
                "end",
                "type",
                "source",
                "score",
                "strand",
                "phase",
                "attributes",
            }
            attr_fields = [c for c in requested_cols if c not in STATIC]

            # Derive thread/parallel/zero_based from read_options when available
            thread_num = 1
            parallel = False
            zero_based = False  # Default to 1-based (matches Python default)
            if read_options is not None:
                try:
                    gopt = getattr(read_options, "gff_read_options", None)
                    if gopt is not None:
                        tn = getattr(gopt, "thread_num", None)
                        if tn is not None:
                            thread_num = tn
                        par = getattr(gopt, "parallel", None)
                        if par is not None:
                            parallel = par
                        zb = getattr(gopt, "zero_based", None)
                        if zb is not None:
                            zero_based = zb
                except Exception:
                    pass

            # Build fresh read options (object storage options are not readable from Rust class; use safe defaults)
            obj = PyObjectStorageOptions(
                allow_anonymous=True,
                enable_request_payer=False,
                chunk_size=8,
                concurrent_fetches=1,
                max_retries=5,
                timeout=300,
                compression_type="auto",
            )
            # Determine attribute parsing behavior:
            # - if user selected raw "attributes" column: keep provider defaults (None)
            # - if user selected specific attribute columns: pass that list
            # - otherwise: disable attribute parsing with empty list for performance
            if "attributes" in requested_cols:
                _attr = None
            elif attr_fields:
                _attr = attr_fields
            else:
                _attr = []

            gff_opts = GffReadOptions(
                attr_fields=_attr,
                thread_num=thread_num,
                object_storage_options=obj,
                parallel=parallel,
                zero_based=zero_based,
            )
            ropts = _ReadOptions(gff_read_options=gff_opts)

            # Determine which table to query: reuse original unless we must change attr_fields
            table_name_use = table_name
            if projection_pushdown and requested_cols:
                # Only re-register when projection is active (we know column needs)
                table_obj = py_register_table(
                    _ctx, file_path, None, InputFormat.Gff, ropts
                )
                table_name_use = table_obj.name

            # Build SELECT clause respecting projection flag
            if projection_pushdown and requested_cols:
                select_clause = ", ".join([f'"{c}"' for c in requested_cols])
            else:
                select_clause = "*"

            # Build WHERE clause respecting predicate flag
            where_clause = ""
            if predicate_pushdown and predicate is not None:
                try:
                    where_clause = _build_sql_where_from_predicate_safe(predicate)
                except Exception:
                    where_clause = ""

            sql = f"SELECT {select_clause} FROM {table_name_use}"
            if where_clause:
                sql += f" WHERE {where_clause}"
            if n_rows and n_rows > 0:
                sql += f" LIMIT {int(n_rows)}"

            query_df = py_read_sql(_ctx, sql)

            # Stream results, applying any non-pushed operations locally
            df_stream = query_df.execute_stream()
            progress_bar = tqdm(unit="rows")
            for r in df_stream:
                py_df = r.to_pyarrow()
                out = pl.DataFrame(py_df)
                # Apply local filter if we didn't push it down
                if predicate is not None and (
                    not predicate_pushdown or not where_clause
                ):
                    out = out.filter(predicate)
                # Apply local projection if we didn't push it down
                if with_columns is not None and (
                    not projection_pushdown or not requested_cols
                ):
                    out = out.select(with_columns)
                progress_bar.update(len(out))
                yield out
            return

        # Default path (non-GFF): stream from table or DataFrame
        if df_for_stream is not None:
            # SQL path: use the DataFrame directly
            query_df = df_for_stream
        else:
            # Scan path: build SQL query for the registered table
            sql = f"SELECT * FROM {table_name}"
            if n_rows and n_rows > 0:
                sql += f" LIMIT {int(n_rows)}"
            query_df = py_read_sql(_ctx, sql)

        df_stream = query_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        remaining = int(n_rows) if n_rows is not None else None
        for r in df_stream:
            py_df = r.to_pyarrow()
            out = pl.DataFrame(py_df)
            if predicate is not None:
                out = out.filter(predicate)
            if with_columns is not None:
                out = out.select(with_columns)

            if remaining is not None:
                if remaining <= 0:
                    break
                if len(out) > remaining:
                    out = out.head(remaining)
                remaining -= len(out)

            progress_bar.update(len(out))
            yield out
            if remaining is not None and remaining <= 0:
                return

    return register_io_source(_overlap_source, schema=original_schema)


def _extract_column_names_from_expr(with_columns: Union[pl.Expr, list]) -> "List[str]":
    """Extract column names from Polars expressions."""
    if with_columns is None:
        return []

    # Handle different types of with_columns input
    if hasattr(with_columns, "__iter__") and not isinstance(with_columns, str):
        # It's a list of expressions or strings
        column_names = []
        for item in with_columns:
            if isinstance(item, str):
                column_names.append(item)
            elif hasattr(item, "meta") and hasattr(item.meta, "output_name"):
                # Polars expression with output name
                try:
                    column_names.append(item.meta.output_name())
                except Exception:
                    pass
        return column_names
    elif isinstance(with_columns, str):
        return [with_columns]
    elif hasattr(with_columns, "meta") and hasattr(with_columns.meta, "output_name"):
        # Single Polars expression
        try:
            return [with_columns.meta.output_name()]
        except Exception:
            pass

    return []


def _extract_vcf_metadata_from_schema(schema) -> dict:
    """Extract VCF field metadata from a PyArrow schema.

    This extracts the VCF-specific metadata (vcf_number, vcf_type, vcf_description)
    from Arrow field metadata and organizes it for storage in Polars config_meta.

    Args:
        schema: PyArrow schema with VCF field metadata

    Returns:
        Dict with 'info_fields', 'format_fields', and 'sample_names'
    """
    info_fields = {}
    format_fields = {}
    sample_names = []
    seen_samples = set()

    for field in schema:
        if not field.metadata:
            continue

        # Decode bytes to strings
        metadata = {
            k.decode("utf-8") if isinstance(k, bytes) else k: (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in field.metadata.items()
        }

        field_type = metadata.get("vcf_field_type")
        if field_type == "INFO":
            info_fields[field.name] = {
                "number": metadata.get("vcf_number", "."),
                "type": metadata.get("vcf_type", "String"),
                "description": metadata.get("vcf_description", ""),
            }
        elif field_type == "FORMAT":
            format_id = metadata.get("vcf_format_id", field.name)
            if format_id not in format_fields:
                format_fields[format_id] = {
                    "number": metadata.get("vcf_number", "1"),
                    "type": metadata.get("vcf_type", "String"),
                    "description": metadata.get("vcf_description", ""),
                }

            # Extract sample name from column name pattern: {sample}_{format}
            if field.name.endswith(f"_{format_id}"):
                sample = field.name[: -len(format_id) - 1]
                if sample and sample not in seen_samples:
                    seen_samples.add(sample)
                    sample_names.append(sample)

    # Handle single-sample VCFs where column name equals format_id (no sample prefix)
    # In this case, we infer the sample name. The bio-formats library uses a default
    # single sample name, so we check if any FORMAT fields exist without sample prefixes.
    if format_fields and not sample_names:
        # Check if any FORMAT column name matches a format_id directly
        format_ids = set(format_fields.keys())
        for field in schema:
            if field.name in format_ids:
                # Single-sample VCF detected - use "sample" as default name
                sample_names = ["sample"]
                break

    return {
        "info_fields": info_fields if info_fields else None,
        "format_fields": format_fields if format_fields else None,
        "sample_names": sample_names if sample_names else None,
    }


def _extract_vcf_header_extras(schema) -> dict:
    """Extract VCF schema-level metadata from Arrow schema.

    Based on datafusion-bio-formats PR #47 naming convention: bio.vcf.*
    Extracts schema-level metadata that provides provenance and validation info.

    Args:
        schema: PyArrow schema with VCF schema-level metadata

    Returns:
        Dict with optional keys:
        - "version": VCF version (e.g., "VCFv4.2")
        - "contigs": List of contig definitions
        - "filters": List of filter definitions
        - "alt_definitions": List of ALT allele definitions

    Schema-level metadata keys (from datafusion-bio-formats):
        - bio.vcf.file_format: VCF version string
        - bio.vcf.contigs: JSON array of ContigMetadata
        - bio.vcf.filters: JSON array of FilterMetadata
        - bio.vcf.alternative_alleles: JSON array of AltAlleleMetadata
        - bio.vcf.samples: JSON array of sample names (redundant with column-based extraction)
    """
    import json

    extras = {}
    schema_meta = schema.metadata or {}

    # Helper to safely decode bytes or string keys
    def get_meta(key: str):
        # Try string key first, then bytes
        value = schema_meta.get(key) or schema_meta.get(key.encode())
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    # Extract version (plain string)
    version = get_meta("bio.vcf.file_format")
    if version:
        extras["version"] = version

    # Extract JSON-encoded schema-level metadata
    json_fields = [
        ("bio.vcf.contigs", "contigs"),
        ("bio.vcf.filters", "filters"),
        ("bio.vcf.alternative_alleles", "alt_definitions"),
    ]

    for key, target_key in json_fields:
        value = get_meta(key)
        if value:
            try:
                extras[target_key] = json.loads(value)
            except json.JSONDecodeError:
                # Silently skip malformed JSON
                pass

    return extras


def _format_to_string(input_format: InputFormat) -> str:
    """Convert InputFormat enum to string identifier for metadata storage.

    Args:
        input_format: InputFormat enum value

    Returns:
        String identifier (e.g., "vcf", "fastq", "bam")
    """
    # Use string comparison since InputFormat is not hashable
    format_str = str(input_format)
    if "Vcf" in format_str:
        return "vcf"
    elif "Bam" in format_str:
        return "bam"
    elif "Cram" in format_str:
        return "cram"
    elif "Fastq" in format_str:
        return "fastq"
    elif "Fasta" in format_str:
        return "fasta"
    elif "Gff" in format_str:
        return "gff"
    elif "Bed" in format_str:
        return "bed"
    else:
        return "unknown"


def _read_file(
    path: str,
    input_format: InputFormat,
    read_options: ReadOptions,
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
    zero_based: bool = True,
) -> pl.LazyFrame:
    table = py_register_table(ctx, path, None, input_format, read_options)

    # Get schema WITHOUT materializing data - critical for large files!
    schema = py_get_table_schema(ctx, table.name)

    # Extract ALL metadata from schema (works for all formats!)
    from polars_bio.metadata_extractors import extract_all_schema_metadata

    full_metadata = extract_all_schema_metadata(schema)

    # Build format-specific header metadata for backward compatibility
    header_metadata = None
    format_str = _format_to_string(input_format)

    # Extract format-specific metadata from the comprehensive extraction
    format_specific = full_metadata.get("format_specific", {})

    if format_str in format_specific:
        # Use the parsed format-specific metadata
        if format_str == "vcf":
            vcf_meta = format_specific["vcf"]
            header_metadata = {
                "info_fields": vcf_meta.get("info_fields"),
                "format_fields": vcf_meta.get("format_fields"),
                "sample_names": vcf_meta.get("sample_names"),
                "version": vcf_meta.get("version"),
                "contigs": vcf_meta.get("contigs"),
                "filters": vcf_meta.get("filters"),
                "alt_definitions": vcf_meta.get("alt_definitions"),
            }
        elif format_str in ["fastq", "bam", "gff", "fasta", "bed", "cram"]:
            # For other formats, include their specific metadata
            header_metadata = format_specific.get(format_str, {})

    # Note: We don't store _full_metadata to avoid duplication
    # All relevant metadata is already parsed into user-friendly fields
    # (info_fields, format_fields, sample_names, version, etc.)

    lf = _lazy_scan(
        schema,
        projection_pushdown,
        predicate_pushdown,
        table.name,
        input_format,
        path,
        read_options,
    )

    # Set coordinate system metadata
    set_coordinate_system(lf, zero_based)

    # Set source metadata (replaces old VCF-specific metadata setting)
    from polars_bio._metadata import set_source_metadata

    format_str = _format_to_string(input_format)

    # Store DataFusion table name for debugging
    if header_metadata is None:
        header_metadata = {}
    header_metadata["_datafusion_table_name"] = table.name

    set_source_metadata(lf, format=format_str, path=path, header=header_metadata)

    # Wrap GFF LazyFrames with projection-aware wrapper for consistent attribute field handling
    if input_format == InputFormat.Gff:
        return GffLazyFrameWrapper(
            lf, path, read_options, projection_pushdown, predicate_pushdown
        )

    return lf


class GffLazyFrameWrapper:
    """Thin wrapper that preserves type while delegating to the underlying LazyFrame.

    Pushdown is decided exclusively inside the io_source callback based on
    with_columns and predicate; this wrapper only keeps chain type stable.
    """

    def __init__(
        self,
        base_lf: pl.LazyFrame,
        file_path: str,
        read_options: ReadOptions,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
    ):
        self._base_lf = base_lf
        self._file_path = file_path
        self._read_options = read_options
        self._projection_pushdown = projection_pushdown
        self._predicate_pushdown = predicate_pushdown

    def select(self, exprs):
        # Extract requested column names
        columns = []
        try:
            if isinstance(exprs, (list, tuple)):
                for e in exprs:
                    if isinstance(e, str):
                        columns.append(e)
                    elif hasattr(e, "meta") and hasattr(e.meta, "output_name"):
                        columns.append(e.meta.output_name())
            else:
                if isinstance(exprs, str):
                    columns = [exprs]
                elif hasattr(exprs, "meta") and hasattr(exprs.meta, "output_name"):
                    columns = [exprs.meta.output_name()]
        except Exception:
            columns = []

        STATIC = {
            "chrom",
            "start",
            "end",
            "type",
            "source",
            "score",
            "strand",
            "phase",
            "attributes",
        }
        attr_cols = [c for c in columns if c not in STATIC]

        # If selecting attribute fields, run one-shot SQL projection with proper attr_fields
        if columns and (attr_cols or "attributes" in columns):
            from polars_bio.polars_bio import GffReadOptions
            from polars_bio.polars_bio import InputFormat as _InputFormat
            from polars_bio.polars_bio import PyObjectStorageOptions
            from polars_bio.polars_bio import ReadOptions as _ReadOptions
            from polars_bio.polars_bio import (
                py_read_sql,
                py_read_table,
                py_register_table,
                py_register_view,
            )

            from .context import ctx

            # Pull thread_num/parallel/zero_based from original read options
            thread_num = 1
            parallel = False
            zero_based = False  # Default to 1-based (matches Python default)
            try:
                gopt = getattr(self._read_options, "gff_read_options", None)
                if gopt is not None:
                    tn = getattr(gopt, "thread_num", None)
                    if tn is not None:
                        thread_num = tn
                    par = getattr(gopt, "parallel", None)
                    if par is not None:
                        parallel = par
                    zb = getattr(gopt, "zero_based", None)
                    if zb is not None:
                        zero_based = zb
            except Exception:
                pass

            obj = PyObjectStorageOptions(
                allow_anonymous=True,
                enable_request_payer=False,
                chunk_size=8,
                concurrent_fetches=1,
                max_retries=5,
                timeout=300,
                compression_type="auto",
            )
            if "attributes" in columns:
                _attr = None
            elif attr_cols:
                _attr = attr_cols
            else:
                _attr = []

            gff_opts = GffReadOptions(
                attr_fields=_attr,
                thread_num=thread_num,
                object_storage_options=obj,
                parallel=parallel,
                zero_based=zero_based,
            )
            ropts = _ReadOptions(gff_read_options=gff_opts)
            table = py_register_table(
                ctx, self._file_path, None, _InputFormat.Gff, ropts
            )

            # Extract WHERE clause from existing LazyFrame if it has filters applied
            where_clause = ""
            try:
                # Check if the current LazyFrame has filters by examining its plan
                logical_plan_str = str(self._base_lf.explain(optimized=False))

                # Look for FILTER operations in the logical plan
                if "FILTER" in logical_plan_str:
                    # Try to translate polars expressions to SQL WHERE clause
                    where_clause = self._extract_sql_where_clause(logical_plan_str)
            except Exception:
                # If we can't extract the WHERE clause, fall back to the original approach
                # but at least warn that filtering may not work correctly
                pass

            select_clause = ", ".join([f'"{c}"' for c in columns])
            view_name = f"{table.name}_proj"
            sql_query = f"SELECT {select_clause} FROM {table.name}"

            if where_clause:
                sql_query += f" WHERE {where_clause}"

            py_register_view(ctx, view_name, sql_query)
            df_view = py_read_table(ctx, view_name)

            new_lf = _lazy_scan(
                df_view,
                False,
                self._predicate_pushdown,
                view_name,
                _InputFormat.Gff,
                self._file_path,
                self._read_options,
            )
            return GffLazyFrameWrapper(
                new_lf,
                self._file_path,
                self._read_options,
                False,
                self._predicate_pushdown,
            )

        # Otherwise delegate to Polars
        return GffLazyFrameWrapper(
            self._base_lf.select(exprs),
            self._file_path,
            self._read_options,
            self._projection_pushdown,
            self._predicate_pushdown,
        )

    def filter(self, *predicates):
        if not predicates:
            return self
        pred = predicates[0]
        for p in predicates[1:]:
            pred = pred & p
        return GffLazyFrameWrapper(
            self._base_lf.filter(pred),
            self._file_path,
            self._read_options,
            self._projection_pushdown,
            self._predicate_pushdown,
        )

    def _extract_sql_where_clause(self, logical_plan_str):
        """Extract SQL WHERE clause from Polars logical plan string."""
        import re

        # Look for SELECTION in the optimized plan or individual FILTER operations in unoptimized
        selection_match = re.search(r"SELECTION:\s*(.+)", logical_plan_str)
        if selection_match:
            # Use the selection expression from optimized plan
            selection_expr = selection_match.group(1).strip()
            try:
                return _build_sql_where_from_predicate_safe(selection_expr)
            except Exception:
                pass

        # Fallback: look for individual FILTER operations in unoptimized plan
        filter_lines = []
        for line in logical_plan_str.split("\n"):
            if "FILTER" in line and "[" in line:
                filter_lines.append(line.strip())

        if not filter_lines:
            return ""

        # Extract all filter conditions and combine them
        all_conditions = []
        for line in filter_lines:
            # Extract the condition inside brackets
            match = re.search(r"FILTER\s+\[(.+?)\]", line)
            if match:
                condition = match.group(1)
                try:
                    sql_condition = _build_sql_where_from_predicate_safe(condition)
                    if sql_condition:
                        all_conditions.append(sql_condition)
                except Exception:
                    continue

        if all_conditions:
            return " AND ".join(all_conditions)

        return ""

    def _parse_filter_expression(self, filter_expr):
        """Parse filter expression string to SQL WHERE clause."""
        # Use the same logic as _build_sql_where_from_predicate_safe
        # but work with the string directly from the logical plan
        import re

        conditions = []

        # String equality patterns
        str_patterns = [
            r'col\("([^"]+)"\)\.eq\(lit\("([^"]*)"\)\)',  # From logical plan
            r'col\("([^"]+)"\)\s*==\s*"([^"]*)"',  # Standard format
        ]
        for pat in str_patterns:
            for column, value in re.findall(pat, filter_expr):
                conditions.append(f"\"{column}\" = '{value}'")

        # Numeric comparison patterns
        numeric_patterns = [
            (r'col\("([^"]+)"\)\.gt\(lit\((\d+)\)\)', ">"),
            (r'col\("([^"]+)"\)\.lt\(lit\((\d+)\)\)', "<"),
            (r'col\("([^"]+)"\)\.gt_eq\(lit\((\d+)\)\)', ">="),
            (r'col\("([^"]+)"\)\.lt_eq\(lit\((\d+)\)\)', "<="),
            (r'col\("([^"]+)"\)\.neq\(lit\((\d+)\)\)', "!="),
            (r'col\("([^"]+)"\)\.eq\(lit\((\d+)\)\)', "="),
            # Standard format patterns
            (r'col\("([^"]+)"\)\s*>\s*(\d+)', ">"),
            (r'col\("([^"]+)"\)\s*<\s*(\d+)', "<"),
            (r'col\("([^"]+)"\)\s*>=\s*(\d+)', ">="),
            (r'col\("([^"]+)"\)\s*<=\s*(\d+)', "<="),
            (r'col\("([^"]+)"\)\s*!=\s*(\d+)', "!="),
            (r'col\("([^"]+)"\)\s*==\s*(\d+)', "="),
        ]

        for pattern, op in numeric_patterns:
            matches = re.findall(pattern, filter_expr)
            for column, value in matches:
                conditions.append(f'"{column}" {op} {value}')

        # Join conditions with AND
        if conditions:
            return " AND ".join(conditions)

        # Fallback: try to use the existing robust parser on the filter expression
        # by creating a dummy predicate string
        try:
            return _build_sql_where_from_predicate_safe(filter_expr)
        except Exception:
            pass

        return ""

    def __getattr__(self, name):
        return getattr(self._base_lf, name)
