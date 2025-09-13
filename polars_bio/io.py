from typing import Dict, Iterator, Union

import polars as pl
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    BamReadOptions,
    BedReadOptions,
    FastaReadOptions,
    FastqReadOptions,
    GffReadOptions,
    InputFormat,
    PyObjectStorageOptions,
    ReadOptions,
    VcfReadOptions,
    py_describe_vcf,
    py_from_polars,
    py_read_sql,
    py_read_table,
    py_register_table,
)

from .context import ctx

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
        thread_num: int = 1,
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
        Read a VCF file into a DataFrame.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields will be detected automatically from the VCF header. Use this to limit fields for better performance.
            thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

        !!! note
            VCF reader uses **1-based** coordinate system for the `start` and `end` columns.
        """
        return IOOperations.scan_vcf(
            path,
            info_fields,
            thread_num,
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
    def scan_vcf(
        path: str,
        info_fields: Union[list[str], None] = None,
        thread_num: int = 1,
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
        Lazily read a VCF file into a LazyFrame.

        Parameters:
            path: The path to the VCF file.
            info_fields: List of INFO field names to include. If *None*, all INFO fields will be detected automatically from the VCF header. Use this to limit fields for better performance.
            thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

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

        vcf_read_options = VcfReadOptions(
            info_fields=initial_info_fields,
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        return _read_file(path, InputFormat.Vcf, read_options, projection_pushdown)

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

        !!! note
            GFF reader uses **1-based** coordinate system for the `start` and `end` columns.
        """
        return IOOperations.scan_gff(
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
        ).collect()

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

        !!! note
            GFF reader uses **1-based** coordinate system for the `start` and `end` columns.
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

        gff_read_options = GffReadOptions(
            attr_fields=attr_fields,
            thread_num=thread_num,
            object_storage_options=object_storage_options,
            parallel=parallel,
        )
        read_options = ReadOptions(gff_read_options=gff_read_options)
        return _read_file(
            path, InputFormat.Gff, read_options, projection_pushdown, predicate_pushdown
        )

    @staticmethod
    def read_bam(
        path: str,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
    ) -> pl.DataFrame:
        """
        Read a BAM file into a DataFrame.

        Parameters:
            path: The path to the BAM file.
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large-scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large-scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

        !!! note
            BAM reader uses **1-based** coordinate system for the `start`, `end`, `mate_start`, `mate_end` columns.
        """
        return IOOperations.scan_bam(
            path,
            thread_num,
            chunk_size,
            concurrent_fetches,
            allow_anonymous,
            enable_request_payer,
            max_retries,
            timeout,
            projection_pushdown,
        ).collect()

    @staticmethod
    def scan_bam(
        path: str,
        thread_num: int = 1,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        projection_pushdown: bool = False,
    ) -> pl.LazyFrame:
        """
        Lazily read a BAM file into a LazyFrame.

        Parameters:
            path: The path to the BAM file.
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            projection_pushdown: Enable column projection pushdown to optimize query performance by only reading the necessary columns at the DataFusion level.

        !!! note
            BAM reader uses **1-based** coordinate system for the `start`, `end`, `mate_start`, `mate_end` columns.
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

        bam_read_options = BamReadOptions(
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        return _read_file(path, InputFormat.Bam, read_options, projection_pushdown)

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

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            BED reader uses **1-based** coordinate system for the `start`, `end`.
        """
        return IOOperations.scan_bed(
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
        ).collect()

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

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! note
            BED reader uses **1-based** coordinate system for the `start`, `end`.
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

        bed_read_options = BedReadOptions(
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(bed_read_options=bed_read_options)
        return _read_file(path, InputFormat.Bed, read_options, projection_pushdown)

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


def _cleanse_fields(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]


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
    df: Union[pl.DataFrame, pl.LazyFrame],
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
    table_name: str = None,
    input_format: InputFormat = None,
    file_path: str = None,
    read_options: ReadOptions = None,
) -> pl.LazyFrame:

    df_lazy: DataFrame = df
    original_schema = df_lazy.schema()

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # If this is a GFF scan, perform pushdown by building a single SELECT ... WHERE ...
        if input_format == InputFormat.Gff and file_path is not None:
            from polars_bio.polars_bio import GffReadOptions, PyObjectStorageOptions
            from polars_bio.polars_bio import ReadOptions as _ReadOptions
            from polars_bio.polars_bio import (
                py_read_sql,
                py_read_table,
                py_register_table,
                py_register_view,
            )

            from .context import ctx

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

            # Derive thread/parallel from read_options when available
            thread_num = 1
            parallel = False
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
            )
            ropts = _ReadOptions(gff_read_options=gff_opts)

            # Determine which table to query: reuse original unless we must change attr_fields
            table_name_use = table_name
            if projection_pushdown and requested_cols:
                # Only re-register when projection is active (we know column needs)
                table_obj = py_register_table(
                    ctx, file_path, None, InputFormat.Gff, ropts
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

            query_df = py_read_sql(ctx, sql)

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

        # Default path (non-GFF): stream and optionally apply local filter/projection
        query_df = df_lazy
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


def _extract_column_names_from_expr(with_columns: Union[pl.Expr, list]) -> list[str]:
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


def _read_file(
    path: str,
    input_format: InputFormat,
    read_options: ReadOptions,
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
) -> pl.LazyFrame:
    table = py_register_table(ctx, path, None, input_format, read_options)
    df = py_read_table(ctx, table.name)

    lf = _lazy_scan(
        df,
        projection_pushdown,
        predicate_pushdown,
        table.name,
        input_format,
        path,
        read_options,
    )

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

            # Pull thread_num/parallel from original read options
            thread_num = 1
            parallel = False
            try:
                gopt = getattr(self._read_options, "gff_read_options", None)
                if gopt is not None:
                    tn = getattr(gopt, "thread_num", None)
                    if tn is not None:
                        thread_num = tn
                    par = getattr(gopt, "parallel", None)
                    if par is not None:
                        parallel = par
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
            )
            ropts = _ReadOptions(gff_read_options=gff_opts)
            table = py_register_table(
                ctx, self._file_path, None, _InputFormat.Gff, ropts
            )
            select_clause = ", ".join([f'"{c}"' for c in columns])
            view_name = f"{table.name}_proj"
            py_register_view(
                ctx, view_name, f"SELECT {select_clause} FROM {table.name}"
            )
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

    def __getattr__(self, name):
        return getattr(self._base_lf, name)
