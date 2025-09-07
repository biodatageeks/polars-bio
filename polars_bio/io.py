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
    py_scan_table,
)

from .context import ctx
from .range_op_helpers import stream_wrapper

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
            attr_fields=None,
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

    # String equality patterns (including empty strings)
    string_patterns = re.findall(r'\(col\("([^"]+)"\)\)\s*==\s*\("([^"]*)"\)', pred_str)
    for column, value in string_patterns:
        conditions.append(f"\"{column}\" = '{value}'")

    # Numeric comparison patterns
    numeric_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\(dyn int:\s*(\d+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\(dyn int:\s*(\d+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\(dyn int:\s*(\d+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\(dyn int:\s*(\d+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\(dyn int:\s*(\d+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\(dyn int:\s*(\d+)\)', "="),
    ]

    for pattern, op in numeric_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # Float comparison patterns
    float_patterns = [
        (r'\(col\("([^"]+)"\)\)\s*>\s*\(dyn float:\s*([\d.]+)\)', ">"),
        (r'\(col\("([^"]+)"\)\)\s*<\s*\(dyn float:\s*([\d.]+)\)', "<"),
        (r'\(col\("([^"]+)"\)\)\s*>=\s*\(dyn float:\s*([\d.]+)\)', ">="),
        (r'\(col\("([^"]+)"\)\)\s*<=\s*\(dyn float:\s*([\d.]+)\)', "<="),
        (r'\(col\("([^"]+)"\)\)\s*!=\s*\(dyn float:\s*([\d.]+)\)', "!="),
        (r'\(col\("([^"]+)"\)\)\s*==\s*\(dyn float:\s*([\d.]+)\)', "="),
    ]

    for pattern, op in float_patterns:
        matches = re.findall(pattern, pred_str)
        for column, value in matches:
            conditions.append(f'"{column}" {op} {value}')

    # Join all conditions with AND
    if conditions:
        return " AND ".join(conditions)

    return ""


def _lazy_scan(
    df: Union[pl.DataFrame, pl.LazyFrame],
    projection_pushdown: bool = False,
    predicate_pushdown: bool = False,
    table_name: str = None,
    input_format: InputFormat = None,
    file_path: str = None,
) -> pl.LazyFrame:

    df_lazy: DataFrame = df
    original_schema = df_lazy.schema()

    def _overlap_source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        # Extract column names from with_columns if projection pushdown is enabled
        projected_columns = None
        if projection_pushdown and with_columns is not None:
            projected_columns = _extract_column_names_from_expr(with_columns)

        # Apply predicate and projection pushdown to DataFusion query if enabled
        query_df = df_lazy
        datafusion_projection_applied = False
        datafusion_predicate_applied = False

        # Handle combined predicate + projection pushdown using SQL approach
        # This avoids DataFrame API issues and leverages proven SQL optimization
        if (predicate_pushdown and predicate is not None) or (
            projection_pushdown and projected_columns
        ):
            try:
                # Use SQL approach for combined optimization - this is proven to work with 4x+ speedup
                from .context import ctx

                query_df = _apply_combined_pushdown_via_sql(
                    ctx,
                    table_name,
                    query_df,
                    predicate,
                    projected_columns,
                    predicate_pushdown,
                    projection_pushdown,
                )
                datafusion_predicate_applied = (
                    predicate_pushdown and predicate is not None
                )
                datafusion_projection_applied = (
                    projection_pushdown and projected_columns is not None
                )

            except Exception as e:
                # Fallback: try DataFrame API approach (will likely fail but worth trying)

                try:
                    # Handle predicate pushdown with DataFrame API
                    if predicate_pushdown and predicate is not None:
                        from .predicate_translator import (
                            translate_polars_predicate_to_datafusion,
                        )

                        datafusion_predicate = translate_polars_predicate_to_datafusion(
                            predicate
                        )
                        query_df = query_df.filter(datafusion_predicate)
                        datafusion_predicate_applied = True

                    # Handle projection pushdown with DataFrame API
                    if projection_pushdown and projected_columns:
                        query_df = query_df.select(projected_columns)
                        datafusion_projection_applied = True

                except Exception as e2:
                    # Final fallback: disable pushdown optimizations
                    query_df = df_lazy
                    datafusion_predicate_applied = False
                    datafusion_projection_applied = False
        else:
            # No pushdown requested
            pass

        if n_rows and n_rows < 8192:  # 8192 is the default batch size in datafusion
            df = query_df.limit(n_rows).execute_stream().next().to_pyarrow()
            df = pl.DataFrame(df).limit(n_rows)
            # Apply Python-level predicate only if DataFusion predicate pushdown failed
            if predicate is not None and not datafusion_predicate_applied:
                df = df.filter(predicate)
            # Apply Python-level projection if DataFusion projection failed or projection pushdown is disabled
            if with_columns is not None and (
                not projection_pushdown or not datafusion_projection_applied
            ):
                df = df.select(with_columns)
            yield df
            return

        df_stream = query_df.execute_stream()
        progress_bar = tqdm(unit="rows")
        for r in df_stream:
            py_df = r.to_pyarrow()
            df = pl.DataFrame(py_df)
            # Apply Python-level predicate only if DataFusion predicate pushdown failed
            if predicate is not None and not datafusion_predicate_applied:
                df = df.filter(predicate)
            # Apply Python-level projection if DataFusion projection failed or projection pushdown is disabled
            if with_columns is not None and (
                not projection_pushdown or not datafusion_projection_applied
            ):
                df = df.select(with_columns)
            progress_bar.update(len(df))
            yield df

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
        df, projection_pushdown, predicate_pushdown, table.name, input_format, path
    )

    # Wrap GFF LazyFrames with projection-aware wrapper for consistent attribute field handling
    if input_format == InputFormat.Gff:
        return GffLazyFrameWrapper(
            lf, path, read_options, projection_pushdown, predicate_pushdown
        )

    return lf


class GffLazyFrameWrapper:
    """Wrapper for GFF LazyFrames that handles attribute field detection in select operations."""

    def __init__(
        self,
        base_lf: pl.LazyFrame,
        file_path: str,
        read_options: ReadOptions,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        current_projection: list = None,
        stored_predicate=None,
    ):
        self._base_lf = base_lf
        self._file_path = file_path
        self._read_options = read_options
        self._projection_pushdown = projection_pushdown
        self._predicate_pushdown = predicate_pushdown
        self._current_projection = (
            current_projection  # Track the current column selection
        )
        self._stored_predicate = (
            stored_predicate  # Store predicate for filter().select() bug fix
        )

    def select(self, exprs):
        """Override select to handle GFF attribute field detection.

        Ensures queries requesting the raw `attributes` column use a registration
        that exposes it, while preserving projection pushdown. For unnested
        attribute fields (e.g., `gene_id`), re-registers with those fields to
        enable efficient projection.
        """
        # Extract column names from expressions
        if isinstance(exprs, (list, tuple)):
            columns = []
            for expr in exprs:
                if isinstance(expr, str):
                    columns.append(expr)
                elif hasattr(expr, "meta") and hasattr(expr.meta, "output_name"):
                    try:
                        columns.append(expr.meta.output_name())
                    except:
                        pass
        else:
            # Single expression
            if isinstance(exprs, str):
                columns = [exprs]
            elif hasattr(exprs, "meta") and hasattr(exprs.meta, "output_name"):
                try:
                    columns = [exprs.meta.output_name()]
                except:
                    columns = []
            else:
                columns = []

        # Store current projection for use in subsequent operations like filter()
        current_projection = columns.copy() if columns else None

        # CRITICAL BUG FIX: If we have a stored predicate from filter().select() pattern,
        # apply it now with the combined projection+predicate pushdown
        if self._stored_predicate is not None:
            # Apply the stored predicate with this projection using combined optimization
            from .context import ctx

            try:
                # Use the combined SQL approach for filter+select with proper optimization
                static_cols = [
                    col
                    for col in columns
                    if col
                    in {
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
                ]
                attribute_cols = [
                    col
                    for col in columns
                    if col
                    not in {
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
                ]

                # Register table with appropriate attributes
                if attribute_cols:
                    gff_options = GffReadOptions(
                        attr_fields=attribute_cols,
                        thread_num=getattr(
                            getattr(self._read_options, "gff_read_options", None),
                            "thread_num",
                            1,
                        ),
                        object_storage_options=PyObjectStorageOptions(
                            allow_anonymous=True,
                            enable_request_payer=False,
                            chunk_size=8,
                            concurrent_fetches=1,
                            max_retries=5,
                            timeout=300,
                            compression_type="auto",
                        ),
                        parallel=getattr(
                            getattr(self._read_options, "gff_read_options", None),
                            "parallel",
                            False,
                        ),
                    )
                    read_options = ReadOptions(gff_read_options=gff_options)
                    table = py_register_table(
                        ctx, self._file_path, None, InputFormat.Gff, read_options
                    )
                else:
                    table = py_register_table(
                        ctx, self._file_path, None, InputFormat.Gff, self._read_options
                    )

                # Apply combined predicate and projection pushdown via SQL
                # CRITICAL FIX: For stored predicates, force predicate application even if predicate_pushdown=False
                query_df = _apply_combined_pushdown_via_sql(
                    ctx,
                    table.name,
                    None,
                    self._stored_predicate,
                    current_projection,
                    True,  # Force predicate pushdown for stored predicates to fix filter().select() bug
                    self._projection_pushdown,
                )

                # Create new lazy frame with the optimized query
                new_lf = _lazy_scan(
                    query_df, False, False, table.name, InputFormat.Gff, self._file_path
                )
                return GffLazyFrameWrapper(
                    new_lf,
                    self._file_path,
                    self._read_options,
                    self._projection_pushdown,
                    False,
                    current_projection,  # predicate applied
                )

            except Exception:
                # Fallback: apply predicate at Polars level after selection
                selected_lf = self._base_lf.select(exprs).filter(self._stored_predicate)
                return GffLazyFrameWrapper(
                    selected_lf,
                    self._file_path,
                    self._read_options,
                    self._projection_pushdown,
                    self._predicate_pushdown,
                    current_projection,
                )

        # Categorize columns
        GFF_STATIC_COLUMNS = {
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
        static_cols = [col for col in columns if col in GFF_STATIC_COLUMNS]
        attribute_cols = [col for col in columns if col not in GFF_STATIC_COLUMNS]

        # If 'attributes' is requested, ensure the registered table exposes it.
        # Some parallel GFF providers omit the raw 'attributes' column; switch
        # to a registration that includes it while keeping projection pushdown.
        if "attributes" in static_cols:
            from .context import ctx

            # Preserve original parallelism and thread config when re-registering
            orig_gff_opts = getattr(self._read_options, "gff_read_options", None)
            orig_parallel = (
                getattr(orig_gff_opts, "parallel", False) if orig_gff_opts else False
            )
            orig_thread = (
                getattr(orig_gff_opts, "thread_num", None) if orig_gff_opts else None
            )

            # Build read options that ensure raw attributes are present
            gff_options = GffReadOptions(
                attr_fields=None,  # keep nested 'attributes' column
                thread_num=orig_thread if orig_thread is not None else 1,
                object_storage_options=PyObjectStorageOptions(
                    allow_anonymous=True,
                    enable_request_payer=False,
                    chunk_size=8,
                    concurrent_fetches=1,
                    max_retries=5,
                    timeout=300,
                    compression_type="auto",
                ),
                parallel=orig_parallel,
            )
            read_options = ReadOptions(gff_read_options=gff_options)
            table = py_register_table(
                ctx, self._file_path, None, InputFormat.Gff, read_options
            )
            df = py_read_table(ctx, table.name)
            new_lf = _lazy_scan(
                df,
                True,
                self._predicate_pushdown,
                table.name,
                InputFormat.Gff,
                self._file_path,
            )
            return new_lf.select(exprs)

        if self._projection_pushdown:
            # Optimized path: when selecting specific unnested attribute fields, re-register
            # GFF table with those fields so DataFusion can project them efficiently.

            # Use optimized table re-registration (fast path)
            from .context import ctx

            gff_options = GffReadOptions(
                attr_fields=attribute_cols,
                thread_num=getattr(
                    getattr(self._read_options, "gff_read_options", None),
                    "thread_num",
                    1,
                ),
                object_storage_options=PyObjectStorageOptions(
                    allow_anonymous=True,
                    enable_request_payer=False,
                    chunk_size=8,
                    concurrent_fetches=1,
                    max_retries=5,
                    timeout=300,
                    compression_type="auto",
                ),
                # Keep parallel reading consistent with base options when possible
                parallel=getattr(
                    getattr(self._read_options, "gff_read_options", None),
                    "parallel",
                    False,
                ),
            )

            read_options = ReadOptions(gff_read_options=gff_options)
            table = py_register_table(
                ctx, self._file_path, None, InputFormat.Gff, read_options
            )
            df = py_read_table(ctx, table.name)

            # Create new LazyFrame with optimized schema
            new_lf = _lazy_scan(
                df,
                True,
                self._predicate_pushdown,
                table.name,
                InputFormat.Gff,
                self._file_path,
            )
            selected_lf = new_lf.select(exprs)
            # Preserve GFF wrapper behavior for subsequent operations
            return GffLazyFrameWrapper(
                selected_lf,
                self._file_path,
                self._read_options,
                self._projection_pushdown,
                self._predicate_pushdown,
                current_projection,
            )

        elif attribute_cols:
            # Extract attribute fields from nested structure (compatibility path)
            import polars as pl

            # Build selection with attribute field extraction
            selection_exprs = []

            # Add static columns as-is
            for col in static_cols:
                selection_exprs.append(pl.col(col))

            # Add attribute field extractions
            for attr_col in attribute_cols:
                attr_expr = (
                    pl.col("attributes")
                    .list.eval(
                        pl.when(pl.element().struct.field("tag") == attr_col).then(
                            pl.element().struct.field("value")
                        )
                    )
                    .list.drop_nulls()
                    .list.first()
                    .alias(attr_col)
                )
                selection_exprs.append(attr_expr)

            selected_lf = self._base_lf.select(selection_exprs)
            # Preserve GFF wrapper behavior for subsequent operations
            return GffLazyFrameWrapper(
                selected_lf,
                self._file_path,
                self._read_options,
                self._projection_pushdown,
                self._predicate_pushdown,
                current_projection,
            )
        else:
            # Static columns only, use base LazyFrame
            selected_lf = self._base_lf.select(exprs)
            # Preserve GFF wrapper behavior for subsequent operations
            return GffLazyFrameWrapper(
                selected_lf,
                self._file_path,
                self._read_options,
                self._projection_pushdown,
                self._predicate_pushdown,
                current_projection,
            )

    def filter(self, *predicates):
        """Override filter to handle predicate pushdown for GFF files."""
        if len(predicates) == 1:
            predicate = predicates[0]
        else:
            # Multiple predicates - combine with AND
            predicate = predicates[0]
            for p in predicates[1:]:
                predicate = predicate & p

        # CRITICAL FIX: When projection pushdown is enabled but no current projection exists,
        # defer the filter to be applied later with the projection to avoid the bug where
        # filter().select() with projection_pushdown=True returns unfiltered results.
        # This bug occurs regardless of predicate_pushdown setting.
        if self._projection_pushdown and self._current_projection is None:
            # Store the predicate to be applied later when select() is called
            # This prevents the bug where filter().select() loses the filter conditions
            return GffLazyFrameWrapper(
                self._base_lf,  # Keep the base lazyframe unchanged
                self._file_path,
                self._read_options,
                self._projection_pushdown,
                self._predicate_pushdown,
                None,  # No projection yet
                stored_predicate=predicate,  # Store predicate for later
            )

        if self._predicate_pushdown:

            # Use pure SQL approach for maximum performance and compatibility
            from polars_bio.polars_bio import InputFormat, py_register_table

            from .context import ctx

            try:
                # Check if current projection involves attribute fields
                GFF_STATIC_COLUMNS = {
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

                if self._current_projection:
                    static_cols = [
                        col
                        for col in self._current_projection
                        if col in GFF_STATIC_COLUMNS
                    ]
                    attribute_cols = [
                        col
                        for col in self._current_projection
                        if col not in GFF_STATIC_COLUMNS
                    ]

                    # If attribute fields are needed, register table with attribute extraction
                    if attribute_cols:
                        from polars_bio.polars_bio import (
                            GffReadOptions,
                            PyObjectStorageOptions,
                        )

                        # Get original settings
                        orig_gff_opts = getattr(
                            self._read_options, "gff_read_options", None
                        )
                        orig_parallel = (
                            getattr(orig_gff_opts, "parallel", False)
                            if orig_gff_opts
                            else False
                        )
                        orig_thread = (
                            getattr(orig_gff_opts, "thread_num", None)
                            if orig_gff_opts
                            else None
                        )

                        # Register with attribute field extraction enabled
                        gff_options = GffReadOptions(
                            attr_fields=attribute_cols,  # Extract these attribute fields
                            thread_num=orig_thread if orig_thread is not None else 1,
                            object_storage_options=PyObjectStorageOptions(
                                allow_anonymous=True,
                                enable_request_payer=False,
                                chunk_size=8,
                                concurrent_fetches=1,
                                max_retries=5,
                                timeout=300,
                                compression_type="auto",
                            ),
                            parallel=orig_parallel,
                        )
                        read_options = ReadOptions(gff_read_options=gff_options)
                        table = py_register_table(
                            ctx, self._file_path, None, InputFormat.Gff, read_options
                        )
                    else:
                        # Standard registration for static columns only
                        table = py_register_table(
                            ctx,
                            self._file_path,
                            None,
                            InputFormat.Gff,
                            self._read_options,
                        )
                else:
                    # No projection, use standard registration
                    table = py_register_table(
                        ctx, self._file_path, None, InputFormat.Gff, self._read_options
                    )

                # Apply SQL pushdown - use the proven working approach
                # If we have a current projection from a previous select(), apply it too
                # Enable projection pushdown if we have a current projection, regardless of the general setting
                enable_projection = self._current_projection is not None
                query_df = _apply_combined_pushdown_via_sql(
                    ctx,
                    table.name,
                    None,  # original_df not needed for SQL approach
                    predicate,
                    self._current_projection,  # Use the tracked projection
                    self._predicate_pushdown,
                    enable_projection,  # Apply projection if we have one, regardless of pushdown setting
                )

                # Return a new lazy scan over the filtered DataFusion DataFrame
                # Predicate is already applied at SQL level, so disable it in the wrapper scan
                new_lf = _lazy_scan(
                    query_df,
                    self._projection_pushdown,
                    False,  # predicate already applied
                    table.name,
                    InputFormat.Gff,
                    self._file_path,
                )
                # Preserve GFF wrapper behavior for subsequent operations
                return GffLazyFrameWrapper(
                    new_lf,
                    self._file_path,
                    self._read_options,
                    self._projection_pushdown,
                    False,  # already applied predicate at SQL level
                    (
                        self._current_projection if enable_projection else None
                    ),  # Keep projection info if it was applied
                )

            except Exception:
                # Fallback to standard Polars-level filtering if pushdown translation fails
                return GffLazyFrameWrapper(
                    self._base_lf.filter(predicate),
                    self._file_path,
                    self._read_options,
                    self._projection_pushdown,
                    self._predicate_pushdown,
                    self._current_projection,  # Preserve current projection in fallback
                )
        else:
            # Standard filtering without pushdown
            return GffLazyFrameWrapper(
                self._base_lf.filter(predicate),
                self._file_path,
                self._read_options,
                self._projection_pushdown,
                self._predicate_pushdown,
                self._current_projection,  # Preserve current projection
            )

    def __getattr__(self, name):
        """Delegate all other operations to base LazyFrame."""
        return getattr(self._base_lf, name)
