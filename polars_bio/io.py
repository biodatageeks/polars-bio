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
        streaming: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """

        Read a FASTA file into a LazyFrame.

        Parameters:
            path: The path to the FASTA file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            streaming: Whether to read the FASTA file in streaming mode.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.read_fasta("/tmp/test.fasta").limit(1).collect()
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
        if streaming:
            return read_file(path, InputFormat.Fasta, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Fasta, read_options)
            return lazy_scan(df)

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
        streaming: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """
        Read a VCF file into a LazyFrame.

        Parameters:
            path: The path to the VCF file.
            info_fields: The fields to read from the INFO column.
            thread_num: The number of threads to use for reading the VCF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').
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
            info_fields=_cleanse_fields(info_fields),
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        if streaming:
            return read_file(path, InputFormat.Vcf, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Vcf, read_options)
            return lazy_scan(df)

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
        streaming: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """
        Read a GFF file into a LazyFrame.

        Parameters:
            path: The path to the GFF file.
            attr_fields: The fields to unnest from the `attributes` column. If not specified, all fields swill be rendered as `attributes` column containing an array of structures `{'tag':'xxx', 'value':'yyy'}`.
            thread_num: The number of threads to use for reading the GFF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').
            streaming: Whether to read the GFF file in streaming mode.


        !!! Example
            ```shell
            wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_38/gencode.v38.annotation.gff3.gz -O /tmp/gencode.v38.annotation.gff3.gz
            ```
            Read a GFF file **without** unnesting attributes:
            ```python
            import polars_bio as pb
            gff_path = "/tmp/gencode.v38.annotation.gff3.gz"
            pb.read_gff(gff_path).limit(5).collect()
            ```

            ```shell

            shape: (5, 9)
            ┌───────┬───────┬───────┬────────────┬───┬───────┬────────┬───────┬─────────────────────────────────┐
            │ chrom ┆ start ┆ end   ┆ type       ┆ … ┆ score ┆ strand ┆ phase ┆ attributes                      │
            │ ---   ┆ ---   ┆ ---   ┆ ---        ┆   ┆ ---   ┆ ---    ┆ ---   ┆ ---                             │
            │ str   ┆ u32   ┆ u32   ┆ str        ┆   ┆ f32   ┆ str    ┆ u32   ┆ list[struct[2]]                 │
            ╞═══════╪═══════╪═══════╪════════════╪═══╪═══════╪════════╪═══════╪═════════════════════════════════╡
            │ chr1  ┆ 11869 ┆ 14409 ┆ gene       ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"ID","ENSG00000223972.5"}, {… │
            │ chr1  ┆ 11869 ┆ 14409 ┆ transcript ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"ID","ENST00000456328.2"}, {… │
            │ chr1  ┆ 11869 ┆ 12227 ┆ exon       ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"ID","exon:ENST00000456328.2… │
            │ chr1  ┆ 12613 ┆ 12721 ┆ exon       ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"ID","exon:ENST00000456328.2… │
            │ chr1  ┆ 13221 ┆ 14409 ┆ exon       ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"ID","exon:ENST00000456328.2… │
            └───────┴───────┴───────┴────────────┴───┴───────┴────────┴───────┴─────────────────────────────────┘

            ```

            Read a GFF file **with** unnesting attributes:
            ```python
            import polars_bio as pb
            gff_path = "/tmp/gencode.v38.annotation.gff3.gz"
            pb.read_gff(gff_path, attr_fields=["ID", "havana_transcript"]).limit(5).collect()
            ```
            ```shell

            shape: (5, 10)
            ┌───────┬───────┬───────┬────────────┬───┬────────┬───────┬──────────────────────────┬──────────────────────┐
            │ chrom ┆ start ┆ end   ┆ type       ┆ … ┆ strand ┆ phase ┆ ID                       ┆ havana_transcript    │
            │ ---   ┆ ---   ┆ ---   ┆ ---        ┆   ┆ ---    ┆ ---   ┆ ---                      ┆ ---                  │
            │ str   ┆ u32   ┆ u32   ┆ str        ┆   ┆ str    ┆ u32   ┆ str                      ┆ str                  │
            ╞═══════╪═══════╪═══════╪════════════╪═══╪════════╪═══════╪══════════════════════════╪══════════════════════╡
            │ chr1  ┆ 11869 ┆ 14409 ┆ gene       ┆ … ┆ +      ┆ null  ┆ ENSG00000223972.5        ┆ null                 │
            │ chr1  ┆ 11869 ┆ 14409 ┆ transcript ┆ … ┆ +      ┆ null  ┆ ENST00000456328.2        ┆ OTTHUMT00000362751.1 │
            │ chr1  ┆ 11869 ┆ 12227 ┆ exon       ┆ … ┆ +      ┆ null  ┆ exon:ENST00000456328.2:1 ┆ OTTHUMT00000362751.1 │
            │ chr1  ┆ 12613 ┆ 12721 ┆ exon       ┆ … ┆ +      ┆ null  ┆ exon:ENST00000456328.2:2 ┆ OTTHUMT00000362751.1 │
            │ chr1  ┆ 13221 ┆ 14409 ┆ exon       ┆ … ┆ +      ┆ null  ┆ exon:ENST00000456328.2:3 ┆ OTTHUMT00000362751.1 │
            └───────┴───────┴───────┴────────────┴───┴────────┴───────┴──────────────────────────┴──────────────────────┘
            ```
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
            attr_fields=_cleanse_fields(attr_fields),
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(gff_read_options=gff_read_options)
        if streaming:
            return read_file(path, InputFormat.Gff, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Gff, read_options)
            return lazy_scan(df)

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
        streaming: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """
        Read a BAM file into a LazyFrame.

        Parameters:
            path: The path to the BAM file.
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            streaming: Whether to read the BAM file in streaming mode.

        !!! Example

            ```python
            import polars_bio as pb
            bam = pb.read_bam("gs://genomics-public-data/1000-genomes/bam/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam").limit(3)
            bam.collect()
            ```
            ```shell
            INFO:polars_bio:Table: hg00096_mapped_illumina_bwa_gbr_low_coverage_20120522 registered for path: gs://genomics-public-data/1000-genomes/bam/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
            shape: (3, 11)
            ┌────────────────────┬───────┬───────┬───────┬───┬────────────┬────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name               ┆ chrom ┆ start ┆ end   ┆ … ┆ mate_chrom ┆ mate_start ┆ sequence                        ┆ quality_scores                  │
            │ ---                ┆ ---   ┆ ---   ┆ ---   ┆   ┆ ---        ┆ ---        ┆ ---                             ┆ ---                             │
            │ str                ┆ str   ┆ u32   ┆ u32   ┆   ┆ str        ┆ u32        ┆ str                             ┆ str                             │
            ╞════════════════════╪═══════╪═══════╪═══════╪═══╪════════════╪════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ SRR062634.9882510  ┆ chr1  ┆ 10001 ┆ 10044 ┆ … ┆ chr1       ┆ 10069      ┆ TAACCCTAACCCTACCCTAACCCTAACCCT… ┆ 0<>=/0E:7;08FBDIF9;2%=<>+FCDDA… │
            │ SRR062641.21956756 ┆ chr1  ┆ 10001 ┆ 10049 ┆ … ┆ chr1       ┆ 10051      ┆ TAACCCTACCCTAACCCTAACCCTAACCCT… ┆ 0=MLOOPNNPPJHPOQQROQPQQRIQPRJB… │
            │ SRR062641.13613107 ┆ chr1  ┆ 10002 ┆ 10072 ┆ … ┆ chr1       ┆ 10110      ┆ AACCCTAACCCCTAACCCCTAACCCCTAAC… ┆ 0KKNPQOQOQIQRPQPRRRRPQPRRRRPRF… │
            └────────────────────┴───────┴───────┴───────┴───┴────────────┴────────────┴─────────────────────────────────┴─────────────────────────────────┘
            ```

            ```python
            bam.collect_schema()
            Schema({'name': String, 'chrom': String, 'start': UInt32, 'end': UInt32, 'flags': UInt32, 'cigar': String, 'mapping_quality': UInt32, 'mate_chrom': String, 'mate_start': UInt32, 'sequence': String, 'quality_scores': String})
            ```

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
        if streaming:
            return read_file(path, InputFormat.Bam, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Bam, read_options)
            return lazy_scan(df)

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
        streaming: bool = False,
        parallel: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """
        Read a FASTQ file into a LazyFrame.

        Parameters:
            path: The path to the FASTQ file.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            streaming: Whether to read the FASTQ file in streaming mode.
            parallel: Whether to use the parallel reader for BGZF compressed files stored **locally**. GZI index is **required**.

        !!! Example

            ```python
            import polars_bio as pb
            pb.read_fastq("gs://genomics-public-data/platinum-genomes/fastq/ERR194146.fastq.gz").limit(1).collect()
            ```
            ```shell
            shape: (1, 4)
            ┌─────────────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┐
            │ name                ┆ description                     ┆ sequence                        ┆ quality_scores                  │
            │ ---                 ┆ ---                             ┆ ---                             ┆ ---                             │
            │ str                 ┆ str                             ┆ str                             ┆ str                             │
            ╞═════════════════════╪═════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╡
            │ ERR194146.812444541 ┆ HSQ1008:141:D0CC8ACXX:2:1204:1… ┆ TGGAAGGTTCTCGAAAAAAATGGAATCGAA… ┆ ?@;DDBDDBHF??FFB@B)1:CD3*:?DFF… │
            └─────────────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┘

            ```

            Parallel reading of BZGF compressed FASTQ files stored locally:
            ```shell
            ls -1 /tmp/ERR194146.fastq.bgz*
            ERR194146.fastq.bgz
            ERR194146.fastq.bgz.gzi
            ```

            ```python
            import polars_bio as pb
            ## Set the number of target partitions (threads) to 2
            pb.set_option("datafusion.execution.target_partitions", "2")
            pb.read_fastq("/tmp/ERR194146.fastq.bgz", parallel=True).count().collect()
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

        fastq_read_options = FastqReadOptions(
            object_storage_options=object_storage_options, parallel=parallel
        )
        read_options = ReadOptions(fastq_read_options=fastq_read_options)
        if streaming:
            return read_file(path, InputFormat.Fastq, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Fastq, read_options)
            return lazy_scan(df)

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
        streaming: bool = False,
    ) -> Union[pl.LazyFrame, pl.DataFrame]:
        """
        Read a BED file into a LazyFrame.

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
            streaming: Whether to read the BED file in streaming mode.

        !!! Note
            Only **BED4** format is supported. It extends the basic BED format (BED3) by adding a name field, resulting in four columns: chromosome, start position, end position, and name.
            Also unlike other text formats, **GZIP** compression is not supported.

        !!! Example
            ```shell

             cd /tmp
             wget https://webs.iiitd.edu.in/raghava/humcfs/fragile_site_bed.zip -O fragile_site_bed.zip
             unzip fragile_site_bed.zip -x "__MACOSX/*" "*/.DS_Store"
            ```

            ```python
            import polars_bio as pb
            pb.read_bed("/tmp/fragile_site_bed/chr5_fragile_site.bed").limit(5).collect()
            ```

            ```shell

            shape: (5, 4)
            ┌───────┬───────────┬───────────┬───────┐
            │ chrom ┆ start     ┆ end       ┆ name  │
            │ ---   ┆ ---       ┆ ---       ┆ ---   │
            │ str   ┆ u32       ┆ u32       ┆ str   │
            ╞═══════╪═══════════╪═══════════╪═══════╡
            │ chr5  ┆ 28900001  ┆ 42500000  ┆ FRA5A │
            │ chr5  ┆ 92300001  ┆ 98200000  ┆ FRA5B │
            │ chr5  ┆ 130600001 ┆ 136200000 ┆ FRA5C │
            │ chr5  ┆ 92300001  ┆ 93916228  ┆ FRA5D │
            │ chr5  ┆ 18400001  ┆ 28900000  ┆ FRA5E │
            └───────┴───────────┴───────────┴───────┘
            ```
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
        if streaming:
            return read_file(path, InputFormat.Bed, read_options, streaming)
        else:
            df = read_file(path, InputFormat.Bed, read_options)
            return lazy_scan(df)

    @staticmethod
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
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').

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
            ╞═══════════╪═════════╪══════════════════════════════════════════════════════════════════════════════╡
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

    @staticmethod
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


def _cleanse_fields(t: Union[list[str], None]) -> Union[list[str], None]:
    if t is None:
        return None
    return [x.strip() for x in t]


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
            df = df_lazy.limit(n_rows).execute_stream().next().to_pyarrow()
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
