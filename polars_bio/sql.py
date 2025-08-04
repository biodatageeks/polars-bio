from typing import Union

import polars as pl

from polars_bio.polars_bio import (
    BamReadOptions,
    BedReadOptions,
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
    py_register_view,
    py_scan_sql,
    py_scan_table,
)

from .context import ctx
from .io import _cleanse_fields, lazy_scan
from .range_op_helpers import stream_wrapper


class SQL:
    @staticmethod
    def register_vcf(
        path: str,
        name: Union[str, None] = None,
        info_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
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
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').
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
            info_fields=_cleanse_fields(info_fields),
            thread_num=thread_num,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        py_register_table(ctx, path, name, InputFormat.Vcf, read_options)

    @staticmethod
    def register_gff(
        path: str,
        name: Union[str, None] = None,
        attr_fields: Union[list[str], None] = None,
        thread_num: int = 1,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """
        Register a GFF file as a Datafusion table.

        Parameters:
            path: The path to the GFF file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            attr_fields: The fields to unnest from the `attributes` column. If not specified, all fields swill be rendered as `attributes` column containing an array of structures `{'tag':'xxx', 'value':'yyy'}`.
            thread_num: The number of threads to use for reading the GFF file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the GFF file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compression is supported ('bgz' and 'gz').
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
        !!! note
            GFF reader uses **1-based** coordinate system for the `start` and `end` columns.

        !!! Example
            ```shell
            wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_38/gencode.v38.annotation.gff3.gz -O /tmp/gencode.v38.annotation.gff3.gz
            ```
            ```python
            import polars_bio as pb
            pb.register_gff("/tmp/gencode.v38.annotation.gff3.gz", "gencode_v38_annotation3_bgz", attr_fields=["ID", "Parent"])
            pb.sql("SELECT `Parent`, count(*) AS cnt FROM gencode_v38_annotation3_bgz GROUP BY `Parent`").limit(5).collect()
            ```
            ```shell

            shape: (5, 2)
            ┌───────────────────┬───────┐
            │ Parent            ┆ cnt   │
            │ ---               ┆ ---   │
            │ str               ┆ i64   │
            ╞═══════════════════╪═══════╡
            │ null              ┆ 60649 │
            │ ENSG00000223972.5 ┆ 2     │
            │ ENST00000456328.2 ┆ 3     │
            │ ENST00000450305.2 ┆ 6     │
            │ ENSG00000227232.5 ┆ 1     │
            └───────────────────┴───────┘

            ```
        !!! tip
            `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the GFF file. As a rule of thumb for large scale operations (reading a whole GFF), it is recommended to the default values.
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
        py_register_table(ctx, path, name, InputFormat.Gff, read_options)

    @staticmethod
    def register_fastq(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        parallel: bool = True,
    ) -> None:
        """
        Register a FASTQ file as a Datafusion table.

        Parameters:
            path: The path to the FASTQ file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the FASTQ file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compression is supported ('bgz' and 'gz').
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            parallel: Whether to use the parallel reader for BGZF compressed files.

        !!! Example
            ```python
              import polars_bio as pb
              pb.register_fastq("gs://genomics-public-data/platinum-genomes/fastq/ERR194146.fastq.gz", "test_fastq")
              pb.sql("SELECT name, description FROM test_fastq WHERE name LIKE 'ERR194146%'").limit(5).collect()
            ```

            ```shell

              shape: (5, 2)
            ┌─────────────────────┬─────────────────────────────────┐
            │ name                ┆ description                     │
            │ ---                 ┆ ---                             │
            │ str                 ┆ str                             │
            ╞═════════════════════╪═════════════════════════════════╡
            │ ERR194146.812444541 ┆ HSQ1008:141:D0CC8ACXX:2:1204:1… │
            │ ERR194146.812444542 ┆ HSQ1008:141:D0CC8ACXX:4:1206:1… │
            │ ERR194146.812444543 ┆ HSQ1008:141:D0CC8ACXX:3:2104:5… │
            │ ERR194146.812444544 ┆ HSQ1008:141:D0CC8ACXX:3:2204:1… │
            │ ERR194146.812444545 ┆ HSQ1008:141:D0CC8ACXX:3:1304:3… │
            └─────────────────────┴─────────────────────────────────┘

            ```


        !!! tip
            `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the FASTQ file. As a rule of thumb for large scale operations (reading a whole FASTQ), it is recommended to the default values.
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
        py_register_table(ctx, path, name, InputFormat.Fastq, read_options)

    @staticmethod
    def register_bed(
        path: str,
        name: Union[str, None] = None,
        thread_num: int = 1,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """
        Register a BED file as a Datafusion table.

        Parameters:
            path: The path to the BED file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            thread_num: The number of threads to use for reading the BED file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically based on the file extension. BGZF compression is supported ('bgz').
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.

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
            pb.register_bed("/tmp/fragile_site_bed/chr5_fragile_site.bed", "test_bed")
            b.sql("select * FROM test_bed WHERE name LIKE 'FRA5%'").collect()
            ```

            ```shell

                shape: (8, 4)
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
                │ chr5  ┆ 98200001  ┆ 109600000 ┆ FRA5F │
                │ chr5  ┆ 168500001 ┆ 180915260 ┆ FRA5G │
                │ chr5  ┆ 50500001  ┆ 63000000  ┆ FRA5H │
                └───────┴───────────┴───────────┴───────┘
            ```


        !!! tip
            `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the BED file. As a rule of thumb for large scale operations (reading a whole BED), it is recommended to the default values.
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
        py_register_table(ctx, path, name, InputFormat.Bed, read_options)

    @staticmethod
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

    @staticmethod
    def register_bam(
        path: str,
        name: Union[str, None] = None,
        thread_num: int = 1,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
    ) -> None:
        """
        Register a BAM file as a Datafusion table.

        Parameters:
            path: The path to the BAM file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            thread_num: The number of threads to use for reading the BAM file. Used **only** for parallel decompression of BGZF blocks. Works only for **local** files.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
        !!! note
            BAM reader uses **1-based** coordinate system for the `start`, `end`, `mate_start`, `mate_end` columns.

        !!! Example

            ```python
            import polars_bio as pb
            pb.register_bam("gs://genomics-public-data/1000-genomes/bam/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam", "HG00096_bam", concurrent_fetches=1, chunk_size=8)
            pb.sql("SELECT chrom, flags FROM HG00096_bam").limit(5).collect()
            ```
            ```shell

                shape: (5, 2)
                ┌───────┬───────┐
                │ chrom ┆ flags │
                │ ---   ┆ ---   │
                │ str   ┆ u32   │
                ╞═══════╪═══════╡
                │ chr1  ┆ 163   │
                │ chr1  ┆ 163   │
                │ chr1  ┆ 99    │
                │ chr1  ┆ 99    │
                │ chr1  ┆ 99    │
                └───────┴───────┘
            ```
        !!! tip
            `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the BAM file. As a rule of thumb for large scale operations (reading a whole BAM), it is recommended keep the default values.
            For more interactive inspecting a schema, it is recommended to decrease `chunk_size` to **8-16** and `concurrent_fetches` to **1-2**.
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
        py_register_table(ctx, path, name, InputFormat.Bam, read_options)

    @staticmethod
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
