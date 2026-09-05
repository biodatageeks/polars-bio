from typing import Optional, Sequence, Union

import polars as pl

from polars_bio.polars_bio import (
    BamReadOptions,
    BedReadOptions,
    BgenReadOptions,
    BigBedReadOptions,
    BigWigReadOptions,
    CoolReadOptions,
    CramReadOptions,
    FastaReadOptions,
    FastqReadOptions,
    GffReadOptions,
    GtfReadOptions,
    InputFormat,
    PairsReadOptions,
    PgenReadOptions,
    PyObjectStorageOptions,
    ReadOptions,
    VcfReadOptions,
    VcfZarrReadOptions,
    py_from_polars,
    py_read_sql,
    py_read_table,
    py_register_table,
    py_register_view,
)

from .context import _resolve_zero_based, ctx
from .io import (
    _cleanse_fields,
    _lazy_scan,
    _normalize_bigbed_schema_mode,
    _normalize_read_tag_type_hints,
    _validate_bcf_genotype_output,
    _validate_bgen_genotype_fields,
    _validate_bgen_genotype_output,
    _validate_bgen_input_path,
    _validate_bgen_probability_layout,
    _validate_pgen_genotype_fields,
    _validate_pgen_input_path,
    _validate_pgen_missing_sample_policy,
    _validate_pgen_psam_id_mode,
    _validate_tag_type_hints,
    _validate_variant_input_path,
)


class SQL:
    @staticmethod
    def register_vcf(
        path: str,
        name: Union[str, None] = None,
        info_fields: Union[list[str], None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """Register a text VCF file as a DataFusion table.

        Parameters:
            path: The path to the text VCF file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            info_fields: List of INFO field names to register. If *None*, all INFO fields will be detected automatically from the VCF header. Use this to limit registration to specific fields for better performance.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the VCF file. If not specified, it will be detected automatically..
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
        _validate_variant_input_path(path, "vcf", operation="register")
        SQL._register_variant(
            path=path,
            name=name,
            info_fields=info_fields,
            format_fields=None,
            samples=None,
            genotype_output="string",
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            max_retries=max_retries,
            timeout=timeout,
            enable_request_payer=enable_request_payer,
            compression_type=compression_type,
        )

    @staticmethod
    def register_bcf(
        path: str,
        name: Union[str, None] = None,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        samples: Union[list[str], None] = None,
        genotype_output: str = "string",
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """Register a BCF file as a DataFusion table.

        Parameters:
            path: The path to the BCF file. A neighboring `.bcf.csi` index is auto-discovered.
            name: The table name. If *None*, a name is generated from the path.
            info_fields: INFO fields to register. If *None*, all header-defined INFO fields are registered.
            format_fields: FORMAT fields to register. If *None*, all header-defined FORMAT fields are registered.
            samples: Optional sample names to register, in requested order.
            genotype_output: GT representation. `"string"` (default) returns VCF-style calls such as `"0/1"`. `"dosage"` returns the number of ALT alleles per sample as nullable `Int8` (normally 0, 1, or 2 for diploid calls); any missing allele yields null. Dosage requires GT to be the only selected FORMAT field and requires biallelic records. When `format_fields` is *None*, all header-defined FORMAT fields are selected, so pass `format_fields=["GT"]` when the header declares additional fields. Multiallelic records are rejected.
            chunk_size: Object-store chunk size in MB.
            concurrent_fetches: Number of concurrent object-store fetches.
            allow_anonymous: Allow anonymous object-store access.
            max_retries: Maximum number of object-store retries.
            timeout: Object-store timeout in seconds.
            enable_request_payer: Enable AWS request-payer access.
            compression_type: Compression override. The default detects BCF automatically.
        """
        _validate_bcf_genotype_output(genotype_output, format_fields)
        _validate_variant_input_path(path, "bcf", operation="register")
        SQL._register_variant(
            path=path,
            name=name,
            info_fields=info_fields,
            format_fields=format_fields,
            samples=samples,
            genotype_output=genotype_output,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            max_retries=max_retries,
            timeout=timeout,
            enable_request_payer=enable_request_payer,
            compression_type=compression_type,
        )

    @staticmethod
    def _register_variant(
        path: str,
        name: Union[str, None],
        info_fields: Union[list[str], None],
        format_fields: Union[list[str], None],
        samples: Union[list[str], None],
        genotype_output: str,
        chunk_size: int,
        concurrent_fetches: int,
        allow_anonymous: bool,
        max_retries: int,
        timeout: int,
        enable_request_payer: bool,
        compression_type: str,
    ) -> None:

        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        if info_fields is not None:
            all_info_fields = info_fields
        else:
            all_info_fields = None
            try:
                from .io import IOOperations

                variant_schema_df = IOOperations._describe_variant(
                    path,
                    allow_anonymous=allow_anonymous,
                    enable_request_payer=enable_request_payer,
                    compression_type=compression_type,
                )
                all_info_fields = (
                    variant_schema_df.filter(pl.col("field_type") == "INFO")
                    .select("name")
                    .to_series()
                    .to_list()
                )
            except Exception:
                all_info_fields = []

        vcf_read_options = VcfReadOptions(
            info_fields=all_info_fields,
            format_fields=format_fields,
            samples=samples,
            object_storage_options=object_storage_options,
            genotype_output=genotype_output,
        )
        read_options = ReadOptions(vcf_read_options=vcf_read_options)
        py_register_table(ctx, path, name, InputFormat.Vcf, read_options)

    @staticmethod
    def register_vcf_zarr(
        path: str,
        name: Union[str, None] = None,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        use_zero_based: Union[bool, None] = None,
        samples: Union[list[str], None] = None,
        genotype_encoding_raw: bool = True,
    ) -> None:
        """
        Register a local VCF Zarr store as a Datafusion table.

        Parameters:
            path: The path to the VCF Zarr store directory.
            name: The table name. If *None*, the table name is generated from the path.
            info_fields: Optional list of INFO field names to include. If *None*, local INFO arrays are discovered automatically. Use [] to disable INFO fields.
            format_fields: Optional list of FORMAT field names to include. If *None*, local FORMAT arrays are discovered automatically. Use [] to disable FORMAT fields.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None, uses the global configuration.
            samples: Optional list of sample names to include.
            genotype_encoding_raw: If True, output GT as raw typed allele calls. If False, output VCF-style GT strings.
        """
        zero_based = _resolve_zero_based(use_zero_based)
        vcf_zarr_read_options = VcfZarrReadOptions(
            info_fields=info_fields,
            format_fields=format_fields,
            samples=samples,
            zero_based=zero_based,
            genotype_encoding_raw=genotype_encoding_raw,
        )
        read_options = ReadOptions(vcf_zarr_read_options=vcf_zarr_read_options)
        py_register_table(ctx, path, name, InputFormat.VcfZarr, read_options)

    @staticmethod
    def register_gff(
        path: str,
        name: Union[str, None] = None,
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
            pb.register_gff("/tmp/gencode.v38.annotation.gff3.gz", "gencode_v38_annotation3_bgz")
            pb.sql("SELECT attributes, count(*) AS cnt FROM gencode_v38_annotation3_bgz GROUP BY attributes").limit(5).collect()
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
            attr_fields=None,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(gff_read_options=gff_read_options)
        py_register_table(ctx, path, name, InputFormat.Gff, read_options)

    @staticmethod
    def register_gtf(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """
        Register a GTF file as a Datafusion table.

        GTF (Gene Transfer Format) shares the same 9-column structure as GFF but uses
        different attribute syntax (``key "value"`` vs GFF's ``key=value``).

        Parameters:
            path: The path to the GTF file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the GTF file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compression is supported ('bgz' and 'gz').
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.

        !!! note
            GTF reader uses **1-based** coordinate system for the `start` and `end` columns.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_gtf("/tmp/annotations.gtf", "my_gtf")
            pb.sql("SELECT chrom, type, start FROM my_gtf").limit(5).collect()
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

        gtf_read_options = GtfReadOptions(
            attr_fields=None,
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(gtf_read_options=gtf_read_options)
        py_register_table(ctx, path, name, InputFormat.Gtf, read_options)

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
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(fastq_read_options=fastq_read_options)
        py_register_table(ctx, path, name, InputFormat.Fastq, read_options)

    @staticmethod
    def register_bed(
        path: str,
        name: Union[str, None] = None,
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
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the BED file. If not specified, it will be detected automatically..
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
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(bed_read_options=bed_read_options)
        py_register_table(ctx, path, name, InputFormat.Bed, read_options)

    @staticmethod
    def register_fasta(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """
        Register a FASTA file as a Datafusion table.

        Parameters:
            path: The path to the FASTA file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            chunk_size: The size in MB of a chunk when reading from an object store. The default is 8 MB. For large scale operations, it is recommended to increase this value to 64.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. The default is 1. For large scale operations, it is recommended to increase this value to 8 or even more.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            compression_type: The compression type of the FASTA file. If not specified, it will be detected automatically based on the file extension. BGZF and GZIP compressions are supported ('bgz', 'gz').
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.

        !!! Example
            ```shell
            wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
            ```

            ```python
            import polars_bio as pb
            pb.register_fasta("/tmp/test.fasta", "test_fasta")
            pb.sql("select name, description from test_fasta limit 1").collect()
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
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(fasta_read_options=fasta_read_options)
        py_register_table(ctx, path, name, InputFormat.Fasta, read_options)

    @staticmethod
    def register_bigwig(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        use_zero_based: Union[bool, None] = None,
    ) -> None:
        """
        Register a BigWig file as a DataFusion table.
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

        bigwig_read_options = BigWigReadOptions(
            object_storage_options=object_storage_options,
            zero_based=_resolve_zero_based(use_zero_based),
        )
        read_options = ReadOptions(bigwig_read_options=bigwig_read_options)
        py_register_table(ctx, path, name, InputFormat.BigWig, read_options)

    @staticmethod
    def register_bigbed(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        use_zero_based: Union[bool, None] = None,
        schema: str = "auto",
    ) -> None:
        """
        Register a BigBed file as a DataFusion table.
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

        bigbed_read_options = BigBedReadOptions(
            object_storage_options=object_storage_options,
            zero_based=_resolve_zero_based(use_zero_based),
            schema=_normalize_bigbed_schema_mode(schema),
        )
        read_options = ReadOptions(bigbed_read_options=bigbed_read_options)
        py_register_table(ctx, path, name, InputFormat.BigBed, read_options)

    @staticmethod
    def register_cool(
        path: str,
        name: Union[str, None] = None,
        resolution: Union[int, None] = None,
        join_bins: bool = True,
        include_weights: bool = False,
        use_zero_based: Union[bool, None] = None,
    ) -> None:
        """
        Register a Cooler (`.cool`/`.mcool`) file as a DataFusion table.

        Parameters:
            path: The path to the `.cool`/`.mcool` file, or a cooler URI (`file.mcool::/resolutions/10000`).
            name: The name of the table. If *None*, a name is derived from the file name.
            resolution: Bin size selecting an `.mcool` data collection. Optional for `.cool` files and single-resolution `.mcool` files.
            join_bins: If *True* (default), join pixels with bin coordinates; if *False*, expose the raw COO triple.
            include_weights: If *True*, expose balancing weights as `weight1`/`weight2`.
            use_zero_based: Coordinate system override. Cooler is natively 0-based half-open; set to *False* to emit 1-based closed coordinates, or *None* to use the global default.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_cool("contacts.mcool", "hic", resolution=10000)
            pb.sql("SELECT chrom1, count FROM hic LIMIT 5").collect()
            ```
        """
        cool_read_options = CoolReadOptions(
            resolution=resolution,
            join_bins=join_bins,
            include_weights=include_weights,
            zero_based=_resolve_zero_based(use_zero_based),
        )
        read_options = ReadOptions(cool_read_options=cool_read_options)
        py_register_table(ctx, path, name, InputFormat.Cool, read_options)

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
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Union[list[str], None] = None,
    ) -> None:
        """
        Register a BAM file as a Datafusion table.

        Parameters:
            path: The path to the BAM file.
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            tag_fields: List of BAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.
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

        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        bam_read_options = BamReadOptions(
            object_storage_options=object_storage_options,
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        py_register_table(ctx, path, name, InputFormat.Bam, read_options)

    @staticmethod
    def register_sam(
        path: str,
        name: Union[str, None] = None,
        tag_fields: Union[list[str], None] = None,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Union[list[str], None] = None,
    ) -> None:
        """
        Register a SAM file as a Datafusion table.

        SAM (Sequence Alignment/Map) is the plain-text counterpart of BAM.
        This function reuses the BAM table provider, which auto-detects
        the format from the file extension.

        Parameters:
            path: The path to the SAM file.
            name: The name of the table. If *None*, the name will be generated automatically from the path.
            tag_fields: List of SAM tag names to include as columns (e.g., ["NM", "MD", "AS"]).
                If None, no optional tags are parsed (default).
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_sam("test.sam", "my_sam")
            pb.sql("SELECT chrom, flags FROM my_sam").limit(5).collect()
            ```
        """
        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        bam_read_options = BamReadOptions(
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(bam_read_options=bam_read_options)
        py_register_table(ctx, path, name, InputFormat.Sam, read_options)

    @staticmethod
    def register_cram(
        path: str,
        name: Union[str, None] = None,
        tag_fields: Union[list[str], None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        infer_tag_types: bool = True,
        infer_tag_sample_size: int = 100,
        tag_type_hints: Union[list[str], None] = None,
    ) -> None:
        """
        Register a CRAM file as a Datafusion table.

        !!! warning "Embedded Reference Required"
            Currently, only CRAM files with **embedded reference sequences** are supported.
            CRAM files requiring external reference FASTA files cannot be registered.
            Most modern CRAM files include embedded references by default.

            To create a CRAM file with embedded reference using samtools:
            ```bash
            samtools view -C -o output.cram --output-fmt-option embed_ref=1 input.bam
            ```

        Parameters:
            path: The path to the CRAM file (local or cloud storage: S3, GCS, Azure Blob).
            name: The name of the table. If *None*, the name of the table will be generated automatically based on the path.
            tag_fields: List of CRAM tag names to include as columns (e.g., ["NM", "MD", "AS"]). If None, no optional tags are parsed (default). Common tags include: NM (edit distance), MD (mismatch string), AS (alignment score), XS (secondary alignment score), RG (read group), CB (cell barcode), UB (UMI barcode).
            chunk_size: The size in MB of a chunk when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **8-16**.
            concurrent_fetches: [GCS] The number of concurrent fetches when reading from an object store. Default settings are optimized for large scale operations. For small scale (interactive) operations, it is recommended to decrease this value to **1-2**.
            allow_anonymous: [GCS, AWS S3] Whether to allow anonymous access to object storage.
            enable_request_payer: [AWS S3] Whether to enable request payer for object storage. This is useful for reading files from AWS S3 buckets that require request payer.
            max_retries:  The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            infer_tag_types: If True (default), sample the file to auto-detect types for custom/unknown tags.
            infer_tag_sample_size: Number of records to sample for tag type inference (default: 100).
            tag_type_hints: Explicit SAM-style type hints for tags (e.g., ["pt:i", "ML:B:C", "FZ:B:S"]). Supported forms: TAG:TYPE, TAG:B, or TAG:B:SUBTYPE where TYPE is one of A, c, C, s, S, i, I, f, Z, H and SUBTYPE is one of c, C, s, S, i, I, f.
        !!! note
            CRAM reader uses **1-based** coordinate system for the `start`, `end`, `mate_start`, `mate_end` columns.

        !!! tip
            `chunk_size` and `concurrent_fetches` can be adjusted according to the network bandwidth and the size of the CRAM file. As a rule of thumb for large scale operations (reading a whole CRAM), it is recommended to keep the default values.
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

        if tag_type_hints is not None:
            _validate_tag_type_hints(tag_type_hints)
            tag_type_hints = _normalize_read_tag_type_hints(tag_type_hints)
        cram_read_options = CramReadOptions(
            reference_path=None,
            object_storage_options=object_storage_options,
            tag_fields=tag_fields,
            infer_tag_types=infer_tag_types,
            infer_tag_sample_size=infer_tag_sample_size,
            tag_type_hints=tag_type_hints,
        )
        read_options = ReadOptions(cram_read_options=cram_read_options)
        py_register_table(ctx, path, name, InputFormat.Cram, read_options)

    @staticmethod
    def register_pairs(
        path: str,
        name: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
    ) -> None:
        """
        Register a Pairs (Hi-C) file as a Datafusion table.

        The Pairs format (4DN project) stores chromatin contact data with columns:
        readID, chr1, pos1, chr2, pos2, strand1, strand2.

        Parameters:
            path: The path to the Pairs file (.pairs, .pairs.gz, .pairs.bgz).
            name: The name of the table. If *None*, the name will be generated automatically from the path.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression type. If not specified, it will be detected automatically.

        !!! note
            Pairs format uses **1-based** coordinate system for pos1 and pos2.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_pairs("contacts.pairs.gz", "hic_contacts")
            pb.sql("SELECT * FROM hic_contacts WHERE chr1 = 'chr1'").collect()
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

        pairs_read_options = PairsReadOptions(
            object_storage_options=object_storage_options,
        )
        read_options = ReadOptions(pairs_read_options=pairs_read_options)
        py_register_table(ctx, path, name, InputFormat.Pairs, read_options)

    @staticmethod
    def register_bgen(
        path: str,
        name: Union[str, None] = None,
        genotype_output: str = "probability",
        probability_layout: str = "nested",
        samples: Union[list[str], None] = None,
        genotype_fields: Union[list[str], None] = None,
        sample_path: Union[str, None] = None,
        bgi_path: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
    ) -> None:
        """
        Register a BGEN file as a DataFusion table.

        Parameters:
            path: The path to the BGEN file. The path must end in `.bgen`. A neighbouring `.bgen.bgi` index is auto-discovered.
            name: The name of the table. If *None*, the name will be generated automatically from the path.
            genotype_output: Genotype representation. `"probability"` (default) keeps every format-defined state in `genotypes.GP`. `"dosage"` emits `genotypes.DS`, the expected copy count of `alleles[1]`, and rejects multiallelic variants.
            probability_layout: How probability states are stored. `"nested"` (default) gives each sample a variable-length list and reads every BGEN file. `"fixed"` gives each sample a fixed-width list, dropping the per-sample offsets that are about a quarter of the emitted probability bytes for a diploid biallelic cohort; it requires every variant to store the same number of states and rejects a file that mixes them. Ignored when `genotype_output="dosage"`.
            samples: Sample identifiers to register, in requested order.
            genotype_fields: Children of the `genotypes` struct to emit, from the output mode's value child — `"DS"` for dosage, `"GP"` for probability — and `"PLOIDY"`, in the requested order. If *None*, all of them are emitted. `"PLOIDY"` is a byte per genotype, so register with `["DS"]` when only the dosages are queried.
            sample_path: An explicit Oxford `.sample` companion, used only when the BGEN has no embedded sample identifiers.
            bgi_path: An explicit `.bgi` index location.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override. BGEN block compression is read from the file header.
            use_zero_based: If True, register 0-based half-open coordinates. If False, 1-based closed. If None (default), uses the global configuration.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_bgen("cohort.bgen", "cohort", genotype_output="dosage")
            pb.sql("SELECT rsid, genotypes FROM cohort WHERE chrom = '22'").collect()
            ```
        """
        _validate_bgen_genotype_output(genotype_output)
        _validate_bgen_probability_layout(probability_layout)
        _validate_bgen_genotype_fields(genotype_fields)
        _validate_bgen_input_path(path, operation="register")
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        bgen_read_options = BgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_output=genotype_output,
            probability_layout=probability_layout,
            samples=samples,
            genotype_fields=genotype_fields,
            sample_path=sample_path,
            bgi_path=bgi_path,
            zero_based=_resolve_zero_based(use_zero_based),
        )
        read_options = ReadOptions(bgen_read_options=bgen_read_options)
        py_register_table(ctx, path, name, InputFormat.Bgen, read_options)

    @staticmethod
    def register_pgen(
        path: str,
        name: Union[str, None] = None,
        genotype_fields: Sequence[str] = ("GT",),
        samples: Union[list[str], None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        max_range_gap: Union[int, None] = None,
        max_range_bytes: Union[int, None] = None,
        batch_soft_byte_limit: Union[int, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
        *,
        max_companion_bytes: Union[int, None] = None,
        max_decompressed_companion_bytes: Union[int, None] = None,
        max_variants: Union[int, None] = None,
    ) -> None:
        """
        Register a PLINK 2 PGEN fileset as a DataFusion table.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`. Neighbouring `.pvar` and `.psam` companions are auto-discovered.
            name: The name of the table. If *None*, the name will be generated automatically from the path.
            genotype_fields: Genotype children to emit, from `"GT"`, `"ALT_COUNT"`, `"PHASED"`, `"DS"`, `"DS_STORED"`, and `"HDS"`. Defaults to `("GT",)`.
            samples: Sample identifiers to register, in requested order.
            missing_sample_policy: `"error"` (default) rejects an absent requested sample; `"ignore"` omits it.
            psam_id_mode: `"iid"` (default), `"fid_iid"`, or `"fid_iid_sid"`.
            pvar_path: An explicit `.pvar` companion.
            psam_path: An explicit `.psam` companion.
            pgi_path: An explicit `.pgi` index.
            max_range_gap: The largest run of unselected bytes bridged when coalescing reads, in bytes. The provider default is 0, which never bridges a gap and issues one read per contiguous run of selected variants. Raising it trades wasted bytes for fewer requests, which matters most on object storage. If *None*, the provider default is used.
            max_range_bytes: The largest coalesced read, in bytes. If *None*, the provider default is used.
            batch_soft_byte_limit: A soft target for genotype bytes in one RecordBatch. If *None*, the provider default is used.
            max_companion_bytes: The largest on-disk size accepted for the `.pvar` or `.psam` companion, in bytes. The provider default is 4 GiB. Companions are streamed, so this bounds work rather than memory. If *None*, the provider default is used.
            max_decompressed_companion_bytes: The largest decoded size accepted for a companion, in bytes. The provider default is 16 GiB. If *None*, the provider default is used.
            max_variants: The largest PVAR row count accepted. The provider default is 250 million; the parsed variant table costs a few tens of bytes per row, so this is the cap that bounds resident memory. If *None*, the provider default is used.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override.
            use_zero_based: If True, register 0-based half-open coordinates. If False, 1-based closed. If None (default), uses the global configuration.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_pgen("cohort.pgen", "cohort", genotype_fields=["DS"])
            pb.sql("SELECT id, genotypes FROM cohort WHERE chrom = '1'").collect()
            ```
        """
        _validate_pgen_input_path(path, operation="register")
        _validate_pgen_genotype_fields(genotype_fields)
        _validate_pgen_psam_id_mode(psam_id_mode)
        _validate_pgen_missing_sample_policy(missing_sample_policy)
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=list(genotype_fields),
            samples=samples,
            missing_sample_policy=missing_sample_policy,
            psam_id_mode=psam_id_mode,
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            max_range_gap=max_range_gap,
            max_range_bytes=max_range_bytes,
            batch_soft_byte_limit=batch_soft_byte_limit,
            max_companion_bytes=max_companion_bytes,
            max_decompressed_companion_bytes=max_decompressed_companion_bytes,
            max_variants=max_variants,
            zero_based=_resolve_zero_based(use_zero_based),
        )
        read_options = ReadOptions(pgen_read_options=pgen_read_options)
        py_register_table(ctx, path, name, InputFormat.Pgen, read_options)

    @staticmethod
    def sql(query: str) -> pl.LazyFrame:
        """
        Execute a SQL query on the registered tables.

        Parameters:
            query: The SQL query.

        !!! Example
              ```python
              import polars_bio as pb
              pb.register_vcf("/tmp/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_v4_1_sv")
              pb.sql("SELECT * FROM gnomad_v4_1_sv LIMIT 5").collect()
              ```
        """
        df = py_read_sql(ctx, query)
        return _lazy_scan(df)
