## Genomic ranges operations

| Features                                           | Bioframe           | polars-bio         | PyRanges           | Pybedtools         | PyGenomics         | GenomicRanges      |
|----------------------------------------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| [overlap](api.md#polars_bio.overlap)               | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| [nearest](api.md#polars_bio.nearest)               | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [count_overlaps](api.md#polars_bio.count_overlaps) | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| cluster                                            | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: |                    |                    |
| [merge](api.md#polars_bio.merge)                   | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| complement                                         | :white_check_mark: | :construction:     |                    | :white_check_mark: | :white_check_mark: |                    |
| [coverage](api.md#polars_bio.coverage)             | :white_check_mark: |  :white_check_mark:                  | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [expand](api.md#polars_bio.LazyFrame.expand)       | :white_check_mark: | :white_check_mark:     | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [sort](api.md#polars_bio.LazyFrame.sort_bedframe)  | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [read_table](api.md#polars_bio.read_table)         | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |


## Coordinate systems support
polars-bio supports both 0-based and 1-based coordinate systems. Please check `overlap_filter` parameter of a given operation to choose the appropriate coordinate system, e.g. [overlap](api.md#polars_bio.overlap) operation.

### API comparison between libraries
There is no standard API for genomic ranges operations in Python.
This table compares the API of the libraries. The table is not exhaustive and only shows the most common operations used in benchmarking.

| operation  | Bioframe                                                                                                | polars-bio                                 | PyRanges0                                                                                                           | PyRanges1                                                                                                     | Pybedtools                                                                                                                                    | GenomicRanges                                                                                                                                      |
|------------|---------------------------------------------------------------------------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| overlap    | [overlap](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.overlap)          | [overlap](api.md#polars_bio.overlap)       | [join](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.join)<sup>1</sup>    | [join_ranges](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html#pyranges.PyRanges.join_ranges) | [intersect](https://bedtools.readthedocs.io/en/latest/content/tools/intersect.html?highlight=intersect#usage-and-option-summary)<sup>2</sup>  | [find_overlaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.find_overlaps)<sup>3</sup> |
| nearest    | [closest](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.closest)          | [nearest](api.md#polars_bio.nearest)       | [nearest](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.nearest)          | [nearest](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html#pyranges.PyRanges.nearest)         | [closest](https://daler.github.io/pybedtools/autodocs/pybedtools.bedtool.BedTool.closest.html#pybedtools.bedtool.BedTool.closest)<sup>4</sup> | [nearest](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.nearest)<sup>5</sup>             |
| read_table | [read_table](https://bioframe.readthedocs.io/en/latest/api-fileops.html#bioframe.io.fileops.read_table) | [read_table](api.md#polars_bio.read_table) | [read_bed](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/readers/index.html#pyranges.readers.read_bed) | [read_bed](https://pyranges1.readthedocs.io/en/latest/pyranges_module.html#pyranges.read_bed)                 | [BedTool](https://daler.github.io/pybedtools/topical-create-a-bedtool.html#creating-a-bedtool)                                                | [read_bed](https://biocpy.github.io/GenomicRanges/tutorial.html#from-bioinformatic-file-formats)                                                   |

!!! note
1. There is an [overlap](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.overlap) method in PyRanges, but its output is only limited to indices of intervals from the other Dataframe that overlap.
   In Bioframe's [benchmark](https://bioframe.readthedocs.io/en/latest/guide-performance.html#vs-pyranges-and-optionally-pybedtools) also **join** method instead of overlap was used.
2. **wa** and **wb** options used to obtain a comparable output.
3. Output contains only a list with the same length as query, containing hits to overlapping indices. Data transformation is required to obtain the same output as in other libraries.
   Since the performance was far worse than in more efficient libraries anyway, additional data transformation was not included in the benchmark.
4. **s=first** was used to obtain a comparable output.
5. **select="arbitrary"** was used to obtain a comparable output.


### File formats support

| Format                                | Support level      |
|---------------------------------------|--------------------|
| [BED](api.md#polars_bio.read_table)   | :white_check_mark: |
| [VCF](api.md#polars_bio.read_vcf)     | :white_check_mark: |
| [BAM](api.md#polars_bio.read_bam)     | :white_check_mark: |
| [FASTQ](api.md#polars_bio.read_fastq) | :white_check_mark: |
| [FASTA](api.md#polars_bio.read_fasta) | :white_check_mark: |
| GFF                                   | :construction:     |
| GTF                                   | :construction:     |
| Indexed VCF                           | :construction:     |
| Indexed BAM                           | :construction:     |


## SQL-powered data processing
polars-bio provides a SQL-like API for bioinformatic data querying or manipulation.
Check [SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) for more details.

```python
import polars_bio as pb
pb.register_vcf("gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_sv", thread_num=1, info_fields=["SVTYPE", "SVLEN"])
pb.sql("SELECT * FROM gnomad_sv WHERE SVTYPE = 'DEL' AND SVLEN > 1000").limit(3).collect()
```

You can use [view](api.md/#polars_bio.register_view) mechanism to create a virtual table from a DataFrame that contain preprocessing steps and reuse it in multiple steps.
To avoid materializing the intermediate results in memory, you can run your processing in  [streaming](features.md#streaming) mode.

```python


```shell
shape: (3, 10)
┌───────┬───────┬───────┬────────────────────────────────┬───┬───────┬────────────┬────────┬───────┐
│ chrom ┆ start ┆ end   ┆ id                             ┆ … ┆ qual  ┆ filter     ┆ svtype ┆ svlen │
│ ---   ┆ ---   ┆ ---   ┆ ---                            ┆   ┆ ---   ┆ ---        ┆ ---    ┆ ---   │
│ str   ┆ u32   ┆ u32   ┆ str                            ┆   ┆ f64   ┆ str        ┆ str    ┆ i32   │
╞═══════╪═══════╪═══════╪════════════════════════════════╪═══╪═══════╪════════════╪════════╪═══════╡
│ chr1  ┆ 22000 ┆ 30000 ┆ gnomAD-SV_v3_DEL_chr1_fa103016 ┆ … ┆ 999.0 ┆ HIGH_NCR   ┆ DEL    ┆ 8000  │
│ chr1  ┆ 40000 ┆ 47000 ┆ gnomAD-SV_v3_DEL_chr1_b26f63f7 ┆ … ┆ 145.0 ┆ PASS       ┆ DEL    ┆ 7000  │
│ chr1  ┆ 79086 ┆ 88118 ┆ gnomAD-SV_v3_DEL_chr1_733c4ef0 ┆ … ┆ 344.0 ┆ UNRESOLVED ┆ DEL    ┆ 9032  │
└───────┴───────┴───────┴────────────────────────────────┴───┴───────┴────────────┴────────┴───────┘

```

## Parallel engine 🏎️
It is straightforward to parallelize operations in polars-bio. The library is built on top of [Apache DataFusion](https://datafusion.apache.org/)  you can set
the degree of parallelism using the `datafusion.execution.target_partitions` option, e.g.:
```python
import polars_bio as pb
pb.set_option("datafusion.execution.target_partitions", "8")
```
!!! tip
    1. The default value is **1** (parallel execution disabled).
    2. The `datafusion.execution.target_partitions` option is a global setting and affects all operations in the current session.
    3. Check [available strategies](performance.md#parallel-execution-and-scalability) for optimal performance.
    4. See  the other configuration settings in the Apache DataFusion [documentation](https://datafusion.apache.org/user-guide/configs.html).


## Cloud storage ☁️
polars-bio supports direct streamed reading from cloud storages (e.g. S3, GCS) enabling processing large-scale genomics data without materializing in memory.
It is built upon the [OpenDAL](https://opendal.apache.org/) project, a unified data access layer for cloud storage, which allows to read and write data from/to various cloud storage providers.

### Example
```python
import polars_bio as pb
## Register VCF files from Google Cloud Storage that will be streamed - no need to download them to the local disk, size ~0.8TB
pb.register_vcf("gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz", "gnomad_big", allow_anonymous=True)
pb.register_vcf("gs://gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz", "gnomad_sv", allow_anonymous=True)
pb.overlap("gnomad_sv", "gnomad_big", streaming=True).sink_parquet("/tmp/overlap.parquet")
```
It is  especially useful when combined with [SQL](features.md#sql-powered-data-processing) support for preprocessing and [streaming](features.md#streaming) processing capabilities.

### Supported features

| Feature              | AWS S3             | Google Cloud Storage | Azure Blob Storage |
|----------------------|--------------------|----------------------|--------------------|
| Anonymous access     | :white_check_mark: | :white_check_mark:   |                    |
| Authenticated access | :white_check_mark: | :white_check_mark:   | :white_check_mark: |
| Requester Pays       | :white_check_mark: |                      |                    |
| Concurrent requests  |                    | :white_check_mark:   |                    |
| Streaming reads      | :white_check_mark: | :white_check_mark:   | :white_check_mark: |


### AWS S3 configuration
Supported environment variables:

| Variable              | Description                                                 |
|-----------------------|-------------------------------------------------------------|
| AWS_ACCESS_KEY_ID     | AWS access key ID for authenticated access to S3.           |
| AWS_SECRET_ACCESS_KEY | AWS secret access key for authenticated access to S3.       |
| AWS_ENDPOINT_URL      | Custom S3 endpoint URL for accessing S3-compatible storage. |
| AWS_REGION            | AWS region for accessing S3.                                |

### Google Cloud Storage configuration

Supported environment variables:

| Variable                       | Description                                                                        |
|--------------------------------|------------------------------------------------------------------------------------|
| GOOGLE_APPLICATION_CREDENTIALS | Path to the Google Cloud service account key file for authenticated access to GCS. |

### Azure Blob Storage configuration
Supported environment variables:

| Variable              | Description                                                                |
|-----------------------|----------------------------------------------------------------------------|
| AZURE_STORAGE_ACCOUNT | Azure Storage account name for authenticated access to Azure Blob Storage. |
| AZURE_STORAGE_KEY     | Azure Storage account key for authenticated access to Azure Blob Storage.  |
| AZURE_ENDPOINT_URL    | Azure Blob Storage endpoint URL for accessing Azure Blob Storage.          |

## Streaming 🚂
polars-bio supports out-of-core processing with Apache DataFusion async [streams](https://docs.rs/datafusion/46.0.0/datafusion/physical_plan/trait.ExecutionPlan.html#tymethod.execute) and Polars LazyFrame [streaming](https://docs.pola.rs/user-guide/concepts/_streaming/) option.
It can bring  significant speedup as well reduction in memory usage allowing to process large datasets that do not fit in memory.
See our benchmark [results](performance.md#calculate-overlaps-and-export-to-a-csv-file-7-8).
There are 2 ways of using streaming mode:

1. By setting the `output_type` to `datafusion.DataFrame` and using the Python DataFrame [API](https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html), including methods such as [count](https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html#datafusion.dataframe.DataFrame.count), [write_parquet](https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html#datafusion.dataframe.DataFrame.write_parquet) or [write_csv](https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html#datafusion.dataframe.DataFrame.write_csv) or [write_json](https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html#datafusion.dataframe.DataFrame.write_json). In this option you completely bypass the polars streaming engine.

    ```python
    import polars_bio as pb
    import polars as pl
    pb.overlap("/tmp/gnomad.v4.1.sv.sites.parquet", "/tmp/gnomad.exomes.v4.1.sites.chr1.parquet", output_type="datafusion.DataFrame").write_parquet("/tmp/overlap.parquet")
    >>> pl.scan_parquet("/tmp/overlap.parquet").collect().count()
    shape: (1, 6)
    ┌────────────┬────────────┬────────────┬────────────┬────────────┬────────────┐
    │ chrom_1    ┆ start_1    ┆ end_1      ┆ chrom_2    ┆ start_2    ┆ end_2      │
    │ ---        ┆ ---        ┆ ---        ┆ ---        ┆ ---        ┆ ---        │
    │ u32        ┆ u32        ┆ u32        ┆ u32        ┆ u32        ┆ u32        │
    ╞════════════╪════════════╪════════════╪════════════╪════════════╪════════════╡
    │ 2629727337 ┆ 2629727337 ┆ 2629727337 ┆ 2629727337 ┆ 2629727337 ┆ 2629727337 │
    └────────────┴────────────┴────────────┴────────────┴────────────┴────────────┘
    ```

    !!! tip
        If you only need to write the results as fast as possible into one of the above file formats or quickly get the row count, then it is in the most cases the **best** option.


2. With the `streaming` option in the operation - using polars streaming (**experimental** and check limitations).:
    ```python
    import os
    import polars_bio as pb
    os.environ['BENCH_DATA_ROOT'] = "/Users/mwiewior/research/data/databio"
    os.environ['POLARS_MAX_THREADS'] = "1"
    os.environ['POLARS_VERBOSE'] = "1"

    cols=["contig", "pos_start", "pos_end"]
    BENCH_DATA_ROOT = os.getenv('BENCH_DATA_ROOT', '/data/bench_data/databio')
    df_1 = f"{BENCH_DATA_ROOT}/exons/*.parquet"
    df_2 =  f"{BENCH_DATA_ROOT}/exons/*.parquet"
    pb.overlap(df_1, df_2, cols1=cols, cols2=cols, streaming=True).collect(streaming=True).limit()
    ```

    ```bash
    INFO:polars_bio.operation:Running in streaming mode...
    INFO:polars_bio.operation:Running Overlap operation with algorithm Coitrees and 1 thread(s)...
    'STREAMING:\n  Anonymous SCAN []\n  PROJECT */6 COLUMNS'
    ```

    ```python
    pb.overlap(df_1, df_2, cols1=cols, cols2=cols, streaming=True).collect(streaming=True).limit()
    ```
    ```bash
    INFO:polars_bio.operation:Running in streaming mode...
    INFO:polars_bio.operation:Running Overlap operation with algorithm Coitrees and 1 thread(s)...
    RUN STREAMING PIPELINE
    [anonymous -> ordered_sink]
    shape: (5, 6)
    ┌──────────┬─────────────┬───────────┬──────────┬─────────────┬───────────┐
    │ contig_1 ┆ pos_start_1 ┆ pos_end_1 ┆ contig_2 ┆ pos_start_2 ┆ pos_end_2 │
    │ ---      ┆ ---         ┆ ---       ┆ ---      ┆ ---         ┆ ---       │
    │ str      ┆ i32         ┆ i32       ┆ str      ┆ i32         ┆ i32       │
    ╞══════════╪═════════════╪═══════════╪══════════╪═════════════╪═══════════╡
    │ chr1     ┆ 11873       ┆ 12227     ┆ chr1     ┆ 11873       ┆ 12227     │
    │ chr1     ┆ 12612       ┆ 12721     ┆ chr1     ┆ 12612       ┆ 12721     │
    │ chr1     ┆ 13220       ┆ 14409     ┆ chr1     ┆ 13220       ┆ 14409     │
    │ chr1     ┆ 14361       ┆ 14829     ┆ chr1     ┆ 13220       ┆ 14409     │
    │ chr1     ┆ 13220       ┆ 14409     ┆ chr1     ┆ 14361       ┆ 14829     │
    └──────────┴─────────────┴───────────┴──────────┴─────────────┴───────────┘

    ```


!!! Limitations
    1. Single threaded.
    2. Because of the [bug](https://github.com/biodatageeks/polars-bio/issues/57) only Polars *sink* operations, such as `collect`, `sink_csv` or `sink_parquet` are supported.





## DataFrames support
| I/O              | Bioframe           | polars-bio             | PyRanges           | Pybedtools | PyGenomics | GenomicRanges          |
|------------------|--------------------|------------------------|--------------------|------------|------------|------------------------|
| Pandas DataFrame | :white_check_mark: | :white_check_mark:     | :white_check_mark: |            |            | :white_check_mark:     |
| Polars DataFrame |                    | :white_check_mark:     |                    |            |            | :white_check_mark:     |
| Polars LazyFrame |                    | :white_check_mark:     |                    |            |            |                        |
| Native readers   |                    | :white_check_mark:     |                    |            |            |                        |
