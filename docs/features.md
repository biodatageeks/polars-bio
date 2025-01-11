## Features

### Genomic ranges operations

| Features                                          | Bioframe           | polars-bio         | PyRanges           | Pybedtools         | PyGenomics         | GenomicRanges      |
|---------------------------------------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| [overlap](api.md#polars_bio.overlap)              | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| [nearest](api.md#polars_bio.nearest)              | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| cluster                                           | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: |                    |                    |
| merge                                             | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| complement                                        | :white_check_mark: | :construction:     |                    | :white_check_mark: | :white_check_mark: |                    |
| coverage                                          | :white_check_mark: |                    | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [expand](api.md#polars_bio.LazyFrame.expand)                                 | :white_check_mark: | :white_check_mark:     | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [sort](api.md#polars_bio.LazyFrame.sort_bedframe) | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |
| [read_table](api.md#polars_bio.read_table)        | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |                    | :white_check_mark: |


#### API comparison between libraries
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


#### File formats support

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





### Parallel engine and streaming processing 🏎️
It is straightforward to parallelize operations in polars-bio. The library is built on top of [Apache DataFusion](https://datafusion.apache.org/)  you can set
the degree of parallelism using the `datafusion.execution.target_partitions` option, e.g.:
```python
import polars_bio as pb
pb.ctx.set_option("datafusion.execution.target_partitions", "8")
```
!!! tip
    1. The default value is **1** (parallel execution disabled).
    2. The `datafusion.execution.target_partitions` option is a global setting and affects all operations in the current session.
    3. Check [available strategies](performance.md#parallel-execution-and-scalability) for optimal performance.
    4. See  the other configuration settings in the Apache DataFusion [documentation](https://datafusion.apache.org/user-guide/configs.html).


#### Streaming (out-of-core processing) [Exeprimental]🧪
polars-bio supports out-of-core processing with Polars LazyFrame [streaming](https://docs.pola.rs/user-guide/concepts/_streaming/) option.
It can bring  significant speedup as well reduction in memory usage allowing to process large datasets that do not fit in memory.
See our benchmark [results](performance.md#calculate-overlaps-and-export-to-a-csv-file-7-8).

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





#### DataFrames support
| I/O              | Bioframe           | polars-bio             | PyRanges           | Pybedtools | PyGenomics | GenomicRanges          |
|------------------|--------------------|------------------------|--------------------|------------|------------|------------------------|
| Pandas DataFrame | :white_check_mark: | :white_check_mark:     | :white_check_mark: |            |            | :white_check_mark:     |
| Polars DataFrame |                    | :white_check_mark:     |                    |            |            | :white_check_mark:     |
| Polars LazyFrame |                    | :white_check_mark:     |                    |            |            |                        |
| Native readers   |                    | :white_check_mark:     |                    |            |            |                        |
