# Features

polars-bio is a fast, memory-efficient Python DataFrame library for genomics, built on
[Apache DataFusion](https://datafusion.apache.org/), [Apache Arrow](https://arrow.apache.org/), and [Polars](https://pola.rs/).
The features below follow the flow of a typical analysis — **read → operate → write** — and then how to scale it.

!!! tip "Prefer lazy & streaming execution"
    Whenever possible, use lazy scans (`scan_*`) and lazy operations instead of their eager
    counterparts. Lazy execution lets polars-bio push filters and column projections down to the
    scan, so far less data is read and materialized in memory. For datasets that don't fit in
    memory, process them **out-of-core**: collect with Polars'
    [streaming engine](https://docs.pola.rs/user-guide/concepts/streaming/) via
    `collect(engine="streaming")`, or bypass it entirely with `output_type="datafusion.DataFrame"`
    to stream results straight to Parquet/CSV/JSON or to count rows. See
    [Benchmarking DataFrame paths in polars-bio](../blog/posts/dataframe-paths-benchmark-2026-04.md)
    for how much the input and execution path matters in practice.

## Typical workflow

A common end-to-end pipeline stays lazy from input to output — **read → operate → write** — so nothing
larger than a batch is ever held in memory:

```python
import polars as pl
import polars_bio as pb

# 1. Read — lazily scan bioinformatic files (no data is read yet)
variants = pb.scan_vcf("cohort.vcf.gz")
exons = pb.scan_gff("gencode.annotation.gff3.gz").filter(pl.col("type") == "exon")

# 2. Operate — range operation: keep variants that overlap an exon
in_exons = pb.overlap(variants, exons)

# 3. Write — stream the result straight to Parquet, never fully materialized
in_exons.sink_parquet("variants_in_exons.parquet")
```

Each step is covered in detail below: [reading files](reading.md), [genomic operations](operations.md),
and [writing files](writing.md).

## [Genomic operations](operations.md)

[Overlap](../api/operations.md#polars_bio.range_operations.overlap), [nearest](../api/operations.md#polars_bio.range_operations.nearest), [count](../api/operations.md#polars_bio.range_operations.count_overlaps), [coverage](../api/operations.md#polars_bio.range_operations.coverage), [merge](../api/operations.md#polars_bio.range_operations.merge), [cluster](../api/operations.md#polars_bio.range_operations.cluster), [complement](../api/operations.md#polars_bio.range_operations.complement), [subtract](../api/operations.md#polars_bio.range_operations.subtract), and more — exposed through a DataFrame API and a native parallel engine. Also covers [pileup/depth](../api/operations.md#polars_bio.pileup_operations.depth) computation and flexible [overlap output modes](../api/operations.md#polars_bio.range_operations.overlap).

## [Reading files](reading.md)

Eager (`read_*`), lazy (`scan_*`), and SQL-ready (`register_*`) access to all supported input
formats (BED, VCF, VCF Zarr, BAM, CRAM, FASTQ, FASTA, GFF3, GTF, Pairs, BigWig, BigBed). Prefer
`scan_*` — it enables indexed reads with predicate and projection pushdown. Also covers optional
BAM tags, schema inspection, coordinate-system handling, and the metadata attached to every
DataFrame.

## [Writing files](writing.md)

Eager (`write_*`) and streaming (`sink_*`) output, coordinate-sorted writes, header preservation, BAM/SAM tag writing, and compression.

## [SQL processing](sql.md)

Register datasets as tables and query them with SQL (Apache DataFusion), access registered tables programmatically, and build reusable views.

## [DataFrames support](dataframes.md)

Use polars-bio with file paths, Polars, or Pandas DataFrames — and how the backend/dtype choice affects performance (Arrow-backed Pandas ≈ Polars).

## [Cloud storage](cloud.md)

Stream bioinformatic files directly from S3, GCS, and Azure via Apache OpenDAL, with per-provider configuration.

## [Parallel processing](parallel.md)

One global `target_partitions` setting controls parallelism for both input reads and range operations across CPU cores.

