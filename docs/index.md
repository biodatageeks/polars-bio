# Next-gen Python DataFrame operations for genomics!

![logo](assets/logo-large.png){ align=center style="height:350px;width:350px" }


polars-bio is a :rocket:blazing [fast](performance.md#results-summary-) Python DataFrame library for genomics🧬  built on top of [Apache DataFusion](https://datafusion.apache.org/), [Apache Arrow](https://arrow.apache.org/)
and  [polars](https://pola.rs/).
It is designed to be easy to use, fast and memory efficient with a focus on genomics data.


## Key Features
* optimized for [peformance](performance.md#results-summary-) and large-scale genomics datasets
* popular genomics [operations](features.md#genomic-ranges-operations) with a DataFrame API (both [Pandas](https://pandas.pydata.org/) and [polars](https://pola.rs/))
* [SQL](features.md#sql-powered-data-processing)-powered bioinformatic data querying or manipulation
* native parallel engine powered by Apache DataFusion and [sequila-native](https://github.com/biodatageeks/sequila-native)
* [out-of-core/streaming](features.md#streaming-out-of-core-processing-exeprimental) processing (for data too large to fit into a computer's main memory)  with [Apache DataFusion](https://datafusion.apache.org/) and [polars](https://pola.rs/)
* support for direct *streamed* reading data from cloud storages (e.g. S3, GCS) enabling processing large-scale genomics data without materializing in memory
* zero-copy data exchange with [Apache Arrow](https://arrow.apache.org/)
* bioinformatics file [formats](features.md#file-formats-support) with [noodles](https://github.com/zaeleus/noodles) and [exon](https://github.com/wheretrue/exon)
* pre-built wheel packages for *Linux*, *Windows* and *MacOS* (*arm64* and *x86_64*) available on [PyPI](https://pypi.org/project/polars-bio/#files)

## Single-thread performance 🏃‍
![overlap-single.png](assets/overlap-single.png)

![overlap-single.png](assets/nearest-single.png)

![count-overlaps-single.png](assets/count-overlaps-single.png)

## Parallel performance 🏃‍🏃‍
![overlap-parallel.png](assets/overlap-parallel.png)

![overlap-parallel.png](assets/nearest-parallel.png)

![count-overlaps-parallel.png](assets/count-overlaps-parallel.png)


[//]: # (* support for common genomics file formats &#40;VCF, BAM and FASTQ&#41;)
