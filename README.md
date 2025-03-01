# polars-bio - Next-gen Python DataFrame operations for genomics!
![PyPI - Version](https://img.shields.io/pypi/v/polars-bio)
![GitHub License](https://img.shields.io/github/license/biodatageeks/polars-bio)
![PyPI - Downloads](https://img.shields.io/pypi/dm/polars-bio)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/biodatageeks/polars-bio)

![CI](https://github.com/biodatageeks/polars-bio/actions/workflows/publish_to_pypi.yml/badge.svg?branch=master)
![Docs](https://github.com/biodatageeks/polars-bio/actions/workflows/publish_documentation.yml/badge.svg?branch=master)
![logo](docs/assets/logo-large.png)



[polars-bio](https://pypi.org/project/polars-bio/) is a Python library for genomics built on top of [polars](https://pola.rs/), [Apache Arrow](https://arrow.apache.org/) and [Apache DataFusion](https://datafusion.apache.org/).
It provides a DataFrame API for genomics data and is designed to be blazing fast, memory efficient and easy to use.

## Key Features
* optimized for [peformance](https://biodatageeks.org/polars-bio/performance/) and large-scale genomics datasets
* popular genomics [operations](https://biodatageeks.org/polars-bio/features/#genomic-ranges-operations) with a DataFrame API (both [Pandas](https://pandas.pydata.org/) and [polars](https://pola.rs/))
* [SQL](features.md#sql-powered-data-processing)-powered bioinformatic data querying or manipulation
* native parallel engine powered by Apache DataFusion and [sequila-native](https://github.com/biodatageeks/sequila-native)
* [out-of-core/streaming](features.md#streaming-out-of-core-processing-exeprimental) processing (for data too large to fit into a computer's main memory)  with [Apache DataFusion](https://datafusion.apache.org/) and [polars](https://pola.rs/)
* support for direct *streamed* reading data from cloud storages (e.g. S3, GCS) enabling processing large-scale genomics data without materializing in memory
* zero-copy data exchange with [Apache Arrow](https://arrow.apache.org/)
* bioinformatics file [formats](features.md#file-formats-support) with [noodles](https://github.com/zaeleus/noodles) and [exon](https://github.com/wheretrue/exon)
* pre-built wheel packages for *Linux*, *Windows* and *MacOS* (*arm64* and *x86_64*) available on [PyPI](https://pypi.org/project/polars-bio/#files)

## Single-thread performance 🏃‍
![overlap-single.png](docs/assets/overlap-single.png)

![overlap-single.png](docs/assets/nearest-single.png)

![count-overlaps-single.png](docs/assets/count-overlaps-single.png)

## Parallel performance 🏃‍🏃‍
![overlap-parallel.png](docs/assets/overlap-parallel.png)

![overlap-parallel.png](docs/assets/nearest-parallel.png)

![count-overlaps-parallel.png](docs/assets/count-overlaps-parallel.png)



Read the [documentation](https://biodatageeks.github.io/polars-bio/)