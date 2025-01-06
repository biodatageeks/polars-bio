# Next-gen Python DataFrame operations for genomics!

![logo](assets/logo-large.png){ align=center style="height:350px;width:350px" }


polars-bio is a :rocket:blazing [fast](performance.md#results-summary-) Python DataFrame library for genomics🧬  built on top of [Apache DataFusion](https://datafusion.apache.org/), [Apache Arrow](https://arrow.apache.org/)
and  [polars](https://pola.rs/).
It is designed to be easy to use, fast and memory efficient with a focus on genomics data.

## Key Features
* optimized for [peformance](#performance) and large-scale genomics datasets
* popular genomics [operations](features.md#genomic-ranges-operations) with a DataFrame API (both [Pandas](https://pandas.pydata.org/) and [polars](https://pola.rs/))
* native parallel engine powered by Apache DataFusion and [sequila-native](github.com/sequila/sequila-native)
* out-of-core processing ([streaming](https://docs.pola.rs/user-guide/concepts/_streaming/)) with [polars](https://pola.rs/)
* zero-copy data exchange with Apache Arrow
* pre-built wheel packages for Linux, Windows and MacOS (arm64 and x86_64) available on [PyPI](https://pypi.org/project/polars-bio/#files)

[//]: # (* support for common genomics file formats &#40;VCF, BAM and FASTQ&#41;)
