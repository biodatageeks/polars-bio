# Next-gen Python DataFrame operations for genomics!

![logo](assets/logo-large.png){ align=center style="height:350px;width:350px" }


polars-bio is a :rocket:blazing [fast](performance.md#results-summary-) Python DataFrame library for genomics🧬  built on top of [Apache DataFusion](https://datafusion.apache.org/), [Apache Arrow](https://arrow.apache.org/)
and  [polars](https://pola.rs/).
It is designed to be easy to use, fast and memory efficient with a focus on genomics data.




## Key Features
* optimized for [performance](performance.md#results-summary-) and memory [efficiency](performance.md#memory-characteristics) for large-scale genomics datasets analyses both when reading input data and performing operations
* popular genomics [operations](features.md#genomic-ranges-operations) with a DataFrame API (both [Pandas](https://pandas.pydata.org/) and [polars](https://pola.rs/))
* [SQL](features.md#sql-powered-data-processing)-powered bioinformatic data querying or manipulation/pre-processing
* native parallel engine powered by Apache DataFusion and [sequila-native](https://github.com/biodatageeks/sequila-native)
* [out-of-core/streaming](features.md#streaming) processing (for data too large to fit into a computer's main memory)  with [Apache DataFusion](https://datafusion.apache.org/) and [polars](https://pola.rs/)
* support for *federated* and *streamed* reading data from [cloud storages](features.md/#cloud-storage) (e.g. S3, GCS) with [Apache OpenDAL](https://github.com/apache/opendal)  enabling processing large-scale genomics data without materializing in memory
* zero-copy data exchange with [Apache Arrow](https://arrow.apache.org/)
* bioinformatics file [formats](features.md#file-formats-support) with [noodles](https://github.com/zaeleus/noodles)
* fast overlap operations with [COITrees: Cache Oblivious Interval Trees](https://github.com/dcjones/coitrees)
* pre-built wheel packages for *Linux*, *Windows* and *MacOS* (*arm64* and *x86_64*) available on [PyPI](https://pypi.org/project/polars-bio/#files)


See [quick start](quickstart.md) for the installation options.

## Citing

If you use **polars-bio** in your work, please cite:

```bibtex
@article {Wiewiorka2025.03.21.644629,
	author = {Wiewiorka, Marek and Khamutou, Pavel and Zbysinski, Marek and Gambin, Tomasz},
	title = {polars-bio - fast, scalable and out-of-core operations on large genomic interval datasets},
	elocation-id = {2025.03.21.644629},
	year = {2025},
	doi = {10.1101/2025.03.21.644629},
	publisher = {Cold Spring Harbor Laboratory},
	URL = {https://www.biorxiv.org/content/early/2025/03/25/2025.03.21.644629},
	eprint = {https://www.biorxiv.org/content/early/2025/03/25/2025.03.21.644629.full.pdf},
	journal = {bioRxiv}
}
```
## Performance benchmarks
### Single-thread 🏃‍
![overlap-single.png](assets/overlap-single.png)

![overlap-single.png](assets/nearest-single.png)

![count-overlaps-single.png](assets/count-overlaps-single.png)

![coverage-single.png](assets/coverage-single.png)

### Parallel 🏃‍🏃‍
![overlap-parallel.png](assets/overlap-parallel.png)

![overlap-parallel.png](assets/nearest-parallel.png)

![count-overlaps-parallel.png](assets/count-overlaps-parallel.png)

![coverage-parallel.png](assets/coverage-parallel.png)


[//]: # (* support for common genomics file formats &#40;VCF, BAM and FASTQ&#41;)
