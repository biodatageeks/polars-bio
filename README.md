# polars-bio - Next-gen Python DataFrame operations for genomics!
![PyPI - Version](https://img.shields.io/pypi/v/polars-bio)
![GitHub License](https://img.shields.io/github/license/biodatageeks/polars-bio)
![PyPI - Downloads](https://img.shields.io/pypi/dm/polars-bio)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/biodatageeks/polars-bio)
[![](https://dcbadge.limes.pink/api/server/https://discord.gg/bpxQ4Yxhk5?style=flat)](https://discord.gg/bpxQ4Yxhk5)

![CI](https://github.com/biodatageeks/polars-bio/actions/workflows/publish_to_pypi.yml/badge.svg?branch=master)
![Docs](https://github.com/biodatageeks/polars-bio/actions/workflows/publish_documentation.yml/badge.svg?branch=master)
![logo](docs/assets/logo-large.png)




[polars-bio](https://pypi.org/project/polars-bio/) is a Python library for genomics built on top of [polars](https://pola.rs/), [Apache Arrow](https://arrow.apache.org/) and [Apache DataFusion](https://datafusion.apache.org/).
It provides a DataFrame API for genomics data and is designed to be blazing fast, memory efficient and easy to use.


![img.png](docs/assets/ashg-2025.png/img.png)

## Key Features
* optimized for [performance](https://biodatageeks.org/polars-bio/performance/) and memory [efficiency](https://biodatageeks.org/polars-bio/performance/#memory-characteristics) for large-scale genomics datasets analyses both when reading input data and performing operations
* popular genomics [operations](https://biodatageeks.org/polars-bio/features/#genomic-ranges-operations) with a DataFrame API (both [Pandas](https://pandas.pydata.org/) and [polars](https://pola.rs/))
* [SQL](https://biodatageeks.org/polars-bio/features/#sql-powered-data-processing)-powered bioinformatic data querying or manipulation
* native parallel engine powered by Apache DataFusion and [sequila-native](https://github.com/biodatageeks/sequila-native)
* [out-of-core/streaming](https://biodatageeks.org/polars-bio/features/#streaming) processing (for data too large to fit into a computer's main memory)  with [Apache DataFusion](https://datafusion.apache.org/) and [polars](https://pola.rs/)
* support for *federated* and *streamed* reading data from [cloud storages](https://biodatageeks.org/polars-bio/features/#cloud-storage) (e.g. S3, GCS) with [Apache OpenDAL](https://github.com/apache/opendal) enabling processing large-scale genomics data without materializing in memory
* zero-copy data exchange with [Apache Arrow](https://arrow.apache.org/)
* bioinformatics file [formats](https://biodatageeks.org/polars-bio/features/#file-formats-support) with [noodles](https://github.com/zaeleus/noodles)
* fast overlap operations with [COITrees: Cache Oblivious Interval Trees](https://github.com/dcjones/coitrees)
* pre-built wheel packages for *Linux*, *Windows* and *MacOS* (*arm64* and *x86_64*) available on [PyPI](https://pypi.org/project/polars-bio/#files)

## Performance benchmarks
![summary-results.png](docs/assets/summary-results.png)

For developers: See [`benchmarks/README_BENCHMARKS.md`](benchmarks/README_BENCHMARKS.md) for information about running performance benchmarks via GitHub Actions.


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

Read the [documentation](https://biodatageeks.github.io/polars-bio/)