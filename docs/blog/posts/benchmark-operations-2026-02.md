---
draft: false
date:
  created: 2026-02-20
categories:
  - performance
  - benchmarks
---

# Interval operations benchmark — update February 2026

## Introduction

Back in [September 2025](benchmark-operations-2025-09.md) we benchmarked three libraries across three operations. A lot has changed since then. In December 2025, pyranges1 published a [preprint](https://www.biorxiv.org/content/10.64898/2025.12.11.693639v1) describing its Rust-powered backend ([ruranges](https://github.com/pyranges/ruranges)) and an expanded set of interval operations. On the [polars-bio](https://github.com/biodatageeks/polars-bio) side, version 0.24.0 ships a fully rewritten range-operations engine built on upstream DataFusion UDTF providers (OverlapProvider, NearestProvider, and the new coverage/cluster/complement/merge/subtract providers from [datafusion-bio-function-ranges](https://github.com/biodatageeks/datafusion-bio-functions)), replacing the earlier sequila-native backend.

<!-- more -->

This rewrite also expanded the operation set from three to eight. In addition to **overlap**, **nearest**, and **count_overlaps**, polars-bio 0.24.0 supports **coverage**, **cluster**, **complement**, **merge**, and **subtract** — covering all the everyday interval manipulation tasks that genomics workflows depend on.

We also added a fourth contender: [Bioframe](https://github.com/open2c/bioframe), a pandas-based genomic interval library widely used in the 3D genomics community. This gives us a broader view of the Python genomic interval landscape.

For comparability with our previous benchmarks, we continue to use the same [AIList](/polars-bio/supplement/#real-dataset) dataset. All benchmark code and raw results are available in the [polars-bio-bench](https://github.com/biodatageeks/polars-bio-bench) repository.

## Setup

### Software versions

| Library | Version |
|---|---|
| polars-bio | 0.24.0 |
| pyranges1 | 1.2.0 |
| GenomicRanges | 0.8.4 |
| bioframe | 0.8.0 |

### Benchmark test cases

**Binary operations** (overlap, nearest, count_overlaps, coverage):

| Dataset pairs | Size | # of overlaps (1-based) |
|---|---|---|
| 1-2 & 2-1 | Small | 54,246 |
| 3-7 & 7-3 | Medium | 4,408,383 |
| 7-8 & 8-7 | Large | 307,184,634 |

**Unary operations** (cluster, complement, merge, subtract):

| Dataset | Size | Name (intervals) |
|---|---|---|
| 1 | Small | fBrain (199K) |
| 2 | Small | exons (439K) |
| 7 | Medium | ex-anno (1,194K) |
| 3 | Medium | chainOrnAna1 (1,957K) |
| 8 | Large | ex-rna (9,945K) |
| 5 | Large | chainXenTro3Link (50,981K) |

### Operations and tool support

| Operation | polars-bio | PyRanges1 | GenomicRanges | Bioframe |
|---|---|---|---|---|
| overlap | yes | yes | yes | yes |
| nearest | yes | yes | yes | yes |
| count_overlaps | yes | yes | yes | yes |
| coverage | yes | -- | yes | yes |
| cluster | yes | yes | -- | yes |
| complement | yes | yes | yes | yes |
| merge | yes | yes | yes | yes |
| subtract | yes | yes | -- | yes |

## Results

### Speedup comparison across all operations

![all_operations_speedup_comparison.png](figures/benchmark-operations-2026-02/all_operations_speedup_comparison.png)
!!! info
    1. Missing bars indicate that the operation is not supported by the library for the given dataset.
    2. Crash bars indicate that the library failed to complete.

Key takeaways:

- **polars-bio** is the fastest library in 7 out of 8 operations on the large dataset (8-7). The sole exception is **nearest**, where **GenomicRanges** holds a 1.63x advantage.
- On small datasets (1-2, 2-1), **GenomicRanges** leads in overlap (1.74x) and count_overlaps, reflecting lower per-call overhead for small inputs.
- **Bioframe** is consistently the slowest library, falling 5-50x behind polars-bio depending on the operation and dataset size.
- For the new operations (coverage, cluster, complement, merge, subtract), **polars-bio** leads across the board with **PyRanges1** a respectable second (0.23-0.61x relative to polars-bio on the large dataset).

**Summary — dataset 8-7 speedup relative to polars-bio (higher is better for polars-bio):**

| Operation | polars-bio | PyRanges1 | GenomicRanges | Bioframe |
|---|---|---|---|---|
| overlap | 1.00x | 0.18x | 0.73x | 0.13x |
| nearest | 1.00x | 0.06x | 1.63x | 0.04x |
| count_overlaps | 1.00x | 0.24x | 0.19x | 0.02x |
| coverage | 1.00x | -- | 0.36x | 0.05x |
| cluster | 1.00x | 0.61x | -- | 0.20x |
| complement | 1.00x | 0.53x | 0.02x | 0.06x |
| merge | 1.00x | 0.51x | 0.02x | 0.12x |
| subtract | 1.00x | 0.23x | -- | 0.05x |


### Thread scalability (dataset 8-7)

![polars_bio_scalability_8_7.png](figures/benchmark-operations-2026-02/polars_bio_scalability_8_7.png)
One of polars-bio's key advantages is transparent multithreaded execution. The table below shows wall-clock times for all eight operations on the large dataset (8-7) as thread count increases:

| Operation | 1 thread | 8 threads | Speedup |
|---|---|---|---|
| overlap | 4.03s | 0.74s | 5.43x |
| nearest | 2.55s | 0.44s | 5.77x |
| count_overlaps | 1.55s | 0.27s | 5.75x |
| coverage | 1.20s | 0.22s | 5.48x |
| subtract | 1.15s | 0.17s | 6.59x |
| merge | 0.29s | 0.05s | 5.34x |
| complement | 0.29s | 0.06s | 4.99x |
| cluster | 0.48s | 0.15s | 3.27x |

Key takeaways:

- Most operations achieve near-linear scaling, with 5-6x speedup at 8 threads.
- **subtract** scales best at 6.59x, while **cluster** shows more modest scaling (3.27x) — expected, as clustering is inherently sequential within each chromosome.
- At 8 threads, **overlap** on 307M result pairs completes in just 0.74 seconds.

## Summary

- **polars-bio** is the fastest single-threaded library for 7 out of 8 operations at scale, with speedups ranging from 1.5x to 25x over alternatives.
- **GenomicRanges** wins the nearest operation and leads on small datasets, making it a solid choice when input sizes are modest.
- polars-bio delivers excellent thread scaling (5-6x at 8 threads), turning already-fast single-threaded times into sub-second performance on large datasets.
- **PyRanges1**, despite its recent [preprint](https://www.biorxiv.org/content/10.64898/2025.12.11.693639v1) claiming "ultrafast" performance, is slower than polars-bio in every operation tested on large datasets (0.06-0.61x). While it is a solid improvement over its predecessor, the "ultrafast" characterization does not hold when compared against polars-bio's [DataFusion](https://datafusion.apache.org/)-based engine.
- **Bioframe** is consistently the slowest across all operations and dataset sizes.
- The expanded eight-operation benchmark confirms that polars-bio's advantage extends beyond overlap and nearest to all standard range operation categories — coverage, cluster, complement, merge, and subtract.

All benchmark code, raw results, and additional figures are available at [polars-bio-bench](https://github.com/biodatageeks/polars-bio-bench).
