---
draft: false
date:
  created: 2025-09-05
  updated: 2025-09-05
categories:
  - performance
  - benchmarks

---

# Interval operations benchmark — update September 2025

## Introduction
Benchmarking isn’t a one-and-done exercise—it’s a moving target. As tools evolve, new versions can shift performance profiles in meaningful ways, so keeping results current is just as important as the first round of measurements.

Recently, three novel libraries that have started to gain traction: [pyranges1](https://github.com/pyranges/pyranges_1.x), [GenomicRanges](https://github.com/BiocPy/GenomicRanges) and [polars-bio](https://github.com/biodatageeks/polars-bio)
![star-history-202595.png](figures/benchmark-sep-2025/star-history-202595.png)

shipped major updates:

* [pyranges1](https://github.com/pyranges/pyranges_1.x) adopted a new Rust backend ([ruranges](https://github.com/pyranges/ruranges)),
* [GenomicRanges](https://github.com/BiocPy/GenomicRanges) switched its interval core to a Nested Containment List ([NCLS](https://github.com/pyranges/ncls)) and added multithreaded execution,
* polars-bio migrated to the new Polars streaming engine and added support for new interval data structures. As of version `0.12.0` it supports:
    * [COITrees](https://github.com/dcjones/coitrees)
    * [IITree](https://github.com/rust-bio/rust-bio/blob/master/src/data_structures/interval_tree/array_backed_interval_tree.rs)
    * [AVL-tree](https://github.com/rust-bio/rust-bio/blob/master/src/data_structures/interval_tree/avl_interval_tree.rs)
    * [rust-lapper](https://github.com/sstadick/rust-lapper)
    * [superintervals](https://github.com/kcleal/superintervals/)

Each of these changes has the potential to meaningfully alter performance and memory characteristics for common genomic interval tasks.

In this post, we revisit our benchmarks with those releases in mind. We focus on three everyday operations:

* overlap detection,
* nearest feature queries
* overlap counting.

For comparability, we use the same [AIList](/polars-bio/supplement/#real-dataset) dataset from our previous write-up, so you can see exactly how the new backends and data structures change the picture. Let’s dive in and see what’s faster, what’s leaner, and where the trade-offs now live.

## Setup

### Benchmark test cases

| Dataset pairs | Size   | # of overlaps (1-based) |
|---------------|--------|-------------------------|
| 1-2 & 2-1     | Small  | 54,246                  |
| 7-3 & 3-7     | Medium | 4,408,383               |
| 8-7 & 7-8     | Large  | 307,184,634             |



### Software versions

| Library            | Version    |
|--------------------|------------|
| polars_bio         | 0.13.1     |
| pyranges          | 0.1.14     |
| genomicranges     | 0.7.2      |

## Results

### polars-bio interval data structures performance comparison
![combined_multi_testcase.png](figures/benchmark-sep-2025/combined_multi_testcase.png)

Key takeaways:

- **Superintervals** seems to be the best default. Across all three test cases, it is consistently the fastest or tied for fastest, delivering 1.25–1.44x speedups over the **polars-bio default (COITrees)** and avoiding worst‑case behavior.
- Lapper caveat: performs well on 1‑2 and 8‑7, but collapses on 7‑3 (≈25x slower than default), so it’s risky as a general‑purpose algorithm.
- Intervaltree/Arrayintervaltree: reliable but slower. They trail superintervals by 20–70% depending on the case.


### All operations comparison
![all_operations_walltime_comparison.png](figures/benchmark-sep-2025/all_operations_walltime_comparison.png)

![bench-20250-all_operations_speedup_comparison.png](figures/benchmark-sep-2025/bench-20250-all_operations_speedup_comparison.png)

Key takeaways:

- *Overlap*: **GenomicRanges** wins on small inputs (1‑2, 2‑1) by ~2.1–2.3x, but polars‑bio takes over from medium size onward and dominates on large (7‑8, 8‑7), where PyRanges falls far behind. Interesting case of *7-8* vs *8-7* when swapping inputs can significantly affect performance of GenomicRanges.
- *Nearest*: **polars‑bio** leads decisively at every size; speedups over the others grow with input size (orders of magnitude on large datasets).
- *Count overlaps*: **GenomicRanges** edges out polars‑bio on the smallest inputs, while **polars‑bio** is faster on medium and substantially faster on large inputs.

### All operations parallel execution
![benchmark_comparison_genomicranges_vs_polars_bio.png](figures/benchmark-sep-2025/benchmark_comparison_genomicranges_vs_polars_bio.png){.glightbox}

![benchmark_speedup_comparison_genomicranges_vs_polars_bio.png](figures/benchmark-sep-2025/benchmark_speedup_comparison_genomicranges_vs_polars_bio.png){.glightbox}

Key takeaways:

- Thread scaling: **both** libraries (GenomicRanges and polars-bio) benefit from additional threads, but the absolute gap favors **polars‑bio** for medium/large datasets across overlap, nearest, and count overlaps.
- Small overlaps: **GenomicRanges** remains >2x faster at 1–8 threads; on medium/large pairs its relative speed drops below 1x.
- Nearest: **polars‑bio** stays on the 1x reference line; **GenomicRanges** is typically 10–100x slower (log scale) even with more threads.
- Count overlaps: small inputs slightly favor **GenomicRanges**; for larger inputs **polars‑bio** maintains 2–10x advantage with stable scaling.

### End to-end data proecesing

Here we compare end-to-end performance including data loading, overlap operation, and saving results to CSV.

!!! info
     1. `POLARS_MAX_THREADS=1` was set to ensure fair comparison with single-threaded PyRanges.
     2. Since GenomicRanges supports Polars DataFrames as input and output, we used them instead of Pandas to again ensure fair comparison with polars-bio.
     3. GenomicRanges [find_overlaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.find_overlaps) method returns hits-only table (indices of genomic intervals instead of genomic coordinates), we also benchmarked an extended version with additional lookup of intervals (`full rows`, [code](https://github.com/biodatageeks/polars-bio-bench/blob/master/src/utils.py#L99)) for fair comparison.

![combined_benchmark_visualization.png](figures/benchmark-sep-2025/combined_benchmark_visualization.png){.glightbox}

Key takeaways:

- Wall time: **GenomicRanges (hits‑only)** is the fastest end‑to‑end here (~1.16x vs polars_bio) by avoiding full materialization of genomic intervals (unlike PyRanges and polars-bio that return pairs of genomic interval coordinates for each overlap); **PyRanges** is far slower; **GenomicRanges** (full rows, so with the output comparable with PyRanges and polars-bio) is much slower.
- Memory: **polars-bio (streaming)** minimizes peak RAM (~0.7 GB) while keeping speed comparable to **polars-bio**. **GenomicRanges** (full rows) peaks at ~40 GB; hits‑only sits in the middle (~8.2 GB) as it only returns DataFrame with pairs of indices not full genomic coordinates.

## Summary

For small and medium datasets, all tools perform well; at large scale, **polars-bio** excels with better scalability and memory efficiency, achieving an ultra‑low footprint in streaming mode.
