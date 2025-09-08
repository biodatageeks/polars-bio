---
draft: false
date:
  created: 2025-09-08
  updated: 2025-09-08
categories:
  - performance
  - benchmarks
  - file formats

---

# GFF File Reading Performance Enhancements in polars-bio 0.15.0

We're excited to announce significant performance improvements to GFF file reading in polars-bio 0.15.0. This release introduces two major optimizations that dramatically improve both speed and memory efficiency when working with GFF files:

## Key Enhancements

**Projection Pushdown**: Only the columns you need are read from disk, reducing I/O overhead and memory usage. This is particularly beneficial when working with wide GFF files that contain many optional attributes.

**Predicate Pushdown**: Row filtering is applied during the file reading process, eliminating the need to load irrelevant data into memory. This allows for lightning-fast queries on large GFF datasets.

**Fully Streamed Parallel Reads**: BGZF-compressed files can now be read in parallel with true streaming, enabling out-of-core processing of massive genomic datasets without memory constraints.

## Benchmark Methodology

To evaluate these improvements, we conducted comprehensive benchmarks comparing three popular data processing libraries:

- **pandas**: The traditional Python data analysis library
- **polars**: High-performance DataFrame library with lazy evaluation
- **polars-bio**: Our specialized genomic data processing library built on Polars and Apache DataFusion

All benchmarks were performed on a large GFF file (~7.7 million records) with both read-only and filtered query scenarios to demonstrate real-world performance gains.

## Results

## Single-threaded performance




![general_performance.png](figures/gff-read-optimization-2025-09/general_performance.png){.glightbox}

## Memory usage
![memory_comparison.png](figures/gff-read-optimization-2025-09/memory_comparison.png){.glightbox}

## Thread scalability
![thread_scalability.png](figures/gff-read-optimization-2025-09/thread_scalability.png){.glightbox}