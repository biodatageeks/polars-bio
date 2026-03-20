# RNA-seq Feature Counting Benchmark

## What This Is

A benchmark proving polars-bio can match featureCounts accuracy for gene-level read counting while offering competitive speed and significantly lower memory usage. Targets bulk RNA-seq on human-scale data (50-100M reads, Gencode GTF ~60k genes). Results delivered as a Jupyter notebook with reproducible comparisons against featureCounts and HTSeq-count.

## Core Value

Demonstrate that polars-bio's existing interval operations (count_overlaps, overlap) combined with GTF/BAM I/O and streaming evaluation can produce correct gene-level counts faster and with less memory than established RNA-seq quantification tools.

## Requirements

### Validated

- ✓ BAM/SAM/CRAM reading with predicate/projection pushdown — existing
- ✓ GTF/GFF reading with attribute field extraction (gene_id, transcript_id) — existing
- ✓ count_overlaps() interval operation with multiple algorithms (CoITrees, IntervalTree) — existing
- ✓ overlap() with configurable overlap semantics — existing
- ✓ Streaming/lazy evaluation via LazyFrame — existing
- ✓ SQL registration and query execution via DataFusion — existing
- ✓ Multi-format I/O with Arrow C FFI streaming — existing
- ✓ depth()/pileup computation from BAM files — existing

### Active

- [ ] Gene-level read counting from BAM + GTF (exon-level overlap → gene_id groupby)
- [ ] Benchmark harness comparing polars-bio vs featureCounts vs HTSeq-count
- [ ] Correctness validation (count comparison against featureCounts default mode)
- [ ] Performance metrics collection (wall time, peak RSS, per-thread scaling)
- [ ] Human-scale test dataset preparation (Gencode GTF + 50-100M read BAM)
- [ ] Jupyter notebook with reproducible benchmark results and visualizations
- [ ] Ambiguity handling for multi-gene overlapping reads (featureCounts default: discard)

### Out of Scope

- Strand-aware counting — follow-up after unstranded benchmark proves viability
- Single-cell RNA-seq (cell barcode-aware counting) — different problem domain
- Alignment-free quantification (Salmon/kallisto comparison) — different paradigm
- Differential expression analysis (DESeq2/edgeR) — statistical modeling, not interval ops
- FASTQ QC or alignment QC — separate initiatives
- Full featureCounts feature parity (paired-end, chimeric reads, multi-mapping fraction) — benchmark scope only

## Context

polars-bio already has all the building blocks for feature counting:
- `scan_bam()` reads alignments with predicate pushdown on indexed BAMs
- `read_gtf()` extracts gene_id, transcript_id, feature type from annotations
- `count_overlaps()` counts interval intersections using CoITrees (O(n log n) build, O(log n + k) query)
- Streaming evaluation means memory usage scales with partition size, not file size

The key gap is that `count_overlaps()` currently has no strand awareness. For unstranded RNA-seq this doesn't matter — reads count regardless of strand orientation.

featureCounts (Subread package) is the dominant tool: C implementation, multi-threaded, ~10 years of optimization. HTSeq-count is the Python reference implementation, slower but widely used for validation. Matching featureCounts' default unstranded single-end counting mode is the correctness target.

## Constraints

- **Data availability**: Need publicly available human RNA-seq BAM + Gencode GTF for reproducibility
- **Correctness**: Must match featureCounts counts within rounding (identical for unstranded mode)
- **Existing API**: Build on current polars-bio primitives, don't rewrite core interval operations
- **Notebook format**: Results must be reproducible in a Jupyter notebook

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Unstranded first | Simpler correctness validation, strand-awareness is a separate code change | — Pending |
| featureCounts + HTSeq as targets | featureCounts = speed benchmark, HTSeq = correctness reference | — Pending |
| Human-scale data | Realistic production workload, shows scaling advantage | — Pending |
| Notebook-first | Rapid iteration, visual results, publishable format | — Pending |
| Match featureCounts defaults | Most common usage mode, clearest correctness target | — Pending |

---
*Last updated: 2026-03-20 after initialization*
