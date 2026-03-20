# Roadmap: RNA-seq Feature Counting Benchmark

## Overview

Starting from polars-bio's existing interval operation primitives, this benchmark proves that `pb.overlap()` + dual `group_by()` produces gene-level counts that match featureCounts accuracy while delivering competitive wall time and lower peak RSS. The journey runs: validate the dataset → implement the counting pipeline → collect reference runs from featureCounts and HTSeq-count → validate correctness then measure performance → present everything in a reproducible Jupyter notebook.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Dataset Preparation** - Download, validate, and index the benchmark dataset and install reference tools
- [ ] **Phase 2: Core Counting Implementation** - Implement `count_features()` pipeline in polars-bio with unit tests
- [ ] **Phase 3: Baseline Tool Runs** - Run featureCounts and HTSeq-count on the benchmark dataset to produce reference count matrices
- [ ] **Phase 4: Validation and Benchmarking** - Validate correctness against baselines and collect performance metrics
- [ ] **Phase 5: Benchmark Notebook** - Assemble reproducible Jupyter notebook with all results and visualizations

## Phase Details

### Phase 1: Dataset Preparation
**Goal**: The benchmark dataset and reference tools are ready, validated, and consistent — no downstream phase will fail due to data or environment issues
**Depends on**: Nothing (first phase)
**Requirements**: DATA-01, DATA-02, DATA-03
**Success Criteria** (what must be TRUE):
  1. ENCODE ENCSR329MHM BAM file is downloaded, sorted, and BAI-indexed; `samtools flagstat` reports ~36M total reads
  2. GENCODE v49 comprehensive GTF is downloaded and readable by `pb.scan_gtf()`
  3. Chromosome name prefixes match between BAM and GTF (both use `chr`-prefixed contigs, confirmed by spot-check)
  4. featureCounts 2.1.1 and HTSeq-count 2.1.2 are installed in the benchmark conda environment and return expected version strings
**Plans**: 2 plans
Plans:
- [ ] 01-01-PLAN.md — Create benchmark notebook with data download, validation, and tool install cells
- [ ] 01-02-PLAN.md — User runs notebook to download data and verify environment (checkpoint)

### Phase 2: Core Counting Implementation
**Goal**: A working `count_features(bam_path, gtf_path)` function produces a gene-level count table using polars-bio primitives, with unit tests confirming correctness on small synthetic data
**Depends on**: Phase 1
**Requirements**: COUNT-01, COUNT-02, COUNT-03, COUNT-04, COUNT-05, COUNT-06, COUNT-07
**Success Criteria** (what must be TRUE):
  1. `pb.count_features(bam, gtf)` returns a LazyFrame with `gene_id` and `count` columns for the ENCODE HepG2 dataset (non-empty result, total assigned reads > 0)
  2. A spanning-read unit test: a single read overlapping two exons of the same gene produces `count = 1`, not `count = 2`
  3. An ambiguous-read unit test: a read overlapping exons from two different genes is excluded from both gene counts
  4. Primary-alignment filtering is applied — secondary (FLAG 0x100) and supplementary (FLAG 0x800) reads do not appear in the overlap input
**Plans**: TBD

### Phase 3: Baseline Tool Runs
**Goal**: featureCounts and HTSeq-count have been run on the same benchmark dataset with documented parameters, producing reference count matrices saved to disk
**Depends on**: Phase 1
**Requirements**: BASE-01, BASE-02, BASE-03
**Success Criteria** (what must be TRUE):
  1. featureCounts 2.1.1 output exists at a known path; the `.summary` file reports a non-zero `Assigned` count
  2. HTSeq-count 2.1.2 output exists at a known path with gene counts readable as a two-column TSV
  3. Both tools were invoked with unstranded settings (`-s 0` / `--stranded=no`) and the exact command lines are documented
**Plans**: TBD

### Phase 4: Validation and Benchmarking
**Goal**: polars-bio counts are confirmed correct against featureCounts (r > 0.999), and wall-time and peak-RSS measurements exist for all three tools
**Depends on**: Phase 2, Phase 3
**Requirements**: CORR-01, CORR-02, CORR-03, CORR-04, PERF-01, PERF-02, PERF-03
**Success Criteria** (what must be TRUE):
  1. Pearson r and Spearman r between polars-bio and featureCounts gene counts are both > 0.999
  2. The number of genes with non-zero count difference vs featureCounts is documented and differences are explained (expected source: ambiguity edge cases or split-read handling)
  3. hyperfine JSON output exists with wall-clock timing (5+ runs) for all three tools
  4. Peak RSS measurements via `/usr/bin/time` (or `gtime`) exist for all three tools and the comparison table is populated
**Plans**: TBD

### Phase 5: Benchmark Notebook
**Goal**: A single Jupyter notebook runs end-to-end from dataset download to final charts, is reproducible by anyone with the conda environment, and clearly communicates polars-bio's accuracy and performance story
**Depends on**: Phase 4
**Requirements**: NOTE-01, NOTE-02, NOTE-03, NOTE-04
**Success Criteria** (what must be TRUE):
  1. `notebooks/feature_counting_benchmark.ipynb` executes from top to bottom with `Restart & Run All` without errors
  2. A chromosome naming validation cell prints contig prefixes from both BAM and GTF before any counting cell runs
  3. A correctness section shows a log-scale scatter plot of polars-bio vs featureCounts counts and a difference histogram
  4. A performance section shows bar charts comparing wall time and peak RSS across all three tools
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5
(Phase 3 depends only on Phase 1 and may be parallelized with Phase 2 if desired)

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Dataset Preparation | 0/2 | Planning complete | - |
| 2. Core Counting Implementation | 0/TBD | Not started | - |
| 3. Baseline Tool Runs | 0/TBD | Not started | - |
| 4. Validation and Benchmarking | 0/TBD | Not started | - |
| 5. Benchmark Notebook | 0/TBD | Not started | - |
