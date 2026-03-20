# Requirements: RNA-seq Feature Counting Benchmark

**Defined:** 2026-03-20
**Core Value:** Prove polars-bio can produce correct gene-level counts with competitive speed and lower memory than featureCounts

## v1 Requirements

Requirements for initial benchmark release. Each maps to roadmap phases.

### Dataset

- [x] **DATA-01**: Download and index ENCODE ENCSR329MHM BAM (HepG2, SE76, ~36M reads, GRCh38)
- [x] **DATA-02**: Download Gencode v49 comprehensive GTF (GRCh38.p14)
- [x] **DATA-03**: Verify chromosome naming consistency between BAM and GTF (chr-prefixed)

### Counting

- [ ] **COUNT-01**: Filter BAM reads to primary alignments only (exclude FLAG 0x4, 0x100, 0x800)
- [ ] **COUNT-02**: Filter GTF to exon features only (feature == "exon")
- [ ] **COUNT-03**: Extract gene_id attribute from GTF exon rows
- [ ] **COUNT-04**: Overlap filtered reads against filtered exons using pb.overlap()
- [ ] **COUNT-05**: Deduplicate read-gene pairs (read spanning multiple exons of same gene counted once)
- [ ] **COUNT-06**: Aggregate to gene-level counts via group_by(gene_id).count()
- [ ] **COUNT-07**: Produce per-gene count table (gene_id + count columns)

### Baseline

- [ ] **BASE-01**: Run featureCounts 2.1.1 with defaults (-s 0 -t exon -g gene_id -T 1) on benchmark dataset
- [ ] **BASE-02**: Run HTSeq-count 2.1.2 with matching settings (--stranded no --minaqual 0) on benchmark dataset
- [ ] **BASE-03**: Produce baseline count matrices for comparison

### Correctness

- [ ] **CORR-01**: Compare polars-bio gene counts against featureCounts counts
- [ ] **CORR-02**: Report Pearson and Spearman correlation (target: r > 0.999)
- [ ] **CORR-03**: Report number of genes with identical counts and genes with differences
- [ ] **CORR-04**: Investigate and document any count differences (expected: ambiguity handling divergence)

### Performance

- [ ] **PERF-01**: Measure wall-clock time for all three tools using hyperfine (5+ runs)
- [ ] **PERF-02**: Measure peak RSS memory for all three tools using /usr/bin/time or equivalent
- [ ] **PERF-03**: Produce comparison table: tool x metric (time, memory, count)

### Notebook

- [ ] **NOTE-01**: Create Jupyter notebook with reproducible benchmark pipeline
- [ ] **NOTE-02**: Include dataset download and preparation steps
- [ ] **NOTE-03**: Include correctness validation visualizations (scatter plot, difference histogram)
- [ ] **NOTE-04**: Include performance comparison charts (bar charts for time and memory)

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Counting Enhancements

- **COUNT-V2-01**: Multi-mapping read exclusion via NH tag filtering
- **COUNT-V2-02**: Ambiguous-read discard (reads overlapping >1 gene excluded from all genes)
- **COUNT-V2-03**: Summary statistics (Assigned, Unassigned_NoFeatures, Unassigned_Ambiguity, Unassigned_MultiMapping)
- **COUNT-V2-04**: Strand-aware counting (-s 1 / -s 2 modes)
- **COUNT-V2-05**: Paired-end fragment counting
- **COUNT-V2-06**: Thread-scaling benchmark (1, 4, 8 cores)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Alignment-free quantification (Salmon/kallisto) | Different paradigm, not interval-based |
| Single-cell barcode-aware counting | Different computational domain |
| Differential expression analysis | Statistical modeling, not counting |
| Isoform-level quantification | Requires transcript-aware overlap, separate initiative |
| Minimum overlap fraction filters | Non-default featureCounts parameters |
| Multi-mapping fractional counting | Breaks integer-count invariant, non-default |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DATA-01 | Phase 1 | Complete |
| DATA-02 | Phase 1 | Complete |
| DATA-03 | Phase 1 | Complete |
| COUNT-01 | Phase 2 | Pending |
| COUNT-02 | Phase 2 | Pending |
| COUNT-03 | Phase 2 | Pending |
| COUNT-04 | Phase 2 | Pending |
| COUNT-05 | Phase 2 | Pending |
| COUNT-06 | Phase 2 | Pending |
| COUNT-07 | Phase 2 | Pending |
| BASE-01 | Phase 3 | Pending |
| BASE-02 | Phase 3 | Pending |
| BASE-03 | Phase 3 | Pending |
| CORR-01 | Phase 4 | Pending |
| CORR-02 | Phase 4 | Pending |
| CORR-03 | Phase 4 | Pending |
| CORR-04 | Phase 4 | Pending |
| PERF-01 | Phase 4 | Pending |
| PERF-02 | Phase 4 | Pending |
| PERF-03 | Phase 4 | Pending |
| NOTE-01 | Phase 5 | Pending |
| NOTE-02 | Phase 5 | Pending |
| NOTE-03 | Phase 5 | Pending |
| NOTE-04 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 24 total
- Mapped to phases: 24
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-20*
*Last updated: 2026-03-20 after roadmap creation*
