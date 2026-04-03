# Feature Research

**Domain:** RNA-seq feature counting benchmark — polars-bio vs featureCounts vs HTSeq-count
**Researched:** 2026-03-20
**Confidence:** HIGH (featureCounts/HTSeq-count docs verified; polars-bio from source)

---

## What "Correctness" Means Here

A benchmark is valid when polars-bio produces counts that are **identical** (not merely correlated) to featureCounts defaults for the same input. The counting algorithm must reproduce the same read-assignment decisions, not just similar aggregate totals. Differences larger than a handful of reads indicate a semantic mismatch, not noise.

The target is **featureCounts default mode**: unstranded, single-end, exon-level features aggregated to gene-level meta-features, 1 bp minimum overlap, no multi-mapping, MAPQ >= 0, discard reads overlapping more than one gene.

HTSeq-count is included as a secondary cross-check because it uses a different algorithm and its differences from featureCounts are well-characterized — understanding those differences validates that polars-bio's approach is sound, not accidentally matching a bug.

---

## Feature Landscape

### Table Stakes (Must Match for Valid Benchmark)

Features that must be implemented correctly or the benchmark has no validity.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Exon-level interval overlap with gene grouping | Core of gene counting: overlap reads against exon features, then group by gene_id to get per-gene counts | MEDIUM | polars-bio `count_overlaps` + GTF exon filter + groupby already plausible; need to verify 1-based GTF vs 0-based BAM coordinate reconciliation |
| 1 bp minimum overlap threshold | featureCounts default: any base overlapping an exon counts. Lower threshold = more counted reads | LOW | polars-bio `overlap()` uses FilterOp.Strict (0-based) or FilterOp.Weak (1-based); need to confirm behavior matches featureCounts for junction-spanning reads |
| Unstranded counting (strand-agnostic) | featureCounts `-s 0` is the default; strand is ignored during overlap resolution | LOW | polars-bio `count_overlaps` has no strand awareness currently — this is already the correct behavior for unstranded mode |
| Primary-alignment-only filtering | featureCounts excludes secondary (FLAG 0x100) and supplementary (FLAG 0x800) alignments by default; unmapped (FLAG 0x4) excluded | LOW | polars-bio `scan_bam()` must filter these SAM flags; verify predicate pushdown handles FLAG filtering |
| Multi-mapping read exclusion | featureCounts excludes multi-mappers (NH > 1) by default unless `-M` is passed | MEDIUM | polars-bio must filter on NH tag; need to verify NH tag is exposed in BAM scan schema |
| Ambiguous-read discard (multi-gene overlap) | Reads overlapping more than one gene are discarded by featureCounts by default (no `-O` flag) | MEDIUM | This is the hardest semantic to reproduce: count_overlaps returns a count but does not identify _which_ reads overlap >1 gene. The implementation must detect ambiguous reads and exclude them from all gene counts |
| Exon features only from GTF | featureCounts uses `-t exon` by default; UTRs, introns, gene-level features excluded from overlap query | LOW | `read_gtf()` can filter `feature == "exon"`; straightforward |
| gene_id as meta-feature grouping key | featureCounts `-g gene_id` groups exon-level hits by gene_id; single read counted once per gene even if it spans multiple exons of that gene | LOW | Groupby on gene_id after overlap is straightforward; edge case is read spanning exons of same gene — must count once |
| Output: per-gene count table | TSV with gene_id and integer counts, one row per gene from GTF | LOW | Standard groupby+count output |
| Output: summary statistics | Count of Assigned, Unassigned_NoFeatures, Unassigned_Ambiguity, Unassigned_MultiMapping | MEDIUM | Required to validate that the same reads are being counted/excluded |

### Differentiators (Where polars-bio Can Win)

Features not required for correctness but where polars-bio has structural advantages.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Streaming memory usage | polars-bio processes data in partitions; featureCounts loads index structures; HTSeq-count is fully in-memory. At 100M reads, polars-bio should use <1/10th the RAM | LOW (already built) | Key benchmark metric: peak RSS. polars-bio's LazyFrame + streaming evaluation is the architectural basis |
| Parallel interval queries | polars-bio uses DataFusion's parallel execution; featureCounts is multi-threaded but single-process C; both should scale with cores but polars-bio's overhead is lower for I/O-bound workloads | LOW (already built) | Measure wall time at 1, 4, 8 threads |
| Python-native API | featureCounts requires subprocess or Rsubread; polars-bio integrates directly into a Polars/pandas workflow without shell invocations | LOW (already built) | Notebook demo value |
| Lazy evaluation / composability | The counting pipeline can be composed with downstream filters (e.g., filter to expressed genes) without materializing intermediate results | LOW (already built) | Differentiates from featureCounts black-box approach |
| Arrow output | Results are Arrow-native LazyFrames; direct integration with DuckDB, pandas, R via Arrow C FFI | LOW (already built) | featureCounts outputs TSV requiring re-parse |

### Anti-Features (Deliberately Out of Scope for Benchmark)

Features that look like they belong but would invalidate the benchmark scope or cause scope creep.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Strand-aware counting | Most production RNA-seq libraries are stranded; users will ask for it | Requires strand metadata in BAM scan + strand-aware overlap resolution; adds a code change orthogonal to proving the core benchmark | Implement after unstranded benchmark proves viability; PROJECT.md explicitly defers this |
| Paired-end fragment counting | featureCounts `-p` counts fragments not reads; more biologically correct | Requires grouping read pairs by QNAME and resolving fragment extent from paired reads; doubles implementation complexity and makes correctness comparison harder | Benchmark singles first; paired-end is a follow-up milestone |
| Multi-mapping fractional counting | featureCounts `-M --fraction` distributes multi-mapper counts proportionally | Fractional counts break the integer-count invariant; requires tracking NH tag and dividing; significantly complicates the counting pipeline | Discard multi-mappers (featureCounts default); document the -M behavior as a future flag |
| Overlap count `-O` mode | featureCounts `-O` counts reads for all overlapping genes | Changes the definition of ambiguous; benchmark targets the default (no `-O`) | Document `-O` as a non-default parameter; can be added as a flag later |
| Minimum overlap fraction | `--fracOverlap` and `--fracOverlapFeature` filter reads by what % of the read overlaps the feature | These are non-default; adding them now creates more parameters to validate and more ways to diverge from featureCounts defaults | Implement after default-mode correctness is validated |
| GTF/GFF filtering beyond feature type | Gene biotype filters (protein_coding only), transcript support level filters | Production use case but not what featureCounts does by default; would cause systematic divergence from the benchmark target | Document as user-level filter step applied before passing GTF to the counting function |
| Isoform-level quantification | Users often want transcript-level counts for DEXSeq or dexSEQ | Entirely different problem: requires transcript-aware overlap, exon-bin construction; featureCounts has `-f` flag for feature-level but this is not the benchmark target | Separate initiative; use Salmon/kallisto for isoforms |
| Single-cell barcode-aware counting | STARsolo/Alevin pattern; cell × gene count matrix | Different computational domain; requires cell barcode tag extraction and grouping | Separate initiative |

---

## Feature Dependencies

```
[Exon-level overlap query]
    └──requires──> [GTF exon filter: feature == "exon"]
    └──requires──> [Coordinate system alignment: 1-based GTF vs 0-based BAM]
    └──requires──> [Primary/secondary SAM flag filtering]
    └──requires──> [Multi-mapping NH tag filtering]

[Gene-level count aggregation]
    └──requires──> [Exon-level overlap query]
    └──requires──> [Ambiguous read detection and discard]

[Ambiguous read detection]
    └──requires──> [Exon-level overlap query returning read → gene mapping]
    └──requires──> [Read × gene join to identify reads hitting >1 gene]

[Summary statistics output]
    └──requires──> [Gene-level count aggregation]
    └──requires──> [Ambiguous read detection (to count Unassigned_Ambiguity)]
    └──requires──> [Multi-mapping filter (to count Unassigned_MultiMapping)]

[Benchmark harness]
    └──requires──> [Gene-level count aggregation (polars-bio path)]
    └──requires──> [featureCounts invocation + output parsing]
    └──requires──> [HTSeq-count invocation + output parsing]
    └──requires──> [Count correlation and diff computation]
```

### Dependency Notes

- **Ambiguous read detection requires a read-level join**: `count_overlaps` returns per-interval counts but does not expose which reads map to which genes. To detect ambiguous reads, the implementation must use `overlap()` (not `count_overlaps()`) to get a read × gene pair table, then identify reads with more than one distinct gene_id, discard those reads, and then count the remaining unique assignments per gene. This is the critical design decision.

- **Coordinate system must be reconciled before overlap**: GTF uses 1-based closed intervals; BAM alignment positions are 0-based. polars-bio's `count_overlaps` auto-detects this from DataFrame metadata. The GTF and BAM DataFrames must both carry the correct `coordinate_system_zero_based` attribute or be converted to a common system before the overlap query.

- **SAM flag filtering precedes overlap**: Secondary and supplementary alignments must be excluded before the interval query, not after. Filtering after would require materializing the full overlap join unnecessarily.

- **NH tag filtering precedes overlap**: Multi-mapping reads (NH > 1) must be excluded before the overlap query for the same reason.

---

## Critical Algorithm Detail: featureCounts vs HTSeq-count Ambiguity Resolution

This is the most important behavioral difference to understand for implementing a valid benchmark.

### featureCounts Default (no `-O`)

1. For each read, find all exons it overlaps (minimum 1 bp).
2. Collect the set of distinct gene_ids for those exons.
3. If exactly one gene_id: assign read to that gene. Count += 1 for that gene.
4. If more than one gene_id: read is **Unassigned_Ambiguity**. Discard.
5. Special case for paired-end (out of benchmark scope): if one mate overlaps gene A and the other overlaps gene A + gene B, featureCounts assigns to gene A (more overlap). HTSeq-count would discard as ambiguous. This is the main source of difference between the tools.

### HTSeq-count Default (union mode, --nonunique none)

1. For each position in the read, find all features overlapping that position.
2. Take the union of all feature sets across all read positions.
3. If union has exactly one gene: count for that gene.
4. If union is empty: `__no_feature`.
5. If union has more than one gene: `__ambiguous`. Discard.

### polars-bio Implementation Target

Match featureCounts default. The difference from HTSeq-count is that featureCounts uses the read as the atomic unit (does the read overlap this gene at all?), while HTSeq-count uses per-position feature sets. For single-end unstranded counting without paired-end special cases, the algorithms produce very similar results. The ambiguous rate is typically <0.5% of total reads, so matching featureCounts exactly on this category is required for the benchmark to show identical rather than merely correlated counts.

---

## featureCounts Default Parameter Reference

For implementation validation — these are the exact defaults to match:

| Parameter | Default | polars-bio Implementation |
|-----------|---------|--------------------------|
| `-s` strandedness | 0 (unstranded) | No strand filter in `count_overlaps` — already correct |
| `-t` feature type | `exon` | Filter GTF: `pl.col("feature") == "exon"` |
| `-g` ID attribute | `gene_id` | Group by `gene_id` attribute from GTF |
| `--minOverlap` | 1 bp | polars-bio `overlap()` with 1-based FilterOp.Weak — need to verify |
| `--fracOverlap` | 0.0 (disabled) | Not needed for default benchmark |
| `-Q` MAPQ threshold | 0 (no filtering) | Do not apply MAPQ filter for default; note HTSeq default is MAPQ >= 10 |
| `-M` multi-mapping | off (exclude NH>1) | Filter BAM: `pl.col("NH") <= 1` before overlap |
| `--primary` | off (but secondary/supplementary excluded by default) | Filter BAM: FLAG & 0x100 == 0, FLAG & 0x800 == 0 |
| `-O` allow multi-overlap | off (discard multi-gene reads) | Read-level join to detect and discard ambiguous reads |
| Unmapped read handling | excluded (FLAG 0x4) | Filter BAM: FLAG & 0x4 == 0 |

### HTSeq-count Default Differences (for cross-validation awareness)

| Parameter | featureCounts Default | HTSeq-count Default | Impact |
|-----------|-----------------------|---------------------|--------|
| MAPQ threshold | 0 | 10 | HTSeq will discard more reads; use `--min-quality 0` in HTSeq for fair comparison |
| Input sort | not required (builds index) | requires name-sort or coordinate-sort | HTSeq is slower on coordinate-sorted BAM without index |
| Strandedness | unstranded | **stranded** (yes) | Critical: must pass `--stranded no` to HTSeq for valid comparison |
| Ambiguity resolution | read-level (more assigned) | position-union (fewer assigned) | <0.5% difference expected; HTSeq counts will be slightly lower |

---

## MVP Definition

### Launch With (v1 — valid benchmark)

- [x] GTF reading with `feature == "exon"` filter and `gene_id` extraction — already in `read_gtf()`
- [x] BAM reading with predicate/projection pushdown — already in `scan_bam()`
- [ ] Pre-overlap BAM filtering: unmapped (FLAG 0x4), secondary (FLAG 0x100), supplementary (FLAG 0x800), multi-mappers (NH > 1)
- [ ] Coordinate system reconciliation: GTF 1-based and BAM 0-based into common frame before `overlap()`
- [ ] Read × gene pair table via `overlap()` (not `count_overlaps()`) to enable ambiguity detection
- [ ] Ambiguous read discard: reads with >1 distinct gene_id in their overlap result removed from all genes
- [ ] Gene-level count: after ambiguity removal, count reads per gene_id
- [ ] Summary statistics: totals for Assigned, Unassigned_NoFeature, Unassigned_Ambiguity, Unassigned_MultiMapping
- [ ] Benchmark harness: featureCounts CLI invocation + output parsing + count comparison
- [ ] Benchmark harness: HTSeq-count CLI invocation (with `--stranded no`, `--min-quality 0`) + output parsing
- [ ] Correctness metric: per-gene Pearson r, RMSE, count of genes with |delta| > 0
- [ ] Performance metrics: wall time, peak RSS at 1/4/8 threads
- [ ] Human-scale dataset: publicly available ENCODE or SRA BAM (50-100M reads) + Gencode GTF

### Add After Validation (v1.x)

- [ ] MAPQ threshold parameter (to match HTSeq-count default for comparison parity)
- [ ] `-O` multi-overlap mode (count reads for all overlapping genes)
- [ ] Fractional overlap parameters (minOverlap, fracOverlap)
- [ ] Convenience wrapper: `pb.count_features(bam, gtf)` that packages the full pipeline

### Future Consideration (v2+)

- [ ] Strand-aware counting (`-s 1`, `-s 2`) — requires strand column in overlap query
- [ ] Paired-end fragment counting — requires QNAME grouping and fragment extent computation
- [ ] Multi-mapping fractional counting (`-M --fraction`) — fractional counts
- [ ] Isoform-level counting — separate algorithm, not an extension of gene-level

---

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Pre-overlap BAM flag/NH filtering | HIGH (required for correctness) | LOW | P1 |
| Coordinate system reconciliation | HIGH (required for correctness) | LOW-MEDIUM | P1 |
| Read × gene overlap join | HIGH (foundation of counting) | LOW | P1 |
| Ambiguous read discard | HIGH (must match featureCounts default) | MEDIUM | P1 |
| Gene-level count aggregation | HIGH (the output) | LOW | P1 |
| Summary statistics | HIGH (validates correctness) | LOW | P1 |
| Benchmark harness (featureCounts) | HIGH (the deliverable) | LOW | P1 |
| Benchmark harness (HTSeq-count) | MEDIUM (secondary validation) | LOW | P1 |
| Human-scale dataset acquisition | HIGH (makes benchmark realistic) | LOW (download task) | P1 |
| Correctness metrics (Pearson, RMSE) | HIGH (the proof) | LOW | P1 |
| Performance metrics (time, RSS) | HIGH (the value story) | LOW | P1 |
| MAPQ threshold parameter | LOW (non-default) | LOW | P2 |
| Multi-overlap `-O` mode | LOW (non-default) | MEDIUM | P2 |
| `pb.count_features()` convenience API | MEDIUM (usability) | LOW | P2 |
| Strand-aware counting | MEDIUM (production need) | HIGH | P3 |
| Paired-end fragment counting | HIGH (production need) | HIGH | P3 |

**Priority key:**
- P1: Must have for benchmark to be valid and publishable
- P2: Should have once core counting is correct
- P3: Future milestone; out of scope for this benchmark

---

## Competitor Feature Analysis

| Feature | featureCounts | HTSeq-count | polars-bio target |
|---------|--------------|-------------|-------------------|
| Core algorithm | C, multi-threaded, index-based | Python, SAM iteration | Rust/DataFusion, parallel interval query |
| Strandedness support | Yes (0/1/2) | Yes (yes/no/reverse) | No (unstranded only for benchmark) |
| Overlap mode | At-least-1-bp, any-exon-of-gene | Per-position union | At-least-1-bp via polars-bio `overlap()` |
| Ambiguity resolution | Read-level: discard multi-gene | Position-union: discard multi-gene | Read-level: match featureCounts |
| Multi-mapping | Exclude by default (-M to include) | Exclude by default | Exclude (NH tag filter) |
| MAPQ filter | 0 default | 10 default | 0 default (match featureCounts) |
| Paired-end | Yes (-p flag) | Yes | Not in scope |
| Feature type | Configurable (-t exon default) | Configurable (exon default) | exon fixed for benchmark |
| Output | TSV + .summary file | TSV with __no_feature etc. | Polars DataFrame + summary dict |
| Memory | Moderate (chromosome index) | High (whole BAM in-memory) | Low (streaming partitions) |
| Speed (relative) | Fast (C, multithreaded) | ~20x slower than featureCounts | Target: competitive with featureCounts |
| Python integration | Via Rsubread or subprocess | Native Python | Native Python/Polars |

---

## Edge Cases That Must Be Handled

### Split Reads (CIGAR N operations)

Reads aligned by splice-aware aligners (STAR, HISAT2) contain `N` operations in CIGAR strings representing intron skips. The aligned bases are discontinuous. featureCounts handles this correctly: the two (or more) aligned segments are evaluated independently for exon overlap. A read spanning an exon–intron–exon junction counts once for the gene if either segment overlaps any exon.

**polars-bio risk**: `scan_bam()` exposes the read start and end positions. If only the full span (start to end including the intron gap) is used for overlap, reads will incorrectly match intronic features. The overlap must be against the actual aligned segments, not the span.

**Mitigation**: Check whether the BAM schema from `scan_bam()` exposes CIGAR or split segments. If not, the simplest correct approximation is to use read start + CIGAR-derived end. For genes without dense intronic annotation features, this rarely matters; but for overlapping genes on opposite strands sharing a locus, it can create false ambiguity. Flag this as a potential source of count differences vs featureCounts.

### Reads Overlapping Multiple Exons of the Same Gene

A read spanning two exons of gene A counts once for gene A, not twice. featureCounts' meta-feature logic handles this by deduplicating the gene_id set. The polars-bio implementation using `overlap()` + `group_by(read_id).n_distinct(gene_id)` must similarly deduplicate.

### Reads Where No Exon Overlaps

Intronic reads, intergenic reads: no exon overlap → `Unassigned_NoFeature`. Not counted. This is handled naturally by a left-semi join structure.

### Genes Sharing Exonic Sequence

Overlapping genes or read-through transcripts where two different gene_ids share the same genomic coordinates. Any read overlapping this shared region hits both genes → ambiguous → discarded. This is the dominant source of Unassigned_Ambiguity counts.

### Reads on Non-standard Chromosomes

BAM may contain reads on chrUn_*, EBV, or mitochondrial (chrM) contigs. GTF may or may not have annotations for these. Unmatch → Unassigned_NoFeature. No special handling needed beyond ensuring chromosome name matching (chr prefix consistency).

---

## Sources

- [HTSeq-count 2.0.5 documentation](https://htseq.readthedocs.io/en/latest/htseqcount.html) — HIGH confidence
- [featureCounts manual page (Debian testing)](https://manpages.debian.org/testing/subread/featureCounts.1.en.html) — HIGH confidence
- [featureCounts paper: Liao et al., Bioinformatics 2014](https://academic.oup.com/bioinformatics/article/30/7/923/232889) — HIGH confidence
- [CVR Bioinformatics: featureCounts or HTSeq-count?](https://bioinformatics.cvr.ac.uk/featurecounts-or-htseq-count/) — MEDIUM confidence (blog, verified against papers)
- [Biostars: featureCounts vs HTSeq-count differences](https://www.biostars.org/p/416608/) — MEDIUM confidence (community, consistent with paper)
- [Biostars: minOverlap and fracOverlap parameters](https://support.bioconductor.org/p/9142082/) — MEDIUM confidence
- polars-bio source: `/Users/mwiewior/research/git/polars-bio/polars_bio/range_op.py` — HIGH confidence

---

*Feature research for: RNA-seq feature counting benchmark (polars-bio vs featureCounts vs HTSeq-count)*
*Researched: 2026-03-20*
