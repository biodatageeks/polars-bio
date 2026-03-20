# Pitfalls Research

**Domain:** RNA-seq feature counting benchmark (polars-bio vs featureCounts vs HTSeq-count)
**Researched:** 2026-03-20
**Confidence:** HIGH (correctness pitfalls verified across multiple official sources and community discussions)

---

## Critical Pitfalls

### Pitfall 1: Double-Counting Reads that Span Multiple Exons of the Same Gene

**What goes wrong:**
A naive `count_overlaps()` between BAM reads and a per-exon GTF table will count a single read multiple times — once per exon it touches — if the read spans an exon-exon junction or is simply long enough to overlap two exons of the same gene. The sum per gene will be inflated relative to featureCounts output.

**Why it happens:**
`count_overlaps()` counts interval intersections, not gene-level unique reads. The exon table for a gene like ACTB might have 6 exon rows. A 150-bp read bridging two exons produces 2 overlap events. Without deduplication by `gene_id` within each read's hit list, the gene receives count 2 instead of 1.

featureCounts explicitly handles this: "when meta-features are used in read summarization, if a read overlaps two or more features belonging to the same meta-feature it is counted only once for that meta-feature." HTSeq uses a set of gene_ids per read position — set semantics prevent double-counting automatically.

**How to avoid:**
Use `overlap()` (not `count_overlaps()`) to get the join of reads × exons, then apply a `group_by(read_id, gene_id).agg(pl.count())` to get unique (read, gene) pairs, and only then `group_by(gene_id).agg(pl.count())` to get gene-level counts. Alternatively: collapse the exon GTF to non-overlapping gene-body intervals before counting — one interval per gene guarantees one hit per read.

**Warning signs:**
Total assigned read count exceeds the number of alignments in the BAM. Gene counts for highly-spliced genes (many isoforms, many exons) are disproportionately inflated. Compare a simple gene with 2 exons: exon-level sum should match gene-level count.

**Phase to address:**
Gene counting implementation phase — the first phase where `overlap()` / `count_overlaps()` is applied to BAM + GTF.

---

### Pitfall 2: Coordinate System Mismatch Between GTF (1-based) and BAM Intervals

**What goes wrong:**
GTF annotations use 1-based closed coordinates (both start and end are inclusive). BAM/SAM POS fields are also 1-based. polars-bio's `scan_gtf()` and `scan_bam()` set coordinate system metadata (`coordinate_system_zero_based`), but the coordinate system passed to `count_overlaps()` / `overlap()` must match. Using the wrong `FilterOp` (Strict vs Weak) shifts all interval boundaries by 1, producing systematically wrong counts: every boundary read is either missed or falsely assigned.

**Why it happens:**
The polars-bio metadata system defaults to lenient mode (`datafusion.bio.coordinate_system_check = false`), which falls back to global config and emits a warning instead of raising an error. In a notebook environment warnings are easy to miss. A developer who constructs intervals manually (e.g., `pl.DataFrame({"chrom": ..., "start": ..., "end": ...})`) without calling `set_coordinate_system()` will silently get wrong FilterOp.

**How to avoid:**
Always load GTF and BAM via `pb.scan_gtf()` and `pb.scan_bam()` — these set the correct metadata. Never construct genomic interval DataFrames manually for input to `overlap()`/`count_overlaps()` without calling `set_coordinate_system()`. Enable strict checking early: `pb.set_option("datafusion.bio.coordinate_system_check", True)`.

**Warning signs:**
`UserWarning: Coordinate system metadata is missing` in notebook output. Count-level validation against featureCounts shows a small but consistent negative bias on reads mapping to exon boundaries.

**Phase to address:**
Gene counting implementation phase — verify coordinate metadata is propagated through all intermediate steps before the overlap join.

---

### Pitfall 3: Chromosome Naming Convention Mismatch (chr1 vs 1)

**What goes wrong:**
UCSC-style BAM files use `chr1, chr2, ... chrX` while Ensembl-style GTF files (including some Gencode releases) use `1, 2, ... X`. An overlap between `chr1:100-200` and `1:100-200` produces zero results. The counting pipeline runs to completion, producing a valid-looking result with all zeros — no exception is thrown.

**Why it happens:**
The same Gencode project distributes GTF files in both conventions (GRCh38 primary assembly uses `chr` prefix, GRCh38 no-chr uses bare integers). Public BAM files on SRA/ENCODE can be aligned to either reference. Mixing is common and silent.

**How to avoid:**
Verify chromosome naming before running the benchmark: check `pl.scan_bam(...).select("contig").unique().collect()` and the first column of the GTF. If they differ, add a normalization step (strip or prepend `chr`). Document in the notebook which genome build was used for alignment and which GTF version is used.

**Warning signs:**
Zero counts for all genes. Total "Unassigned_NoFeatures" in the summary equals total aligned reads. A quick `df.filter(pl.col("contig") == "chr1").count()` vs `df.filter(pl.col("contig") == "1").count()` will immediately reveal the mismatch.

**Phase to address:**
Dataset preparation phase — include a chromosome naming compatibility check as part of data validation before any counting runs.

---

### Pitfall 4: Including Secondary, Supplementary, and Unmapped Alignments

**What goes wrong:**
featureCounts default mode counts only the **primary** alignment for multi-mapped reads (SAM flag 0x100 / secondary = excluded, 0x800 / supplementary = excluded). polars-bio's BAM reader may return all alignment records including secondary and supplementary, inflating counts relative to featureCounts — or worse, producing different per-gene distributions because multi-mappers often align to repeated regions.

**Why it happens:**
"Read all BAM records" is the natural behavior of an interval scanner. The filtering of FLAG bits is an RNA-seq-domain convention that must be applied explicitly. featureCounts documents: "The default counting option for reads having multiple mapping locations is 'primary', where only the primary alignment of a multi-mapped read is considered."

**How to avoid:**
Filter BAM reads before counting: exclude records with `FLAG & 0x100 != 0` (secondary) and `FLAG & 0x800 != 0` (supplementary) and `FLAG & 0x4 != 0` (unmapped). Apply as a predicate in `scan_bam()`. Document the filter flags explicitly in the notebook. Confirm total input read counts match between tools (check featureCounts summary "Assigned + Unassigned_*" = total reads).

**Warning signs:**
polars-bio total input read count exceeds featureCounts "Total reads" summary line. Genes in highly repetitive regions (ribosomal RNA loci, pseudogenes) show outsized count inflation.

**Phase to address:**
Gene counting implementation phase — establish the canonical BAM filter predicate before correctness validation.

---

### Pitfall 5: Ambiguous Read Assignment — Multi-Gene Overlapping Reads

**What goes wrong:**
Some genomic regions have overlapping genes on different strands, or nested genes. A read overlapping two distinct genes is "ambiguous." featureCounts default discards ambiguous reads (assigns them to "Unassigned_Ambiguity"). HTSeq-count also discards them in `union` mode. A polars-bio implementation that assigns reads to all overlapping genes, or picks one arbitrarily, will overcounts some genes and differ from both reference tools.

**Why it happens:**
The natural output of `overlap()` for an ambiguous read is two rows (one per gene). Grouping by `gene_id` and counting produces a count +1 for each gene in the overlap. There is no built-in ambiguity detection in `count_overlaps()`.

**How to avoid:**
After the read × gene join, compute the number of distinct genes per read. Exclude reads where that count > 1 before aggregating to gene-level counts. This exactly replicates featureCounts default "discardAmbiguous" behavior. Document what fraction of reads is discarded.

**Warning signs:**
Per-gene count totals differ systematically at loci with known overlapping annotations (e.g., antisense gene pairs). The discrepancy is larger than rounding error and correlates with gene density.

**Phase to address:**
Gene counting implementation phase — implement ambiguity detection in the same step as double-count deduplication.

---

### Pitfall 6: Mapping Quality Threshold Mismatch Between Tools

**What goes wrong:**
HTSeq-count default minimum MAPQ is 10. featureCounts default is 0 (no filter). A benchmark comparing polars-bio against HTSeq-count without aligning on MAPQ thresholds will show systematic count differences for all genes, making the correctness comparison meaningless.

**Why it happens:**
Tool developers set different defaults. The benchmark author applies "default settings" to all tools without checking whether defaults are equivalent.

**How to avoid:**
Explicitly align filter parameters before any correctness comparison. For correctness validation against featureCounts, use MAPQ >= 0 (no filter) in polars-bio. For the HTSeq-count comparison, either set `-a 0` in HTSeq or apply `MAPQ >= 10` in polars-bio. Document the chosen MAPQ threshold prominently in the notebook.

**Warning signs:**
Correlation between tools is high (> 0.99) but absolute counts differ by a fixed percentage across all genes. The percentage approximately equals the fraction of low-MAPQ reads in the BAM (typically 5–15%).

**Phase to address:**
Dataset preparation / benchmark harness phase — establish and document the filter parameter matrix before running comparisons.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Collapse GTF to gene bodies before counting | Avoids double-count deduplication logic | Cannot distinguish exonic vs intronic reads; diverges from featureCounts exon-union model | Never — the exon-union model is what featureCounts validates against |
| Use `count_overlaps()` directly on exon table | Single function call, simple code | Silent double-counting; invalidates correctness claim | Never — must deduplicate by gene per read |
| Skip FLAG filtering and trust BAM content | Faster development | Inflated counts from secondary/supplementary alignments | Only for SAM files from aligners that never emit secondary alignments (rare) |
| Hard-code chromosome names ("chr1") | Avoids normalization code | Breaks on any Ensembl-style dataset; benchmark not reproducible across labs | Never — normalization check must be in the notebook |
| Measure only wall time, skip RSS | Simpler benchmarking | Misses polars-bio's main advantage (memory efficiency); benchmark story is incomplete | Never — memory is a core claim of the project |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| GTF read via `scan_gtf()` | Using raw GTF rows including `gene`, `transcript`, `CDS` feature types | Filter to `feature == "exon"` only before overlap; featureCounts uses exon rows exclusively for gene-level counting |
| BAM via `scan_bam()` | Assuming `start` column is 0-based because polars-bio defaults to 0-based BED | BAM POS is 1-based; verify metadata with `pb.get_metadata()["coordinate_system_zero_based"]` |
| featureCounts subprocess | Parsing stdout to get counts | Parse the `.txt` output file; the TSV summary counts are in `.summary`; total counts are in the main output with a 2-row header skip |
| HTSeq-count subprocess | Running without `--stranded=no` flag | For unstranded RNA-seq, must pass `--stranded=no`; default is stranded, which will discard ~50% of reads |
| Gencode GTF download | Using GRCh38 comprehensive annotation (includes pseudogenes, lncRNAs) | For comparison with featureCounts default settings, use the same annotation; note that gene count totals are annotation-version-dependent |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Loading full BAM into memory with `read_bam()` instead of `scan_bam()` | OOM error or swap thrashing on 50–100M read BAM (20–40 GB) | Always use `scan_bam()` (lazy/streaming); the streaming advantage is polars-bio's core performance claim | Any BAM over ~5 GB on typical laptop (16 GB RAM) |
| Collecting intermediate LazyFrame before overlap | Memory spike equal to full read set; loses streaming benefit | Chain `scan_bam()` → filter → `overlap()` without `.collect()` until final count aggregation | 50M+ read BAM files |
| Warm-cache benchmark runs | First run slow (cold I/O), subsequent runs 3–5x faster; if only reporting warm runs, featureCounts looks unfairly slow | Clear page cache (`sudo purge` on macOS, `echo 3 > /proc/sys/vm/drop_caches` on Linux) before each tool run, or report both cold and warm measurements | Always relevant for I/O-bound workloads on NVMe vs spinning disk |
| Measuring featureCounts with thread count unmatched to polars-bio | featureCounts defaults to 1 thread; polars-bio may use all cores | Pin both tools to the same thread count; report per-thread scaling separately | Multi-core machines; default thread mismatch overstates polars-bio speedup |
| RSS measurement inside Python process | `psutil.memory_info().rss` includes Python interpreter and library overhead; not comparable to featureCounts C process RSS | Use `/usr/bin/time -v` (Linux) or `command time -l` (macOS) to measure peak RSS externally for all tools uniformly | Any in-process measurement |

---

## "Looks Done But Isn't" Checklist

- [ ] **Correctness validation:** Count comparison must use the same MAPQ filter, same ambiguity handling, same `--stranded=no` flag, same GTF file, and same BAM file. A high Pearson correlation alone is insufficient — verify zero-count genes are zero in both tools, and the assigned-read totals match within 1%.
- [ ] **Double-count prevention:** Verify by testing on a synthetic 2-exon gene where a single read spans both exons — count must be 1, not 2.
- [ ] **FLAG filtering:** Confirm the BAM filter excludes secondary (0x100) and supplementary (0x800) and unmapped (0x4) flags. Print the filter predicate explicitly in the notebook.
- [ ] **featureCounts comparison summary:** featureCounts `.summary` file reports Assigned, Unassigned_Ambiguity, Unassigned_NoFeatures, etc. Generate an equivalent summary from polars-bio to show the same categories. Missing this makes the comparison look shallow.
- [ ] **GTF feature type filter:** Confirm only `feature == "exon"` rows enter the overlap join. `len(gtf_exons) < len(gtf_all)` should be true.
- [ ] **Chromosome naming verified:** Add a cell that prints unique chromosome names from both BAM and GTF and confirms they match exactly.
- [ ] **Memory measurement method:** RSS must be measured externally with `/usr/bin/time` or equivalent, not via psutil inside the same process. Include the measurement command in the notebook.
- [ ] **Reproducibility**: Notebook must specify exact Gencode GTF version, Ensembl/ENCODE BAM accession, featureCounts version, HTSeq version, and polars-bio version.

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Double-counting discovered after benchmark | MEDIUM | Rewrite the aggregation step to deduplicate (read_id, gene_id) pairs; re-run counting; update notebook figures |
| Coordinate system mismatch discovered after counting | LOW | Enable strict coordinate check; re-run with correct FilterOp; counts change by ≤1 read per exon boundary |
| Chromosome naming mismatch (all zeros result) | LOW | Add normalization step before overlap; re-run counting |
| FLAG filtering omitted | MEDIUM | Add predicate pushdown filter to `scan_bam()` call; re-run full benchmark and update figures |
| MAPQ threshold mismatch noticed after comparison | LOW | Add MAPQ filter to scan_bam() predicate; re-run; document which threshold was aligned to which tool |
| Warm-cache bias discovered | MEDIUM | Add cache-clearing cells to notebook; re-run all timing measurements in cold-cache conditions; performance numbers will change significantly |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Double-counting via multi-exon reads | Gene counting implementation | Synthetic test: 2-exon gene + 1 spanning read = count of 1 |
| Coordinate system mismatch | Gene counting implementation | `pb.get_metadata()` confirms both BAM and GTF are 1-based; strict mode enabled |
| Chromosome naming mismatch | Dataset preparation | Notebook cell prints and confirms chromosome name sets match |
| Secondary/supplementary alignments included | Gene counting implementation | FLAG filter predicate applied; total input reads matches featureCounts summary total |
| Ambiguous read handling wrong | Gene counting implementation | Count of "ambiguous" reads reported and matches featureCounts Unassigned_Ambiguity |
| MAPQ threshold mismatch | Benchmark harness | Parameter matrix table in notebook explicitly documents thresholds per tool |
| Warm-cache benchmark bias | Benchmark harness | Cache-clear step precedes each tool invocation; cold-cache results reported |
| RSS measurement inaccuracy | Benchmark harness | `/usr/bin/time` output captured for all tools; polars-bio RSS measured as external subprocess |

---

## Sources

- featureCounts paper: [Liao et al., Bioinformatics 2014](https://academic.oup.com/bioinformatics/article/30/7/923/232889) — canonical description of exon-union algorithm and meta-feature deduplication
- [featureCounts vs HTSeq-count comparison (CVR Bioinformatics)](https://bioinformatics.cvr.ac.uk/featurecounts-or-htseq-count/) — MAPQ defaults, paired-end ambiguity differences
- [HTSeq counting modes documentation](https://htseq.readthedocs.io/en/latest/counting.html) — union/intersection-strict/intersection-nonempty; set-based deduplication logic
- [Biostars: HTSeq vs featureCounts discrepancy](https://www.biostars.org/p/9604786/) — community-verified causes of count differences
- [Biostars: HTSeq-count end coordinate exclusion](https://www.biostars.org/p/96176/) — GTF interval boundary handling difference between tools
- [featureCounts minOverlap default behavior](https://support.bioconductor.org/p/9142082/) — 1 bp default overlap threshold
- [Gencode FAQ on genome/annotation version matching](https://www.gencodegenes.org/pages/faq.html) — authoritative on coordinate conventions
- [PSutil peak RSS limitation with memory-mapped files](https://github.com/erikbern/ann-benchmarks/issues/581) — why in-process RSS measurement is unreliable
- [Bioinformatics benchmark cache effects (PLOS CompBiol)](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1009244) — I/O vs compute measurement methodology
- [Quartet RNA-seq benchmarking study (Nature Comms 2024)](https://www.nature.com/articles/s41467-024-50420-y) — real-world sources of inter-tool variation
- polars-bio `_metadata.py` source — coordinate system propagation and FilterOp (Strict vs Weak) logic reviewed directly

---
*Pitfalls research for: RNA-seq feature counting benchmark (polars-bio vs featureCounts vs HTSeq-count)*
*Researched: 2026-03-20*
