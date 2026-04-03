# Project Research Summary

**Project:** polars-bio RNA-seq Feature Counting Benchmark
**Domain:** Bioinformatics — read-level gene counting benchmark (polars-bio vs featureCounts vs HTSeq-count)
**Researched:** 2026-03-20
**Confidence:** HIGH

## Executive Summary

This project delivers a reproducible RNA-seq feature counting benchmark comparing polars-bio against featureCounts (the production standard) and HTSeq-count (the historical reference). The counting algorithm is well-understood: overlap each BAM read against GTF exon intervals, group by gene_id, and discard reads that overlap more than one gene. The scientific novelty is not the algorithm but the runtime story — polars-bio's streaming DataFusion execution should deliver peak RSS significantly lower than HTSeq-count (fully in-memory) and competitive wall time with featureCounts (C, multithreaded), while remaining a native Python/Polars API.

The recommended implementation composes existing polars-bio primitives without any Rust changes: `scan_bam()` for FLAG-filtered reads, `scan_gtf()` for exon rows, `pb.overlap()` for the spatial join, and two Polars `group_by()` calls for ambiguity resolution and final aggregation. This "overlap-then-double-groupby" pattern is the only correct composition — using `count_overlaps()` directly produces inflated counts due to double-counting reads spanning multiple exons. The benchmark dataset is ENCODE ENCSR329MHM (HepG2, ~36M single-end 76bp reads, GRCh38) with GENCODE v49 comprehensive annotation; both are publicly accessible by accession and have no authentication requirement.

The dominant correctness risk is semantic precision in the ambiguity resolution step: reads overlapping exons from two different genes must be detected and discarded from all gene tallies, matching featureCounts' `Unassigned_Ambiguity` category. The dominant performance risk is the intermediate overlap join size (150–300M rows for 50–100M reads), which must be kept lazy and streamed through the groupby stages without `.collect()`. Dataset preparation (chromosome name validation, BAM sort/index, GTF version matching) must be completed and verified before any counting runs to avoid silent zero-count results from chr-prefix mismatches.

---

## Key Findings

### Recommended Stack

The benchmark uses a separate conda environment (`rnaseq-benchmark`) for featureCounts (subread 2.1.1) and HTSeq-count (2.0.5) to avoid C-extension conflicts with Arrow builds. polars-bio itself runs in its existing dev environment. Timing is measured with hyperfine 1.20.0 (warmup + 5 runs, exports JSON/Markdown) and peak RSS with `/usr/bin/time -v` (Linux) or `gtime -v` (macOS) for all three tools uniformly — not psutil inside the Python process, which is unreliable for cross-tool comparison. All benchmark dependencies (memory-profiler, psutil, py-cpuinfo, jupyter, matplotlib) are already present in polars-bio's dev extras.

**Core technologies:**
- featureCounts 2.1.1 (subread): primary speed target and correctness reference — de-facto nf-core/rnaseq standard, 16–175x faster than HTSeq
- HTSeq-count 2.0.5: secondary correctness cross-check — well-characterized algorithmic differences from featureCounts for single-end unstranded mode
- hyperfine 1.20.0: wall-clock timing with statistics — handles CLI subprocesses uniformly across all three tools
- ENCODE ENCSR329MHM (HepG2 SE76): benchmark dataset — STAR-aligned GRCh38, ~36M reads, directly downloadable by accession
- GENCODE v49 comprehensive GTF: annotation — must match across all three tools; use comprehensive (not basic) to match production pipeline conventions

### Expected Features

**Must have (table stakes — required for a valid benchmark):**
- Pre-overlap BAM filtering: exclude unmapped (FLAG 0x4), secondary (0x100), supplementary (0x800), multi-mappers (NH > 1)
- Coordinate system reconciliation: both scan_gtf() and scan_bam() default to 1-based; use polars-bio defaults and enable strict coordinate checking
- Read × gene pair table via `pb.overlap()` — not `count_overlaps()` — to support per-read ambiguity detection
- Ambiguous read discard: group_by(read_name) → filter n_distinct(gene_id) == 1 before aggregation
- Gene-level count aggregation: group_by(gene_id) → count after ambiguity filter
- Summary statistics equivalent to featureCounts .summary: Assigned, Unassigned_NoFeature, Unassigned_Ambiguity, Unassigned_MultiMapping
- Benchmark harness: featureCounts CLI invocation with `-s 0 -t exon -g gene_id`, output parsing
- Benchmark harness: HTSeq-count CLI with `--stranded=no --min-quality 0`, output parsing
- Correctness metrics: per-gene Pearson r, RMSE, count of genes with |delta| > 0 vs featureCounts
- Performance metrics: wall time at 1 and N threads; peak RSS for all three tools

**Should have (differentiators — polars-bio value story):**
- Streaming memory profile via `mprof run` showing sub-linear memory growth with dataset size
- Thread scaling table (1, 2, 4, 8 threads) for both featureCounts and polars-bio
- Chromosome naming compatibility check cell in notebook (prevents silent zero-count failures)
- `pb.count_features(bam, gtf)` convenience wrapper packaging the full pipeline

**Defer (v2+):**
- Strand-aware counting (`-s 1`/`-s 2`) — requires strand column in overlap query; no current polars-bio support
- Paired-end fragment counting — requires QNAME-grouping and fragment extent computation
- Multi-mapping fractional counting (`-M --fraction`) — fractional counts break integer invariant
- Isoform-level quantification — entirely different algorithm (Salmon/kallisto domain)

### Architecture Approach

The pipeline is a pure composition of existing polars-bio primitives: no Rust changes are needed. A new `polars_bio/feature_count.py` module exposes `count_features(bam_path, gtf_path) -> pl.LazyFrame` that chains scan → filter → overlap → dual-groupby. The only non-trivial preprocessing step outside existing primitives is CIGAR-derived end position computation: `scan_bam()` exposes `pos` (start) but not a `cigar_end` column, so a `_compute_read_end()` helper must parse CIGAR strings or approximate end as `pos + uniform_read_length`. The benchmark notebook lives in `notebooks/feature_counting_benchmark.ipynb` and calls both the polars-bio API and subprocesses for featureCounts/HTSeq-count.

**Major components:**
1. GTF Loader — `pb.scan_gtf(attr_fields=["gene_id"]).filter(pl.col("feature") == "exon").select(["seqname","start","end","gene_id"])` producing the exon interval table
2. BAM Loader — `pb.scan_bam().filter((pl.col("flags").cast(pl.Int32) & 0xD04) == 0)` with NH tag filter for multi-mappers; includes CIGAR-end computation helper
3. Interval Join — `pb.overlap(reads_lf, exons_lf)` using CoITrees spatial join; returns (read_name, gene_id) pairs — must keep read_name through this step
4. Ambiguity Resolution — `.group_by("name").agg(pl.col("gene_id").n_unique(), pl.col("gene_id").first()).filter(pl.col("n_genes") == 1)`
5. Count Aggregation — `.group_by("gene_id").agg(pl.len().alias("count"))` producing the final LazyFrame
6. Benchmark Harness — hyperfine timing, `/usr/bin/time` RSS measurement, count correlation and diff computation in notebook

### Critical Pitfalls

1. **Double-counting via multi-exon reads** — `count_overlaps()` directly on the exon table counts once per exon hit, not once per gene per read. A read spanning 2 exons of gene ACTB produces count=2. Fix: use `pb.overlap()` + dual `group_by()` (deduplicate by read_name first, then aggregate by gene_id). Verify with a synthetic 2-exon gene test where a single spanning read must produce count=1.

2. **Ambiguous read assignment** — reads overlapping exons from two different genes must be detected and discarded from all gene counts before aggregation. The natural `overlap()` output produces two rows for an ambiguous read; a naive `group_by(gene_id).count()` assigns count +1 to both genes. Fix: the first `group_by(read_name)` step with `n_unique(gene_id)` filter catches this. Total Unassigned_Ambiguity should match featureCounts .summary within 1%.

3. **Chromosome naming mismatch (chr1 vs 1)** — produces all-zero counts silently (no exception). ENCODE BAMs use UCSC chr-prefix; some Ensembl GTF builds use bare integers. Fix: add a validation cell that prints unique contig names from both BAM and GTF before any counting run. Use GENCODE v49 (chr-prefix) matched with ENCODE GRCh38 BAM.

4. **Secondary/supplementary alignment inclusion** — featureCounts default excludes FLAG 0x100 (secondary) and 0x800 (supplementary); including them inflates counts for multi-mapped and chimeric reads. Fix: apply `(flags & 0xD04) == 0` predicate on `scan_bam()` output before overlap; confirm total input read count matches featureCounts summary "Total reads" line.

5. **MAPQ threshold mismatch between tools** — HTSeq-count default is MAPQ >= 10; featureCounts default is 0. Comparing polars-bio (MAPQ=0) against HTSeq-count (MAPQ=10) will show a systematic percentage difference across all genes. Fix: for featureCounts comparison use MAPQ=0 (no filter); for HTSeq-count comparison use `--min-quality 0` in HTSeq invocation. Document the parameter matrix explicitly.

---

## Implications for Roadmap

Based on research, the pipeline has clear dependency order: data must be validated before counting, counting must be correct before benchmarking, and benchmarking must be complete before the notebook is written.

### Phase 1: Dataset Preparation and Environment Setup
**Rationale:** All downstream phases depend on having the correct BAM, GTF, and tools installed. Chromosome naming, BAM index, and version verification must be done once here. Failures discovered later (all-zero counts from chr mismatch) cost a full re-run.
**Delivers:** Sorted, indexed BAM (hepg2_rep1_sorted.bam); GENCODE v49 comprehensive GTF; featureCounts 2.1.1 and HTSeq-count 2.0.5 installed in conda env; reference count matrices from both tools; chromosome name compatibility confirmed.
**Addresses:** Human-scale dataset acquisition (P1); benchmark harness setup
**Avoids:** Chromosome naming mismatch (Pitfall 3); MAPQ threshold mismatch (Pitfall 6); warm-cache benchmark bias (document cache-clear procedure here)

### Phase 2: Core Counting Implementation
**Rationale:** The counting logic is the scientific core. It must be implemented and unit-tested against small synthetic data before being run at scale. CIGAR-end computation and the dual-groupby pattern are the two non-obvious steps that require careful testing.
**Delivers:** `polars_bio/feature_count.py` with `count_features(bam, gtf)` function; `_compute_read_end()` helper; unit tests in `tests/test_feature_count.py` with small BAM+GTF fixture including a 2-exon spanning-read test.
**Addresses:** Pre-overlap BAM filtering (P1); coordinate system reconciliation (P1); read × gene overlap join (P1); ambiguous read discard (P1); gene-level count aggregation (P1)
**Avoids:** Double-counting via multi-exon reads (Pitfall 1); ambiguous read assignment (Pitfall 5); coordinate system mismatch (Pitfall 2); secondary alignment inclusion (Pitfall 4)

### Phase 3: Correctness Validation
**Rationale:** Correctness must be confirmed before performance numbers are meaningful. A fast-but-wrong benchmark is worse than no benchmark. Validate on a small subset first, then on the full ENCODE dataset.
**Delivers:** Per-gene Pearson r and Spearman r vs featureCounts; RMSE; count of genes with |delta| > 0; summary statistics comparison (Assigned / Unassigned_Ambiguity / Unassigned_NoFeature totals); documented explanation of any residual differences (expected: split reads, chr boundary reads).
**Addresses:** Correctness metrics (P1); summary statistics output (P1)
**Uses:** featureCounts 2.1.1 and HTSeq-count 2.0.5 reference runs from Phase 1

### Phase 4: Performance Benchmarking
**Rationale:** Only run performance measurements after correctness is confirmed. Timing or RSS numbers from an incorrect implementation are meaningless.
**Delivers:** hyperfine timing results (JSON + Markdown) for all three tools at 1 thread; peak RSS measured via `/usr/bin/time -v` for all three tools; thread-scaling table (1, 2, 4, 8); memory profile via `mprof run` for polars-bio process.
**Addresses:** Performance metrics (P1); streaming memory advantage demonstration
**Avoids:** Warm-cache benchmark bias (clear page cache before each run); RSS measurement inaccuracy (external measurement, not psutil in-process); thread count mismatch (pin both tools to same core count for apples-to-apples)

### Phase 5: Benchmark Notebook
**Rationale:** Results presentation in a reproducible notebook is the deliverable. All cells must be idempotent: dataset download, reference runs, polars-bio counting, correctness metrics, performance charts.
**Delivers:** `notebooks/feature_counting_benchmark.ipynb` with six sections: dataset description + environment, timing comparison (table + bar chart), memory comparison (table + bar chart), count correlation scatter plot (log scale), discrepancy analysis, scaling table.
**Addresses:** Python-native API demonstration; Arrow output differentiator; reproducibility (pinned versions, ENCODE accession, GENCODE release)
**Uses:** All Phase 1–4 outputs; matplotlib/seaborn for charts; scipy.stats for correlation; pandas for count matrix comparison

### Phase Ordering Rationale

- Phases 1–2 are strictly ordered by dependency: no valid counting without valid data; no valid benchmark without valid counting.
- Phase 3 (correctness) gates Phase 4 (performance): reporting performance numbers from a semantically wrong implementation would invalidate the benchmark paper.
- Phase 5 (notebook) is last because it consumes all prior outputs and must be re-runnable from scratch to satisfy reproducibility requirements.
- Rust changes are not needed in any phase — the entire feature can be implemented in Python using existing primitives.

### Research Flags

Phases likely needing deeper investigation during planning:
- **Phase 2 (Core Counting):** CIGAR-end position computation is under-specified. Research confirmed `scan_bam()` does not expose a `cigar_end` column directly. The implementation must choose between: (a) CIGAR string parsing in Polars (`str.extract_all` regex over CIGAR operations), (b) uniform read-length approximation (`pos + 76` for SE76 data), or (c) checking if any BAM tag encodes the alignment end. Option (b) is sufficient for SE76 data but not general. Needs a concrete decision before coding.
- **Phase 2 (Core Counting):** NH tag availability in `scan_bam()` schema. Research assumes NH tag is accessible as a column for multi-mapper filtering but this was not verified from the BAM schema output. Confirm `pb.scan_bam().schema` includes NH before implementing the filter.
- **Phase 3 (Correctness Validation):** Split-read handling. ARCHITECTURE.md flagged that reads with CIGAR `N` operations (intron-spanning) may produce false positives if only the full span (start to end including the gap) is used for overlap. For a valid benchmark, this needs to be characterized: measure the fraction of reads with N operations in the ENCODE HepG2 BAM and the resulting count difference vs featureCounts for junction-spanning reads.

Phases with standard patterns (skip additional research):
- **Phase 1 (Dataset Preparation):** Fully documented. ENCODE accession, GENCODE URLs, conda install commands, and samtools validation steps are all specified in STACK.md.
- **Phase 4 (Benchmarking):** hyperfine + `/usr/bin/time` methodology is standard and fully specified in STACK.md with exact command-line invocations.
- **Phase 5 (Notebook):** Jupyter notebook structure is standard; section headings and chart types are specified in STACK.md.

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | ENCODE accession verified via direct portal fetch; GENCODE v49 URLs verified; featureCounts/HTSeq versions from official bioconda/PyPI; hyperfine from GitHub releases |
| Features | HIGH | featureCounts defaults from official manual (Debian) and original 2014 paper; HTSeq defaults from official readthedocs; polars-bio API verified from source code |
| Architecture | HIGH | polars-bio overlap API verified from `range_op.py` source; GTF scan API from `io.py` source; coordinate system behavior from `docs/features.md`; dual-groupby pattern is logically derived and correct |
| Pitfalls | HIGH | Correctness pitfalls cross-validated across official docs, peer-reviewed papers, and community sources; performance pitfalls (cache bias, RSS measurement) from benchmark methodology literature |

**Overall confidence:** HIGH

### Gaps to Address

- **CIGAR-end column availability:** Confirm whether `scan_bam()` exposes an alignment end position or whether CIGAR parsing is required. If CIGAR parsing is needed, benchmark the overhead of doing this in Polars on 36M rows. Resolve in Phase 2 planning before coding.
- **NH tag column availability:** Confirm `scan_bam()` schema includes the NH optional tag. If not, multi-mapper filtering must be approximated via MAPQ == 0 as a proxy (MAPQ 0 reads are typically multi-mappers), which introduces semantic difference from featureCounts. Resolve in Phase 2 before coding.
- **Split-read (CIGAR N) count impact:** Quantify what fraction of HepG2 SE76 reads are junction-spanning and whether using full-span overlap vs segment-level overlap produces a meaningful count difference. This determines whether the count difference vs featureCounts is acceptable or requires a CIGAR-segment decomposition step.
- **low_memory flag behavior:** ARCHITECTURE.md recommends `low_memory=True` on `pb.overlap()` for 50–100M read datasets. Validate that this flag exists and has the expected batch-size-limiting semantics in the current polars-bio version before relying on it for peak RSS claims.

---

## Sources

### Primary (HIGH confidence)
- [Bioconductor Rsubread v2.22.1/Subread v2.1.1 Users Guide](https://bioconductor.org/packages/release/bioc/vignettes/Rsubread/inst/doc/SubreadUsersGuide.pdf) — featureCounts defaults and algorithm
- [HTSeq 2.0.5 official documentation](https://htseq.readthedocs.io/) — counting mode semantics, MAPQ default
- [featureCounts paper: Liao et al., Bioinformatics 2014](https://academic.oup.com/bioinformatics/article/30/7/923/232889) — canonical algorithm description, meta-feature deduplication
- [ENCODE experiment ENCSR329MHM (HepG2, SE76)](https://www.encodeproject.org/experiments/ENCSR329MHM/) — dataset accession, read counts, alignment metadata
- [GENCODE Human Release 49](https://www.gencodegenes.org/human/) — annotation URLs, chromosome convention
- [hyperfine GitHub releases v1.20.0](https://github.com/sharkdp/hyperfine/releases) — timing tool
- polars-bio source: `polars_bio/range_op.py`, `polars_bio/io.py`, `docs/features.md` — API and coordinate system behavior

### Secondary (MEDIUM confidence)
- [CVR Bioinformatics: featureCounts or HTSeq-count?](https://bioinformatics.cvr.ac.uk/featurecounts-or-htseq-count/) — MAPQ defaults, algorithmic differences
- [Biostars: featureCounts vs HTSeq-count differences](https://www.biostars.org/p/416608/) — community validation of count difference sources
- [Subread SourceForge version history](https://subread.sourceforge.net/) — version 2.1.1 release date

### Tertiary (MEDIUM-LOW confidence)
- [PSutil peak RSS limitation with memory-mapped files](https://github.com/erikbern/ann-benchmarks/issues/581) — in-process RSS unreliability
- [Bioinformatics benchmark cache effects (PLOS CompBiol)](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1009244) — I/O vs compute measurement methodology
- [Quartet RNA-seq benchmarking study (Nature Comms 2024)](https://www.nature.com/articles/s41467-024-50420-y) — real-world sources of inter-tool variation

---
*Research completed: 2026-03-20*
*Ready for roadmap: yes*
