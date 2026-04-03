# Stack Research

**Domain:** RNA-seq feature counting benchmark (polars-bio vs featureCounts vs HTSeq-count)
**Researched:** 2026-03-20
**Confidence:** MEDIUM-HIGH

---

## Context: What This Stack Is For

This is a benchmark stack, not a production stack. The goal is to run a reproducible
comparison of gene-level read counting (polars-bio, featureCounts, HTSeq-count) on
human-scale RNA-seq data, collect wall time and peak RSS, validate count correctness,
and present results in a Jupyter notebook. polars-bio's existing runtime stack (Rust,
Python, DataFusion, Polars) is already defined in `.planning/codebase/STACK.md` and
is not repeated here.

---

## Recommended Stack

### Core Technologies: Comparison Targets

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| featureCounts (Subread) | 2.1.1 | Primary speed benchmark target; C implementation, multi-threaded | De-facto standard for bulk RNA-seq counting in production pipelines. nf-core/rnaseq uses it. ~16-175x faster than HTSeq. Set the bar polars-bio must approach. |
| HTSeq-count | 2.0.5 | Correctness reference target; pure Python | The original "union" mode algorithm that most labs used for years. Counts slightly fewer reads than featureCounts due to open-position handling of feature boundaries. Use as a second correctness anchor, not speed target. |

**featureCounts installation:** `conda install -c bioconda subread=2.1.1`
**HTSeq installation:** `pip install HTSeq==2.0.5` or `conda install -c bioconda htseq=2.0.5`

**featureCounts default command to match** (unstranded, single-end, gene-level):
```bash
featureCounts -T 1 -s 0 -t exon -g gene_id \
  -a gencode.v49.annotation.gtf \
  -o counts_fc.txt \
  sample.bam
```

`-s 0` = unstranded, `-t exon` = feature type, `-g gene_id` = meta-feature grouping.
This is exactly what polars-bio must replicate: exon-level overlaps aggregated by
gene_id, ambiguous reads (overlapping multiple genes) discarded.

---

### Benchmark Datasets

#### Primary Dataset: ENCODE HepG2 polyA RNA-seq (SE76)

| Property | Value | Confidence |
|----------|-------|------------|
| ENCODE experiment | ENCSR329MHM | HIGH — verified via ENCODE portal |
| Cell line | HepG2 (human hepatocellular carcinoma) | HIGH |
| Read type | Single-end, 76 bp | HIGH — confirmed in experiment files |
| Read counts | Rep1: ~36M reads (ENCFF588KDY), Rep2: ~26M reads (ENCFF085DBP) | HIGH |
| Genome assembly | GRCh38 | HIGH |
| Aligner | STAR (ENCODE4 v1.2.3 pipeline) | HIGH |
| Download | https://www.encodeproject.org/experiments/ENCSR329MHM/ | HIGH |
| BAM accessions (GRCh38) | ENCFF588KDY (Rep1), ENCFF085DBP (Rep2) | HIGH — verified |

**Why this dataset:** Single-end reads match unstranded benchmark scope. ~36M reads
is real-world scale without being excessive for initial benchmark runs. ENCODE
provenance means alignment quality is controlled and reproducible. GRCh38 + STAR
matches what Gencode v49 GTF is designed for.

**Why not ENCSR000CPH (K562):** That experiment is paired-end PE76 with ~113M reads.
Paired-end counting is out of scope for this milestone (PROJECT.md: "unstranded
single-end counting mode is the correctness target"). Use K562 in a follow-up
strand-aware or paired-end milestone.

#### Annotation: GENCODE Release 49

| Property | Value | Confidence |
|----------|-------|------------|
| Release | v49 (GRCh38.p14) | HIGH — verified via gencodegenes.org |
| GTF URL (comprehensive) | https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gtf.gz | HIGH |
| GTF URL (basic, recommended) | https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.basic.annotation.gtf.gz | HIGH |
| Gene count | ~60k genes (GENCODE releases historically ~60k for comprehensive) | MEDIUM |
| Exon features | ~1.4M exon rows in comprehensive | MEDIUM |

**Use the comprehensive annotation GTF** (`gencode.v49.annotation.gtf.gz`), not basic.
featureCounts benchmarks and HTSeq both default to using the full annotation.
Matching the same GTF across all three tools is the only way to make count
comparisons valid.

**Do not use Ensembl GTF** as a substitute. Gene_id format differs (Ensembl strips
version suffixes) and exon count boundaries differ slightly, making cross-tool
count comparisons meaningless.

---

### Benchmarking Tools

| Tool | Version | Purpose | Why |
|------|---------|---------|-----|
| hyperfine | 1.20.0 | Wall-clock timing with statistics across N runs | Multiple runs, warmup runs, exports JSON/CSV/Markdown. Statistical outlier detection. Standard tool for CLI benchmark papers. Export to `--export-json` for notebook ingestion. |
| GNU time (`/usr/bin/time -v`) | system (Linux) / `gtime -v` (macOS) | Peak RSS memory per process, single-run wrapper | The canonical way to measure `Maximum resident set size` for any subprocess. Required because hyperfine does not report memory. |
| memory-profiler | 0.61.0 | Time-series memory profile for the polars-bio Python process | Already in polars-bio dev deps. `mprof run` produces `.dat` files; `mprof plot` visualizes memory over time. Use for polars-bio only — featureCounts/HTSeq are measured via GNU time. |
| psutil | 6.1.1 | Programmatic peak RSS within Python | Already in polars-bio dev deps. Use inside the benchmark notebook to query process memory at checkpoints. |
| Jupyter | ^1.1.0 | Notebook execution and result presentation | Already in polars-bio dev deps. |
| matplotlib | (optional extra) | Memory profile plots in notebook | Already listed as optional extra in polars-bio. |

**hyperfine installation:** `cargo install hyperfine` or `conda install -c conda-forge hyperfine`

---

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| samtools | 1.21+ | Sort and index BAM files, validate with flagstat | Dataset preparation: `samtools sort`, `samtools index`, `samtools flagstat` to confirm read counts before benchmark |
| tabix | (bundled with samtools/htslib) | Index GTF if needed | Some scan paths in polars-bio benefit from tabix-indexed GTF |
| pandas | (polars-bio optional extra) | Count matrix comparison and correlation in notebook | Load counts from all three tools, compute Spearman/Pearson correlation, identify discrepant genes |
| scipy | system | Spearman correlation for correctness validation | `scipy.stats.spearmanr(polars_bio_counts, featurecounts_counts)` |
| seaborn / matplotlib | system | Scatter plots and timing bar charts in notebook | Visualize count correlation, timing comparison, memory comparison |

---

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| conda / mamba | Isolated environment for featureCounts + HTSeq | Do NOT install featureCounts into the polars-bio Python env — it has C extension conflicts with some Arrow builds. Use a separate `benchmark` conda env. |
| snakemake or shell script | Orchestrate multi-tool benchmark runs | A simple shell script is sufficient for this milestone. Snakemake only if the benchmark grows to multiple datasets/conditions. |
| py-cpuinfo | 9.0.0 | Record CPU model in benchmark metadata | Already in polars-bio dev deps. Embed in notebook preamble. |

---

## Installation

```bash
# Benchmark conda environment (separate from polars-bio dev env)
conda create -n rnaseq-benchmark python=3.11
conda activate rnaseq-benchmark
conda install -c bioconda subread=2.1.1 htseq=2.0.5 samtools

# hyperfine (Rust binary, install globally or into benchmark env)
cargo install hyperfine
# OR:
conda install -c conda-forge hyperfine

# Python benchmark dependencies (in polars-bio dev env, already present)
# memory-profiler 0.61.0, psutil 6.1.1, py-cpuinfo 9.0.0, jupyter, matplotlib
# are all already in polars-bio pyproject.toml [tool.poetry.dev-dependencies]

# GTF download
wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_49/gencode.v49.annotation.gtf.gz
gunzip gencode.v49.annotation.gtf.gz

# BAM download (ENCODE — requires no authentication, direct HTTPS)
wget https://www.encodeproject.org/files/ENCFF588KDY/@@download/ENCFF588KDY.bam
samtools sort -o hepg2_rep1_sorted.bam ENCFF588KDY.bam
samtools index hepg2_rep1_sorted.bam
samtools flagstat hepg2_rep1_sorted.bam  # verify ~36M reads
```

---

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| featureCounts 2.1.1 (CLI) | Rsubread R package | Only if the audience is R-centric and Bioconductor compatibility is a goal. CLI featureCounts is simpler to script and time. |
| HTSeq 2.0.5 | summarizeOverlaps (Bioconductor) | summarizeOverlaps gives identical counts to featureCounts for unstranded mode; useful as a third reference but adds R dependency with no additional insight for this benchmark. |
| ENCODE ENCSR329MHM (HepG2 SE) | SRA download + re-alignment | ENCODE provides pre-aligned, high-quality BAM directly. Re-aligning from FASTQ adds complexity with no benefit for counting benchmarks. |
| GENCODE v49 (comprehensive) | Ensembl GTF | Ensembl uses different gene_id format (no version suffix). Cross-tool count comparison becomes invalid. Stick with GENCODE. |
| hyperfine + GNU time | pytest-benchmark | pytest-benchmark benchmarks Python functions, not CLI subprocesses. Useless for timing featureCounts (C binary) or HTSeq-count (CLI script). hyperfine handles all three tools uniformly. |
| memory-profiler (mprof) | Valgrind massif | massif provides allocator-level detail but enormous overhead (~10-20x slowdown). For comparing peak RSS across tools, GNU time is sufficient. Use massif only if a specific allocation pattern needs debugging. |

---

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Salmon / kallisto as benchmark targets | Alignment-free, completely different paradigm (transcript-level pseudo-counts vs read-level overlap counting). Comparing counts is invalid. | Explicitly out of scope per PROJECT.md |
| `time` shell builtin | Does not report memory, accuracy is lower than GNU time, not portable | `/usr/bin/time -v` (Linux) or `gtime -v` (macOS via `brew install gnu-time`) |
| Simulated/synthetic BAM files | Simulation introduces alignment artifact assumptions; real ENCODE data is more credible for a benchmark paper and reproducible via accession number | ENCODE ENCSR329MHM real data |
| GENCODE basic annotation | Missing many transcripts; comprehensive annotation is what production RNA-seq pipelines use | gencode.v49.annotation.gtf.gz (comprehensive) |
| paired-end ENCODE datasets for this milestone | Paired-end counting requires fragment deduplication logic not yet in polars-bio; correctness validation is ambiguous without it | ENCSR329MHM (single-end) |
| Strand-specific mode (`-s 1` or `-s 2`) | polars-bio `count_overlaps()` has no strand awareness. Results would be wrong. | Unstranded mode only (`-s 0` for featureCounts, `--stranded=no` for HTSeq) |

---

## Benchmark Methodology

### Wall Time
```bash
hyperfine \
  --warmup 1 \
  --runs 5 \
  --export-json results/timing.json \
  --export-markdown results/timing.md \
  "featureCounts -T 1 -s 0 -t exon -g gene_id -a gencode.v49.annotation.gtf -o /dev/null hepg2_rep1_sorted.bam" \
  "htseq-count --stranded=no --type=exon --idattr=gene_id hepg2_rep1_sorted.bam gencode.v49.annotation.gtf" \
  "python benchmark/run_polars_bio_count.py hepg2_rep1_sorted.bam gencode.v49.annotation.gtf"
```

Run featureCounts with `-T 1` (single thread) first to produce an apples-to-apples
comparison. Then separately run featureCounts with `-T 8` to show its multi-threaded
ceiling. polars-bio can be run with `POLARS_MAX_THREADS=1` and then with default
parallelism for the same comparison.

### Peak RSS Memory
```bash
/usr/bin/time -v featureCounts ... 2>&1 | grep "Maximum resident"
/usr/bin/time -v htseq-count  ... 2>&1 | grep "Maximum resident"
/usr/bin/time -v python benchmark/run_polars_bio_count.py ... 2>&1 | grep "Maximum resident"
```

On macOS use `gtime -v` (from `brew install gnu-time`). Report in MB. This is the
number to headline: polars-bio's streaming evaluation means peak RSS should scale
with partition size, not file size.

### Correctness Validation
1. Run featureCounts and HTSeq with identical GTF and BAM to produce two count matrices
2. Run polars-bio to produce its count matrix
3. Compute Pearson and Spearman correlation of polars-bio counts vs featureCounts counts across all genes
4. Report number of genes with exact count match, off-by-one, and outliers
5. Investigate and document discrepant genes (likely ambiguous/multi-gene-overlap reads)

Target: Spearman r > 0.999 vs featureCounts for unambiguous genes.

### Reporting Structure (Notebook Sections)
1. Dataset and environment description (cell line, read count, GTF release, tool versions, CPU model)
2. Timing comparison table + bar chart (single-thread and multi-thread)
3. Memory comparison table + bar chart
4. Count correlation scatter plot (polars-bio vs featureCounts, log scale)
5. Discrepancy analysis (ambiguous reads, genes with disagreement)
6. Scaling table (1, 2, 4, 8 threads for featureCounts and polars-bio)

---

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| subread 2.1.1 | Python 3.10+ (CLI only, no Python dep) | Install via bioconda; do not import as Python package |
| HTSeq 2.0.5 | Python 3.8-3.12 | Check Python 3.13 compatibility before using in polars-bio dev env |
| GENCODE v49 GTF | featureCounts 2.0+ | GENCODE v49 uses GRCh38.p14; ensure BAM was aligned to GRCh38 (not hg19) |
| hyperfine 1.20.0 | Any OS | macOS via `brew install hyperfine`; Linux via cargo or conda-forge |
| memory-profiler 0.61.0 | Python 3.10+ | Already in polars-bio dev deps; uses psutil backend by default |

---

## Sources

- [Subread SourceForge — version 2.1.0 released March 2025, 2.0.8 November 2024](https://subread.sourceforge.net/) — MEDIUM confidence (search result, not direct docs page)
- [Bioconductor Rsubread v2.22.1/Subread v2.1.1 Users Guide (April 2025)](https://bioconductor.org/packages/release/bioc/vignettes/Rsubread/inst/doc/SubreadUsersGuide.pdf) — HIGH confidence
- [HTSeq 2.0.5 official documentation](https://htseq.readthedocs.io/) — HIGH confidence
- [hyperfine GitHub releases — v1.20.0](https://github.com/sharkdp/hyperfine/releases) — HIGH confidence
- [GENCODE Human Release 49 (current)](https://www.gencodegenes.org/human/) — HIGH confidence, verified via direct fetch
- [ENCODE experiment ENCSR329MHM (HepG2, SE76)](https://www.encodeproject.org/experiments/ENCSR329MHM/) — HIGH confidence, verified via direct fetch
- [ENCODE experiment ENCSR000CPH (K562, PE76) — excluded as paired-end](https://www.encodeproject.org/experiments/ENCSR000CPH/) — HIGH confidence
- [featureCounts vs HTSeq methodology differences — CVR Bioinformatics](https://bioinformatics.cvr.ac.uk/featurecounts-or-htseq-count/) — MEDIUM confidence
- [featureCounts original paper — Bioinformatics 2014](https://academic.oup.com/bioinformatics/article/30/7/923/232889) — HIGH confidence (peer-reviewed)
- [polars-bio performance page — existing benchmark methodology reference](https://biodatageeks.org/polars-bio/performance/) — HIGH confidence
- [memory-profiler PyPI](https://pypi.org/project/memory-profiler/) — HIGH confidence

---

*Stack research for: RNA-seq feature counting benchmark (polars-bio milestone)*
*Researched: 2026-03-20*
