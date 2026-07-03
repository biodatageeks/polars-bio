---
draft: false
date:
  created: 2026-07-02
categories:
  - performance
  - benchmarks
---

# Streaming FastQC in polars-bio: exact, and 15× faster

[FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/) is the de-facto first look at any sequencing run — but it is a single-threaded Java tool, and the fast Rust reimplementations tend to trade away correctness. polars-bio now runs the **full FastQC module suite as a single streaming pass** over FASTQ, computed on Apache DataFusion: **bit-exact against FastQC 0.12.1**, and a fraction of the time and memory.

<!-- more -->

## What shipped

Each QC module is a streaming accumulator (`update` → `merge` → `finalize`) folded over Arrow batches, so the work parallelizes across partitions and merges **exactly**. All **12 core FastQC modules** are implemented and validated bit-for-bit against FastQC 0.12.1 golden output. One call computes them all in a single out-of-core pass:

```python
import polars_bio as pb

qc = pb.fastqc("reads_R1.fastq.gz")
qc.per_base_quality.collect()   # any module as a LazyFrame
qc.summary().collect()          # PASS / WARN / FAIL per module

# ...or as SQL
pb.sql("SELECT * FROM fastqc('reads_R1.fastq.gz')").collect()
```

## The dataset

We benchmark on a **real, citable clinical run** rather than a synthetic file: [**SRR39421268**](https://www.ncbi.nlm.nih.gov/sra/SRR39421268), a *Homo sapiens* HER2 breast-cancer targeted-capture (exome) library.

| Field | Value |
|---|---|
| Run | SRR39421268 (experiment SRX34138143) |
| Study / BioProject | SRP714544 / [PRJNA1484801](https://www.ncbi.nlm.nih.gov/bioproject/PRJNA1484801) |
| BioSample | SAMN61257782 (`HER2_PT06_OP_N`) |
| Organism | *Homo sapiens* |
| Instrument | Illumina NovaSeq 6000 |
| Library | Targeted-Capture (hybrid selection), genomic, paired |
| Reads | 32,167,982 pairs → **64,335,964 reads @ 101 bp** (~6.5 Gbp) |
| Size | 3.3 GB BGZF |

```bash
prefetch SRR39421268
fasterq-dump --split-spot SRR39421268 | bgzip -i > SRR39421268.fastq.gz  # BGZF + .gzi index
```

The `-i`/reindex step writes the BGZF **`.gzi` index** — that is what lets polars-bio split the file and scan it in parallel. All three tools are compared on the 11 modules FastQC runs by default.[^1]

## Environment

All three tools were run on the same laptop; wall-clock is best-of-two warm runs, memory via `/usr/bin/time -l` (footprint) with the FastQC JVM sampled separately.

| | |
|---|---|
| Machine | Apple **M3 Max** — 16 cores (12 performance + 4 efficiency) |
| Memory | 64 GB |
| OS | macOS 15.6 (Darwin 24.6.0), arm64 |
| polars-bio | 0.32.0 (FastQC Phase 1) · Python 3.12 |
| FastQC | 0.12.1 (OpenJDK) |
| RastQC | 0.1.0 |
| Data prep | sra-tools 3.4.1 · htslib/bgzip 1.21 |

**Tools compared**

| Tool | Source | Reference |
|---|---|---|
| FastQC | [s-andrews/FastQC](https://github.com/s-andrews/FastQC) | Andrews S. (2010), [FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/), Babraham Bioinformatics |
| RastQC | [Huang-lab/RastQC](https://github.com/Huang-lab/RastQC) | Huang K-L, *RastQC: A fast, Rust-based quality control tool…*, [bioRxiv (2026)](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2) |
| polars-bio | [biodatageeks/polars-bio](https://github.com/biodatageeks/polars-bio) | this post |

**How each tool was run** — all three compute the same **11 default FastQC modules** (Kmer Content off):

```bash
# Fetch + convert to an indexed BGZF (.gzi lets polars-bio split it for parallel scan)
prefetch SRR39421268
fasterq-dump --split-spot SRR39421268 | bgzip -i > reads.fastq.gz

# FastQC — 11 default modules, single-threaded
fastqc --nogroup reads.fastq.gz

# RastQC — disable its default Kmer Content so it runs the same 11 modules
printf 'kmer\tignore\t1\n' >> limits.txt
rastqc -t 8 --limits limits.txt reads.fastq.gz          # -t 1/2/4/8

# polars-bio — N partitions (target_partitions = 1/2/4/8)
python - <<'PY'
import polars_bio as pb
pb.set_option("datafusion.execution.target_partitions", "8")
pb.fastqc("reads.fastq.gz").tidy.collect()
PY
```

## Performance

Lower is better.

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC (1 thread)</span></div><svg viewBox="0 0 900 330" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Wall-clock time by core count">
<line x1="50" y1="290.0" x2="878" y2="290.0" stroke="#eef1f4"/>
<text x="42" y="293.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="50" y1="224.0" x2="878" y2="224.0" stroke="#eef1f4"/>
<text x="42" y="227.0" font-size="11" fill="#8b94a0" text-anchor="end">60</text>
<line x1="50" y1="158.0" x2="878" y2="158.0" stroke="#eef1f4"/>
<text x="42" y="161.0" font-size="11" fill="#8b94a0" text-anchor="end">120</text>
<line x1="50" y1="92.0" x2="878" y2="92.0" stroke="#eef1f4"/>
<text x="42" y="95.0" font-size="11" fill="#8b94a0" text-anchor="end">180</text>
<line x1="50" y1="26.0" x2="878" y2="26.0" stroke="#eef1f4"/>
<text x="42" y="29.0" font-size="11" fill="#8b94a0" text-anchor="end">240</text>
<text x="42" y="18" font-size="11" fill="#8b94a0" text-anchor="end">sec</text>
<text x="50.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">1</text>
<text x="326.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">2</text>
<text x="602.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">4</text>
<text x="878.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">8</text>
<line x1="50" y1="76.9" x2="878" y2="76.9" stroke="#E69F00" stroke-width="2" stroke-dasharray="2 5"/>
<text x="878" y="68.9" font-size="11.5" font-weight="600" fill="#E69F00" text-anchor="end">FastQC — 193.7s</text>
<polyline points="50.0,183.4 326.0,235.4 602.0,262.4 878.0,275.8" fill="none" stroke="#0072B2" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="183.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="50.0" y="201.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">96.9</text>
<circle cx="326.0" cy="235.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="326.0" y="253.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">49.6</text>
<circle cx="602.0" cy="262.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="602.0" y="280.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">25.1</text>
<circle cx="878.0" cy="275.8" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="878.0" y="293.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">12.9</text>
<polyline points="50.0,43.8 326.0,162.9 602.0,224.2 878.0,229.6" fill="none" stroke="#009E73" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="43.8" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="50.0" y="32.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">223.8</text>
<circle cx="326.0" cy="162.9" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="326.0" y="151.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">115.5</text>
<circle cx="602.0" cy="224.2" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="602.0" y="213.2" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">59.8</text>
<circle cx="878.0" cy="229.6" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="878.0" y="218.6" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">54.9</text>
</svg></div>

polars-bio scales **7.5×** to **12.9 s at 8 cores** — **15× faster than FastQC** (194 s, single-threaded) and **4.3× faster than RastQC's best** (54.9 s; RastQC plateaus at 4 threads). polars-bio is faster than both at every thread count here.

| cores / threads | polars-bio | RastQC | FastQC |
|---:|---:|---:|---:|
| 1 | 96.9 s | 223.8 s | 193.7 s (1 thr) |
| 2 | 49.6 s | 115.5 s | — |
| 4 | 25.1 s | 59.8 s | — |
| 8 | **12.9 s** | 54.9 s | — |

!!! note "One bug this benchmark caught"
    The first run on this 64M-read file used tens of GB of memory and ran slowly. The cause: `duplication_levels`/`overrepresented` tracked *every* distinct sequence. Adopting FastQC's own **100,000-unique observation cutoff** bounded them (**~20 GB → 382 MB**) and, as a bonus, made them match FastQC's duplication *estimate* exactly. The numbers above are post-fix.

## Correctness

Speed is easy; being *right* is the hard part. Measured against the FastQC 0.12.1 golden output, polars-bio is **bit-exact** on every deterministic module (and byte-identical across core counts); **RastQC** — the other fast tool — is close on most but not bit-exact, and one module is badly wrong:

| Module | polars-bio | RastQC |
|---|---|---|
| per_base_quality | ✅ **EXACT** | ≈ float tol (~5e-3)[^3] |
| **per_sequence_quality** | ✅ **EXACT** | ❌ **~24M reads misbinned** |
| per_base_content | ✅ **EXACT** | ≈ float tol |
| per_sequence_gc | ✅ **EXACT** | ✅ EXACT |
| per_base_n | ✅ **EXACT** | ≈ float tol |
| sequence_length | ✅ **EXACT** | ✅ EXACT |
| overrepresented | ✅ **EXACT** | ✅ EXACT |
| adapter_content | ✅ **EXACT** | ≈ (different position grouping) |
| basic_statistics | ✅ **EXACT**[^2] | ⚠️ %GC rounds vs truncates |
| duplication_levels | ✅ matches FastQC's 100k estimate | ≈ float tol |
| per_tile_quality | ✅ exact* (\*FastQC subsamples 10%) | ≈ float tol |

RastQC's headline failure is **per-sequence quality**: a mean-quality rounding error shifts the peak bin, misbinning **~24 million reads** (a third of the file). polars-bio reproduces FastQC exactly:

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span></div><svg viewBox="0 0 900 320" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Per-sequence quality histogram">
<line x1="52" y1="282.0" x2="884" y2="282.0" stroke="#eef1f4"/>
<text x="44" y="285.0" font-size="11" fill="#8b94a0" text-anchor="end">0M</text>
<line x1="52" y1="194.7" x2="884" y2="194.7" stroke="#eef1f4"/>
<text x="44" y="197.7" font-size="11" fill="#8b94a0" text-anchor="end">14M</text>
<line x1="52" y1="107.3" x2="884" y2="107.3" stroke="#eef1f4"/>
<text x="44" y="110.3" font-size="11" fill="#8b94a0" text-anchor="end">28M</text>
<line x1="52" y1="20.0" x2="884" y2="20.0" stroke="#eef1f4"/>
<text x="44" y="23.0" font-size="11" fill="#8b94a0" text-anchor="end">42M</text>
<rect x="190.0" y="43.5" width="44.0" height="238.5" rx="4" fill="#E69F00"/>
<text x="212.0" y="37.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">38.24M</text>
<rect x="238.0" y="43.5" width="44.0" height="238.5" rx="4" fill="#0072B2"/>
<text x="260.0" y="37.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">38.24M</text>
<rect x="286.0" y="194.2" width="44.0" height="87.8" rx="4" fill="#009E73"/>
<text x="308.0" y="188.2" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">14.07M</text>
<text x="260.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">Q36 (mean quality 36)</text>
<rect x="606.0" y="216.7" width="44.0" height="65.3" rx="4" fill="#E69F00"/>
<text x="628.0" y="210.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">10.47M</text>
<rect x="654.0" y="216.7" width="44.0" height="65.3" rx="4" fill="#0072B2"/>
<text x="676.0" y="210.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">10.47M</text>
<rect x="702.0" y="39.5" width="44.0" height="242.5" rx="4" fill="#009E73"/>
<text x="724.0" y="33.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">38.87M</text>
<text x="676.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">Q37 (mean quality 37)</text>
</svg></div>

| mean quality | FastQC | polars-bio | RastQC |
|---|---:|---:|---:|
| Q36 | 38,239,995 | 38,239,995 | 14,072,517 |
| Q37 | 10,472,730 | 10,472,730 | 38,869,411 |

## Memory

The fair metric is *private* memory (anonymous footprint); polars-bio's larger resident-set is mostly the memory-mapped file — reclaimable OS page cache, not pressure.

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>private footprint</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#b9c2cc;display:inline-block"></span>max RSS</span></div><svg viewBox="0 0 900 320" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Peak memory by tool">
<line x1="52" y1="282.0" x2="884" y2="282.0" stroke="#eef1f4"/>
<text x="44" y="285.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="52" y1="216.5" x2="884" y2="216.5" stroke="#eef1f4"/>
<text x="44" y="219.5" font-size="11" fill="#8b94a0" text-anchor="end">300</text>
<line x1="52" y1="151.0" x2="884" y2="151.0" stroke="#eef1f4"/>
<text x="44" y="154.0" font-size="11" fill="#8b94a0" text-anchor="end">600</text>
<line x1="52" y1="85.5" x2="884" y2="85.5" stroke="#eef1f4"/>
<text x="44" y="88.5" font-size="11" fill="#8b94a0" text-anchor="end">900</text>
<line x1="52" y1="20.0" x2="884" y2="20.0" stroke="#eef1f4"/>
<text x="44" y="23.0" font-size="11" fill="#8b94a0" text-anchor="end">1200</text>
<rect x="144.7" y="198.6" width="44.0" height="83.4" rx="4" fill="#0072B2"/>
<text x="166.7" y="192.6" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">382</text>
<rect x="192.7" y="44.9" width="44.0" height="237.1" rx="4" fill="#b9c2cc"/>
<text x="214.7" y="38.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">1086</text>
<text x="190.7" y="307" font-size="11" fill="#5b6470" text-anchor="middle">polars-bio (8c)</text>
<rect x="422.0" y="194.9" width="44.0" height="87.1" rx="4" fill="#009E73"/>
<text x="444.0" y="188.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">399</text>
<rect x="470.0" y="139.2" width="44.0" height="142.8" rx="4" fill="#b9c2cc"/>
<text x="492.0" y="133.2" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">654</text>
<text x="468.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">RastQC (8t)</text>
<rect x="723.3" y="134.8" width="44.0" height="147.2" rx="4" fill="#b9c2cc"/>
<text x="745.3" y="128.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">674</text>
<text x="745.3" y="307" font-size="11" fill="#5b6470" text-anchor="middle">FastQC (JVM)</text>
</svg></div>

polars-bio's true allocation is **382 MB** at 8 cores — in the same league as RastQC (399 MB) and well under FastQC's ~674 MB JVM. Its higher RSS (1086 MB) is mmap'd file pages.

| tool (config) | private footprint (MB) | max RSS (MB) |
|---|---:|---:|
| polars-bio (1 core) | 173 | 517 |
| polars-bio (8 cores) | 382 | 1086 |
| RastQC (1 thread) | 58 | 156 |
| RastQC (8 threads) | 399 | 654 |
| FastQC (JVM) | — | 674 |

## It generalizes: the RastQC paper's own datasets

The 64M-read exome above is a single run. To check the pattern holds across sizes, we repeated the matched comparison — all three tools, the same 11 default modules — on three short-read libraries taken from the [RastQC preprint](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2) itself: **0.72M**, **4.3M**, and **24.8M** reads, thread-scaled 1 → 8. (All fetched with `prefetch` + `fasterq-dump`, converted to indexed BGZF; FastQC single-threaded as it has no parallel mode.)

The largest, **DRR013000** (24.8M reads), tells the whole story:

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC (1 thread)</span></div><svg viewBox="0 0 900 330" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Wall-clock time by core count">
<line x1="50" y1="290.0" x2="878" y2="290.0" stroke="#eef1f4"/>
<text x="42" y="293.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="50" y1="252.3" x2="878" y2="252.3" stroke="#eef1f4"/>
<text x="42" y="255.3" font-size="11" fill="#8b94a0" text-anchor="end">10</text>
<line x1="50" y1="214.6" x2="878" y2="214.6" stroke="#eef1f4"/>
<text x="42" y="217.6" font-size="11" fill="#8b94a0" text-anchor="end">20</text>
<line x1="50" y1="176.9" x2="878" y2="176.9" stroke="#eef1f4"/>
<text x="42" y="179.9" font-size="11" fill="#8b94a0" text-anchor="end">30</text>
<line x1="50" y1="139.1" x2="878" y2="139.1" stroke="#eef1f4"/>
<text x="42" y="142.1" font-size="11" fill="#8b94a0" text-anchor="end">40</text>
<line x1="50" y1="101.4" x2="878" y2="101.4" stroke="#eef1f4"/>
<text x="42" y="104.4" font-size="11" fill="#8b94a0" text-anchor="end">50</text>
<line x1="50" y1="63.7" x2="878" y2="63.7" stroke="#eef1f4"/>
<text x="42" y="66.7" font-size="11" fill="#8b94a0" text-anchor="end">60</text>
<line x1="50" y1="26.0" x2="878" y2="26.0" stroke="#eef1f4"/>
<text x="42" y="29.0" font-size="11" fill="#8b94a0" text-anchor="end">70</text>
<text x="42" y="18" font-size="11" fill="#8b94a0" text-anchor="end">sec</text>
<text x="50.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">1</text>
<text x="326.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">2</text>
<text x="602.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">4</text>
<text x="878.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">8</text>
<line x1="50" y1="49.2" x2="878" y2="49.2" stroke="#E69F00" stroke-width="2" stroke-dasharray="2 5"/>
<text x="878" y="41.2" font-size="11.5" font-weight="600" fill="#E69F00" text-anchor="end">FastQC — 63.8s</text>
<polyline points="50.0,182.0 326.0,235.1 602.0,262.0 878.0,274.5" fill="none" stroke="#0072B2" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="182.0" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="50.0" y="200.0" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">28.6</text>
<circle cx="326.0" cy="235.1" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="326.0" y="253.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">14.6</text>
<circle cx="602.0" cy="262.0" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="602.0" y="280.0" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">7.43</text>
<circle cx="878.0" cy="274.5" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="878.0" y="292.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">4.11</text>
<polyline points="50.0,32.0 326.0,157.8 602.0,207.3 878.0,206.5" fill="none" stroke="#009E73" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="32.0" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="50.0" y="21.0" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">68.4</text>
<circle cx="326.0" cy="157.8" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="326.0" y="146.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">35.1</text>
<circle cx="602.0" cy="207.3" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="602.0" y="196.3" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">21.9</text>
<circle cx="878.0" cy="206.5" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="878.0" y="195.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">22.1</text>
</svg></div>

| cores / threads | polars-bio | RastQC | FastQC |
|---:|---:|---:|---:|
| 1 | 28.63 s | 68.42 s | 63.84 s (1 thr) |
| 2 | 14.57 s | 35.06 s | — |
| 4 | 7.43 s | 21.92 s | — |
| 8 | **4.11 s** | 22.15 s | — |

polars-bio scales ~7× to **4.1 s at 8 cores — 15.5× faster than FastQC** and **5.3× faster than RastQC's best** (21.9 s at 4 threads, after which RastQC plateaus). Even single-threaded, polars-bio (28.6 s) is more than 2× faster than FastQC.

Across all three sizes the ranking never changes: polars-bio is fastest at every thread count, and its **single-threaded** time already beats FastQC on every run.

| dataset | reads | FastQC (1t) | RastQC (best) | polars-bio (best) | pb vs FastQC | pb vs RastQC |
|---|---:|---:|---:|---:|---:|---:|
| DRR609229 R1 | 0.72M | 3.49 s | 2.29 s (1t) | **0.12 s** (8c) | 29.1× | 19.1× |
| ERR5897746 R1 | 4.3M | 16.63 s | 4.81 s (8t) | **1.30 s** (8c) | 12.8× | 3.7× |
| DRR013000 R1 | 24.8M | 63.84 s | 21.92 s (4t) | **4.11 s** (8c) | 15.5× | 5.3× |

Paired R2 mates behave identically (full 1/2/4/8 grid in the repo). Two things stand out in RastQC: on the small 0.72M file it shows **no thread benefit at all** (~2.3 s flat, 1→8t), and its **single-threaded time is *slower* than FastQC** on both larger runs — whereas polars-bio scales cleanly and leads throughout.

## The takeaway

On a 64-million-read clinical exome run, polars-bio is the only tool that is **both** exact against FastQC and genuinely fast: **15× faster than FastQC**, **4.3× faster than RastQC**, at ~380 MB of real memory — while RastQC, the other fast option, silently misbins a third of the reads in one module. That lead is not an artefact of one file — it holds from 0.7M to 64M reads. And it is just another table in the engine: `SELECT * FROM fastqc('reads.fastq.gz')`.

[^1]: FastQC ships Kmer Content disabled by default, so the cross-tool comparison covers the 11 default modules. polars-bio implements Kmer Content too (12/12), parity-tested separately; its FastQC-style top-20 output is inherently non-deterministic on real data — a known property of FastQC's Kmer module.
[^2]: FastQC prints `%GC` as a truncated integer; our full-precision value floors to FastQC's. On this run all three report `%GC = 50`.
[^3]: RastQC uses float32 internally, so on the deterministic modules it agrees with FastQC only to ~1e-3, not bit-exactly. "≈ float tol" marks that; `per_sequence_quality` (verified on this run) and `%GC` are genuine algorithmic differences, not precision.
