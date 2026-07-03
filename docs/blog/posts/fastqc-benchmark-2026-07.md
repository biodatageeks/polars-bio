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

Speed is easy; being *right* is the hard part. polars-bio is **bit-exact with FastQC** on every deterministic module, and its parallel output is byte-identical across core counts:

| Module | polars-bio vs FastQC 0.12.1 |
|---|---|
| per_base_quality · per_sequence_quality | ✅ **EXACT** |
| per_base_content · per_sequence_gc | ✅ **EXACT** |
| per_base_n · sequence_length | ✅ **EXACT** |
| overrepresented · adapter_content | ✅ **EXACT** |
| basic_statistics | ✅ **EXACT**[^2] |
| duplication_levels | ✅ matches FastQC's 100k estimate |
| per_tile_quality | ✅ exact* (\*FastQC subsamples 10% of reads) |

The cautionary contrast is **RastQC**, the other fast tool. Its per-sequence-quality histogram is badly wrong — a mean-quality rounding error shifts the peak bin, misbinning **~24 million reads** (a third of the file). polars-bio reproduces FastQC exactly:

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

## The takeaway

On a 64-million-read clinical exome run, polars-bio is the only tool that is **both** exact against FastQC and genuinely fast: **15× faster than FastQC**, **4.3× faster than RastQC**, at ~380 MB of real memory — while RastQC, the other fast option, silently misbins a third of the reads in one module. And it is just another table in the engine: `SELECT * FROM fastqc('reads.fastq.gz')`.

[^1]: FastQC ships Kmer Content disabled by default, so the cross-tool comparison covers the 11 default modules. polars-bio implements Kmer Content too (12/12), parity-tested separately; its FastQC-style top-20 output is inherently non-deterministic on real data — a known property of FastQC's Kmer module.
[^2]: FastQC prints `%GC` as a truncated integer; our full-precision value floors to FastQC's. On this run all three report `%GC = 50`.
