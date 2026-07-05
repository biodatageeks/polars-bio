---
draft: false
date:
  created: 2026-07-02
categories:
  - performance
  - benchmarks
---

# Streaming FastQC in polars-bio: compatible and scalable

[FastQC](https://www.bioinformatics.babraham.ac.uk/projects/fastqc/) is the de-facto first look at any sequencing run — but it is a single-threaded Java tool, and the fast Rust reimplementations tend to trade away correctness. polars-bio now runs the **full FastQC module suite as a single streaming pass** over FASTQ, computed on Apache DataFusion: **FastQC 0.12.1-compatible under explicit parity checks** (bit-exact for deterministic/count modules, tolerance-checked for floating metrics), and a fraction of the time and memory.

<!-- more -->

## What shipped

Each QC module is a streaming accumulator (`update` → `merge` → `finalize`) folded over Arrow batches, so the work parallelizes across partitions and merges deterministically. All **12 core FastQC modules** are implemented and validated against FastQC 0.12.1 golden output — **bit-for-bit on deterministic/count modules**, with floating metrics checked to tight tolerances (Kmer Content matches the FastQC top-20 set at single-partition parity; FastQC's top-20 k-mer list is inherently order-sensitive on real data). One call computes them all in a single out-of-core pass:

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
fasterq-dump --split-spot -Z SRR39421268 | bgzip -c > SRR39421268.fastq.gz  # -Z streams to stdout; bgzip -c writes BGZF
bgzip -r SRR39421268.fastq.gz                                               # build the .gzi index (what lets polars-bio split for parallel scan)
```

The `bgzip -r` reindex step writes the BGZF **`.gzi` index** — that is what lets polars-bio split the file and scan it in parallel. All three tools are compared on the 11 modules FastQC runs by default.[^1]

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
fasterq-dump --split-spot -Z SRR39421268 | bgzip -c > reads.fastq.gz  # -Z → stdout; bgzip -c → BGZF
bgzip -r reads.fastq.gz                                               # write the .gzi index (required for parallel scan)

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

## Performance: four real datasets, 0.7M → 64M reads

We benchmark on **four** real, citable libraries: our flagship 64M-read clinical HER2 exome (**SRR39421268**) plus the **three short-read runs from the [RastQC preprint](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2)** itself (0.72M, 4.3M, 24.8M reads). All three tools compute the same 11 default modules; lower is better.

### Our flagship: the 64M-read clinical exome

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC (1 thread)</span></div><svg viewBox="0 0 900 330" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Wall-clock time by core count">
<line x1="50" y1="290.0" x2="878" y2="290.0" stroke="#eef1f4"/>
<text x="42" y="293.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="50" y1="237.2" x2="878" y2="237.2" stroke="#eef1f4"/>
<text x="42" y="240.2" font-size="11" fill="#8b94a0" text-anchor="end">50</text>
<line x1="50" y1="184.4" x2="878" y2="184.4" stroke="#eef1f4"/>
<text x="42" y="187.4" font-size="11" fill="#8b94a0" text-anchor="end">100</text>
<line x1="50" y1="131.6" x2="878" y2="131.6" stroke="#eef1f4"/>
<text x="42" y="134.6" font-size="11" fill="#8b94a0" text-anchor="end">150</text>
<line x1="50" y1="78.8" x2="878" y2="78.8" stroke="#eef1f4"/>
<text x="42" y="81.8" font-size="11" fill="#8b94a0" text-anchor="end">200</text>
<line x1="50" y1="26.0" x2="878" y2="26.0" stroke="#eef1f4"/>
<text x="42" y="29.0" font-size="11" fill="#8b94a0" text-anchor="end">250</text>
<text x="42" y="18" font-size="11" fill="#8b94a0" text-anchor="end">sec</text>
<text x="50.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">1</text>
<text x="326.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">2</text>
<text x="602.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">4</text>
<text x="878.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">8</text>
<line x1="50" y1="65.6" x2="878" y2="65.6" stroke="#E69F00" stroke-width="2" stroke-dasharray="2 5"/>
<text x="878" y="57.6" font-size="11.5" font-weight="600" fill="#E69F00" text-anchor="end">FastQC — 212.5s</text>
<polyline points="50.0,165.7 326.0,226.5 602.0,258.1 878.0,273.7" fill="none" stroke="#0072B2" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="165.7" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="50.0" y="183.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">117.7</text>
<circle cx="326.0" cy="226.5" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="326.0" y="244.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">60.1</text>
<circle cx="602.0" cy="258.1" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="602.0" y="276.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">30.2</text>
<circle cx="878.0" cy="273.7" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="878.0" y="291.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">15.5</text>
<polyline points="50.0,32.8 326.0,158.5 602.0,224.8 878.0,229.7" fill="none" stroke="#009E73" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="32.8" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="50.0" y="21.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">243.6</text>
<circle cx="326.0" cy="158.5" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="326.0" y="147.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">124.6</text>
<circle cx="602.0" cy="224.8" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="602.0" y="213.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">61.8</text>
<circle cx="878.0" cy="229.7" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="878.0" y="218.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">57.1</text>
</svg></div>

polars-bio scales **7.6×** to **15.5 s at 8 cores** — **13.8× faster than FastQC** (213 s, single-threaded) and **3.7× faster than RastQC's best** (57.1 s; RastQC barely improves past 4 threads). polars-bio is faster than both at every thread count — and does it at **~2.5× less total CPU** than RastQC.

Each row lists **wall** (elapsed) and **total CPU** (user + system CPU-seconds — the actual machine work):

| cores / threads | pb wall | pb CPU-s | RastQC wall | RastQC CPU-s | FastQC |
|---:|---:|---:|---:|---:|---:|
| 1 | 117.7 s | 117.7 | 243.6 s | 298.1 | 212.5 s (1 thr) |
| 2 | 60.1 s | 119.9 | 124.6 s | 303.1 | — |
| 4 | 30.2 s | 120.3 | 61.8 s | 302.5 | — |
| 8 | **15.5 s** | 120.8 | 57.1 s | 302.6 | — |

polars-bio's **total CPU stays flat** (~120 CPU-s) as threads rise — each partition adds ~1 core of real work, so wall time falls near-linearly. RastQC spends **~2.5× more total CPU** (~300 CPU-s) yet its wall time floors at ~57 s: it is throwing more machine at the problem and getting less back.

### It also holds on the RastQC paper's datasets

The flagship is a single run. To check the pattern holds across sizes, we repeated the matched comparison — all three tools, the same 11 default modules — on the three short-read libraries taken from the [RastQC preprint](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2): **0.72M**, **4.3M**, and **24.8M** reads, thread-scaled 1 → 8. (All fetched with `prefetch` + `fasterq-dump`, converted to indexed BGZF; FastQC single-threaded as it has no parallel mode.)

The largest, **DRR013000** (24.8M reads), tells the whole story:

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC (1 thread)</span></div><svg viewBox="0 0 900 330" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Wall-clock time by core count">
<line x1="50" y1="290.0" x2="878" y2="290.0" stroke="#eef1f4"/>
<text x="42" y="293.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="50" y1="257.0" x2="878" y2="257.0" stroke="#eef1f4"/>
<text x="42" y="260.0" font-size="11" fill="#8b94a0" text-anchor="end">10</text>
<line x1="50" y1="224.0" x2="878" y2="224.0" stroke="#eef1f4"/>
<text x="42" y="227.0" font-size="11" fill="#8b94a0" text-anchor="end">20</text>
<line x1="50" y1="191.0" x2="878" y2="191.0" stroke="#eef1f4"/>
<text x="42" y="194.0" font-size="11" fill="#8b94a0" text-anchor="end">30</text>
<line x1="50" y1="158.0" x2="878" y2="158.0" stroke="#eef1f4"/>
<text x="42" y="161.0" font-size="11" fill="#8b94a0" text-anchor="end">40</text>
<line x1="50" y1="125.0" x2="878" y2="125.0" stroke="#eef1f4"/>
<text x="42" y="128.0" font-size="11" fill="#8b94a0" text-anchor="end">50</text>
<line x1="50" y1="92.0" x2="878" y2="92.0" stroke="#eef1f4"/>
<text x="42" y="95.0" font-size="11" fill="#8b94a0" text-anchor="end">60</text>
<line x1="50" y1="59.0" x2="878" y2="59.0" stroke="#eef1f4"/>
<text x="42" y="62.0" font-size="11" fill="#8b94a0" text-anchor="end">70</text>
<line x1="50" y1="26.0" x2="878" y2="26.0" stroke="#eef1f4"/>
<text x="42" y="29.0" font-size="11" fill="#8b94a0" text-anchor="end">80</text>
<text x="42" y="18" font-size="11" fill="#8b94a0" text-anchor="end">sec</text>
<text x="50.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">1</text>
<text x="326.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">2</text>
<text x="602.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">4</text>
<text x="878.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">8</text>
<line x1="50" y1="64.4" x2="878" y2="64.4" stroke="#E69F00" stroke-width="2" stroke-dasharray="2 5"/>
<text x="878" y="56.4" font-size="11.5" font-weight="600" fill="#E69F00" text-anchor="end">FastQC — 68.4s</text>
<polyline points="50.0,164.3 326.0,226.0 602.0,257.4 878.0,272.4" fill="none" stroke="#0072B2" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="164.3" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="50.0" y="182.3" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">38.1</text>
<circle cx="326.0" cy="226.0" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="326.0" y="244.0" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">19.4</text>
<circle cx="602.0" cy="257.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="602.0" y="275.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">9.88</text>
<circle cx="878.0" cy="272.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="878.0" y="290.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">5.34</text>
<polyline points="50.0,57.9 326.0,172.8 602.0,216.1 878.0,215.6" fill="none" stroke="#009E73" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="57.9" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="50.0" y="46.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">70.3</text>
<circle cx="326.0" cy="172.8" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="326.0" y="161.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">35.5</text>
<circle cx="602.0" cy="216.1" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="602.0" y="205.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">22.4</text>
<circle cx="878.0" cy="215.6" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="878.0" y="204.6" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">22.6</text>
</svg></div>

| cores / threads | pb wall | pb CPU-s | RastQC wall | RastQC CPU-s | FastQC |
|---:|---:|---:|---:|---:|---:|
| 1 | 38.1 s | 38.1 | 70.3 s | 92.3 | 68.4 s (1 thr) |
| 2 | 19.4 s | 38.5 | 35.5 s | 92.6 | — |
| 4 | 9.9 s | 38.8 | 22.4 s | 93.2 | — |
| 8 | **5.3 s** | 39.2 | 22.6 s | 93.7 | — |

polars-bio scales ~7× to **5.3 s at 8 cores — 12.8× faster than FastQC** and **4.2× faster than RastQC's best** (22.4 s at 4 threads, after which RastQC plateaus). Even single-threaded, polars-bio (38.1 s) is nearly 2× faster than FastQC — and its total CPU (~39 CPU-s) is **less than half** of RastQC's (~93 CPU-s).

### All four, side by side

Across all four sizes the ranking never changes: polars-bio is fastest at every thread count, and its **single-threaded** time already beats FastQC on every run.

Each tool's best config, as **wall / total-CPU-s**:

| dataset | reads | BGZF | FastQC (1t) | RastQC best (wall / CPU) | pb best (wall / CPU) | pb vs FastQC | pb vs RastQC |
|---|---:|---:|---:|---:|---:|---:|---:|
| DRR609229 R1 | 0.72M | 28 MB | 4.56 s | 2.38 s / 2.4 (2t) | **0.14 s** / 1.0 (8c) | 31.9× | 16.6× |
| ERR5897746 R1 | 4.3M | 315 MB | 17.59 s | 5.03 s / 23.7 (4t) | **1.58 s** / 9.1 (8c) | 11.1× | 3.2× |
| DRR013000 R1 | 24.8M | 1.5 GB | 68.35 s | 22.40 s / 93.2 (4t) | **5.34 s** / 39.2 (8c) | 12.8× | 4.2× |
| **SRR39421268** (ours) | 64.3M | 3.3 GB | 212.5 s | 57.1 s / 302.6 (8t) | **15.5 s** / 120.8 (8c) | 13.8× | 3.7× |

Paired R2 mates behave identically (full 1/2/4/8 grid in the repo). Two things stand out in RastQC. On the small 0.72M file it shows **no thread benefit at all** (~2.4 s flat, 1→8t) — but that is *by design*: its ~28 MB BGZF is below RastQC's 50 MB parallelism threshold, so `-t` is a no-op there (polars-bio parallelizes it anyway, down to 0.14 s). On the two larger runs, which *do* clear the threshold, RastQC's **single-threaded time is still *slower* than FastQC** (18.6 vs 17.6 s; 70.3 vs 68.4 s; 243.6 vs 212.5 s) — whereas polars-bio scales cleanly and leads throughout, at 2–2.5× less total CPU.

## Memory

We measure peak memory exactly as [RastQC's own benchmark](https://github.com/Huang-lab/RastQC/blob/main/benchmark/run_benchmark.sh) does — `/usr/bin/time -l`, **maximum resident set size** — applied identically to every tool. Because the rest of this comparison runs the same 11 default modules (Kmer off), we report RastQC **both ways**: its default (Kmer *on*, the config the paper measured) and Kmer *off* (strict 11-module parity). polars-bio always runs the 11-module parallel path; FastQC is single-threaded per file.

```bash
# macOS /usr/bin/time -l, "maximum resident set size" (bytes ÷ 1048576 = MB), same for all
/usr/bin/time -l python - <<'PY'                                  # polars-bio (target_partitions=N)
import polars_bio as pb
pb.set_option("datafusion.execution.target_partitions", "8")
pb.fastqc("reads.fastq.gz").tidy.collect()
PY
/usr/bin/time -l rastqc -t 8 -q --time            -o out reads.fastq.gz   # RastQC default (Kmer on)
/usr/bin/time -l rastqc -t 8 --limits kmer_off.txt -o out reads.fastq.gz  # RastQC 11-module (Kmer off)
/usr/bin/time -l fastqc -t 1 --quiet              -o out reads.fastq.gz   # FastQC (single-threaded JVM)
```

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC</span></div><svg viewBox="0 0 900 300" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Peak RSS on the 64M flagship">
<line x1="52" y1="262.0" x2="884" y2="262.0" stroke="#eef1f4"/>
<text x="44" y="265.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="52" y1="201.5" x2="884" y2="201.5" stroke="#eef1f4"/>
<text x="44" y="204.5" font-size="11" fill="#8b94a0" text-anchor="end">400</text>
<line x1="52" y1="141.0" x2="884" y2="141.0" stroke="#eef1f4"/>
<text x="44" y="144.0" font-size="11" fill="#8b94a0" text-anchor="end">800</text>
<line x1="52" y1="80.5" x2="884" y2="80.5" stroke="#eef1f4"/>
<text x="44" y="83.5" font-size="11" fill="#8b94a0" text-anchor="end">1200</text>
<line x1="52" y1="20.0" x2="884" y2="20.0" stroke="#eef1f4"/>
<text x="44" y="23.0" font-size="11" fill="#8b94a0" text-anchor="end">1600</text>
<rect x="134.0" y="158.7" width="44.0" height="103.3" rx="4" fill="#0072B2"/>
<text x="156.0" y="152.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">683</text>
<text x="156.0" y="287" font-size="11" fill="#5b6470" text-anchor="middle">polars-bio (8t)</text>
<rect x="342.0" y="163.5" width="44.0" height="98.5" rx="4" fill="#009E73"/>
<text x="364.0" y="157.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">651</text>
<text x="364.0" y="287" font-size="11" fill="#5b6470" text-anchor="middle">RastQC kmer-on (8t)</text>
<rect x="550.0" y="29.8" width="44.0" height="232.2" rx="4" fill="#009E73"/>
<text x="572.0" y="23.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">1535</text>
<text x="572.0" y="287" font-size="11" fill="#5b6470" text-anchor="middle">RastQC kmer-off (8t)</text>
<rect x="758.0" y="165.8" width="44.0" height="96.2" rx="4" fill="#E69F00"/>
<text x="780.0" y="159.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">636</text>
<text x="780.0" y="287" font-size="11" fill="#5b6470" text-anchor="middle">FastQC (1t)</text>
</svg></div>

Peak RSS (MB) across all four datasets at **1 / 4 / 8 threads** (FastQC is single-threaded, so one value):

| dataset | reads | polars-bio | RastQC (Kmer on) | RastQC (Kmer off) | FastQC (1t) |
|---|---:|---:|---:|---:|---:|
| DRR609229 | 0.72M | 268 / 266 / 287 | 52 / 52 / 52 | 27 / 27 / 27 | 466 |
| ERR5897746 | 4.3M | 292 / 433 / 620 | 122 / 358 / 590 | 144 / 360 / 518 | 559 |
| DRR013000 | 24.8M | 302 / 466 / 630 | 146 / 344 / 622 | 367 / 687 / 858 | 528 |
| SRR39421268 | 64M | 330 / 497 / 683 | 167 / 419 / 651 | **757 / 1483 / 1535** | 636 |

Four honest takeaways — no tool wins memory outright:

1. **RastQC's default is genuinely lean** and reproduces the paper: 52 / 358 / 344 MB at 4 threads vs its reported 49 / 332 / 315 — which confirms our method matches theirs.
2. **A RastQC quirk: turning Kmer *off* *raises* its memory** 2–3× on large files (flagship at 4 threads: 419 → 1483 MB). Disabling a module via `--limits` drops RastQC's duplication-memory bounding — so the strict 11-module-parity comparison paradoxically *penalizes* RastQC. We flag it rather than lean on it.
3. **polars-bio's memory is the most predictable** — 270–680 MB across a **90× range** of input sizes, growing gently with threads and barely at all with file size (its duplication tracking is bounded by FastQC's 100k-unique cutoff). It is competitive with RastQC's default and never balloons.
4. **FastQC's JVM RSS (~470–640 MB at 1 thread)** sits in the paper's range, but is heap/GC-driven, not data-driven: `fastqc -t 4` reserves ~250 MB per worker and inflates a single-file run to ~900 MB for **zero** speedup, because **FastQC's `-t` is per-file, not intra-file**. That is true in the source, not just observed: [`AnalysisQueue`](https://github.com/s-andrews/FastQC/blob/v0.12.1/uk/ac/babraham/FastQC/Analysis/AnalysisQueue.java#L44-L46) sets its slot count to `threads`, then [starts one `Thread` per whole file](https://github.com/s-andrews/FastQC/blob/v0.12.1/uk/ac/babraham/FastQC/Analysis/AnalysisQueue.java#L60-L67) (one `AnalysisRunner` = one `SequenceFile`), and [`AnalysisRunner.run()`](https://github.com/s-andrews/FastQC/blob/v0.12.1/uk/ac/babraham/FastQC/Analysis/AnalysisRunner.java#L64-L91) reads that file's sequences serially and applies every module in a plain loop — no thread is spawned inside a file. Measured: **eff cores = 1.0 at `-t 1`, `-t 2`, and `-t 4`** on one file.

## Correct at *every* thread count

FastQC is single-threaded, so "correct" is unambiguous. Both fast tools are multi-threaded, which raises a question the speed charts don't answer: **does the output change with the thread count?** We diffed every module at 1 vs *N* threads on a second run — [ERR5897746](https://www.ebi.ac.uk/ena/browser/view/ERR5897746), 4.25M reads:

- **polars-bio is value-identical** at 1 and 4 partitions after parsing — the checked output values match exactly across partition counts.
- **RastQC is not** — its duplication and k-mer modules return materially different numbers at `-t 1` vs `-t 4`.

Here is the full per-module drift against the FastQC 0.12.1 golden. Each cell is the largest deviation across *every* data point in that module; the per-base rows are re-binned into RastQC's own position bins for a like-for-like comparison (RastQC has no `--nogroup`, so it always groups positions):

| Module | polars-bio | RastQC `-t 1` | RastQC `-t 4` |
|---|:--|:--|:--|
| Total sequences | ✅ exact | ✅ exact | ✅ exact |
| %GC | ✅ exact | ❌ off by 1 | ❌ off by 1 |
| Per base quality | ✅ exact | ✅ ≈exact (0.005 phred) | ✅ ≈exact |
| Per base content | ✅ exact | ✅ exact | ✅ exact |
| Per base N | ✅ exact | ✅ exact | ✅ exact |
| Per sequence GC | ✅ exact | ✅ exact | ✅ exact |
| **Per sequence quality** | ✅ exact | ❌ **671k reads misbinned** | ❌ **671k reads misbinned** |
| Sequence length | ✅ exact | ✅ exact | ✅ exact |
| **Duplication levels** | ✅ exact | ✅ ≈exact (0.005 pts) | ❌ **+6.16 pts total dedup** |
| **Kmer content** | ✅ same top-20; Δobs/exp 0.0019 | ⚠ top k-mer correct; list differs | ❌ **top k-mer wrong** |
| Adapter content | ✅ ≈exact (float eps) | ⚠ diff panel | ⚠ diff panel |
| Per tile / Overrepresented | \- no data on this run \- | | |

RastQC's *per-base* plots are fine — the coarser look is just grouping, not error — and its adapter-content values match after re-binning, although it emits two tail bins per adapter that FastQC does not. But **per-sequence quality is badly wrong**: 671,121 reads land in a Q40 bin FastQC never emits (the same rounding defect that misbinned a third of the 64M run), **%GC is off by one**, and — uniquely — its **duplication and k-mer outputs change with the thread count**. At `-t 1`, RastQC's FastQC-comparable duplication percentages match within rounding (Total Deduplicated Percentage is +0.002 points), but at `-t 4` total dedup jumps from 89.48% to 95.64% (+6.16 points) and the level-1 total bin is +9.95 points. Its most-enriched k-mer is correct at `-t 1` but the top-20 list already differs; at `-t 4`, even the top k-mer is wrong.

polars-bio reproduces FastQC bit-for-bit on deterministic/count modules, stays within tight tolerances for floating metrics, and returns the **same parsed values at the checked partition counts** — its per-partition accumulators merge associatively, and Kmer Content (the one non-associative module) is computed on a single partition, so it is partition-invariant in this comparison too. You never have to ask which thread setting gave you the trustworthy number.

## The takeaway

On a 64-million-read clinical exome run, polars-bio is the only tool that is **both** FastQC-compatible under these parity checks and genuinely fast: **13.8× faster than FastQC**, **3.7× faster than RastQC**, at **~2.5× less total CPU** and with the steadiest memory of the three (~270–680 MB, never ballooning) — while RastQC, the other fast option, silently misbins a third of the reads in one module — and gives different duplication and k-mer numbers depending on the thread count. That lead is not an artefact of one file — it holds from 0.7M to 64M reads. And it is just another table in the engine: `SELECT * FROM fastqc('reads.fastq.gz')`.

[^1]: FastQC ships Kmer Content disabled by default, so the cross-tool comparison covers the 11 default modules. polars-bio implements Kmer Content too (12/12), parity-tested separately; its FastQC-style top-20 output is inherently non-deterministic on real data — a known property of FastQC's Kmer module.
