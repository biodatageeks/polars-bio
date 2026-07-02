---
draft: false
date:
  created: 2026-07-02
categories:
  - performance
  - benchmarks
---

# Streaming FastQC in polars-bio: exact, and 12× faster

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

## Setup

One real file, three tools, same machine (Apple Silicon, arm64).

| Item | Value |
|---|---|
| Input | `partial_reads.fastq.gz` — 523 MB BGZF |
| Reads | 26,527,426 @ 148 bp (Illumina NovaSeq) |
| Oracle | FastQC 0.12.1 |
| Baselines | RastQC 0.1.0, polars-bio (FastQC Phase 1) |
| Modules compared | the 11 FastQC runs by default[^1] |

## Performance

Lower is better.

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC (1 thread)</span></div><svg viewBox="0 0 900 330" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Wall-clock time by core count">
<line x1="50" y1="290.0" x2="878" y2="290.0" stroke="#eef1f4"/>
<text x="42" y="293.0" font-size="11" fill="#8b94a0" text-anchor="end">0</text>
<line x1="50" y1="224.0" x2="878" y2="224.0" stroke="#eef1f4"/>
<text x="42" y="227.0" font-size="11" fill="#8b94a0" text-anchor="end">15</text>
<line x1="50" y1="158.0" x2="878" y2="158.0" stroke="#eef1f4"/>
<text x="42" y="161.0" font-size="11" fill="#8b94a0" text-anchor="end">30</text>
<line x1="50" y1="92.0" x2="878" y2="92.0" stroke="#eef1f4"/>
<text x="42" y="95.0" font-size="11" fill="#8b94a0" text-anchor="end">45</text>
<line x1="50" y1="26.0" x2="878" y2="26.0" stroke="#eef1f4"/>
<text x="42" y="29.0" font-size="11" fill="#8b94a0" text-anchor="end">60</text>
<text x="42" y="18" font-size="11" fill="#8b94a0" text-anchor="end">sec</text>
<text x="50.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">1</text>
<text x="326.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">2</text>
<text x="602.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">4</text>
<text x="878.0" y="316" font-size="11" fill="#8b94a0" text-anchor="middle">8</text>
<line x1="50" y1="44.5" x2="878" y2="44.5" stroke="#E69F00" stroke-width="2" stroke-dasharray="2 5"/>
<text x="878" y="36.5" font-size="11.5" font-weight="600" fill="#E69F00" text-anchor="end">FastQC — 55.8s</text>
<polyline points="50.0,140.4 326.0,213.0 602.0,250.8 878.0,269.6" fill="none" stroke="#0072B2" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="140.4" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="50.0" y="158.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">34.0</text>
<circle cx="326.0" cy="213.0" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="326.0" y="231.0" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">17.5</text>
<circle cx="602.0" cy="250.8" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="602.0" y="268.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">8.90</text>
<circle cx="878.0" cy="269.6" r="5" fill="#fff" stroke="#0072B2" stroke-width="2.5"/>
<text x="878.0" y="287.6" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">4.64</text>
<polyline points="50.0,51.5 326.0,169.4 602.0,221.4 878.0,220.9" fill="none" stroke="#009E73" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>
<circle cx="50.0" cy="51.5" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="50.0" y="40.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">54.2</text>
<circle cx="326.0" cy="169.4" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="326.0" y="158.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">27.4</text>
<circle cx="602.0" cy="221.4" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="602.0" y="210.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">15.6</text>
<circle cx="878.0" cy="220.9" r="5" fill="#fff" stroke="#009E73" stroke-width="2.5"/>
<text x="878.0" y="209.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">15.7</text>
</svg></div>

polars-bio scales near-linearly to **4.64 s at 8 cores** (7.3× over its 1-core time). RastQC plateaus at 4 threads (~15.6 s) — single-stream gzip decompression bounds it. FastQC is single-threaded at ~55.8 s.

| cores / threads | polars-bio (s) | RastQC (s) | FastQC (s) |
|---:|---:|---:|---:|
| 1 | 34.0 | 54.2 | 55.8 |
| 2 | 17.5 | 27.4 | — |
| 4 | 8.9 | 15.6 | — |
| 8 | **4.64** | 15.7 | — |

## Correctness — and a cautionary tale

Speed is easy; being *right* is the hard part. polars-bio is **bit-exact with FastQC** on every deterministic module. The two starred rows are FastQC's *own* approximations (it estimates duplication from the first ~100k sequences, and subsamples 10% of reads for per-tile after the first 10k) — our all-reads results are the exact truth there.

| Module | polars-bio vs FastQC | RastQC vs FastQC |
|---|---|---|
| per_base_quality | ✅ **EXACT** | ~match (5e-3) |
| per_sequence_quality | ✅ **EXACT** | ❌ **diverges · 13.4M misbinned** |
| per_base_content | ✅ **EXACT** | ~match (5e-5) |
| per_sequence_gc | ✅ **EXACT** | ✅ EXACT |
| per_base_n | ✅ **EXACT** | ~match (5e-7) |
| sequence_length | ✅ **EXACT** | ✅ EXACT |
| overrepresented | ✅ **EXACT** | ✅ EXACT |
| adapter_content | ✅ **EXACT** | ~match · groups differ |
| basic_statistics (%GC) | ✅ **EXACT**[^2] | ❌ off by 1 (rounds vs truncates) |
| duplication_levels | exact* | ~match (5e-3) |
| per_tile_quality | exact* | ~match (6e-3) |

The standout is **per-sequence quality**. RastQC gets the read-count histogram badly wrong — it shifts ~13.4 million reads (half the file) into the wrong quality bin via a mean-quality rounding error. polars-bio reproduces FastQC exactly:

<div markdown="0" style="background:#fff;border:1px solid #e4e7ec;border-radius:14px;padding:16px 18px;margin:1rem 0;overflow-x:auto"><div style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.78rem;color:#8b94a0;margin:.4rem 0 .6rem"><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#E69F00;display:inline-block"></span>FastQC</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#0072B2;display:inline-block"></span>polars-bio</span><span style="display:inline-flex;align-items:center;gap:6px;margin-right:18px"><span style="width:11px;height:11px;border-radius:3px;background:#009E73;display:inline-block"></span>RastQC</span></div><svg viewBox="0 0 900 320" style="width:100%;height:auto;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Per-sequence quality histogram">
<line x1="52" y1="282.0" x2="884" y2="282.0" stroke="#eef1f4"/>
<text x="44" y="285.0" font-size="11" fill="#8b94a0" text-anchor="end">0M</text>
<line x1="52" y1="216.5" x2="884" y2="216.5" stroke="#eef1f4"/>
<text x="44" y="219.5" font-size="11" fill="#8b94a0" text-anchor="end">6M</text>
<line x1="52" y1="151.0" x2="884" y2="151.0" stroke="#eef1f4"/>
<text x="44" y="154.0" font-size="11" fill="#8b94a0" text-anchor="end">12M</text>
<line x1="52" y1="85.5" x2="884" y2="85.5" stroke="#eef1f4"/>
<text x="44" y="88.5" font-size="11" fill="#8b94a0" text-anchor="end">18M</text>
<line x1="52" y1="20.0" x2="884" y2="20.0" stroke="#eef1f4"/>
<text x="44" y="23.0" font-size="11" fill="#8b94a0" text-anchor="end">24M</text>
<rect x="190.0" y="109.1" width="44.0" height="172.9" rx="4" fill="#E69F00"/>
<text x="212.0" y="103.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">15.84M</text>
<rect x="238.0" y="109.1" width="44.0" height="172.9" rx="4" fill="#0072B2"/>
<text x="260.0" y="103.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">15.84M</text>
<rect x="286.0" y="244.8" width="44.0" height="37.2" rx="4" fill="#009E73"/>
<text x="308.0" y="238.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">3.41M</text>
<text x="260.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">Q40 (mean quality 40)</text>
<rect x="606.0" y="195.8" width="44.0" height="86.2" rx="4" fill="#E69F00"/>
<text x="628.0" y="189.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">7.90M</text>
<rect x="654.0" y="195.8" width="44.0" height="86.2" rx="4" fill="#0072B2"/>
<text x="676.0" y="189.8" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">7.90M</text>
<rect x="702.0" y="49.1" width="44.0" height="232.9" rx="4" fill="#009E73"/>
<text x="724.0" y="43.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">21.33M</text>
<text x="676.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">Q41 (mean quality 41)</text>
</svg></div>

FastQC and polars-bio are **identical** (15.84M reads at Q40, 7.90M at Q41). RastQC inverts them — 21.3M reads pile into Q41.

| mean quality | FastQC | polars-bio | RastQC |
|---|---:|---:|---:|
| Q40 | 15,842,020 | 15,842,020 | 3,408,573 |
| Q41 | 7,902,229 | 7,902,229 | 21,334,982 |

!!! note "Parallel, but never approximate"
    Because each module merges order-independently, polars-bio's output is **bit-identical at 1, 2, 4, and 8 cores** for all 11 default modules. Splitting the file to go faster changes nothing about the result.

## Memory

Being fast should not cost the machine. The fair metric is *private* memory (anonymous footprint); polars-bio's larger resident-set is mostly the memory-mapped file — reclaimable OS page cache, not pressure.

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
<rect x="144.7" y="222.4" width="44.0" height="59.6" rx="4" fill="#0072B2"/>
<text x="166.7" y="216.4" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">273</text>
<rect x="192.7" y="37.7" width="44.0" height="244.3" rx="4" fill="#b9c2cc"/>
<text x="214.7" y="31.7" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">1119</text>
<text x="190.7" y="307" font-size="11" fill="#5b6470" text-anchor="middle">polars-bio (8c)</text>
<rect x="422.0" y="238.1" width="44.0" height="43.9" rx="4" fill="#009E73"/>
<text x="444.0" y="232.1" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">201</text>
<rect x="470.0" y="173.9" width="44.0" height="108.1" rx="4" fill="#b9c2cc"/>
<text x="492.0" y="167.9" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">495</text>
<text x="468.0" y="307" font-size="11" fill="#5b6470" text-anchor="middle">RastQC (8t)</text>
<rect x="723.3" y="133.5" width="44.0" height="148.5" rx="4" fill="#b9c2cc"/>
<text x="745.3" y="127.5" font-size="11.5" font-weight="600" fill="#14181d" text-anchor="middle">680</text>
<text x="745.3" y="307" font-size="11" fill="#5b6470" text-anchor="middle">FastQC (JVM)</text>
</svg></div>

polars-bio's true allocation is **273 MB**; its 1119 MB RSS is ~450 MB of mmap'd file pages plus buffers. FastQC's ~680 MB is a genuine JVM allocation. RastQC is leanest in absolute terms.

| tool (config) | private footprint (MB) | max RSS (MB) |
|---|---:|---:|
| polars-bio (1 core) | 198 | 655 |
| polars-bio (8 cores) | 273 | 1119 |
| RastQC (1 thread) | 71 | 219 |
| RastQC (8 threads) | 201 | 495 |
| FastQC (JVM) | — | 680 |

## The takeaway

On a 26.5-million-read file, polars-bio is the only tool that is **both** exact against FastQC and genuinely fast: **~12× faster than FastQC**, **~3.4× faster than RastQC's best**, at ~270 MB of real memory — while RastQC, the other fast option, silently misbins half the reads in one module. And it is just another table in the engine: `SELECT * FROM fastqc('reads.fastq.gz')`.

[^1]: FastQC ships Kmer Content disabled by default, so the cross-tool comparison covers the 11 default modules. polars-bio implements Kmer Content too (12/12), parity-tested separately; its FastQC-style top-20 output is inherently non-deterministic on real data — a known property of FastQC's Kmer module.
[^2]: FastQC prints `%GC` as a truncated integer; our full-precision `39.707` floors to FastQC's `39`. RastQC reports `40`.
