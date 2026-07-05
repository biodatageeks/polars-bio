---
draft: true
date:
  created: 2026-07-05
categories:
  - performance
  - benchmarks
  - file formats

---

# Benchmarking Genomic Format Readers in Python, Revisited (2026 Update)

In [February](../../2026/02/14/benchmarking-genomic-format-readers-in-python-with-polars/) we benchmarked four Python libraries — pysam, oxbow, biobear, and polars-bio — for reading BAM, VCF, and FASTQ files. That post ended with a promise: *"A follow-up post will dive into the performance improvements..."* This is that follow-up — and it turned into something more interesting than a version bump, because re-running the benchmark surfaced a **real performance regression in polars-bio itself**, which we then tracked down and fixed. Both the numbers and that investigation are below.

<!-- more -->

## What changed since February

Every library in the comparison moved on, and polars-bio jumped from **0.23.0 to 0.32.0**. We re-ran the identical benchmark — same data, same M3 Max machine, same methodology — with everything bumped to current releases:

| Library | Feb 2026 | Jul 2026 |
|---|---|---|
| [pysam](https://github.com/pysam-developers/pysam) | 0.23.3 | 0.24.0 |
| [oxbow](https://github.com/abdenlab/oxbow) | 0.5.1 | 0.8.1 |
| [biobear](https://github.com/wheretrue/biobear) | 0.23.7 | 0.23.7 |
| [polars-bio](https://github.com/biodatageeks/polars-bio) | 0.23.0 | 0.32.0 |

Supporting stack: **polars 1.42.1**, **pyarrow 24.0.0**. polars-bio 0.32.0 is built on **[datafusion-bio-formats](https://github.com/biodatageeks/datafusion-bio-formats) v1.8.7** (Apache DataFusion 53) with [noodles](https://github.com/zaeleus/noodles) under the hood.

## Counting rows honestly

We measure read throughput with a full row count. There is a subtlety worth stating up front, because it is a trap you can fall into with **any** Arrow-based library, not just polars-bio.

`LazyFrame.count()` counts non-null values *per column*, so on Polars' **default in-memory engine** it materializes every column of the whole file before aggregating. For a 10.8M-read FASTQ that is ~3.5 GB of decompressed strings:

| How you count 10.8M FASTQ reads | Peak memory |
|---|---|
| `pb.sql("SELECT count(*) FROM fq")` | **229 MB** |
| `.count().collect(engine="streaming")` | **379 MB** |
| `.count().collect()` *(default in-memory engine)* | **4.2 GB** |

The reader streams perfectly; the 4.2 GB is Polars materializing the frame before the count. So **all polars-bio numbers below use the streaming engine** — the honest, apples-to-apples comparison against oxbow's and pysam's streaming/chunked modes. (If you count rows in your own code, reach for `engine="streaming"` or a SQL `count(*)`.)

## A regression we caught — and fixed

Re-running the benchmark, FASTQ was unchanged but **BAM and VCF reads had gotten 15–45% slower** at a single thread than February's polars-bio 0.23. That is the opposite of a "performance improvements" follow-up, so we stopped and investigated properly before publishing anything.

The trail, in order of what we ruled out:

- **Not the collect engine** — the streaming engine was actually *faster* than the default one.
- **Not a recent reader-threading change** (issue #212) — building 0.32 against the bio-formats version just before and just after that change gave identical times.
- **A clean release bisect** (0.27 ✓, 0.31 ✓, 0.32 ✗) pinned the regression to the 0.31 → 0.32 jump.
- **Decomposition** showed decompression and record parsing were identical between versions; the entire slowdown was in the per-record **field-extraction + Arrow-build** path — and, narrowing further, in the `start`/`end` **coordinate columns** specifically.
- **A CPU profile** revealed the smoking gun: polars-bio 0.31 ran with **mimalloc** as its global allocator; 0.32 was on the **macOS system allocator**.

The cause was a one-line dependency change. polars-bio ≤0.31 inherited mimalloc *for free* because [datafusion-python](https://crates.io/crates/datafusion-python) enables it in its default features. In 0.32, polars-bio set `default-features = false` on that dependency — for unrelated reasons — and silently lost the allocator. The allocation-heavy per-record work of computing VCF `END` positions then paid the full system-malloc tax; FASTQ, which allocates little per record, barely noticed. That asymmetry is exactly what we saw.

**The fix** restores a fast global allocator directly in polars-bio (mimalloc by default, jemalloc available as an opt-in build feature), which brings every format back to — or slightly under — February's numbers:

| single thread | Feb 0.23 | 0.32 (regressed) | **0.32 (fixed)** |
|---|---|---|---|
| BAM without tags | 15.0 s | 17.3 s | **14.9 s** |
| BAM with tags | 20.8 s | 25.7 s | **22.2 s** |
| VCF without INFO | 33.6 s | 46.6 s | **34.5 s** |
| VCF with INFO | 69.1 s | 86.2 s | **71.2 s** |

All polars-bio results below are on the fixed build.

## Test data

Unchanged from February:

| Format | File | Rows |
|---|---|---|
| BAM | NA12878 WES chr1 (~2 GB) | 19.3M |
| VCF | Ensembl chr1 (bgzipped, 989 MB) | 86.8M |
| FASTQ | ERR194158 (bgzipped, 926 MB) | 10.8M |

Methodology: median of 2 runs of the timed read section; peak RSS (process + children) via `psutil`; each benchmark in its own subprocess; hardware Apple M3 Max. polars-bio runs at 1/2/4/8 threads; the other libraries are single-threaded (oxbow and pysam offer streaming/chunked variants, shown separately).

## Results

### FASTQ — 10.8M reads

| Library | Mode | Time | Peak mem |
|---|---|---|---|
| **polars-bio 0.32** | streaming, 8T | **0.61 s** | 316 MB |
| **polars-bio 0.32** | streaming, 1T | 4.28 s | 222 MB |
| oxbow 0.8.1 | stream | 7.19 s | 236 MB |
| oxbow 0.8.1 | lazy | 7.56 s | 3.9 GB |
| pysam_chunked 0.24 | chunked | 11.02 s | 3.1 GB |
| biobear 0.23.7 | eager | 11.07 s | 3.3 GB |
| pysam 0.24 | eager | 11.41 s | 8.3 GB |

At one thread polars-bio reads 10.8M records in **4.3 s** — faster than oxbow's streaming mode — in the same few-hundred-MB envelope. With 8 threads it drops to **0.61 s**: **19× faster than pysam**, **12× faster than oxbow_stream**, at ~300 MB.

### BAM — without tags (19.3M records)

| Library | Mode | Time | Peak mem |
|---|---|---|---|
| **polars-bio 0.32** | streaming, 8T | **4.27 s** | 323 MB |
| **polars-bio 0.32** | streaming, 1T | 14.85 s | 232 MB |
| biobear 0.23.7 | eager | 20.05 s | 32.2 GB |
| oxbow 0.8.1 | lazy | 24.91 s | 6.3 GB |
| oxbow 0.8.1 | stream | 25.32 s | 272 MB |
| pysam_chunked 0.24 | chunked | 59.97 s | 8.0 GB |
| pysam 0.24 | eager | 165.6 s | 36.2 GB |

polars-bio is the fastest library at 1 thread (14.9 s, 1.3× ahead of biobear, 11× ahead of pysam), and **4.27 s at 8 threads** — at **232 MB** versus biobear's 32 GB and pysam's 36 GB. Only oxbow's streaming mode matches its memory, at 5–6× the wall time.

### BAM — with tags (19.3M records, 13 auxiliary tags)

| Library | Mode | Time | Peak mem |
|---|---|---|---|
| **polars-bio 0.32** | streaming, 8T | **6.29 s** | 407 MB |
| **polars-bio 0.32** | streaming, 1T | 22.17 s | 251 MB |
| oxbow 0.8.1 | lazy | 24.95 s | 6.3 GB |
| oxbow 0.8.1 | stream | 25.19 s | 270 MB |
| biobear 0.23.7 | eager | 32.90 s | 40.0 GB |
| pysam_chunked 0.24 | chunked | 105.5 s | 20.7 GB |
| pysam 0.24 | eager | 269.9 s | 36.7 GB |

Auxiliary tags multiply the data per record. polars-bio at 8 threads reads all 19.3M records with 13 tags in **6.3 s — 43× faster than pysam** — under 410 MB, while pysam and biobear sit at 20–40 GB. (oxbow reads BAM at the same speed with or without tags here — a nice 2× improvement over 0.5.1 for the with-tags case.)

### VCF — without INFO (86.8M records)

| Library | Mode | Time | Peak mem |
|---|---|---|---|
| **polars-bio 0.32** | streaming, 8T | **5.44 s** | 277 MB |
| **polars-bio 0.32** | streaming, 1T | 34.45 s | 225 MB |
| biobear 0.23.7 | eager | 42.10 s | 19.2 GB |
| oxbow 0.8.1 | lazy | 67.01 s | 11.7 GB |
| oxbow 0.8.1 | stream | 68.64 s | 169 MB |
| pysam_chunked 0.24 | chunked | 104.4 s | 18.6 GB |
| pysam 0.24 | eager | 108.3 s | 19.8 GB |

For the 86.8M-row VCF, polars-bio at 1 thread (34.5 s) leads every competitor, and at 8 threads finishes in **5.4 s — 20× faster than pysam** — at 277 MB versus pysam's ~20 GB.

### VCF — with INFO (86.8M records)

The hardest test: parsing the full INFO field across 86.8M records.

| Library | Mode | Time | Peak mem |
|---|---|---|---|
| **polars-bio 0.32** | streaming, 8T | **9.76 s** | 290 MB |
| **polars-bio 0.32** | streaming, 1T | 71.15 s | 229 MB |
| oxbow 0.8.1 | lazy | 111.7 s | 19.1 GB |
| oxbow 0.8.1 | stream | 113.2 s | 181 MB |
| pysam_chunked 0.24 | chunked | 574.6 s | 37.9 GB |
| pysam 0.24 | eager | **✗ timeout** (>600 s) | — |
| biobear 0.23.7 | — | ✗ fails on this dataset | — |

Parsing 86.8M records × the full INFO field is where the field separates. **pysam times out, biobear fails, pysam_chunked needs ~10 minutes.** polars-bio does it at **1 thread in 71 s and 8 threads in 9.8 s**, never leaving the hundreds of MB — the only library that delivers both correctness and practical performance here.

## Thread scaling

polars-bio memory stays remarkably flat as threads rise — **~225–410 MB across every format and thread count**, so you can use all cores without watching memory. Speedups at 8 threads:

| Format | 1T → 8T | Speedup |
|---|---|---|
| FASTQ | 4.28 → 0.61 s | 7.0× |
| BAM without tags | 14.85 → 4.27 s | 3.5× |
| BAM with tags | 22.17 → 6.29 s | 3.5× |
| VCF without INFO | 34.45 → 5.44 s | 6.3× |
| VCF with INFO | 71.15 → 9.76 s | 7.3× |

## Conclusions

- **polars-bio 0.32 is the fastest of the four across all three formats**, and the only one pairing speed with bounded, streaming memory (hundreds of MB where pysam and biobear consume tens of GB).
- Re-benchmarking is worth doing: it caught a **real allocator regression** in polars-bio 0.32 that a version bump alone would have shipped. Reading numbers you didn't measure yourself — including your own from a prior release — is how regressions hide.
- Two honesty caveats that apply to any Arrow library: count rows with a **streaming** aggregation, and remember that **peak memory depends on the engine**, not just the reader.

## Try it yourself

```bash
pip install polars-bio
```

```python
import polars_bio as pb
import polars as pl

# Count rows in bounded memory (streaming aggregation)
n = pb.scan_fastq("reads.fastq.bgz").count().collect(engine="streaming").item(0, 0)

# Scan lazily with predicate + projection pushdown
lf = pb.scan_bam("sample.bam")
lf.filter(pl.col("chrom") == "chr1").sink_parquet("/tmp/chr1.parquet")
```

- [Documentation](https://biodatageeks.org/polars-bio/)
- [GitHub](https://github.com/biodatageeks/polars-bio)
- [Benchmark repository](https://github.com/biodatageeks/bioformats-benchmark)
- [February 2026 benchmark](../../2026/02/14/benchmarking-genomic-format-readers-in-python-with-polars/)
