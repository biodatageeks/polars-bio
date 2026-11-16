---
draft: true
date:
  created: 2026-11-16
categories:
  - releases
  - performance
  - benchmarks

---

# polars-bio 0.38.0: Sequence Operations, DataFusion 55.2, and Smaller, Faster Wheels

polars-bio 0.38.0 adds a `seq` namespace to Polars expressions: reverse
complement, GC content, translation, mean quality and more, running as native
Polars plugins. At one thread they are **2.4–3.6× faster than seqkit** on BGZF
FASTQ and 4.8× faster on translation, while matching seqkit and Biopython on
golden test corpora. The release also
moves to DataFusion 55.2, makes plain-gzip FASTQ reads about 2× faster, and builds
the PyPI wheels with fat link-time optimisation: **30% smaller and up to 14% faster**.

<!-- more -->

## Sequence operations as Polars expressions

Until now, transforming sequences in polars-bio meant leaving the query engine:
a `map_elements` or `map_batches` callback into Python, or a round trip through
another tool. 0.38.0 adds the operations people reach for in
[seqkit](https://github.com/shenwei356/seqkit) and
[seqfu](https://github.com/telatin/seqfu2) as Polars expressions. Importing
`polars_bio` registers `pl.Expr.seq`:

```python
import polars as pl
import polars_bio as pb  # registers pl.Expr.seq

reads = (
    pb.scan_fastq("reads.fastq.gz")
    .with_columns(
        gc=pl.col("sequence").seq.gc_content(),
        qual=pl.col("quality_scores").seq.mean_quality(),
    )
    .filter((pl.col("gc") > 0.4) & (pl.col("qual") >= 20))
    .collect(engine="streaming")
)
```

| Expression | What it computes | Matches |
|---|---|---|
| `.seq.revcomp(alphabet="dna", strict=False)` | reverse complement | seqkit `seq -r -p`, seqfu `rc`, seqtk `seq -r`, Biopython |
| `.seq.reverse()` | reversed string, e.g. a FASTQ quality alongside `revcomp` | Polars `str.reverse()`, much faster on ASCII |
| `.seq.complement(alphabet="dna", strict=False)` | complement, not reversed | Biopython `complement` |
| `.seq.gc_content(denominator="length")` | GC fraction, 0 to 1 | seqkit `fx2tab -B GC`; with `"acgt"`, seqkit `fx2tab -g` |
| `.seq.translate(frame=1, table=1, to_stop=False)` | protein translation, frames ±1–3, every NCBI table | Biopython `Seq.translate`, seqkit `translate` |
| `.seq.mean_quality(offset=33, method="error_prob")` | mean Phred quality | seqkit `fx2tab -q`; `"arithmetic"` is FastQC's mean |

A few properties worth knowing:

- **They are native Polars plugins**, compiled into the polars-bio extension
  module. They run without the GIL, in parallel, and stream with
  `collect(engine="streaming")` or `sink_*`.
- **They work on any String column**, not only on frames from polars-bio readers.
- **Nulls stay nulls**, and a ratio with nothing to divide by (an empty read)
  gives null rather than NaN, so `pl.col("gc") > 0.3` drops it instead of keeping
  it. Polars orders NaN above every number.
- **The same kernels are available in SQL** as `seq_revcomp`, `seq_reverse`,
  `seq_complement`, `seq_gc_content`, `seq_translate` and `seq_mean_quality`:

```python
pb.register_fastq("reads.fastq.gz", "reads")
pb.sql(
    "SELECT name, seq_gc_content(sequence) AS gc, "
    "seq_translate(sequence, -1) AS aa FROM reads"
).collect()
```

A reverse-complemented FASTQ, with the quality string reversed alongside it, is
one short pipeline:

```python
lf = pb.scan_fastq("reads.fastq.gz").with_columns(
    sequence=pl.col("sequence").seq.revcomp(),
    quality_scores=pl.col("quality_scores").seq.reverse(),
)
pb.sink_fastq(lf, "reads.rc.fastq")
```

`.seq.reverse()` exists because Polars' own `str.reverse()` reverses Unicode
grapheme clusters, which is correct for text but slow for quality strings. On
4.25M reads at one thread, adding `str.reverse()` to a FASTQ scan took the
pipeline from 1.67 s to 4.20 s; `.seq.reverse()` costs about 0.2 CPU-seconds and
doesn't change the wall time.

### Correctness first

The kernels live in a new upstream crate,
`datafusion-bio-function-seq`, and are checked against seqkit 2.14.0 and Biopython
1.88 on golden corpora: every IUPAC code in both cases, every NCBI translation
table, all six frames, `to_stop` on and off. Every polars-bio output in the
benchmarks below was also checked against seqkit's.

Where the reference tools disagree, polars-bio follows one of them and says so
in the docs. For example, for an ambiguous codon such as `RAT` (D or N),
Biopython returns `B`, while seqkit and polars-bio return `X`. The corpus also
turned up a seqkit 2.14.0 bug: `seqkit translate` reverses but doesn't complement
IUPAC ambiguity codes in negative frames, so `RTT` at frame −1 becomes `K`
instead of `N`. polars-bio agrees with Biopython there.

## Benchmarks: one thread against seqkit, seqtk, seqfu and Biopython

Three pipelines, each producing the same file from the same input:

| Operation | polars-bio | seqkit | seqtk | seqfu | Biopython |
|---|---|---|---|---|---|
| Reverse complement → FASTQ | `.seq.revcomp()` + `.seq.reverse()`, `sink_fastq` | `seq -r -p` | `seq -r` | `rc` | `FastqGeneralIterator` + `reverse_complement()` |
| GC % + mean quality → TSV | `.seq.gc_content("acgt")`, `.seq.mean_quality()`, `sink_csv` | `fx2tab -g -q` | `comp` | — | `FastqGeneralIterator` + Python loop |
| Translation → FASTA | `.seq.translate()`, `sink_fasta` | `translate` | — | — | `translate()` |

Inputs are public FASTQ runs of 0.72M (DRR609229), 4.25M (ERR5897746), 24.8M
(DRR013000) and 64.3M (SRR39421268) reads, and the Ensembl GRCh38 CDS FASTA, each
as BGZF and as plain gzip. Every tool runs with one thread (`-j 1`,
`POLARS_MAX_THREADS=1`, `target_partitions=1`). For Biopython, import time is
excluded. Times are the best of three runs on an Apple M3 Max (64 GB).

![Sequence operations at one thread](figures/release-0.38.0/seqops-one-thread.png)

**Wall time / CPU time in seconds, BGZF input:**

| Operation | Reads | polars-bio | seqkit | seqtk | seqfu | Biopython |
|---|---|---|---|---|---|---|
| Reverse complement | 0.72M | **0.24 / 0.49** | 0.57 / 0.57 | 0.69 / 0.68 | 1.75 / 1.92 | 1.46 / 1.45 |
| | 4.25M | **2.09 / 3.34** | 5.78 / 5.54 | 5.44 / 5.19 | 10.20 / 11.94 | 11.26 / 10.28 |
| | 24.8M | **8.34 / 13.68** | 27.21 / 23.84 | 26.76 / 22.55 | 49.30 / 57.09 | — |
| GC + mean quality | 0.72M | **0.25 / 0.42** | 0.77 / 0.75 | 2.31 / 2.27 | — | 4.04 / 4.01 |
| | 4.25M | **1.57 / 2.57** | 5.71 / 5.70 | 12.67 / 12.61 | — | 28.17 / 28.07 |
| | 24.8M | **7.81 / 12.61** | 26.59 / 26.56 | 69.17 / 68.67 | — | — |
| | 64.3M | **18.95 / 33.0** | 66.78 / 66.1 | 173.75 / 171.4 | — | — |
| Translation | Ensembl CDS | **0.74 / 1.26** | 3.57 / 3.56 | — | — | 22.03 / 21.92 |

polars-bio is the fastest tool in every case: 2.4–3.6× faster than the next
tool on reverse complement and GC + quality, and 4.8× faster than seqkit on
translation.

One caveat is visible in the CPU column. polars-bio's "one thread" uses about 1.6
cores: the reader decodes one partition while Polars processes the previous
batch. Counted by CPU time, it is still ahead everywhere on BGZF: about half of
seqkit's CPU time on the large files, and a third on translation.

### Plain gzip: 35–49% faster in this release

Plain gzip (not BGZF) can't be split, so it is always read as one stream. Before
this release that stream went through an asynchronous path that awaited every
record, and polars-bio was about 1.8× slower on gzip than on BGZF.
[datafusion-bio-formats#275](https://github.com/biodatageeks/datafusion-bio-formats/pull/275)
moves plain gzip, BGZF without a `.gzi` index, and uncompressed single-partition
FASTQ to the synchronous batch loop that the parallel BGZF path already used,
decompressing with `flate2`'s multi-member decoder on zlib-rs.

At the reader level (4.25M reads, one partition):

| Input | Before #275 | After #275 |
|---|---|---|
| plain gzip | 4.09–4.38 s | **2.03–2.08 s** |
| BGZF without `.gzi` | 2.11–2.22 s | 1.82–1.90 s |
| uncompressed | 1.19–1.20 s | 0.77–0.80 s |

End to end, at one thread on plain gzip (wall / CPU seconds):

| Pipeline | Reads | Before #275 | After #275 | Change | seqkit |
|---|---|---|---|---|---|
| Reverse complement → FASTQ | 4.25M | 3.21 / 4.7 | **1.90 / 3.4** | −41% | 4.9–5.0 / 4.9–5.0 |
| | 24.8M | 15.12 / 21.1 | **9.89 / 15.7** | −35% | 22.0–22.7 / 22.0–22.4 |
| GC + mean quality → TSV | 4.25M | 3.00 / 4.1 | **1.90 / 2.9** | −37% | 5.4–5.6 |
| | 24.8M | 14.51 / 19.8 | **8.76 / 13.5** | −40% | 26.1 |
| | 64.3M | 41.51 / 54.5 | **21.13 / 35.1** | −49% | 63–65 / 63 |

Gzip input is now within about 10% of BGZF, and polars-bio uses less CPU time
than seqkit on gzip as well as less wall time.

## DataFusion 55.2

polars-bio moves from DataFusion 53.0 to **55.2.0**, together with
datafusion-python 55.0, PyO3 0.29, Arrow 59.3 and the 55.x releases of
`datafusion-bio-formats` (v1.16.1) and `datafusion-bio-functions` (v0.26.0).

- **Upgrade `datafusion` together with polars-bio.** The Python requirement is
  now `datafusion>=55.0.0,<56`.
- **External sorts no longer fail with `Resources exhausted`** when several
  partitions sort at once under `FairSpillPool`
  ([apache/datafusion#20642](https://github.com/apache/datafusion/pull/20642)).
- **Grouped aggregations need a little more memory before they can spill.** With
  `datafusion.runtime.memory_limit` at 2 MB, a 1M-group aggregation now fails
  instead of spilling; from 4 MB up it spills and completes as before.
- **Remote reads that fail mid-stream** (a truncated body, a timeout or a
  connection reset) now return an error instead of retrying forever.
- **Building from source needs Rust 1.95** or newer.

We re-ran the interval benchmarks on 0.37.0, on the DataFusion 55 build, and on
the DataFusion 55 build with fat LTO (10 runs × 4 rounds). All three are within
±2%:

| Case | 0.37.0 | DataFusion 55 | DataFusion 55 + LTO |
|---|---|---|---|
| `count_overlaps` 8-7, 1 thread | 0.940 s | 0.939 s | 0.925 s |
| `count_overlaps` 8-7, 8 threads | 0.165 s | 0.165 s | 0.165 s |
| `coverage` 8-0, 1 thread | 0.668 s | 0.670 s | 0.674 s |
| `coverage` 8-0, 8 threads | 0.152 s | 0.149 s | 0.147 s |

## Smaller, faster wheels: stripping and fat LTO

The DataFusion 55 bump and the new plugin pushed the Linux aarch64 wheel to
95.4 MiB, within 5 MiB of PyPI's default 100 MiB per-file limit. Two build
changes bring it well clear:

- **Stripped symbols** (`strip = "symbols"`) remove about 11–15 MiB from each
  wheel. Native panic backtraces now show addresses instead of function names.
- **Fat link-time optimisation with one codegen unit**, in a new `dist` build
  profile used for the PyPI wheels.

Measured on macOS arm64, all builds stripped, median of three runs:

| Build | Wheel | Runtime vs stripped baseline | Build time |
|---|---|---|---|
| Stripped baseline | 79.5 MB | — | 7.5 min |
| Thin LTO | 82.3 MB (+3.5%) | −8% on BAM and VCF at 1 thread | 10.8 min |
| **Fat LTO** (`dist`) | **55.5 MB (−30%)** | **−10…−14% BAM, −12% VCF, −5% FASTQ** at 1 thread | ~24 min |

Fat LTO is both the smallest and the fastest build, but it costs about three
times the build time. So it runs only for release tags, a weekly scheduled build
and manual runs. Pull requests keep the faster stripped `release` profile.
`maturin develop --release` is unchanged; `maturin build --profile dist` builds a
wheel like the published one.

**The Linux aarch64 wheel's link needs about 34 GiB** with fat LTO, against 12 GiB
on macOS arm64. That doesn't fit on GitHub's 16 GB ARM runners, so the release
wheel is cross-compiled on a self-hosted x86_64 runner with swap, then installed
and tested on a native ARM runner. It is now tagged `manylinux_2_28` (glibc
2.28) instead of `manylinux2014`, matching the aarch64 wheels of pyarrow and
datafusion, which polars-bio already requires.

## Also in 0.38.0

- **`pb.set_progress_bar(False)`** turns off polars-bio's processed-rows progress
  bar at any time, also after `polars_bio` is imported, without silencing other
  `tqdm` bars ([#504](https://github.com/biodatageeks/polars-bio/issues/504)).
- **`df.config_meta` no longer keeps large frames alive** until the cyclic garbage
  collector runs. In a 10 × 160 MB loop the peak memory fell from 1.7 GB to
  0.33 GB ([#503](https://github.com/biodatageeks/polars-bio/issues/503)).

## Try it

```bash
pip install -U "polars-bio==0.38.0"
```

The [Sequence operations guide](../../features/sequences.md) covers semantics,
alphabets and translation in detail, and the
[API reference](../../api/sequences.md) lists every parameter. The benchmark
harness is in
[polars-bio-bench](https://github.com/biodatageeks/polars-bio-bench).
