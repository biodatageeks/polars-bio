---
draft: false
date:
  created: 2026-08-12
  updated: 2026-08-17
categories:
  - performance
  - benchmarks
  - file formats

---

# Genotype Readers in Python: BCF, PGEN, and BGEN at One Thread

Genotype reader benchmarks compare unlike work more often than not. One library
counts records, another retains metadata, a third materializes every genotype —
and comparisons quietly mix thread counts and output dtypes. This post measures
polars-bio against snputils on the same chromosome in three formats, with one
constraint held fixed throughout: **every reader gets one thread, and every
comparison produces the same values.**

polars-bio is faster than snputils on BCF and slower on PGEN and BGEN. Where it
loses, the reason is identified rather than glossed. In all three formats
polars-bio is bit-identical to an independent reference implementation across
2.53 billion genotypes; snputils is not, on BGEN.

<!-- more -->

## One thread, always

The single most common way to overstate a result is to compare a parallel
reader against a serial one. snputils, pgenlib, and the `bgen` package are all
single-threaded. polars-bio can use partition parallelism, and does well with
it — but a table that puts eight polars-bio partitions next to one snputils
thread is not measuring the reader, it is measuring core count.

**Every number in this post is one thread against one thread.** polars-bio runs
with `datafusion.execution.target_partitions=1`, and Polars, Rayon, OpenMP,
OpenBLAS, MKL, Accelerate, and NumExpr pools are capped at one for every reader.

## Dataset

The phased, biallelic 1000 Genomes GRCh38 chromosome 22 callset, converted to
each format from the same source VCF so all three see identical variants and
sample order.

| Property | Value |
|---|---:|
| Variants | 993,881 |
| Samples | 2,548 |
| Genotype cells | 2,532,408,788 |
| BCF / PGEN / BGEN size | 135.1 MB / 79.9 MB / 160.5 MB |

A genotype becomes the number of allele-index-1 calls: `0|0 → 0`, `0|1` and
`1|0 → 1`, `1|1 → 2`, with missing calls marked. Variant and sample order must
remain unchanged.

## Correctness first

A speed number is worthless if the outputs differ, so every comparison is gated
on an element-wise check with **no tolerance** — cells that differ bitwise, not
cells that differ by more than an epsilon.

| Format | Reference | polars-bio | snputils |
|---|---|---|---|
| BCF | cross-reader equivalence | **0 differing** | 0 differing |
| PGEN | pgenlib (PLINK 2's own reader) | **0 differing** | 0 differing |
| BGEN | `bgen` package | **0 differing** | **differs** |

polars-bio matches the reference in all three formats across all 2,532,408,788
cells, at every partition count tested.

Two caveats worth stating plainly:

**snputils is not an independent check on PGEN.** Its PGEN reader wraps pgenlib
and calls `read_list` directly, so `snputils vs pgenlib` is close to a
tautology. The load-bearing comparison there is polars-bio against pgenlib.

**snputils differs from the `bgen` reference on BGEN.** polars-bio reproduces
the `bgen` package bit for bit; snputils does not. The differences are small and
consistent with probability quantization, but they are differences, and a
benchmark that used snputils as its BGEN oracle would be checking the wrong
thing.

The comparison is also checked to be capable of failing: the PGEN verifier
corrupts a single cell and asserts the corruption is detected, aborting if it
is not.

## Results

Medians of three fresh-process runs, one thread each. Lower is better.

### BCF — int8 dosage

| Reader | Time | Peak RSS |
|---|---:|---:|
| **polars-bio** | **5.251 s** | **2,681 MB** |
| snputils | 8.285 s | 10,067 MB |

polars-bio is **1.578× faster** and uses **3.755× less memory**. This is the
one format where it wins outright at equal core count.

### PGEN — hardcall allele count (int8) and dosage (float32)

| Reader | Hardcall | Dosage |
|---|---:|---:|
| pgenlib | **0.797 s** | **1.782 s** |
| snputils | 1.467 s | 3.252 s |
| polars-bio | 4.202 s | 5.615 s |

polars-bio is 2.9× slower than snputils on hardcalls and 1.7× slower on dosage.
Note that snputils *is* pgenlib plus a NumPy wrapper here — the ~0.67 s between
them is wrapper overhead — so the meaningful comparison is against pgenlib's
0.797 s.

Two representation notes. PLINK 2 stores hardcalls as two bits per genotype, so
`int8` output is the natural target; polars-bio grew an `ALT_COUNT` column for
exactly this, which cut its peak memory from 17.9 GB to 8.5 GB. Its `DS` column
stays `float32` because PGEN dosages are genuinely fractional — a dosage fileset
holds values like `0.125` that no integer type can carry. pgenlib pays the same
tax: identical records cost it 0.797 s as `int8` and 1.782 s as `float32`.

### BGEN — float32 dosage

| Reader | Time | Peak RSS |
|---|---:|---:|
| bgen | **15.064 s** | 19,718 MB |
| snputils | 21.171 s | 19,329 MB |
| polars-bio | 25.804 s | 20,462 MB |

polars-bio is 1.22× slower than snputils and 1.71× slower than the `bgen`
package. BGEN spends most of its time in per-variant zlib decompression, which
is where a single-threaded C extension is strongest.

Given more than one partition polars-bio overtakes both: at eight partitions it
reads the same file in **6.296 s** against the `bgen` package's 15.024 s and
snputils' 21.171 s. That is a real capability, and it is reported here as an
aside rather than in the table above, because it is not a one-thread result.

## Where polars-bio loses, and why

The losses are mechanical, not mysterious.

**BGEN** is decompression-bound. snputils' reader is a C extension built around
libdeflate; polars-bio decompresses the same blocks and then builds Arrow arrays
on top. Per-core there is no structural advantage to be had, and this is the
format where partition parallelism matters most, as the eight-partition figure
above shows.

**PGEN** is bound by LD reconstruction. `plink2 --make-pgen` writes
LD-compressed records — 81% of this fixture — where each variant is a difflist
against the previous one. polars-bio reconstructs a code per sample and a second
pass converts it to output; pgenlib fuses the two. Profiling the Rust scan alone
shows no single hotspot above about a fifth of samples, so closing it means
restructuring the decode core rather than tuning a loop.

Recent provider work cut the PGEN single-thread time by roughly 3×, and the
remaining gap is documented rather than hidden.

**BCF** is where the architecture pays off: typed FORMAT/GT decoding straight
into Arrow buffers, with no per-record Python object and no intermediate
matrix. That is also why its memory is a quarter of snputils'.

## What this does not measure

- **Multi-partition throughput.** Apart from the single BGEN aside above,
  polars-bio's scaling is excluded, because the comparison readers cannot use
  it. Format-specific writeups in the benchmark repository report it in full.
- **Query workloads.** Every test materializes a complete genotype matrix. That
  is the worst case for a query engine: it pays a full pass to consolidate
  chunked output into one contiguous array, which a streaming or SQL consumer
  never pays. Reading a region, filtering, or aggregating is a different
  measurement.
- **Anything but chromosome 22 on one machine.** A 16-core Apple M3 Max with
  64 GiB and macOS 15.6.

## Build and measurement controls

Each measurement runs in a fresh Python process; imports and thread-pool
configuration happen before the timer. The filesystem cache is warm and reader
order is deterministically rotated. Peak RSS is process `ru_maxrss` after the
output is retained; hashing runs outside the timer.

The polars-bio extension **must** be built optimized — a plain
`maturin develop` is a debug build and measured 3.1× slower, enough to invert a
conclusion:

```bash
RUSTFLAGS="-C target-cpu=native" maturin develop --release --locked
```

The runners record the loaded extension's size so the build profile can be
verified after the fact.

| Component | Version |
|---|---|
| polars-bio | 0.33.1 (`a8d5ef1`) |
| datafusion-bio-formats | `8fbed14` |
| snputils | 1.1.1.dev17+gbdb1a56b5 |
| pgenlib / bgen | 0.94.1 / 1.10.0 |
| Polars / PyArrow / NumPy | 1.42.1 / 24.0.0 / 2.5.2 |
| Python | 3.12.9 |

## Try it

```python
import polars_bio as pb

pb.set_option("datafusion.execution.target_partitions", "1")

bcf  = pb.scan_bcf("ALL.chr22.phased.bcf", format_fields=["GT"])
pgen = pb.scan_pgen("chr22.full.pgen", genotype_fields=["ALT_COUNT"])
bgen = pb.scan_bgen("chr22.full.bgen", genotype_output="dosage")
```

Runners, fixtures, and full result JSON — including every raw run, the
equivalence hashes, and the element-wise verifications — are in the
[bioformats-benchmark](https://github.com/biodatageeks/bioformats-benchmark)
repository as `run_bcf_benchmarks.py`, `run_pgen_benchmarks.py`, and
`run_bgen_benchmarks.py`.
