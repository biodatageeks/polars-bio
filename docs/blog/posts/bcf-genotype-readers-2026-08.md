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

polars-bio is faster than snputils on BCF and PGEN, and slower on BGEN. On PGEN
it also comes within a few percent of pgenlib, PLINK 2's own C reader. Where it
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
| pgenlib | **0.827 s** | **1.779 s** |
| **polars-bio** | **0.940 s** | **1.849 s** |
| snputils | 1.487 s | 3.181 s |

polars-bio is **1.6× faster than snputils on hardcalls and 1.7× faster on
dosage**, and lands **1.14× and 1.04× of pgenlib's time**. Note that snputils
*is* pgenlib plus a NumPy wrapper here, so the meaningful comparison is against
pgenlib itself, and coming within 4% of a C reader that decodes straight into
the caller's array is the result worth reporting.

This is a large change from the previous revision of this post, which had
polars-bio at 4.202 s and 5.615 s. Three things moved it, and one of them was a
harness bug — details in [Where the time went](#where-the-time-went).

Two representation notes. PLINK 2 stores hardcalls as two bits per genotype, so
`int8` output is the natural target; polars-bio's `ALT_COUNT` column emits
exactly that, one byte per genotype rather than the four `DS` needs — 2.53 GB of
output on this chromosome instead of 10.13 GB. Its `DS` column stays `float32`
because PGEN dosages are genuinely fractional — a dosage fileset holds values
like `0.125` that no integer type can carry. pgenlib pays the same tax:
identical records cost it 0.827 s as `int8` and 1.779 s as `float32`.

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

## Where the time went

**BGEN** is decompression-bound, and is the one format where polars-bio still
loses at one thread. snputils' reader is a C extension built around libdeflate;
polars-bio decompresses the same blocks and then builds Arrow arrays on top.
Per-core there is no structural advantage to be had, and this is the format
where partition parallelism matters most, as the eight-partition figure above
shows.

**PGEN** went from 5.615 s to 1.849 s on dosage over three changes, and it is
worth being precise about which did what, because one of them was a measurement
fix rather than a speedup.

*The decode was doing a pass the record does not need.* 81% of a
`plink2 --make-pgen` fileset is a single common genotype for every sample plus a
sparse list of exceptions. That record has no per-sample base to reconstruct, so
filling a buffer of codes and then reading it back to write the output was one
pass too many; the values are now written once, straight from the common
category. The Rust scan alone went 2.31 s → 1.19 s for dosage and
1.65 s → 0.59 s for hardcalls.

Notably this is the *opposite* of keeping the data packed the way PLINK does.
pgenlib fills a packed array and then expands it, writing `sample_ct/4 +
sample_ct` bytes; writing the output directly costs `sample_ct` and nothing
else. For the record type that dominates, packing would have been a regression.

*Building a matrix cost a second copy of every value.* Getting a contiguous
array out of a DataFrame consolidates the scan's record batches into one Arrow
buffer before NumPy ever sees them — a whole extra 10 GB here. A new
`read_pgen_matrix` streams batches into a preallocated array instead, so the
values are written once. That is the difference between 3.225 s and 1.849 s, and
between 22.3 GB and 13.3 GB of peak memory.

*And the timer was charging polars-bio for importing itself.* Each measurement
runs in a fresh process, and every reader adapter imported its library inside
the timed function — about 0.46 s for polars-bio's 228 MB extension against
0.03 s for pgenlib and snputils. The harness had always documented imports as
excluded; it now actually excludes them, for every reader alike. Charged the old
way, polars-bio's dosage figure would read 2.26 s rather than 1.849 s, so this
one is worth knowing about before comparing against the earlier revision.

What is left is one copy from the scan's Arrow batches into the destination
array, and it cannot be removed on this path: Arrow's `ListArray` uses 32-bit
offsets, so a batch holds at most 842,811 rows at this sample count and the
matrix can never arrive as a single zero-copy buffer. Closing it means the
decoder writing into the caller's buffer, which is a different API rather than a
tuning change.

**BCF** is where the architecture pays off: typed FORMAT/GT decoding straight
into Arrow buffers, with no per-record Python object and no intermediate
matrix. That is also why its memory is a quarter of snputils'.

## What this does not measure

- **Multi-partition throughput.** Apart from the single BGEN aside above,
  polars-bio's scaling is excluded, because the comparison readers cannot use
  it. Format-specific writeups in the benchmark repository report it in full.
- **Query workloads.** Every test materializes a complete genotype matrix. That
  is the worst case for a query engine: it pays a copy from chunked output into
  one contiguous array, which a streaming or SQL consumer never pays. Reading a
  region, filtering, or aggregating is a different measurement.
- **Anything but chromosome 22 on one machine.** A 16-core Apple M3 Max with
  64 GiB and macOS 15.6.

## Build and measurement controls

Each measurement runs in a fresh Python process; imports happen before the timer
and are recorded separately, and thread-pool configuration is set up front. The
filesystem cache is warm and reader order is deterministically rotated. Peak RSS
is process `ru_maxrss` after the output is retained; hashing runs outside the
timer.

The import exclusion is enforced as of this revision. It was always the stated
contract, but the PGEN adapters imported inside the timed function, which
charged each reader for its own module load — a cost that is paid once per
process however many filesets are then read, and that differs by more than an
order of magnitude across these libraries. The BCF and BGEN figures in this post
are unaffected.

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
| polars-bio | 0.33.1 (branch `feat/bgen-pr220-bench`) |
| datafusion-bio-formats | `1fc3673` (branch `perf/pgen-batch-array-build`) |
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

The PGEN figures above are `read_pgen_matrix`, which returns the dense NumPy
matrix directly rather than a DataFrame — the counterpart to `pgenlib.read_list`
and what this benchmark's workload asks for:

```python
matrix = pb.read_pgen_matrix("chr22.full.pgen", field="ALT_COUNT")
matrix.values.shape          # (variants, samples), int8
matrix.values.mean(axis=0)   # per-variant ALT frequency x 2
```

Runners, fixtures, and full result JSON — including every raw run, the
equivalence hashes, and the element-wise verifications — are in the
[bioformats-benchmark](https://github.com/biodatageeks/bioformats-benchmark)
repository as `run_bcf_benchmarks.py`, `run_pgen_benchmarks.py`, and
`run_bgen_benchmarks.py`.
