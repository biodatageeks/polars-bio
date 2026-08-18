---
draft: false
date:
  created: 2026-08-12
  updated: 2026-08-18
  # second revision of this date: BGEN re-measured, PGEN snputils column corrected
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

polars-bio is the fastest reader measured here in all three formats, at one
thread. On PGEN that includes pgenlib, PLINK 2's own C reader, on both
workloads; on BGEN it includes the `bgen` package. It did not start that way —
BGEN was the format it lost, and an earlier revision of this post said so — and
what closed that gap was measuring where the time actually went rather than
trusting the explanation. In all three formats polars-bio is bit-identical to an
independent reference implementation across 2.53 billion genotypes; snputils is
not, on BGEN.

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

The PGEN and BGEN blocks were both re-measured on 2026-08-18 against provider
`5f3dcf3`, each with all of its readers re-run together, so each table is
internally consistent; they are two separate sessions and should not be compared
against each other. The BCF figures are unchanged from the earlier session. The
PGEN table also carries a harness fix that moved only the snputils column — see
the note under that table.

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
| **polars-bio** | **0.684 s** | **1.277 s** |
| pgenlib | 0.873 s | 1.884 s |
| snputils | 0.875 s | 2.651 s |

polars-bio is **1.48× faster than pgenlib on dosage and 1.28× on hardcalls**, and
2.08×/1.28× faster than snputils. Note that snputils *is* pgenlib plus a NumPy
wrapper here, so the meaningful comparison is against pgenlib itself — and on
hardcalls the two are now within 2 ms of each other, which is what that wrapper
should cost.

An earlier revision of this post had snputils at 1.470 s and 3.462 s. That was a
harness bug, not the reader: the timer excludes library imports, and the warm-up
imported each reader's top-level package, which does not reach snputils' lazily
loaded reader. It was charged ~0.94 s of module loading inside its own timed
region while every other reader had that excluded. Worth stating plainly that
the correction moves against this post's own subject.

Two caveats. pgenlib drifted by up to 4.6% between sessions earlier in this
work, and this session shows the same thing from the other side: it reproduced
its dosage figure to within 0.1% but ran 4.2% faster on hardcalls than the
previous run, with snputils moving 2.7% and 4.4% on the same two workloads.
Read the margins as measured within a session, and prefer "faster" to a third
decimal place across them. And pgenlib still uses less memory in both workloads
— 12.1 GB against 12.6 GB on dosage — because its output array *is* its working
buffer, though the gap is 4% rather than the 65% it was.

Given more than one core polars-bio pulls further ahead: at eight partitions it
reads the same file in **0.385 s** (dosage) and **0.238 s** (hardcall). That is
not a one-thread result and is reported as an aside, the same way the BGEN
figure below is.

This is a large change from the first revision of this post, which had polars-bio
at 4.202 s and 5.615 s. Six things moved it, and one of them was a harness bug —
details in [Where the time went](#where-the-time-went).

Two representation notes. PLINK 2 stores hardcalls as two bits per genotype, so
`int8` output is the natural target; polars-bio's `ALT_COUNT` column emits
exactly that, one byte per genotype rather than the four `DS` needs — 2.53 GB of
output on this chromosome instead of 10.13 GB. Its `DS` column stays `float32`
because PGEN dosages are genuinely fractional — a dosage fileset holds values
like `0.125` that no integer type can carry. pgenlib pays the same tax:
identical records cost it 0.873 s as `int8` and 1.884 s as `float32`.

### BGEN — float32 dosage

| Reader | Time | Peak RSS |
|---|---:|---:|
| **polars-bio** | **14.142 s** | 24,291 MB |
| bgen | 15.680 s | 20,377 MB |
| snputils | 21.807 s | 21,020 MB |

polars-bio is **1.11× faster than the `bgen` package and 1.54× faster than
snputils**. Its three runs were 13.600 s, 14.181 s and 14.142 s, all below the
`bgen` package's slowest, so the ranges do not overlap.

An earlier revision of this post had polars-bio last here at 25.804 s, 1.71×
slower than the `bgen` package, and explained it as decompression being where a
single-threaded C extension is strongest. That explanation was wrong in a way
worth recording, and [Where the time went](#where-the-time-went) has the
measurement that replaced it.

**Memory is the one axis that moved the wrong way.** polars-bio peaks 19% above
the `bgen` package, and 9% above where it peaked when it was slower. That is not
explained; a scan that produces batches twice as fast plausibly keeps more of
them in flight ahead of a Python consumer that did not get faster, but that is a
hypothesis, not a measurement.

Given more than one partition it pulls further ahead: at eight partitions it
reads the same file in **3.789 s**. That is not a one-thread result and is
reported as an aside rather than in the table above.

## Where the time went

**BGEN** was the format polars-bio lost at one thread, and the explanation this
post gave for it was wrong. The claim was that decompression dominates and a C
extension wins there, so per core there was no structural advantage to be had.
Two measurements retired that.

*Decompression is a floor every reader pays, in the same library.* Walking the
variant records and inflating every payload with libdeflate — which is all any
reader of this file must do — takes **9.5 s** and produces 7.61 GB from a 160 MB
file. The `bgen` package reads the whole file in 15.7 s, so its own work is
about 6 s. polars-bio's was 14.8 s. The gap was never decompression; both use
libdeflate.

*And Arrow construction is four milliseconds.* The decoder writes Arrow's layout
as it goes, so building a batch only wraps buffers. "Builds Arrow arrays on top"
was measuring nothing.

The cost was the loop turning a decompressed block into output, at 5.15 ns per
genotype across 2.53 billion of them. Profiling it found two things. First, the
two helpers it calls per sample were **out-of-line calls** — 15% and 18% of the
scan — and neither was reachable by a hint: one already carried `#[inline]` and
LLVM declined it, the other's callers were marked but it was not. Second, the
loop **decided per sample what the read had already decided for all of them**:
it gathered through the selected-sample index array even when the selection was
the whole cohort in order, wrote a uniform ploidy a byte at a time, and tested a
missingness that a fully called variant does not have.

Inlining both helpers and filling a whole-cohort dosage read straight from the
stored byte pairs took the non-decompression work from **14.8 s to 2.3 s**.
Decompression is now 80% of a one-partition scan, against 39% before. The output
is bit-identical through both changes — the dosages are written by the same
expression the per-sample path uses, not an equivalent one.

**PGEN** went from 5.615 s to 1.221 s on dosage over six changes, and it is
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
values are written once. Measured against the same provider on both sides, that
was 3.225 s and 22.3 GB through the DataFrame against 1.849 s and 13.3 GB
through the matrix reader — 1.7× the time and 1.7× the memory, for a copy the
job does not need.

*Opening the fileset parsed 108 MB of text before anything else started.* The
`.pvar` companion lists every variant, and it was parsed on one thread — 0.257 s,
paid before any partition ran, and a fixed floor under every read no matter how
many cores it was given. Splitting it across threads takes it to 0.068 s. That
is the change that put polars-bio ahead of pgenlib rather than beside it.

*And the timer was charging polars-bio for importing itself.* Each measurement
runs in a fresh process, and every reader adapter imported its library inside
the timed function — about 0.46 s for polars-bio's 228 MB extension against
0.03 s for pgenlib and snputils. The harness had always documented imports as
excluded; it now actually excludes them, for every reader alike. That is worth
~0.43 s of the dosage figure, so it is worth knowing about before comparing
against the earlier revision.

*And then the copy went away entirely.* Streaming batches into a preallocated
array still means writing every value twice — once into Arrow, once into the
result — and that second write could not be removed on the Arrow path, because
`ListArray` uses 32-bit offsets and a batch holds at most 842,811 rows at this
sample count, so the matrix can never arrive as one zero-copy buffer. The
decoder was given a path that writes at the destination instead. That is worth
1.3× at one core and 2.3× at eight, and it is most of why scaling improved from
1.85× to 3.28× — the copy saturated memory bandwidth at about 2.8× regardless of
thread count, so it was the ceiling.

That change did briefly make *hardcalls* slower at one core, 0.694 s to 0.759 s,
which is worth recording because of how it happened. Building a matrix means
asking the file how big it is, allocating, then filling — and asking reopened
the fileset, so the 108 MB PVAR was parsed twice. Dosage's decode is large
enough to hide that; the hardcall decode is not. Holding the fileset open across
both questions fixed it, and hardcalls are now 0.653 s at one core and 0.234 s
at eight.

*And the input was still being copied.* The matrix reader read every byte range
of every partition into a buffer of its own before starting a decoder, so the
whole 79.9 MB fileset sat in memory on top of the destination and the reads ran
one after another however many decoders were asked for. It now fetches and
decodes a round at a time — one range per partition, read concurrently — and the
decoders read out of the readers' own buffers, so a range is not copied at all.
Resident input is bounded by the range budget rather than by the file, which is
the point of the change; the time it is worth is small and hard to pin, because
the reference readers moved between the two sessions too. Isolating the Rust
decode alone puts it at 2.8% on dosage at one thread, which is the honest figure
— the 5.0% the end-to-end table shows includes whatever the machine contributed.

What is left is one PVAR parse and the decode itself, which already scales 5×.

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

The import exclusion needed two goes to get right. It was always the stated
contract, but the PGEN adapters imported inside the timed function, which
charged each reader for its own module load — a cost paid once per process
however many filesets are then read. Warming the imports fixed that for
polars-bio and pgenlib but not for snputils, which loads its readers lazily:
importing the package costs ~0.03 s while the first touch of `read_pgen` costs
~0.94 s, and that touch stayed inside the timer. Warming now reaches the
attribute each adapter calls.

Measured cold, the imports are ~0.59 s for polars-bio's 228 MB extension,
~0.94 s for snputils' PGEN reader, ~0.88 s for its BGEN reader, and under
0.06 s for pgenlib and the `bgen` package — so this exclusion is not a courtesy
to polars-bio, and while it was broken it was costing snputils the most. The
BCF and BGEN figures are unaffected: their adapters import at module scope,
before the clock.

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
| datafusion-bio-formats | `5f3dcf3` (branch `perf/bgen-bulk-dosage-fill`, [#235](https://github.com/biodatageeks/datafusion-bio-formats/pull/235) on [#234](https://github.com/biodatageeks/datafusion-bio-formats/pull/234)); its PGEN provider is [#232](https://github.com/biodatageeks/datafusion-bio-formats/pull/232) as merged |
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
