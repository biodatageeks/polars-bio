---
draft: false
date:
  created: 2026-07-04
categories:
  - opinion
  - architecture
  - postmortem
---

# Cheap to rewrite, expensive to get right: the "FastQC-compatible" trap

A wave of AI-assisted rewrites is coming to bioinformatics. Decades-old tools written in Perl, Java, and C are being reimplemented in Rust and Go, often by an LLM in an afternoon. The [rewrites.bio](https://rewrites.bio/) manifesto puts it well: *"The question is not whether it will happen, but whether it will happen well."* Because — and this is the whole point — **cheap code is not the same as correct code.**

Here is the thesis of this post: in the age of cheap AI-assisted rewrites, writing the code is no longer the hard part. The hard, still-valuable part is the engineering judgment around it — **sound architecture, verified quality, and the discipline of spec- (SDD) and test-driven development (TDD)** — plus the honesty to report what you actually built. A rewrite is not a translation to a new language; it is a *validation contract*. We recently shipped a streaming FastQC in [polars-bio](https://github.com/biodatageeks/polars-bio), and along the way [benchmarked](fastqc-benchmark-2026-07.md) [RastQC](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2), a Rust reimplementation of FastQC. RastQC turns out to be a clean worked example of what happens when a rewrite skips those principles.

<!-- more -->

## The false promise: what "FastQC-compatible" must mean

FastQC has been the first look at a sequencing run for fifteen years. Its value is not the code — it is the *trust*: a WARN or FAIL on a FastQC module means something because the community has looked at millions of reports. A rewrite inherits that trust only if it produces the same answers. rewrites.bio [§2.2](https://rewrites.bio/#emulate-exactly) states it plainly: *"The goal is a faster tool that produces the same results."* For deterministic tools that means *"byte-for-byte identical output files"*; for floating-point ones, *"results within acceptable numerical precision (defined by scientists, not convenience)."* And *"everything counts: header formats, column ordering, file naming, summary statistics."*

Measured against that contract, RastQC does not hold up. On a 4.25M-read Illumina run (ERR5897746), comparing every module against FastQC 0.12.1 as the golden reference:

| Metric | polars-bio | RastQC `-t 1` | RastQC `-t 4` |
|---|---:|---:|---:|
| Per-sequence quality | ✅ exact | ❌ **671k reads misbinned** | ❌ **671k reads misbinned** |
| %GC | ✅ exact | ❌ off by 1 | ❌ off by 1 |
| Duplication level | ✅ exact | ❌ **9.7 pts off** | ❌ **14.2 pts off** |
| Top k-mer | ✅ exact | ✅ exact | ❌ **wrong k-mer** |

Per-sequence quality is the worst: RastQC invents a Q40 bin FastQC never emits and drops **671,121 reads** into it — a mean-quality rounding bug. %GC is off by one because it rounds where FastQC truncates. But the deepest tell is in the last two rows: **RastQC's output changes with the thread count.** Its duplication estimate and reported k-mers differ between `-t 1` and `-t 4` — its sampling runs per-worker, so the "answer" depends on how many cores you gave it. polars-bio is byte-identical across thread counts and bit-exact vs FastQC on every comparable module. (The full 12-module drift table is in the [benchmark post](fastqc-benchmark-2026-07.md#correct-at-every-thread-count).)

## Postmortem: how a "FastQC-compatible" tool shipped incompatible

None of this is a personal attack — RastQC is a young [`v0.1.0`](https://github.com/Huang-lab/RastQC/releases/tag/v0.1.0) tool, and the failure mode is instructive precisely because it is so common. But "young" is not the same as "unaccountable": there is already a [preprint](https://www.biorxiv.org/content/10.64898/2026.03.31.715630v2) making concrete performance and compatibility claims, and it is fair to hold a tool to the claims it publishes. So let's do a blameless postmortem — not *what* is wrong, but *why it shipped*. The answer is entirely in the test suite.

*Why each bug slipped through:*

| Bug | The test gap that let it pass |
|---|---|
| Per-seq quality misbins 671k reads | [`per_sequence_quality.rs`](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/modules/per_sequence_quality.rs) has **zero unit tests**; the only test that touches it greps for the header string `>>Per sequence quality scores`. Nothing inspects a bin count. |
| %GC off by one | [`basic_stats.rs`](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/modules/basic_stats.rs) *has* a GC test — but on a synthetic **50%-GC** input, where round and truncate both give 50. It also checks the internal float, not the printed integer, and never compares to FastQC. |
| Duplication & k-mer drift with `-t` | [`duplication.rs`](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/modules/duplication.rs) and [`kmer_content.rs`](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/modules/kmer_content.rs) have **zero unit tests**, and nothing runs the same input at `-t 1` vs `-t N`. Worse, every fixture is 1–3 reads and the parallel path only engages [above 50 MB](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/parallel.rs#L151-L152) — so the code that causes the drift is *never executed by the tests at all*. |

The systemic gaps behind those:

1. **No golden/oracle parity.** The "FastQC-compatible" promise has zero verification against FastQC output anywhere in the repo.
2. **No per-module value tests** on 12 of 13 modules — all the statistical logic (binning, rounding, per-worker sampling, the duplication freeze) is untested.
3. **No determinism tests** — nothing asserts output is stable across `-t`, despite parallelism being the tool's entire differentiator.
4. **Inputs too small and too uniform** — 1–3 reads of `IIII…` never reach the parallel path, the duplication freeze, k-mer enrichment (which needs thousands of reads for 2% sampling), or a quality distribution where the rounding bug bites.
5. **Presence-only assertions** — `contains(">>Module")` gives a false "all 15 modules work" signal while guaranteeing nothing about correctness.

This is the Composable Data Management System Manifesto's **time-to-market fallacy** in miniature: a quick prototype with a subset of functionality *"understates the high cost of stabilizing (hardening) the software … resulting in products with incomplete and inconsistent features."* And it is rewrites.bio [§4.2](https://rewrites.bio/#build-what-you-need) made literal: *"A rewrite that does four things correctly is more valuable than one that claims fifteen and does twelve right."* RastQC claims fifteen modules. It does not do twelve right.

## The fix: emulate exactly is a discipline, not a wish

You do not *hope* for parity — you *specify* it, then *verify* it. That is spec-driven and test-driven development, and it is the concrete practice behind "emulate exactly."

Specify first. rewrites.bio's planning phase — Think Big, then Work Small: *"Start with the simplest function that produces testable output. Validate it. Then extend."* Pin the equivalence contract before writing a line: which modules, which FastQC version, what tolerance. Then test first. For every module we write the FastQC assertion *before* the implementation, watch it fail against real golden output, and implement until it goes green. polars-bio's FastQC has a dedicated golden test for **all 12 modules** — value assertions, not header greps — plus a partition-invariance test that runs the same input at 1 and N partitions and demands identical output. On **real data** ([§4.1](https://rewrites.bio/#test-and-benchmark)), against a **pinned** FastQC 0.12.1 ([§4.3](https://rewrites.bio/#pin-versions)). That is why the drift table above has a column of green checks.

There is a pleasing symmetry here: the same AI that makes the code cheap can also run the discipline that keeps it honest. This rewrite was built with exactly that loop — a spec-driven brainstorm that produced a written design, then test-driven implementation (the open-source ["superpowers"](https://github.com/obra/superpowers) skill set encodes both: brainstorm→spec→plan for SDD, and a red-green loop for TDD). Cheap generation and rigorous validation are not in tension; the second is what makes the first trustworthy.

## Exact isn't enough: re-architect, don't reinvent

Now the other half — and it starts with an honest look at the *performance* claim, because presenting results fairly matters as much as producing them.

RastQC's preprint reports that its *"streaming parallel pipeline with adaptive batch sizing delivers 1.8–3.2× speedup on short-read Illumina data and 4.7–6.5× speedup on long-read ONT/PacBio data."* Read that carefully: by its own wording the speedup is **parallelism**. FastQC is single-threaded per file, so this is a multi-thread tool measured against a single-thread one — the rewrite-vs-original effect is never isolated. Isolate it, and the story flips. At **one thread**, on real files, the Rust rewrite is *slower than the JVM it replaced*: 17.6s → **18.6s** on the 4.3M run, 68.4s → **70.3s** on the 24.8M run. A natively-compiled tool losing to the JVM at equal footing means the port bought its "speedup" with cores, not with better code or algorithms — and the same per-core sampling is exactly what breaks thread-invariance. Worse, that parallelism is silently gated: RastQC only turns it on [above a hard-coded 50 MB](https://github.com/Huang-lab/RastQC/blob/v0.1.0/src/parallel.rs#L151-L152), so on a smaller file — our 0.72M-read run is a 28 MB BGZF — the parallel path never engages and even `-t 4` runs single-threaded. A multi-thread "speedup" reported on a file that size is measuring nothing. polars-bio, by contrast, is **1.8–2× faster than FastQC at a single thread** (8.6s and 38.1s), and *then* scales.

Where does a genuine single-thread win come from? Not from re-typing FastQC in Rust. rewrites.bio [§3.1](https://rewrites.bio/#think-big) says *Think Big* — design what a pipeline built from scratch would look like. The Composable Data Management System Manifesto (Pedreira, Erling, Karanasos, Schneider, **McKinney**, Valluri, Zait, Nadeau — VLDB 2023) makes the architectural case: fragmentation *"has forced developers to reinvent the wheel, duplicating work."* The answer is to **compose from open, battle-tested components** — Apache Arrow for memory, Parquet for storage, a shared execution engine — rather than rebuild each layer per tool.

RastQC took the monolith path: it re-implements a whole QC engine, its own FASTQ reader, its own thread pool, and its own report writer, from scratch and in isolation. polars-bio takes the **composable path**. It does not build an engine at all — each FastQC module is a streaming aggregation expressed on [Apache DataFusion](https://datafusion.apache.org/); reads arrive as [Apache Arrow](https://arrow.apache.org/) batches and land directly in a [Polars](https://pola.rs/) or [Pandas](https://pandas.pydata.org/) DataFrame. You can call it as a native Python function *or* in SQL — both stream the file once and hand back a lazy, Arrow-backed frame:

```python
import polars_bio as pb

# Native Python — the result and every module are Polars LazyFrames
qc = pb.fastqc("reads.fastq.gz")
qc.tidy.collect()               # the full tidy table
qc.summary().collect()          # PASS / WARN / FAIL per module
qc.per_base_quality.collect()   # ...or any single module

# The identical query in SQL
pb.sql("SELECT * FROM fastqc('reads.fastq.gz')").collect()
```

The `collect()` runs the single streaming pass and returns a tidy DataFrame — one row per (module, metric), ready to filter, join, or plot:

```text
shape: (2_508, 6)
┌──────────────────┬───────┬──────────┬─────────────┬───────────┬───────────┐
│ module           ┆ label ┆ position ┆ metric      ┆ value     ┆ value_str │
│ ---              ┆ ---   ┆ ---      ┆ ---         ┆ ---       ┆ ---       │
│ str              ┆ str   ┆ i32      ┆ str         ┆ f64       ┆ str       │
╞══════════════════╪═══════╪══════════╪═════════════╪═══════════╪═══════════╡
│ basic_stats      ┆ null  ┆ null     ┆ n_seq       ┆ 200.0     ┆ null      │
│ basic_stats      ┆ null  ┆ null     ┆ total_bases ┆ 20200.0   ┆ null      │
│ basic_stats      ┆ null  ┆ null     ┆ gc_pct      ┆ 47.226117 ┆ null      │
│ …                ┆ …     ┆ …        ┆ …           ┆ …         ┆ …         │
│ kmer_content     ┆ null  ┆ null     ┆ status      ┆ null      ┆ PASS      │
└──────────────────┴───────┴──────────┴─────────────┴───────────┴───────────┘
```

…and `summary()` gives the familiar PASS / WARN / FAIL per module:

```text
shape: (12, 2)
┌──────────────────┬────────┐
│ module           ┆ status │
│ ---              ┆ ---    │
│ str              ┆ str    │
╞══════════════════╪════════╡
│ basic_stats      ┆ PASS   │
│ per_base_quality ┆ PASS   │
│ per_seq_quality  ┆ PASS   │
│ per_base_content ┆ WARN   │
│ per_seq_gc       ┆ WARN   │
│ per_base_n       ┆ WARN   │
│ seq_length       ┆ PASS   │
│ overrepresented  ┆ WARN   │
│ adapter_content  ┆ PASS   │
│ dup_levels       ┆ PASS   │
│ per_tile_quality ┆ PASS   │
│ kmer_content     ┆ PASS   │
└──────────────────┴────────┘
```

<small>(Real output, run on FastQC's 200-read `example.fastq` fixture.)</small>

And `fastqc` is not welded to polars-bio's API. It is a DataFusion **user-defined table-valued function (UDTVF)** — a `TableFunctionImpl` that streams a FASTQ file into an Arrow result — packaged in a small [reusable crate](https://github.com/biodatageeks/datafusion-bio-functions). Register that crate into *any* DataFusion `SessionContext` — a Rust service, another DataFusion-powered engine, a different Python binding — and `SELECT * FROM fastqc(...)` works there too. The FastQC logic is a portable engine component, not a feature trapped inside one binary. That is the composable dividend the manifesto is about.

The rest of the payoff comes *for free*, inherited from an engine used by thousands of production systems. Single-thread speed comes from DataFusion's vectorized execution. Thread-invariance — the property RastQC's hand-rolled parallelism breaks — comes from expressing each module as an associative accumulator that merges identically regardless of partition count. We did not engineer thread-safety; we chose an architecture in which it is the default. And because the output is just an Arrow table, it is a first-class citizen of the Python data ecosystem, not a `.zip` of HTML you have to re-parse.

## Take-homes

Three principles for the rewrite itself, and one for how you present it:

1. **Emulate exactly — quality is validated, not asserted.** Byte-for-byte, or to a scientist-defined tolerance, *proven with golden tests against the original.* Not vibes, not a header grep.
2. **Compose, don't reinvent — architecture on open, battle-tested components.** Build on Arrow, Parquet, and a real query engine; be a citizen of the Python DataFrame ecosystem. The durable value is the architecture, not the port.
3. **Practice the discipline — spec-driven and test-driven development.** Specify the equivalence contract before coding; write the failing golden test before the implementation. This is the *how* that makes the first two real and repeatable.
4. **Benchmark and claim honestly.** Compare like-for-like, and claim only what you validated. Do not present multi-thread-vs-single-thread parallelism as a rewrite "speedup," and do not call a tool "compatible" without the proof.

The meta-point: generating and migrating code is cheap now. What stays scarce — and valuable — is engineering judgment: architecture, quality, disciplined process, and honest reporting of what you built.

## Do it well

The wave of AI-assisted rewrites is coming whether we like it or not, and much of it will be genuinely good — real tools, faster, in memory-safe languages. But a rewrite is a promise to everyone downstream: the scientists who trust a FASTQ report, the pipelines that gate on a WARN, the reviewers who read a benchmark. Rewriting is translation only in the most trivial sense. It is fundamentally an act of architecture, quality, and discipline — and the honest version of "faster" and "compatible" is the one you can *prove*.
