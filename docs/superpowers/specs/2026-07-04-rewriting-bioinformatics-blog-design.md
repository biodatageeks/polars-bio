# Design: Follow-up blog post — "Rewriting bioinformatics software, fairly"

**Date:** 2026-07-04
**Type:** Blog post (opinion + postmortem case study + how-to, combined)
**Branch:** `feat/fastqc-phase1`
**Status:** Approved outline; drafting pending user review of this spec.

**Working title (recommended):** *Cheap to rewrite, expensive to be fair: the "FastQC-compatible" trap*
("fair" is the organizing frame — fair code and fair results.)
Alternatives: "A fair rewrite: honest code, honest benchmarks" · "AI made code cheap. It didn't make fairness cheap."

## Purpose

A follow-up to the FastQC benchmark post that argues a single thesis: **doing a rewrite well
is, at bottom, a question of fairness — fairness in the *code* (does the rewrite truly do what
the original does, and can you prove it?) and fairness in the *results you present* (is the
comparison honest and like-for-like, or a favorable framing?). AI made writing the code cheap;
it did not make fairness cheap — that still takes architecture, quality, and the discipline of
spec- and test-driven development.** Rewriting a bioinformatics tool well is an act of
re-architecture under a validation contract, not a lift-and-shift to a new language. The post
uses RastQC as a named, fair, evidence-first cautionary example of a rewrite that was unfair on
both axes, and polars-bio as the counter-example.

**The organizing frame is fairness, on two axes**, and every beat is an instance of it:
"compatible" that isn't → unfair to the scientists who trust the output; tests that grep for
headers → unfair validation; 4-thread-vs-1-thread → unfair benchmark; "15 modules" with 3 wrong
→ unfair claim.

Combines three genres in one piece: an opinion/manifesto argument, a data-backed case
study (our reproducible RastQC findings), and a practical how-to (emulate-exactly via
TDD + golden tests; compose on Arrow/DataFusion).

## Audience & tone

- **Audience:** bioinformaticians and data engineers who build or evaluate tools; anyone
  weighing "should we rewrite X in Rust?"
- **Tone:** technical, evidence-first, pointed but non-personal — matches the existing
  benchmark post. RastQC is named and critiqued with reproducible data; framed as a
  *symptom of skipping the principles*, not an attack on its authors.
- **Length:** ~2,250 words (~9 min read).

## The three take-home messages

The two fairnesses — **fair code** and **fair results** — plus the enabler that makes a fair win possible:

1. **Be fair to the original — emulate exactly** (per
   [rewrites.bio](https://rewrites.bio/#philosophy)): the rewrite must do what the original does,
   byte-for-byte / to scientist-defined precision, *proven with golden tests, not asserted*. **(fair code)**
2. **Be fair in what you present** — benchmark like-for-like and claim only what you validated:
   don't sell multi-thread-vs-single-thread parallelism as a rewrite "speedup," and don't call a
   tool "compatible" without the golden-test proof. **(fair results)**
3. **Earn a fair win by composing, not reinventing** (per the
   [Composable Data Management System Manifesto](https://www.vldb.org/pvldb/vol16/p2679-pedreira.pdf),
   Pedreira, Erling, Karanasos, Schneider, **McKinney**, Valluri, Zait, Nadeau, VLDB 2023):
   build on open, battle-tested components — Arrow · Parquet · DataFusion; be a citizen of the
   Python DataFrame ecosystem (Polars/Pandas) — so your advantage is real (architecture), not a
   JVM-vs-native or core-count artifact. **(the enabler)**

The discipline that *makes* fair code fair: **SDD + TDD**. SDD (specify the equivalence contract
before coding) and TDD (golden-test-first) are the *how*; **superpowers** is the concrete companion
— it embodies both (brainstorming→spec→plan is SDD; the TDD skill is the red-green loop).

Meta-message threaded throughout: **AI made code cheap; it did not make fairness cheap. Honest
engineering and honest benchmarking — the scarce, valued skills — are what turn a fast rewrite into
a trustworthy one.**

## Source grounding (quotes to weave in, verbatim)

**rewrites.bio:**
- §Philosophy: *"A wave of AI-assisted rewrites is coming. The question is not whether it
  will happen, but whether it will happen well."* · *"Cheap code is not the same as correct code."*
- §2.2 Emulate Exactly: *"The goal is a faster tool that produces the same results."* ·
  deterministic → *"byte-for-byte identical output files"* · floating-point → *"results
  within acceptable numerical precision (defined by scientists, not convenience)"* ·
  *"Everything counts: header formats, column ordering, file naming, summary statistics."*
- §3.1 Think Big: rethink the pipeline; design what a "from scratch" pipeline would look like.
- §3.2 Work Small: *"Start with the simplest function that produces testable output.
  Validate it. Then extend."*
- §4.1 Test with Real Data: multiple organisms, platforms, library preps; document hardware/datasets/commands.
- §4.2 Build Only What You Need: *"A rewrite that does four things correctly is more
  valuable than one that claims fifteen and does twelve right"* — and fail loudly for
  unsupported features. (RastQC ships 15 modules — 12 core + 3 long-read.)
- §4.3 Pin Versions: *"Bit-identical output to mytool v1.0, validated on the dataset in benchmarks/."*

**Composable manifesto:**
- Fragmentation *"has forced developers to reinvent the wheel, duplicating work and hurting
  our ability to quickly adapt systems"*; five reusable layers: language frontend,
  intermediate representation, query optimizer, execution engine, execution runtime.
- The **time-to-market fallacy**: a quick prototype with a subset of functionality
  *"understates the high cost of stabilizing (hardening) the software, and the long-tail of
  features required to turn the prototype into a real product … This usually results in
  products with incomplete and inconsistent features, hard to maintain … and generalized tech debt."*
- Cites Apache Arrow, Parquet, Velox, Substrait, Ibis. Co-authored by **Wes McKinney**
  (pandas, Arrow) — direct tie to the Python DataFrame ecosystem thesis.

**Our reproducible findings (from the benchmark post + this session):**
- per_sequence_quality: **671,121 reads** in a phantom Q40 bin FastQC never emits (mean-quality
  rounding bug) — thread-independent.
- %GC off by one (rounds vs truncates).
- **Not thread-invariant:** dedup 89.48% (`-t 1`) → 95.65% (`-t 4`); top k-mer flips
  GTATAAG → TACACAG with `-t`.
- polars-bio: 12/12 modules bit-exact vs FastQC 0.12.1 (dedicated golden tests), byte-identical
  across partition counts, kmer partition-invariance test.

**The performance claim is also parallelism, not code (for beat 5):**
- The RastQC preprint claims its *"streaming parallel pipeline with adaptive batch sizing delivers
  1.8–3.2× speedup on short-read Illumina data and 4.7–6.5× speedup on long-read ONT/PacBio data."*
  By its own wording the speedup is **parallelism** — comparing multi-thread RastQC against
  single-threaded FastQC (FastQC is single-threaded per file). The rewrite-vs-original attribution
  is never isolated.
- Our matched 1-vs-1 (single-thread) numbers show the rewrite is *slower than the JVM at equal footing*:
  ERR5897746 (4.3M) FastQC 16.63s vs **RastQC 18.10s**; DRR013000 (24.8M) FastQC 63.84s vs
  **RastQC 68.42s**. A natively-compiled Rust tool losing to a JVM tool at 1 thread means the port
  bought its "speedup" with cores, not better code or algorithms.
- Contrast polars-bio single-thread: **6.46s** (4.3M) and **28.63s** (24.8M) — 2–2.6× *faster than
  FastQC at one thread*, then scales further. The win survives at equal footing because it comes from
  the architecture (an optimized engine), not core count.

**RastQC test-gap evidence (the crux of beat 3 — why the bugs shipped):**

*Why each bug we found slipped through:*
- **per_seq_quality misbinning (671k reads → phantom Q40):** `per_sequence_quality.rs` has
  **0 unit tests**; the only test that touches it greps for the header string
  `>>Per sequence quality scores`. Nothing inspects a bin count — a module could emit pure noise
  and every test still passes.
- **%GC off-by-one (rounds vs truncates):** `basic_stats.rs` *does* have a GC test, but it asserts
  `(gc_percent - 50.0).abs() < 0.01` on a synthetic exactly-50%-GC input. 50.0 rounds *and*
  truncates to 50, so the divergence is structurally invisible; it also checks the internal float,
  not the displayed integer where the rounding happens, and never compares to FastQC's convention.
- **Thread-dependent duplication & k-mer:** `duplication.rs` and `kmer_content.rs` have **0 unit
  tests**, and nothing anywhere runs the same input at `-t 1` vs `-t N` and compares. Worse, every
  test input is 1–3 reads and RastQC only engages its parallel path on files >50 MB (per its own
  `--no-parallel` help) — so the multi-worker sampling code that causes the drift is *never executed
  by the test suite*.

*The systemic gaps:*
1. **No golden/oracle parity** — the "FastQC-compatible" promise has zero verification against FastQC output.
2. **No per-module value tests** on 12 of 13 modules — all the statistical logic (binning, rounding,
   per-worker sampling, the 100k-unique freeze) is untested.
3. **No determinism / thread-invariance tests** — nothing asserts output is stable across `-t`,
   despite parallelism being the product's differentiator.
4. **Inputs too small and too uniform** — 1–3 reads of `IIII…`/`BBBB…` never reach the parallel path,
   the duplication freeze, k-mer enrichment (needs thousands of reads for 2% sampling), or a realistic
   mean-quality distribution where the rounding bug bites.
5. **Presence-only assertions** (`contains(">>Module")`) give a false "all 15 modules work" signal
   while guaranteeing nothing about correctness.

(These are what polars-bio's approach inverts point-for-point: golden parity per module, real-data
inputs, a partition-invariance test, and value assertions — not header greps.)

## Structure (7 beats, ~2,250 words)

1. **Hook — the wave is here (~200w).** AI made rewriting cheap; quote rewrites.bio (*"Cheap code
   is not the same as correct code."*). Thesis up front: a rewrite is a *promise* — to the original
   authors whose validation you inherit, to the scientists who trust your numbers, to the readers of
   your benchmarks. Doing it well is about **fairness on two axes: fair code and fair results.**
   AI made the code cheap; it did not make fairness cheap.
2. **The false promise: what "FastQC-compatible" must mean — and the symptoms (~350w).** Define
   *compatible* via §2.2 Emulate Exactly (byte-for-byte / scientist-defined precision / "everything
   counts"). Then the symptoms from our runs: per-seq-quality misbins 671k reads into a phantom Q40
   bin, %GC off-by-one, and — the deeper tell — not self-consistent across thread counts (dedup
   89→96%, top k-mer flips with `-t`). Link the full 12-module drift table in the benchmark post.
3. **Postmortem: how a "FastQC-compatible" tool shipped incompatible (~450w).** Blameless,
   root-cause — framed explicitly as a postmortem. *Why each bug slipped through* (compact 3-row
   table: bug → the test gap that let it pass): the two worst modules have **0 unit tests**; the one
   GC test uses a 50%-GC input where round==truncate; nothing runs `-t 1` vs `-t N`, and the parallel
   path (>50 MB) is never exercised by 1–3-read fixtures. Then *the systemic gaps* (5 bullets): no
   golden/oracle parity, no per-module value tests on 12/13 modules, no determinism tests,
   too-small/too-uniform inputs, presence-only `contains(">>Module")` assertions. Diagnosis: the
   composable manifesto's **time-to-market fallacy** + rewrites.bio §4.2 made literal — *"claims
   fifteen and does twelve right."*
4. **The fix — spec-driven + test-driven development (~300w).** The discipline that inverts every
   gap. Emulate-exactly is a contract you *specify* then *verify*: spec first (rewrites.bio Planning:
   Think Big / Work Small — pin the equivalence contract before code), then golden-test-first (write
   the FastQC assertion, watch it fail, implement to green), on **real data** (§4.1) with **pinned
   versions** (§4.3). **superpowers is the concrete companion — it embodies both: brainstorming→spec→plan
   is SDD; the TDD skill is the red-green loop.** polars-bio got 12/12 bit-exact + a partition-invariance
   test this way — value assertions, not header greps.
5. **Exact isn't enough — re-architect, don't reinvent (~550w).** *Open with the performance
   tell:* the preprint's headline "1.8–3.2× / 4.7–6.5× speedup" is a **parallelism** result by its
   own wording (multi-thread RastQC vs single-threaded FastQC; the rewrite-vs-original effect is never
   isolated). Isolate it and the story flips — at **one thread** the Rust rewrite is *slower than the
   JVM* on real files (16.6→18.1s; 63.8→68.4s). A natively-compiled tool losing to the JVM at equal
   footing means the port bought its speedup with cores, not code — it *parallelized* rather than
   *re-architected* (and its parallel sampling is exactly what breaks thread-invariance). Contrast:
   polars-bio is 2–2.6× faster than FastQC *at a single thread*, then scales. *Then the principle:*
   rewrites.bio §3.1 Think Big + the composable manifesto — the win comes from architecture, not core
   count. RastQC re-implements a whole QC engine + FASTQ reader + thread pool + output from scratch,
   in isolation. polars-bio instead composes: FastQC modules as streaming aggregations on **DataFusion**,
   read via **Arrow**, landing in **Polars/Pandas**, `SELECT * FROM fastqc()`. Single-thread speed and
   thread-invariance are both *inherited* from a battle-tested engine, not hand-rolled.
6. **The three take-homes (~300w).** Crystallize fair code (emulate exactly, proven) · fair results
   (honest benchmarks and claims) · the enabler (compose on battle-tested components) + the meta-message.
7. **Close (~150w).** The wave is coming regardless; do it well. A rewrite is a promise to everyone
   downstream — the honest version of "faster" and "compatible" is the one you can *prove*. AI made
   the code cheap; fairness is still the work.

## Placement & format

- New post: `docs/blog/posts/rewriting-bioinformatics-2026-07.md` (mkdocs blog).
- Front-matter: `date.created: 2026-07-04`, `categories: [opinion, architecture, postmortem]`, `draft: false`.
- **Prose-heavy → written directly as Markdown** (no SVG generator; unlike the benchmark post,
  no charts are needed).
- Two compact inline tables: (a) beat 2 — the 4 headline RastQC drifts (per-seq quality, %GC,
  dedup thread-drift, kmer thread-drift); (b) beat 3 postmortem — a 3-row "bug → the test gap that
  let it pass" table. Link the full 12-module drift table in the benchmark post rather than reproduce it.
- Cross-link the benchmark post (and it can later link back).
- Commit to `feat/fastqc-phase1`; verify with `mkdocs build`.

## Success criteria

- Reads as one coherent argument (not three stapled genres); the three take-homes are explicit
  and each cited to its source.
- **Fairness is the visible through-line** — each beat is legibly an instance of fair code or fair
  results, and the two axes are named up front and paid off in the close.
- Every RastQC claim is backed by a reproducible number already published in the benchmark post.
- The postmortem reads as blameless root-cause (bug → test gap → systemic gap → fix), not a takedown.
- The performance critique is fair: the preprint's speedup claim is quoted and attributed, and the
  "parallelism not code" counter-argument rests on our reproducible 1-vs-1 (single-thread) numbers.
- rewrites.bio and the composable manifesto are quoted accurately and linked.
- Spec-driven + test-driven development (superpowers as the companion) appears as the concrete
  *how* behind quality — the discipline that produces "emulate exactly."
- The broadened thesis is explicit: the hard part is architecture **and** quality **and** discipline,
  not architecture alone.
- `mkdocs build` clean (no new post-specific warnings).

## Out of scope (YAGNI)

- No new benchmarks or re-runs; reuse published numbers only.
- No SVG charts or a Python generator for this post.
- No changes to the benchmark post beyond an optional cross-link.
- Not a general survey of Rust bioinformatics tools — RastQC is the single worked example.
