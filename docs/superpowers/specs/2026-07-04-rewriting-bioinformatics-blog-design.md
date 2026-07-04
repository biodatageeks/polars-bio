# Design: Follow-up blog post — "Rewriting bioinformatics software, done right"

**Date:** 2026-07-04
**Type:** Blog post (opinion + case study + how-to, combined)
**Branch:** `feat/fastqc-phase1`
**Status:** Approved outline; drafting pending user review of this spec.

**Working title (recommended):** *Cheap to rewrite, expensive to architect: the "FastQC-compatible" trap*
Alternatives: "A rewrite is an architecture decision, not a translation" · "Emulate exactly, compose everything: how to rewrite a bioinformatics tool"

## Purpose

A follow-up to the FastQC benchmark post that argues a single thesis: **in the age of
cheap AI-assisted rewrites, the code is no longer the hard part — the architecture is.**
Rewriting a bioinformatics tool well is an act of *re-architecture*, not a lift-and-shift
to a new language. The post uses RastQC as a named, fair, evidence-first cautionary
example of a rewrite that skipped the principles, and polars-bio as the counter-example.

Combines three genres in one piece: an opinion/manifesto argument, a data-backed case
study (our reproducible RastQC findings), and a practical how-to (emulate-exactly via
TDD + golden tests; compose on Arrow/DataFusion).

## Audience & tone

- **Audience:** bioinformaticians and data engineers who build or evaluate tools; anyone
  weighing "should we rewrite X in Rust?"
- **Tone:** technical, evidence-first, pointed but non-personal — matches the existing
  benchmark post. RastQC is named and critiqued with reproducible data; framed as a
  *symptom of skipping the principles*, not an attack on its authors.
- **Length:** ~1,800 words (~7 min read).

## The three take-home messages

1. **Emulate exactly** (per [rewrites.bio](https://rewrites.bio/#philosophy)) — parity is
   *validated, not asserted*.
2. **Compose, don't reinvent** (per the [Composable Data Management System Manifesto](https://www.vldb.org/pvldb/vol16/p2679-pedreira.pdf),
   Pedreira, Erling, Karanasos, Schneider, **McKinney**, Valluri, Zait, Nadeau, VLDB 2023) —
   build on Arrow · Parquet · DataFusion; be a citizen of the Python DataFrame ecosystem.
3. **Open & interoperable on battle-tested components** — the durable value is the
   architecture, not the port.

Meta-message threaded throughout: **programming and migration are cheap now; architecting
data systems is what remains highly valued.**

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
- Test-gap analysis: RastQC's suite is plumbing-only; per_sequence_quality / duplication /
  kmer_content have **zero unit tests**; no golden/oracle comparison anywhere; inputs are 1–3
  synthetic reads that never reach the parallel path.
- polars-bio: 12/12 modules bit-exact vs FastQC 0.12.1 (dedicated golden tests), byte-identical
  across partition counts, kmer partition-invariance test.

## Structure (6 beats, ~1,800 words)

1. **Hook — the wave is here (~200w).** AI made rewriting cheap; quote rewrites.bio. Thesis
   up front: migration is cheap, architecture is the moat. A rewrite is an architectural act.
2. **The false promise: "FastQC-compatible" (~350w).** Define *compatible* via §2.2 Emulate
   Exactly. Present the RastQC evidence (misbinning, %GC, thread-drift). Link the drift table
   in the benchmark post. Name the root cause: the time-to-market fallacy + "claims fifteen,
   does twelve right."
3. **Emulate exactly is a discipline → TDD + golden tests (~300w).** Parity is tested, not
   hoped for. Golden-test-first (write the FastQC assertion, watch it fail, implement to green)
   — the **TDD-with-superpowers** companion. polars-bio's 12/12 golden tests + partition-invariance
   test vs RastQC's plumbing-only suite on synthetic reads. §4.1 real data, §4.3 pin versions.
4. **Exact isn't enough — re-architect, don't reinvent (~400w).** rewrites.bio §3.1 Think Big.
   RastQC re-implements a whole QC engine + FASTQ reader + thread pool + output from scratch,
   in isolation. Composable manifesto: compose from open, battle-tested components. polars-bio:
   express modules as streaming aggregations on DataFusion, read via Arrow, land in Polars/Pandas,
   `SELECT * FROM fastqc()`. Thread-invariance is *inherited* from DataFusion's associative-merge
   model, not hand-rolled.
5. **The three take-homes (~300w).** Crystallize the three messages above + the meta-message.
6. **Close (~150w).** The wave is coming regardless; do it well. Rewriting is translation only
   in the trivial sense; it is fundamentally an architecture decision.

## Placement & format

- New post: `docs/blog/posts/rewriting-bioinformatics-2026-07.md` (mkdocs blog).
- Front-matter: `date.created: 2026-07-04`, `categories: [opinion, architecture]`, `draft: false`.
- **Prose-heavy → written directly as Markdown** (no SVG generator; unlike the benchmark post,
  no charts are needed).
- One compact inline table: the 4 headline RastQC drifts (per-seq quality, %GC, dedup thread-drift,
  kmer thread-drift). Link the full 12-module drift table in the benchmark post rather than
  reproduce it.
- Cross-link the benchmark post (and it can later link back).
- Commit to `feat/fastqc-phase1`; verify with `mkdocs build`.

## Success criteria

- Reads as one coherent argument (not three stapled genres); the three take-homes are explicit
  and each cited to its source.
- Every RastQC claim is backed by a reproducible number already published in the benchmark post.
- rewrites.bio and the composable manifesto are quoted accurately and linked.
- TDD-with-superpowers appears as the concrete *how* behind "emulate exactly."
- `mkdocs build` clean (no new post-specific warnings).

## Out of scope (YAGNI)

- No new benchmarks or re-runs; reuse published numbers only.
- No SVG charts or a Python generator for this post.
- No changes to the benchmark post beyond an optional cross-link.
- Not a general survey of Rust bioinformatics tools — RastQC is the single worked example.
