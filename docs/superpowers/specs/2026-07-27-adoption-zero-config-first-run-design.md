# Zero-config first run: conda distribution and parallel-by-default

**Date:** 2026-07-27
**Status:** Design — awaiting review
**Scope:** First sub-project of a larger adoption effort (see [Follow-on sub-projects](#follow-on-sub-projects))

## Problem

polars-bio is at roughly 8.6k PyPI downloads/month. Its peers: bioframe 31k, pyranges 82k,
pybedtools 97k, pysam 1.17M. The library has a published Bioinformatics paper, a fast release
cadence, benchmark-backed performance claims, and a thorough test suite.

A survey of all 137 GitHub issues (46 from external users, 22 distinct external reporters) found
**almost no complaints about performance or about the correctness of interval operations**. The
core value proposition holds up under real use. All observed friction is at the edges: installing
the library, loading the user's own files, and understanding why it is not as fast as advertised.

This sub-project addresses the two edges that gate a new user's first five minutes:

1. **polars-bio cannot be installed with conda.** It is on neither bioconda nor conda-forge, while
   bioframe, pyranges, pybedtools and pysam all are. In bioinformatics, conda is the dominant
   install channel: nf-core modules, Snakemake wrappers and biocontainers resolve packages from
   bioconda, and a bioconda recipe automatically produces a Docker/Singularity biocontainer. Being
   absent excludes polars-bio from every pipeline that follows those conventions.

2. **Parallelism is off by default.** `polars_bio/context.py:36` sets
   `datafusion.execution.target_partitions` to `"1"`, documented in `docs/features/parallel.md` as
   "parallel execution disabled". A new user pip-installs, runs an overlap, and receives none of the
   multicore speedup the README leads with. The headline claim ("native parallel engine", "282x
   with 8 CPU cores") is contradicted by the out-of-the-box experience.

Both are cheap to fix relative to their leverage, and both are prerequisites for the rest of the
adoption work: capability and documentation improvements only reach users who got past the install.

## Goals

- A user on any major platform can run `conda install -c bioconda polars-bio` and get a working
  library.
- A user who writes no configuration gets multicore performance on a multicore machine.
- Neither change introduces a correctness regression. In particular, enabling parallelism by
  default must not silently reorder written records.

## Non-goals

- Switching the default coordinate system to 0-based. This is a breaking change with its own
  OpenSpec proposal (`openspec/changes/switch-default-to-zero-based-coordinates`) and deserves its
  own release cycle. It is the natural next sub-project.
- Any new file format, interval operation, or analysis function.
- Loosening dependency pins beyond what conda packaging requires.

## Design

The work splits into two independent tracks that can proceed in parallel, plus one blocking
correctness fix that gates the second track.

### Track A — conda distribution

polars-bio already has an unshipped, untracked recipe in `bioconda-recipe/` and a release-triggered
workflow at `.github/workflows/update_bioconda_recipe.yml`. Neither has been submitted. The work is
to finish and land them.

**A1. Publish `polars-config-meta` to conda-forge (prerequisite).**
polars-bio depends on `polars-config-meta>=0.3.0,<1`, which is not available on conda-forge or
bioconda. Verified against the Anaconda API on 2026-07-27:

| Dependency | conda-forge | Notes |
|---|---|---|
| `polars` | 1.43.0 | available |
| `pyarrow` | 25.0.0 | available |
| `datafusion` | 54.0.0 | available; polars-bio pins `>=53,<54`, so the 53 line must still be resolvable |
| `polars-config-meta` | **missing** | blocks the bioconda recipe |

`polars-config-meta` is a small pure-Python package, so this is a `noarch: python` recipe submitted
to `conda-forge/staged-recipes`. It is a dependency of polars-bio rather than a bioinformatics tool,
which is why conda-forge is the correct channel for it rather than bioconda.

**A2. Submit polars-bio to bioconda.**
polars-bio is a compiled Rust extension, so the recipe builds from the sdist with a Rust toolchain
rather than repackaging wheels. Bioconda has established precedent for Rust-backed packages. The
recipe must pin `datafusion` to the 53 line to match `pyproject.toml`, and declare the same
`requires-python = ">=3.11,<3.15"` floor.

**A3. Wire the release automation.**
`.github/workflows/update_bioconda_recipe.yml` already computes version and sha256 on release
publish. It needs to be committed, and its target changed from a local recipe edit to opening a pull
request against `bioconda/bioconda-recipes`. Note that bioconda's own autobump bot will often do
this unprompted once the package exists; the workflow's value is making the version bump
deterministic rather than waiting on the bot.

**Verification.** The recipe passes `bioconda-utils lint`, builds under
`bioconda-utils build --docker --mulled-test`, and a post-merge smoke test installs from bioconda in
a clean environment and runs an overlap plus one read per format.

### Track B — parallel by default

**B1. Fix issue #421 first. This blocks B2 and is not optional.**

Issue #421 documents that at `target_partitions > 1`, `write_bam` / `write_cram` / `write_sam` and
their `sink_*` variants do not preserve input row order, and that the output order is
nondeterministic across runs. The alignment write plans are single-partition
(`write_exec` uses `Partitioning::UnknownPartitioning(1)`), so DataFusion inserts a
`CoalescePartitionsExec` that merges parallel read partitions in *completion* order rather than
partition-index order.

The issue explicitly notes that "the default `target_partitions` is `1`, so the test suite and
typical usage are unaffected". **Flipping the default converts this latent bug into a default-on
data-integrity bug**: every user writing alignments on a multicore machine would silently get
records in a nondeterministic order, and downstream tools that assume coordinate-sorted BAM would
need a re-sort. For a project whose stated positioning is correctness and verified parity, shipping
that would be self-defeating.

The fix is to make the coalesce feeding the writer order-preserving — either by declaring an output
ordering on the read exec so DataFusion selects `SortPreservingMergeExec`, or by coalescing in
partition-index order in the write path. This lives in `datafusion-bio-formats` (bam/cram
`write_exec` plus `physical_exec`), followed by a version bump here.

VCF write must be audited for the same failure mode; `tests/test_vcf_write.py` already exercises
`tp > 1` and is the natural place to assert it.

**B2. Change the default to auto-detected cores.**

Remove the hardcoded `"1"` from the `datafusion_conf` dict in `polars_bio/context.py`, letting
DataFusion apply its own default. Verified in `datafusion-common-53.1.0/src/config.rs:506`, that
default is `get_available_parallelism()`, which delegates to `std::thread::available_parallelism()`
(`utils/mod.rs:927`) and is memoized in a `LazyLock`. Two details matter:

- `Context.__init__` builds both the Rust `BioSessionContext` and a mirrored
  `datafusion.context.SessionConfig(datafusion_conf)`. Both must agree on the new default, or
  Python-side and Rust-side plans will disagree on partition count.
- The upstream reader-thread fold (`datafusion-bio-formats` v1.8.7, merged in `25f4d47`) is what
  makes this safe. Before it, each partition spawned its own reader thread and effective core usage
  was roughly 2× `target_partitions`. Post-fix, effective cores track `target_partitions` at ~1×,
  verified across four FastQC datasets. Without that fold, an auto-detected default would
  oversubscribe the machine.

**B3. Audit the test suite for tp-sensitivity.**

The suite currently runs at `tp = 1` and therefore has never exercised the multi-partition path
broadly. Flipping the default runs everything at `tp = N`, which is the point — but it will surface
latent ordering and race assumptions. Two are already known: the `test_streaming.py` flakiness
(Arrow C Stream consumed race) and the singleton-`Context` option leak between tests that caused the
PR #420 BAM/CRAM failures. Tests that genuinely depend on single-partition behavior should set the
option explicitly via a fixture that restores the prior value, rather than relying on the global
default.

**Verification.** A determinism test writes the same input at `tp = 1` and `tp = 8` across repeated
runs and asserts byte-identical or value-identical row order for BAM, CRAM, SAM and VCF. The
existing benchmark suite is re-run to confirm the default now reproduces published multicore
numbers without configuration.

### Documentation

`docs/features/parallel.md` currently states "The default value is **1** (parallel execution
disabled)" and must be rewritten to describe the new default and how to *reduce* parallelism, which
becomes the less common case. The README's performance claims become true as stated for a default
install. A changelog entry should call out the behavior change explicitly, since users who tuned
`target_partitions` around the old default may see different resource usage.

## Risks

**Bioconda build complexity.** Compiling a Rust extension with a large dependency tree inside
bioconda's build containers may hit toolchain or build-time limits. Mitigation: build and mulled-test
locally under Docker before submitting, and treat the conda-forge `polars-config-meta` recipe as the
independent first step so its progress is not blocked by polars-bio's build.

**Raising the default surfaces unknown tp-sensitive bugs.** #421 is the one we know about; the
suite's limited multi-partition coverage means there may be others. Mitigation: B3 exists precisely
to find them, and the tracks are ordered so the default flip lands after the suite runs green at
`tp > 1`.

**Resource usage changes for existing users.** Anyone who relied on the implicit single-threaded
default will see polars-bio consume all available cores. This is lower risk than it first appears:
`std::thread::available_parallelism()` respects cgroup v1/v2 CPU quotas and `sched_getaffinity` on
Linux, so containerized and HPC-scheduler-bound runs get their allotted share rather than the host
core count. Mitigation is therefore a prominent changelog entry plus one integration test asserting
the detected partition count under a constrained cgroup, rather than custom quota detection.

## Sequencing

Track A and Track B are independent and can run concurrently. Within B, B1 strictly precedes B2.

1. A1 — `polars-config-meta` to conda-forge (unblocks A2; long external review latency, so start first)
2. B1 — fix #421 upstream, bump, verify (blocks B2)
3. A2 — polars-bio to bioconda
4. B3 — test-suite audit at `tp > 1`
5. B2 — flip the default, update docs and changelog
6. A3 — release automation

## Follow-on sub-projects

Listed in recommended order, each to get its own spec:

1. **I/O trust on real-world files.** Roughly 22% of external issues are VCF field/INFO/genotype
   parsing problems, several involving *silent* data loss rather than an error (#204 loaded the wrong
   row count from Ensembl VCFs; #312 truncated multi-valued INFO). Nearly every reader bug traces to
   a file shape absent from the test corpus: Ensembl, ClinVar, DeepVariant, Cell Ranger, 10X
   CITE-seq, nanopore tags, multi-member gzip. Proposed: a real-world file corpus as a CI gate, plus
   the multisample genotype ergonomics ask in #394. This is the highest-value follow-on, because
   polars-bio's real competitive surface is the I/O layer where pysam does 1.17M downloads/month, and
   trust there is earned only on files you did not choose.
2. **0-based coordinates by default.** Four independent reporters have been confused by the current
   1-based default (#259, #278, #413, #356). The OpenSpec change already exists.
3. **Capability pull.** No interval operation has any strand awareness — no `strandedness` or
   `ignore_strand` parameter across all eight ops, which is pyranges' single biggest differentiator
   and effectively required for RNA-seq and annotation work. Also: bedtools-style `map` aggregation
   over overlapping features, writers for BED/GFF/GTF/BigWig (annotation round-trips are currently
   impossible), int64 coordinates (#169, currently a 2Gb chromosome cap), and cloud sinks.
4. **Onboarding and positioning.** No migration guide from bioframe/pyranges/pybedtools, one
   tutorial notebook, and issue #260 ("how is this different from just using Polars?") never
   answered in the docs.
