# Handover: PGEN wiring in polars-bio, then BCF/BGEN/PGEN benchmarks

Written at the end of a session that ran out of context. Everything below is
either verified in that session or explicitly marked as unverified.

## Goal

Benchmark **BCF, BGEN and PGEN** through polars-bio and establish three things:

1. faster than `snputils`,
2. no mismatches against it,
3. scalability at `t = 1, 2, 4, 8`.

Two of the three are ready. PGEN is not exposed by polars-bio at all and has to
be wired first — that is the blocking task.

## State right now

### datafusion-bio-formats (upstream provider)

- `master` is at **`e029e08`** and contains the PGEN provider, the BGEN and BCF
  work from #216, and the reference-oracle CI.
- Merged this session: #216 (BGEN/BCF fixes), #231 (PGEN to master + oracles).
- Open: **#230** — the shared BGI cache holds a global lock across downloads and
  evicts entries before validating the replacement. Deferred deliberately;
  affects only *remote* index opens, so local-file benchmarking is unaffected.

### polars-bio

- Branch **`feat/bgen-pr220-bench`** (PR #436), last commit `ad93755`.
- **Uncommitted in the working tree:** the provider pin bumped from `2c06f23` to
  `e029e08` across all 12 `datafusion-bio-format-*` dependencies in `Cargo.toml`,
  plus the matching `Cargo.lock`. Verified: 12 lock entries at the new rev and
  `cargo check --workspace` clean. **Not committed** — decide whether it belongs
  on this branch (scoped to BGEN APIs) or its own.
- `read_bcf` / `scan_bcf` and `read_bgen` / `scan_bgen` already exist.
- **No PGEN anywhere**: `grep -c pgen Cargo.toml` is 0.

### bioformats-benchmark

- Runners: `run_bcf_benchmarks.py`, `run_bgen_benchmarks.py`,
  `run_genotype_matrix_benchmarks.py`.
- Existing writeups: `BCF_BENCHMARK.md`, `BGEN_BENCHMARK.md`,
  `GENOTYPE_READER_BENCHMARK.md`.
- `setup.sh` **requires `plink2`** to build the BGEN and PGEN fixtures, and
  `plink2` is not on this machine's PATH. Resolve before generating fixtures —
  `bioconda::plink2` works (CI installs it that way).

## Task 1 — wire PGEN into polars-bio

Use the BGEN work as the template. `git diff master...feat/bgen-pr220-bench`
shows exactly what a format needs: ~1,291 lines over 20 files. PGEN will be
comparable or slightly larger, because its option surface is wider (allele vs
dosage output, sample selection, `DS` / `HDS` / `PHASED` / `DS_STORED`).

Anchors, current as of this handover:

| File | What BGEN added | Line seen |
|---|---|---|
| `Cargo.toml` | `datafusion-bio-format-bgen` pinned dep | alongside the other 12 |
| `src/lib.rs` | `use datafusion_bio_format_bgen::{...}` | 23–25 |
| `src/option.rs` | `InputFormat::Bgen` variant | 147 |
| `src/option.rs` | `"BGEN"` display arm | 176 |
| `src/option.rs` | `pub bgen_read_options: Option<BgenReadOptions>` | 209 |
| `src/option.rs` | the `#[pyo3(signature = ...)]` list | 215 |
| `src/scan.rs` | provider construction and registration | — |
| `polars_bio/io.py` | `read_bgen` / `scan_bgen` | — |
| `polars_bio/sql.py` | registration and describe | — |
| `polars_bio/metadata_extractors.py` | schema metadata surfacing | — |
| `polars_bio/__init__.py` | exports | — |
| `tests/test_bgen_io.py` | 287 lines | — |
| `tests/data/io/bgen/` | fixtures | — |
| `openspec/changes/add-bgen-support/` | proposal, design, spec, tasks | — |

Note the `#[pyo3(signature = ...)]` list at `option.rs:215` enumerates every
format's options positionally — adding PGEN means extending it, and forgetting
that is a silent binding mismatch rather than a compile error.

The provider API to bind is `PgenTableProvider::try_new(path, PgenReadOptions)`;
see `datafusion/bio-format-pgen/src/table_provider.rs` upstream. Fields worth
exposing: `samples`, `genotype_fields` (e.g. `["DS"]`), and the allele/dosage
output mode.

## Task 2 — benchmarks

Once PGEN reads through polars-bio:

1. `plink2` on PATH, then `bioformats-benchmark/setup.sh` to build fixtures.
2. Run all three runners at `t = 1, 2, 4, 8`.
3. Compare against `snputils` for both **runtime** and **value equality** — the
   "no mismatches" claim needs an actual element-wise comparison, not just equal
   row counts.

The reference oracles are already installed in the venv at
`~/CLionProjects/polars-bio/.venv` (`pgenlib`, `snputils`, `numpy`, `bgen`
confirmed importable), which is also what the upstream parity tests use.

## Warnings earned the hard way this session

Three times a green signal was misleading, each caught only by checking against
something independent:

1. A scripted edit **silently did not apply** — the pattern no longer matched
   after rustfmt reshaped the code — and it compiled cleanly. Only a test written
   for the specific finding exposed it. *Assert that string replacements matched;
   a clean build is not evidence a change landed.*
2. A test **passed for the wrong reason** — it passed with the fix stubbed out,
   because an unrelated code path failed first. *Run every new test against the
   unfixed code before believing it.*
3. A CI run reported **success for a stale commit**. The workflow's `push`
   trigger is scoped to `master`, so pushing a branch runs nothing; a poll that
   asked "is the newest run green?" answered yes about yesterday's commit.
   *Match CI results on the commit SHA.*

The same discipline applies directly to the benchmark numbers: "faster than
snputils" is exactly the kind of claim that is easy to produce and hard to trust.
Record thread counts, input sizes, and the comparison method alongside the
timings, and verify the mismatch check can actually fail — feed it deliberately
altered data once and confirm it reports a mismatch.

## Smaller open items

- **qctool** is the one reference oracle CI cannot require (no package exists in
  any manager). It sits behind `BGEN_REQUIRE_QCTOOL`, unset in CI.
- **Record-range policy, undecided.** Should a malformed record range from a BGI
  be a hard failure regardless of `StaleBgiPolicy`, rather than falling back to
  the walk? Neither review bot engaged with this across seven rounds. Current
  behaviour is the fallback; the intent was to write that into the spec as a
  deliberate decision rather than leave it implicit. Still unwritten.
- **Stale worktree:** `datafusion-bio-formats-genotype-specs` holds uncommitted
  pgen spec edits from before this session and is now far behind master. Untouched
  throughout — reconcile or discard.
- Worktrees `datafusion-bio-formats-merge` and `datafusion-bio-formats-p1fix` are
  now redundant and can be removed.
