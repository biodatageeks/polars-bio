# Handover: PGEN performance and the three-format benchmark

Written 2026-08-17, revised the same day after the common-value + difflist decode
was fused and the PGEN benchmark re-run. Supersedes
`HANDOVER-pgen-benchmarks.md`, whose two tasks (wire PGEN into polars-bio;
benchmark BCF/BGEN/PGEN) are both **done**.

Everything below is committed. Nothing is half-finished. Two branches are ahead
of their remotes — see task 4.

## Start here

```
Read HANDOVER-pgen-perf.md in polars-bio and
datafusion/bio-format-pgen/PERF_HANDOVER.md in the provider worktree at
/Users/mwiewior/CLionProjects/dbf-pgen-perf. Then pick up task 1.
```

## Repository state

| Repo | Branch | Head | Notes |
|---|---|---|---|
| polars-bio | `feat/bgen-pr220-bench` | `ed33b57` | PR #436; pins provider `25d6bd2`. **Not pushed since `6c76f4e`** |
| datafusion-bio-formats | `perf/pgen-batch-array-build` | `25d6bd2` | PR #232, 9 commits, open, pushed |
| bioformats-benchmark | `feat/bgen-benchmark` | `55d7bf0` | local only, **not pushed** |

Provider worktree: `/Users/mwiewior/CLionProjects/dbf-pgen-perf`.
Benchmark venv: `bioformats-benchmark/.venv-bcf` — has polars-bio (editable,
from the checkout), snputils, pgenlib, bgen, cyvcf2, pysam.

`plink2` is installed at `~/.local/bin/plink2` (native arm64, v2.0.0-a.7.3).
PGEN fixtures exist at `/Users/mwiewior/research/data/PGEN/`.

## What was accomplished

PGEN reads through polars-bio (`read_pgen` / `scan_pgen` / `register_pgen` /
`describe_pgen`, 51 tests). All three formats benchmarked at equal core count
with element-wise correctness gates. The provider's single-partition PGEN scan
went **11.2 s → 1.19 s** for dosage and **0.59 s** for hardcall, after fusing
the common-value + difflist decode.

Current numbers, chromosome 22, 993,881 × 2,548 = 2,532,408,788 genotypes,
**one thread each** — pgenlib, snputils and `bgen` are all single-threaded.
PGEN rows re-measured 2026-08-17 with provider `25d6bd2`, all readers
interleaved in one session:

| Format | polars-bio | snputils | reference |
|---|---:|---:|---:|
| BCF (int8) | **5.251 s** | 8.285 s | — |
| PGEN hardcall (int8) | 1.940 s | 1.506 s | pgenlib 0.832 s |
| PGEN dosage (f32) | **3.225 s** | 3.260 s | pgenlib 1.787 s |
| BGEN dosage (f32) | 25.804 s | 21.171 s | bgen 15.064 s |

polars-bio wins on BCF and now edges snputils on PGEN dosage, where it was 1.34×
behind; the PGEN hardcall gap to snputils closed from 1.96× to 1.29×. pgenlib and
snputils reproduced their previous figures to within 1% in the same session, so
the PGEN deltas are the change and not drift. Zero bitwise differences against
the reference in all three formats, with the single-cell corruption self-test
confirming the comparison can still fail.

## Where the remaining PGEN time goes

This is the main thing the re-measurement changed. The Rust scan improved 1.94×
(dosage) and 2.80× (hardcall), but end-to-end only 1.35× and 1.53×, because
materialization into a contiguous array is untouched and is now the larger term:

| | total | scan | materialization |
|---|---:|---:|---:|
| dosage | 3.225 s | 1.19 s | **~2.03 s (63%)** |
| hardcall | 1.940 s | 0.59 s | **~1.35 s (70%)** |

Optimizing the decoder further now buys progressively less. The materialization
path is where the bulk is.

## Tasks, in priority order

### 1. Attack materialization, not the decoder

Per the table above, ~2.0 s of the 3.2 s dosage total and ~1.35 s of the 1.9 s
hardcall total is Python-side assembly of the contiguous array, not decode. Start
by finding out what that time actually is — Arrow-to-numpy copy, per-batch
concatenation, or an avoidable intermediate — before optimizing anything.

The provider-side follow-ups in `PERF_HANDOVER.md` (SIMD the difflist patch loop)
are now chasing a much smaller slice; re-profile before spending time there.

**Do not implement issue #233 as written.** It proposes a 2-bit packed main
track, which is aimed at the LD branch (13%) and would be a regression for the
dominant one. Corrected in a comment on the issue.

### 2. Explain the dosage peak-RSS increase

Still open, and now sharper: with provider `25d6bd2` the dosage path peaks at
**22.31 GB** against pgenlib's 12.09 GB for the identical 10.13 GB output, while
hardcall sits at 8.25 GB against 5.02 GB. Fusion neither caused nor fixed this —
the Rust-only scan peaks at 9.97 GB (dosage) and 2.9 GB (hardcall), unchanged by
the change — so the excess is in the materialization path, which makes this the
same investigation as task 1.

**Resolve before publishing the numbers.** A benchmark carrying 1.85× pgenlib's
memory for identical output is not a clean result.

### 3. Refresh the published figures

`bioformats-benchmark/PGEN_BENCHMARK.md` and
`polars-bio/docs/blog/posts/bcf-genotype-readers-2026-08.md` still carry
pre-fusion PGEN numbers — now two generations stale. Use the table above. The
blog post is otherwise current: it covers all three formats at one thread with
the correctness section.

Raw results for the post-fusion run: `bioformats-benchmark/results/pgen_full_fused.json`
(gitignored, so regenerate with the command below if it is gone).

### 4. Push the branches

`polars-bio` `feat/bgen-pr220-bench` is 1 commit ahead of its remote (the
provider bump), and `bioformats-benchmark` `feat/bgen-benchmark` is 5 commits
ahead, local only.

## Measurement rules — each of these produced a wrong number in the last session

1. **Build release + native.** `RUSTFLAGS="-C target-cpu=native" maturin
   develop --release`. A plain `maturin develop` is debug, measured 3.1×
   slower, and inverted a headline conclusion. Release `.so` ≈ 228 MB, debug
   ≈ 336 MB.
2. **One thread against one thread.** The comparison readers are all serial.
   Comparing polars-bio at 8 partitions against them measures core count. This
   error was made twice, once against pgenlib and once against snputils.
3. **Each reader's native API.** `pgenlib.read_list` (bulk) is 5.5× faster than
   a per-variant loop; `snputils.read_pgen(genotype_mode="dosage")` is 27×
   faster than `PGENReader().read()` plus a 3-D sum.
4. **"Dosage" is overloaded.** snputils' `genotype_mode="dosage"` returns int8
   *hardcall counts*. pgenlib separates `read_list` from `read_dosages_list`.
   polars-bio's `DS` is the fractional dosage track, `ALT_COUNT` the hardcalls.
5. **Interleave readers in one session.** polars-bio timings drifted up to 1.6×
   across a long session while pgenlib stayed within 4%.
6. **snputils is not an independent oracle for PGEN** — it calls
   `pgenlib.read_list`. The load-bearing check is polars-bio vs pgenlib. On
   BGEN, snputils differs from the `bgen` package; polars-bio does not.

## Correctness rules

- The provider's differential oracles must stay green. Run them with real
  reference libraries, not the skip path:
  ```bash
  PGEN_REFERENCE_PYTHON=/Users/mwiewior/CLionProjects/bioformats-benchmark/.venv-bcf/bin/python \
  PGEN_REQUIRE_REFERENCE_ORACLES=1 cargo test --release -p datafusion-bio-format-pgen
  ```
  They caught a real bug where a fast path returned hardcalls where dosages
  were asked for.
- **Never narrow `DS` from `Float32`.** PGEN dosages are fractional; the two
  tracks genuinely disagree:
  ```
  dosages   : [ 0.125  1.0  1.875  missing ]
  hardcalls : [ missing  1  missing  missing ]
  ```
- The benchmark verifier self-tests: it corrupts one cell and asserts the
  corruption is detected. Keep that.

## Commands

The Rust-only scan — no Python, no materialization. Third argument is the
genotype field:

```bash
cd /Users/mwiewior/CLionProjects/dbf-pgen-perf
RUSTFLAGS="-C target-cpu=native" cargo run --release \
  -p datafusion-bio-format-pgen --example pgen_ds_profile -- \
  /Users/mwiewior/research/data/PGEN/chr22.full.pgen 3 DS
# current: DS ~1.19s, ALT_COUNT ~0.59s
```

Full reader comparison with the correctness gate — this is now the number to
optimize, since materialization dominates it:

```bash
cd ~/CLionProjects/bioformats-benchmark
POLARS_BIO_BUILD_PROFILE=release POLARS_BIO_RUSTFLAGS="-C target-cpu=native" \
.venv-bcf/bin/python run_pgen_benchmarks.py --runs 3 --modes dosage hardcall \
  --polars-bio-partitions 1 8 --pgen /Users/mwiewior/research/data/PGEN/chr22.full.pgen \
  --expected-rows 993881 --expected-samples 2548 \
  --output results/pgen_full_t1.json
```

Confirm the run measured the artifact you think it did: the JSON's
`metadata.polars_bio_build` records the profile, the rustflags and the extension
size. Release is ~228 MB, debug ~336 MB.

Provider-side correctness on real records at full scale, no Python — `ALT_COUNT`
and `DS` against the `GT` scan, which takes a different decode path. Exits
nonzero on any disagreement:

```bash
cd /Users/mwiewior/CLionProjects/dbf-pgen-perf
RUSTFLAGS="-C target-cpu=native" cargo run --release \
  -p datafusion-bio-format-pgen --example pgen_field_parity -- \
  /Users/mwiewior/research/data/PGEN/chr22.full.pgen
```

Rebuild polars-bio against a provider change (iterate with a local `[patch]`
section on the git source; remove it before committing):

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
unset CONDA_PREFIX && RUSTFLAGS="-C target-cpu=native" uv run maturin develop --release
uv run pytest tests/test_pgen_io.py -q
```

## Repo gotchas

- **`make pre-commit` in polars-bio is unsafe.** It runs `ruff check --fix`
  across the whole tree and modified 19 unrelated files; it also exits non-zero
  on ~86 pre-existing errors, so it can never pass. Lint only your own files.
- `polars_bio/io.py` is not `ruff format`-clean at HEAD; formatting it rewrites
  regions far from your edit.
- `gh pr edit` fails on this org with a Projects-classic GraphQL error. Use
  `gh api -X PATCH repos/OWNER/REPO/pulls/N --input body.json`.
- `bioformats-benchmark/results/` is gitignored; writeups are tracked, raw JSON
  is not.
