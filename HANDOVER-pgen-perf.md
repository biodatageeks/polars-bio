# Handover: PGEN performance and the three-format benchmark

Written 2026-08-17, revised the same day after the common-value + difflist decode
was fused, `read_pgen_matrix` was added, and the PGEN benchmark was re-run. Supersedes
`HANDOVER-pgen-benchmarks.md`, whose two tasks (wire PGEN into polars-bio;
benchmark BCF/BGEN/PGEN) are both **done**.

Everything below is committed and pushed, except `bioformats-benchmark` — see
task 4. Nothing is half-finished.

## Start here

```
Read HANDOVER-pgen-perf.md in polars-bio and
datafusion/bio-format-pgen/PERF_HANDOVER.md in the provider worktree at
/Users/mwiewior/CLionProjects/dbf-pgen-perf. Then pick up task 1.
```

## Repository state

| Repo | Branch | Head | Notes |
|---|---|---|---|
| polars-bio | `feat/bgen-pr220-bench` | branch tip | PR #436; pins provider `1fc3673` |
| datafusion-bio-formats | `perf/pgen-batch-array-build` | branch tip | PR #232, open, pushed. `1fc3673` is its last code commit — the pin above; later ones are docs |
| bioformats-benchmark | `feat/bgen-benchmark` | branch tip | 8 commits ahead, local only, **not pushed** |

Provider worktree: `/Users/mwiewior/CLionProjects/dbf-pgen-perf`.
Benchmark venv: `bioformats-benchmark/.venv-bcf` — has polars-bio (editable,
from the checkout), snputils, pgenlib, bgen, cyvcf2, pysam.

`plink2` is installed at `~/.local/bin/plink2` (native arm64, v2.0.0-a.7.3).
PGEN fixtures exist at `/Users/mwiewior/research/data/PGEN/`.

## What was accomplished

PGEN reads through polars-bio (`read_pgen` / `scan_pgen` / `register_pgen` /
`describe_pgen` / `read_pgen_matrix`, 67 tests). All three formats benchmarked at
equal core count with element-wise correctness gates. The provider's
single-partition PGEN scan went **11.2 s → 1.19 s** for dosage and **0.59 s** for
hardcall after fusing the common-value + difflist decode, and `read_pgen_matrix`
removed the copy between that scan and a NumPy matrix.

Current numbers, chromosome 22, 993,881 × 2,548 = 2,532,408,788 genotypes,
**one thread each** — pgenlib, snputils and `bgen` are all single-threaded.
PGEN rows re-measured 2026-08-17 with provider `25d6bd2`, all readers
interleaved in one session:

| Format | polars-bio | snputils | reference |
|---|---:|---:|---:|
| BCF (int8) | **5.251 s** | 8.285 s | — |
| PGEN hardcall (int8) | **0.940 s** | 1.487 s | pgenlib 0.827 s |
| PGEN dosage (f32) | **1.849 s** | 3.181 s | pgenlib 1.779 s |
| BGEN dosage (f32) | 25.804 s | 21.171 s | bgen 15.064 s |

PGEN went 4.338 s → 1.849 s (dosage) and 2.959 s → 0.940 s (hardcall) over this
work — 2.35× and 3.15×. polars-bio is now 1.04× pgenlib on dosage and 1.14× on
hardcall, from 2.46× and 3.56×, and is 1.7× and 1.6× faster than snputils. Peak
RSS fell from 22.31 GB to 13.30 GB (dosage) and 8.25 GB to 5.74 GB (hardcall)
against pgenlib's 12.09 GB and 5.02 GB.

pgenlib and snputils reproduced their earlier figures to within 1% in the same
session, so the PGEN deltas are the change and not drift. Zero bitwise
differences against the reference in all three formats, with the single-cell
corruption self-test confirming the comparison can still fail.

Two things about the PGEN figures a reader should know:

- **The harness no longer times module imports.** Every reader used to import its
  library inside its own timed read function. That is a one-time process cost,
  and the magnitudes are not comparable — ~0.46 s for polars-bio's ~228 MB
  extension against ~0.03 s for the others. It is now warmed before the clock for
  every reader alike and recorded as `import_seconds`. Charged as before,
  polars-bio's dosage read would read 2.26 s rather than 1.849 s.
- **polars-bio uses `read_pgen_matrix`, not `read_pgen`.** The DataFrame path
  consolidates the scan's batches into one contiguous Arrow buffer before the
  array exists, a second full copy of the values; it measures 3.225 s / 22.3 GB
  for dosage. The matrix reader is the fair counterpart to pgenlib's
  `read_list`.

## Where the remaining PGEN time goes

| | total | scan | materialization |
|---|---:|---:|---:|
| dosage | 1.849 s | ~1.19 s | ~0.66 s |
| hardcall | 0.940 s | ~0.59 s | ~0.35 s |

What remains of materialization is one copy of the values from the scan's Arrow
batches into the destination array. It cannot be removed on this path: Arrow's
`ListArray` uses i32 offsets, so a batch holds at most 842,811 rows at 2,548
samples and the whole matrix can never arrive as a single zero-copy buffer.
Eliminating it means the decoder writing into the caller's array directly, the
way pgenlib does — a genuinely different API, not a tweak.

## Tasks, in priority order

### 1. Decide whether the last ~4% against pgenlib is worth chasing

polars-bio is at 1.04× pgenlib on dosage and 1.14× on hardcall. Closing the rest
means bypassing Arrow so the decode writes into the destination buffer, which is
a new non-DataFrame API surface in the provider. It is a real feature with real
cost; the gap it closes is small. Probably not worth it — but it is the only
remaining lever, so decide deliberately rather than drifting.

The provider-side follow-up in `PERF_HANDOVER.md` (SIMD the difflist patch loop)
is a smaller lever still; re-profile before spending time there.

**Do not implement issue #233 as written.** It proposes a 2-bit packed main
track, which is aimed at the LD branch (13%) and would be a regression for the
dominant one. Corrected in a comment on the issue.

### 2. Peak RSS — largely resolved, one loose end

Dosage now peaks at 13.30 GB against pgenlib's 12.09 GB for the identical
10.13 GB output, down from 22.31 GB; hardcall is 5.74 GB against 5.02 GB, down
from 8.25 GB. The old 17.9 → 22.8 GB regression is moot: it was the DataFrame
path's second full copy, which `read_pgen_matrix` does not make.

The loose end is small. `read_pgen_matrix` alone measures 10.85 GB for dosage,
but the harness reports 13.30 GB; the extra ~2.4 GB is post-read hashing and
sorting in `pgen_matrix.py`, which pgenlib pays too (12.09 GB against its 10.13 GB
output) but apparently less. Worth a look before publishing, not a blocker.

### 3. Refresh the published figures

`bioformats-benchmark/PGEN_BENCHMARK.md` and
`polars-bio/docs/blog/posts/bcf-genotype-readers-2026-08.md` still carry the
original PGEN numbers — now three generations stale. Use the table above, and
say plainly that the harness no longer times module imports and that polars-bio
is measured through `read_pgen_matrix`; both changed the figures materially.

`read_pgen_matrix` is also new public API and is not in the docs yet.

Raw results: `bioformats-benchmark/results/pgen_full_final.json` (gitignored, so
regenerate with the command below if it is gone).

### 4. Push the benchmark repo

`bioformats-benchmark` `feat/bgen-benchmark` is 8 commits ahead, local only. The
other two repos are pushed.

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
