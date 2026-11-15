# Tasks: add-sequence-operations

Dependency order:
- Phase 1 has no dependencies.
- Phase 2 is upstream work in `datafusion-bio-functions`; it needs a tag
  before phase 3 can pin it.
- Phases 3 and 4 depend on phases 1 and 2.
- Phase 5 depends on new reader work in `datafusion-bio-formats` (5.1), and
  can start in parallel with phases 3–4.
- Phase 6 is the comparative benchmarks in polars-bio-bench:
  - 6.1–6.2 (environment, harness, datasets) can start immediately;
  - 6.3, 6.4 and 6.5 run after phases 3, 4 and 5 respectively.

Each phase is releasable on its own. A phase's announcement waits for its
phase 6 results.

## 1. Packaging groundwork (polars-bio)

- [x] 1.1 Bump `rust-toolchain.toml` to 1.95.0. Confirm that every CI job
  picks it up through `rustup show`, and that `cargo clippy --all-targets -- -D warnings`
  passes on 1.95.
- [x] 1.2 Confirm that conda-forge's `compiler('rust')` resolves to ≥ 1.95 for
  linux-64, osx-64 and osx-arm64. `bioconda-recipe/build.sh` deletes
  `rust-toolchain.toml`. Record the version in `bioconda-recipe/README.md`.
- [x] 1.3 Add `pyo3-polars = { version = "0.28", features = ["derive"] }` and
  `polars = { version = "0.55", default-features = false }`. Check with
  `cargo tree -d` that no duplicate pyo3 or arrow-rs version appears, and
  that `uv lock --check` still passes.
  *Done differently:* the resolution was checked in the spike (no conflicts with
  DataFusion 55 / arrow 59, +74 lock entries). The dependencies are added in
  3.1, together with the plugin code. Added alone, the linker would drop almost
  all of their code, so 1.5 would measure a misleadingly small wheel.
- [x] 1.4 Add a wheel-size step to `publish_to_pypi.yml` that prints every
  wheel's size and fails above the budget (limit − 5 MB). Find out the
  project's actual PyPI per-file limit from the project settings, and record
  it in the workflow comment.
  *Status:* `.github/scripts/check_wheel_size.py` runs in the linux, windows,
  macos and sdist jobs with `PYPI_FILE_LIMIT_MIB=100` (Warehouse's default)
  and a 5 MiB margin. The project's actual limit still has to be read from
  its PyPI settings by an owner; raise `PYPI_FILE_LIMIT_MIB` if it is higher.
- [ ] 1.5 Measure all five platform wheels from the PR's CI artifacts.
  - If any wheel is over budget, enable `[profile.release] strip = "symbols"`
    and verify with `nm -gU` / `nm -D` / `dumpbin /exports` that
    `PyInit_polars_bio` and the `_polars_plugin_*` symbols are still exported.
  - Otherwise, file a PyPI limit request.

Gate: wheels within budget on all platforms; full test suite green on Rust 1.95.

## 2. Kernels and SQL UDFs (datafusion-bio-functions)

- [x] 2.1 Build the oracle corpus and record each tool's exact definitions:
  - **seqkit 2.x:** `fx2tab -g` GC (are S and N counted?), `fx2tab -q` /
    `stats -a` average-quality method, `stats -a` gap characters and
    quartile definition.
  - **Biopython:** `Seq.translate`.
  - **samtools:** `faidx`.

  Write the findings to `oracles.md` in this change, with tool versions.
  Resolve design open question 1.
  *Done:* [oracles.md](oracles.md). This revised D5: GC counts G+C only
  (`denominator="length"|"acgt"` replaces `ignore_n`); unknown bytes pass
  through, with an optional `strict=True`; a `---` codon translates to `-`;
  and `mean_quality` defaults to `error_prob`, seqkit's method. The
  benchmark environment (6.1) reuses the same tool versions.
- [x] 2.2 Create crate `datafusion-bio-function-seq` with a `kernels` module
  of pure `&[u8]` functions: `revcomp`, `complement`, `gc_counts`,
  `translate` (NCBI tables), `phred_mean` (two methods) and `read_metrics`.
  Include unit tests and property tests (revcomp involution, translate frame
  consistency).
  *Done in biodatageeks/datafusion-bio-functions#334:* the 25 NCBI tables are
  generated from Biopython; `Translator` holds the scratch buffer for negative
  frames; `read_metrics` also counts `X` for seqkit's protein `sum_n`.
- [x] 2.3 Add `udf` module: `ScalarUDF`s `seq_revcomp`, `seq_complement`,
  `seq_gc_content`, `seq_translate` and `seq_mean_quality`, accepting `Utf8`,
  `LargeUtf8` and `Utf8View`, with null propagation, unknown-byte
  pass-through, and `ComputeError` only in `strict` mode and for quality bytes
  below the offset.
- [x] 2.4 Add golden tests against the corpus from 2.1. Exact equality for
  strings; for floats, tolerance equal to seqkit's printed precision.
  *Done in #334:* `tests/data/make_golden.py` regenerates the corpus. The
  Biopython/seqkit differences it found are in [oracles.md](oracles.md).
- [ ] 2.5 Tag a functions release containing the crate.

## 3. Sequence expressions (polars-bio)

- [x] 3.1 Pin the functions tag from 2.5. Add `src/seq_plugin.rs` with one
  `#[polars_expr]` adapter per kernel, kwargs deserialized with serde, and no
  `PolarsAllocator`. Add `mod seq_plugin;` in `src/lib.rs`.
  *Status:* all four functions crates are pinned to the #334 commit until it
  is released; switch them to the release tag with 2.5.
- [x] 3.2 Add `polars_bio/seq.py`, which registers the `seq` expression
  namespace with `plugin_path = Path(__file__).parent` and
  `is_elementwise=True`. Import it from `polars_bio/__init__.py`. Validate
  kwargs in Python (`frame`, `table`, `alphabet`, `method`, `offset`) with
  clear `ValueError`s.
- [x] 3.3 Register the SQL UDFs from 2.3 in `create_context()`
  (`src/context.rs`). Add a test that DataFusion's built-in `translate` is
  still available.
- [x] 3.4 Add `tests/test_seq_expressions.py`:
  - kernel semantics (nulls, null-instead-of-NaN, IUPAC, case, gaps, RNA,
    invalid bytes);
  - Biopython translate parity;
  - expression versus SQL parity;
  - `scan_fastq` → plugin streaming;
  - a filter on a `seq` expression through polars-bio's predicate pushdown
    (client-side fallback, correct rows);
  - `group_by().agg()` usage;
  - `LazyFrame.serialize()` round trip.
- [ ] 3.5 Add a CI job (new workflow or a job in `publish_to_pypi.yml`) that
  installs the built wheel and runs the seq tests on Polars `1.40.0`, newest
  `1.x`, `2.0.0` and newest `2.1.x`, as required by the packaging spec.
- [ ] 3.6 Add an in-repo micro-benchmark (`benchmarks/seq/bench_expressions.py`)
  as a regression guard. It runs 4M×150 bp through `revcomp`, `gc_content`,
  `translate` and `mean_quality` under both the in-memory and streaming
  engines, against a pure-Polars or `map_batches` baseline, and records wall
  time and cores used in the PR. Comparisons with other tools are in 6.3.
- [x] 3.7 Add an API reference page (`docs/api/`) and a usage section to
  `docs/features/`, recommending `engine="streaming"`. Add a changelog entry.
- [ ] 3.8 Stress test with polars-bio's allocator (mimalloc and the `jemalloc`
  feature build). Run 200 iterations over 1M rows and check that peak RSS is
  flat after warm-up. Investigate the 1.40.1 RSS growth seen in the spike.

Gate: the packaging-spec matrix is green; parity tests pass; benchmark recorded.

## 4. seq_stats and the seqkit cheatsheet (polars-bio)

- [ ] 4.1 Implement `pb.seq_stats(source)` as in design D6: the
  `read_metrics` plugin, a streaming `group_by("len")` histogram, and a Python
  finalize step for N50/L50, quartiles and percentages. Accept a path, a list
  of paths, or a frame.
- [ ] 4.2 Add `tests/test_seq_stats.py`: parity with `seqkit stats -a -T` on
  the corpus (short and long reads, FASTA, gz and BGZF), multiple files,
  filtered-frame input, empty input, and FASTA with null quality columns.
- [ ] 4.3 Run a memory check on a FASTQ with tens of millions of reads. Peak
  RSS must stay within a small constant factor of a streaming
  `scan_fastq(...).select(pl.len())`.
- [ ] 4.4 Write the docs page "Coming from seqkit / seqfu". It maps each
  common command to a polars-bio one-liner (Polars and SQL), marks commands
  without an equivalent, and links `seq_stats`, the `seq` namespace and
  `fastqc`. Add a changelog entry.

Gate: seqkit stats parity across the corpus; memory check recorded.

## 5. subseq (cross-repository, final phase)

- [ ] 5.1 **datafusion-bio-formats:** add an indexed region reader to
  `bio-format-fasta`, built on noodles-fasta `fai` + `IndexedReader` and
  noodles-bgzf `.gzi`.
  - Reject plain gzip and files without a `.fai`, with errors that suggest
    `bgzip` + `samtools faidx`.
  - Expose a byte-read counter for tests.
  - Tag a formats release.
- [ ] 5.2 **datafusion-bio-functions:** add `SubseqExec` and UDTF
  `subseq(fasta_path, regions_table)` to `datafusion-bio-function-seq`.
  - Stream the region input with one reader per partition.
  - Error on unknown contigs and on regions past the contig end; yield an
    empty sequence for zero-length regions.
  - Tag a functions release.
- [ ] 5.3 **polars-bio:** add `pb.subseq(fasta, regions, *, strand_col=None)`.
  - Normalise inputs as for range operations.
  - Take the coordinate system from the region frame's metadata.
  - Reverse-complement minus-strand regions.
  - Register the SQL UDTF in `create_context()`.
- [ ] 5.4 Add `tests/test_subseq.py`:
  - byte parity with `samtools faidx` (plain and BGZF);
  - minus strand;
  - the same results from 0-based and 1-based metadata;
  - error cases;
  - SQL parity;
  - a byte-read counter assertion (only the overlapping blocks are read).
- [ ] 5.5 Run the region-extraction suite (task 6.5) and link its results
  from the PR.
- [ ] 5.6 Docs: add `subseq` to the API reference and the seqkit cheatsheet.
  Add a changelog entry.

Gate: samtools parity, indexed-read assertion, benchmark recorded.

## 6. Comparative benchmarks (polars-bio-bench)

Follow the method in design D8. Results go under
`results/seqops-<op>-<date>/` with `benchmark_results.md` and
`provenance.json`.

- [ ] 6.1 Environment and harness.
  - Add `envs/seqops.yaml`, a conda environment pinning exact versions of
    seqkit, seqfu, seqtk, samtools and bedtools (bioconda) plus Biopython,
    pyfaidx and pyfastx.
  - Add the `seq_*` operations and tool adapters to `src/operations.py`.
    Command-line adapters use `subprocess.run`, and record CPU time from the
    `RUSAGE_CHILDREN` delta.
  - Add `conf/seqops/*.yaml` with the thread matrix 1/2/4/8 for
    multi-threaded tools.
  - Confirm, or drop, the "candidate" adapters in the D8 coverage matrix
    (pyfastx GC and mean quality, pyfastx region fetch), and record the
    outcome.
- [ ] 6.2 Datasets and correctness gate.
  - Register the D8 datasets in `conf/common.yaml`, with BGZF + `.gzi`
    re-encodings, plain-gzip variants and SHA-256 checksums.
  - Choose the public ONT run.
  - Add the output-normalisation and hash check that marks any tool's output
    differing from the task 2.1 oracle as "mismatch".
- [ ] 6.3 Sequence expressions suite (after phase 3). Run on short and long
  reads, BGZF and gzip, at 1/2/4/8 threads, with polars-bio both in-process
  and as a fresh subprocess:
  - reverse complement: polars-bio vs `seqkit seq -r -p`, `seqfu rc`,
    `seqtk seq -r` and Biopython;
  - per-read GC + mean quality: vs `seqkit fx2tab -n -g -q`, `seqtk comp` and
    Biopython;
  - translation: on the GENCODE CDS FASTA, vs `seqkit translate` and
    Biopython.

  Also measure peak RSS with `run-memory-profiler.py --include-children`.
- [ ] 6.4 Statistics suite (after phase 4). Compare `pb.seq_stats` with
  `seqkit stats -a -T` and `seqfu stats --gc --index` (and `seqtk fqchk` for
  quality-only metrics) on every read dataset at 1/2/4/8 threads. The
  correctness gate compares only the columns both tools report.
- [ ] 6.5 Region extraction suite (after phase 5). Compare `pb.subseq` with
  `samtools faidx -r`, `bedtools getfasta -s`, `seqkit subseq --bed`,
  `seqtk subseq` and pyfaidx, using 1k, 100k and all GENCODE exons from BGZF
  GRCh38. Also record bytes read, from 5.1's counter for polars-bio and from
  `/usr/bin/time` I/O counts for the command-line tools.
- [ ] 6.6 Write up each suite.
  - Add a results table per operation: wall / CPU-s / RSS per tool and thread
    count; 1-thread vs 1-thread; best vs best; mismatches listed.
  - Update the polars-bio `docs/performance.md` with a summary and links.
  - Optionally write a blog post, in the format of the FastQC benchmark post.

Gate: every published number comes from a run whose output passed the
correctness gate. Environment, configs and commands are committed, so anyone
can rerun the results.

## Verification commands

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib
make test
python -m pytest tests/test_seq_expressions.py tests/test_seq_stats.py tests/test_subseq.py -v
openspec validate add-sequence-operations --strict
```
