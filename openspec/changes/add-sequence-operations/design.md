# Design: sequence operations

## Context

This change spans three repositories and introduces a new integration pattern
for polars-bio: a native Polars expression plugin, as opposed to an IO source
or DataFusion table function. Three ways of exposing the kernels were compared
in a spike (2026-11-12/14):

| | A. `map_batches` bridge | B. pyo3-polars plugin | C. SQL only |
|---|---|---|---|
| Python API | `pl.col(..).seq.revcomp()` | identical | SQL only |
| Streaming | per chunk (`is_elementwise=True`) | per chunk, no Python | DataFusion partitions |
| GIL | taken once per chunk | never | never |
| 4M×150 bp `reverse`, streaming | 0.12–0.17 s | **0.013–0.014 s** | n/a |
| Serialized expression | opaque pickled function | named plugin node | n/a |
| Build cost | none | Rust 1.95, +74 crates, +4.1 MB wheel | none |

**Decision: B.** The cost is a toolchain bump and about 4 MB per wheel. In
return the kernels are about 10× faster on cheap kernels and are visible to
polars-bio's predicate translator, so pushdown can be added later. Because A
and B share the same Python API, the choice does not lock users in.

## Goals / Non-Goals

**Goals**
- One kernel implementation per operation, shared by the Polars plugin and the
  DataFusion UDFs.
- Element-wise operations stream in Polars' streaming engine without the GIL.
- `seq_stats` memory is bounded by the number of distinct read lengths, not by
  the number of reads.
- `subseq` reads only the index blocks that overlap the requested regions.
- Parity with independent oracles: seqkit, Biopython and samtools.
- Reproducible comparative benchmarks against the tools users would otherwise
  reach for, published from polars-bio-bench (D8).

**Non-Goals**
- Reimplementing seqkit commands that are already Polars expressions.
- Remote (object-store) `subseq` in this change.
- Pushdown of plugin predicates into DataFusion. This is a follow-up; the
  translator can recognise plugin nodes by function name.

## Decisions

### D1. Kernel location and layering

A new crate `datafusion-bio-function-seq` in `datafusion-bio-functions`
follows the precedent of fastqc, pileup and ranges.

- `kernels` module: pure functions over `&[u8]` that write into a caller-owned
  output buffer. No Arrow, Polars or DataFusion types, so both adapters call
  the same code.
- `udf` module: DataFusion `ScalarUDF`s accepting `Utf8`, `LargeUtf8` and
  `Utf8View`.
- `subseq` module (phase 5): the region-extraction execution plan.

polars-bio adds `src/seq_plugin.rs`, a thin `#[polars_expr]` adapter per
kernel. The Polars `polars` crate never enters the upstream repository, so
datafusion-bio-functions keeps no Polars ABI coupling.

### D2. Plugin packaging

- The plugin symbols live in the existing `polars_bio.abi3.so`, so there is no
  second shared library. `polars_bio/seq.py` registers the namespace with
  `plugin_path = Path(__file__).parent`. The spike verified that Polars finds
  the library there.
- Every element-wise function is registered with `is_elementwise=True`.
- No `PolarsAllocator`. polars-bio already has a `#[global_allocator]`
  (mimalloc by default, jemalloc optionally), and a binary can only have one.
  Buffers cross the boundary through the Arrow C data interface, whose release
  callbacks free memory with the allocator that created it. The spike
  confirmed this over 30 repeated runs, with memory flat on six of seven
  Polars versions. On 1.40.1 peak RSS rose from 461 MB to 513 MB, which was
  not investigated; task 3.8 runs the stress test for longer and checks it
  again.
- Dependency pins: `pyo3-polars = "0.28"` (`derive`) and
  `polars = { version = "0.55", default-features = false }`. Their pyo3
  requirement must equal polars-bio's: pyo3-ffi declares `links = "python"`,
  so a build can contain only one pyo3. Today that is pyo3 0.29, which
  datafusion-python 55 also requires. Future pyo3-polars upgrades move in
  lockstep with datafusion-python's pyo3.

### D3. Rust 1.95

pyo3-polars `derive` → polars-plan → polars-io, which hard-enables
`polars-utils/sysinfo`. Every `sysinfo 0.39.x` declares `rust-version = 1.95`.
Disabling features does not avoid this. The change therefore:
- bumps `rust-toolchain.toml` to 1.95.0;
- checks that the conda-forge `compiler('rust')` used by the bioconda build is
  at least 1.95, because `bioconda-recipe/build.sh` deletes
  `rust-toolchain.toml`.

### D4. Wheel size

Measured on macOS arm64 with a local build:
- the plugin adds 4.1 MB to the wheel (+4.5%);
- master is already 4.9 MB larger than the 0.37.0 PyPI wheel, from the
  DataFusion 55 bump and possibly build-flag differences;
- the 0.37.0 manylinux x86_64 wheel is 94.6 MB, against PyPI's default
  100 MB per-file limit. No polars-bio wheel has exceeded 100 MB, so it is
  unknown whether the project's limit was ever raised. The project's total
  limit has been raised, since it uses 10.8 GB.

Mitigation, in order:
1. Measure every platform wheel in the PR's CI artifacts.
2. If any wheel is within 5 MB of the limit, enable
   `[profile.release] strip = "symbols"`. A local `strip -x` cut the
   compressed wheel by about 13%. Then verify that `PyInit_polars_bio` and
   every `_polars_plugin_*` symbol are still exported.
3. Otherwise, or in addition, request a higher per-file limit from PyPI.

The trade-off of stripping is less informative native backtraces.

### D5. Kernel semantics

These are normative and pinned by tests:
- **Null and empty input.** A null input gives a null output. A ratio whose
  denominator is zero (`gc_content`, `mean_quality`) gives **null**, not NaN.
  Polars orders NaN above every number, so `gc_content("") > 0.3` would keep
  empty reads.
- **Alphabet.** `revcomp` and `complement` accept the IUPAC nucleotide codes
  `ACGTURYSWKMBDHVN`, case-preserving, plus gap characters `-` and `.`, which
  are kept. The complement pairs are A↔T, C↔G, R↔Y, K↔M, B↔V and D↔H; S, W
  and N map to themselves. `alphabet="dna"` (default) maps A→T and U→A;
  `alphabet="rna"` maps A→U and U→A. Any other byte raises `ComputeError`
  naming the character.
- **`gc_content`.** The case-insensitive count of G, C and S, divided by the
  sequence length. With `ignore_n=True` the divisor is the length minus the
  number of N. Returns a fraction between 0 and 1.
- **`translate`.**
  - `frame` is one of ±1, ±2, ±3; negative frames translate the reverse
    complement.
  - `table` is an NCBI translation table id (default 1).
  - A trailing partial codon is dropped, and stops are `*`.
  - An ambiguous codon resolves to an amino acid only if every expansion
    encodes the same one, otherwise `X`. This matches Biopython.
  - `to_stop=True` truncates before the first stop.
- **`mean_quality`.**
  - `offset` defaults to 33; a byte below the offset raises `ComputeError`.
  - `method="arithmetic"` (default) is the mean of the Phred scores.
  - `method="error_prob"` is `-10·log10(mean(10^(-Q/10)))`.

### D6. `seq_stats` pipeline

One plugin kernel returns a per-read struct `{len, gc, n, gap, q20, q30,
qual_sum, err_sum}`. It runs element-wise and streaming, followed by a
streaming `group_by("len")` aggregation into a length histogram. A small
Python step computes N50/L50, quartiles and the final percentages from the
histogram.

Memory is O(number of distinct lengths). That is a few hundred entries for
short reads and at most one per read for long reads. Paths are scanned with
`scan_fasta`/`scan_fastq`, so every source the readers support (compression,
object stores) works without extra code. There is no separate DataFusion
aggregate.

### D7. `subseq` (phase 5)

- **Reader (`datafusion-bio-formats`).** An indexed region reader built on
  noodles-fasta `fai` + `IndexedReader`, plus noodles-bgzf `.gzi` for BGZF.
  Plain gzip cannot be read at random positions and is rejected with an error
  that suggests `bgzip` + `samtools faidx`. Local paths only in this change.
- **Execution (`datafusion-bio-function-seq`).** A `SubseqExec` / UDTF
  `subseq(fasta_path, regions_table)` streams the region input and attaches
  the fetched sequence per row. It keeps one reader per partition.
- **polars-bio.** `pb.subseq(fasta, regions, *, strand_col=None)` takes a
  frame or a path, normalised the same way as range-operation inputs.
  - Coordinates come from the region frame's coordinate-system metadata, via
    the same helpers the range operations use.
  - On the minus strand (`strand_col` value `-`) the region is
    reverse-complemented with `alphabet="dna"`.
  - An unknown contig, or a region past the contig's end, raises an error that
    names the region. A zero-length region yields an empty sequence.

### D8. Comparative benchmarks (polars-bio-bench)

**Where.** The suite lives in polars-bio-bench, which already holds the
range-operation benchmarks used for the paper and the blog. It reuses that
harness:
- YAML configs in `conf/`;
- `timeit` repeats via `num_repeats` / `num_executions`;
- baseline speedups;
- a `threads` matrix;
- `run-memory-profiler.py` with `mprof`.

New pieces:
- `seq_*` operations in `src/operations.py`;
- command-line tool adapters that call `subprocess.run`, like the existing
  pybedtools-based adapters;
- `conf/seqops/*.yaml`;
- `envs/seqops.yaml`, a conda/bioconda environment for the command-line
  tools, with exact versions.

**Coverage matrix.** Only operations each tool actually implements are
compared. "—" means no equivalent; such cells are recorded in the results,
not hidden.

| Operation | polars-bio | seqkit | seqfu | seqtk | Other |
|---|---|---|---|---|---|
| reverse complement | `.seq.revcomp()` → `sink_fastq` | `seq -r -p` (`-j`) | `rc` | `seq -r` | Biopython `reverse_complement` |
| per-read GC + mean quality | `.seq.gc_content()`, `.seq.mean_quality()` → TSV | `fx2tab -n -g -q` (`-j`) | — (`bases`: per-file GC only) | `comp` (counts; GC derived) | Biopython `gc_fraction`; pyfastx (candidate) |
| translate | `.seq.translate()` → FASTA | `translate` (`-j`) | — (`fu-orf` finds ORFs, not equivalent) | — | Biopython `Seq.translate` |
| file statistics | `pb.seq_stats` | `stats -a -T` (`-j`) | `stats --gc --index` (`--threads`) | `fqchk` (quality only) | — |
| region extraction | `pb.subseq` → FASTA | `subseq --bed` | — | `subseq` | `samtools faidx -r`, `bedtools getfasta -s`, pyfaidx, pyfastx (candidate) |

"Candidate" adapters are included only if task 6.1 confirms that the library
supports the operation.

**Fairness rules**
1. **End to end, file to file.** Every tool reads the same input file and
   writes its result to local disk: uncompressed FASTA/FASTQ, TSV, or the
   statistics table. polars-bio results are written with `sink_*` / CSV, not
   collected into an in-memory frame. Python libraries run in-process with the
   same input and output.
2. **The same input bytes for everyone.** FASTQ and FASTA inputs are BGZF
   with a `.gzi`. BGZF is valid gzip, so every tool can read it, and
   polars-bio can also split it into parallel partitions.
   - The plain-gzip variant is run too, so polars-bio's single-stream path is
     measured.
   - Region extraction uses a BGZF reference with `.fai` + `.gzi`; the BED
     regions carry strand.
3. **Threads.**
   - Multi-threaded tools (seqkit `-j`, seqfu `stats --threads`, polars-bio
     `target_partitions` + `POLARS_MAX_THREADS`) run at 1, 2, 4 and 8.
   - Single-threaded tools run once and appear in every thread row.
   - Tables report 1-thread vs 1-thread and best configuration vs best
     configuration.
4. **Metrics.**
   - **Wall time:** best of N warm runs, interleaving tools between repeats.
   - **Total CPU (user + system):** for subprocesses, the delta of
     `RUSAGE_CHILDREN`; in-process, `process_time`.
   - **Peak RSS:** `mprof --include-children`, or `/usr/bin/time` on macOS
     and Linux.

   This is the same wall + CPU-seconds presentation as the FastQC benchmark
   post.
5. **Correctness before speed.** Each run's output is normalised (for
   example, sorted by record name and reduced to the compared field) and
   hashed against the oracle from task 2.1. A tool whose output differs is
   reported as "mismatch", with its time shown separately and never used as
   the baseline. Known definitional differences recorded in `oracles.md` are
   compared on the agreeing subset.
6. **Provenance.** Each results directory records the polars-bio commit or
   version, the exact version of every tool, the conda environment export,
   the machine (CPU, cores, RAM, OS), dataset checksums and the command line,
   in the existing `results/<suite>-<date>/` layout plus a `provenance.json`.

**Datasets.** The `common.yaml` dataset registry is extended with:
- **Short reads:** the four public FASTQ datasets from the FastQC benchmark,
  0.7M–64M reads, re-encoded to BGZF + `.gzi`.
- **Long reads:** one public ONT run, chosen in task 6.2.
- **Reference:** GRCh38 primary assembly as BGZF, with `.fai` + `.gzi`.
- **Regions:** GENCODE exons with strand, sampled to 1k and 100k regions, plus
  the full set.
- **Translation input:** the GENCODE protein-coding CDS FASTA.

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| A future Polars release breaks the plugin ABI | CI matrix on the floor and newest of each supported line (`1.40.0`, latest `1.x`, `2.0.0`, latest `2.1.x`). Raising the upper bound already requires validation under the packaging spec. |
| pyo3-polars lags datafusion-python's pyo3 | Pin both. A pyo3 bump that pyo3-polars cannot follow blocks the bump. If that ever becomes a deadlock, ship the plugin as a separate package with its own pyo3 (`polars-bio-seq`). |
| Wheel crosses PyPI's per-file limit | D4: CI size check, symbol stripping, or a limit request. |
| `seq` namespace collides with another package's namespace | Polars warns when a namespace is overridden; document it. A collision only matters if both packages are imported. |
| Definitions differ from seqkit (GC with S, gap characters, average-quality method) | Task 2.1 records seqkit's exact definitions first. Where they differ from D5, the parity test covers inputs where the definitions agree, and the difference is documented. |
| Benchmarks favour polars-bio through setup choices (input format, output format, thread counts) | The D8 fairness rules are fixed before any results are collected. The plain-gzip variant is reported too. Commands and environment files are published, so anyone can rerun. |
| Command-line tools are timed with process start-up and polars-bio without its import | polars-bio runs are timed from a fresh `python -c` subprocess as well as in-process. The tables state which mode is shown. |

## Open Questions

- Which average-quality method `seqkit stats -a` / `fx2tab -q` uses. This
  decides which `method` is the seqkit parity oracle (task 2.1).
- Whether remote `subseq` (S3/GCS/HTTP ranged reads) goes into a follow-up
  change or is folded in once the local reader exists.
