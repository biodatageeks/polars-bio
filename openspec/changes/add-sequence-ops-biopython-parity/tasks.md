# Tasks: add-sequence-ops-biopython-parity

Dependency order:
- Phases 1, 2 and 3 are independent of each other in content, but share the
  UDF and plugin plumbing. Do them in order, so each builds on the plumbing of
  the one before.
- Within a phase, the upstream tasks (`datafusion-bio-functions`) come first
  and end in a tag. The polars-bio tasks pin that tag.
- The Polars-version CI job (`seq_plugin_polars_matrix`) already runs
  `tests/test_seq_expressions.py`. Keep the new expression tests in that
  file, or add any new test file to the job.
- Phase 4 (docs page and benchmarks) follows phase 3. Its benchmark tasks
  need `add-sequence-operations` tasks 6.1–6.2.

Each of phases 1–3 is releasable on its own.

## 1. Translation parity and small operations

Upstream (`datafusion-bio-functions`, crate `datafusion-bio-function-seq`):

- [x] 1.1 Oracle corpus.
  - Extend `tests/data/make_golden.py` with a CDS corpus: valid sequences for
    every table, alternative start codons, ambiguous first and final codons,
    dual-meaning codons in tables 27, 28 and 31, and at least one sequence per
    failure class.
  - Add golden columns for `Seq.translate(cds=True)` (result or failure
    class), `stop_symbol`, and the unmapped `B`/`Z`/`J` output for every
    table and frame.
  - Add golden columns for `transcribe` and `back_transcribe`.
  - Pin a vsearch version and add a per-read expected-errors golden file.
    Fill in the "Expected errors" section of [oracles.md](oracles.md).
- [x] 1.2 Codon data. Add a checked-in generator script (none exists today;
  `codon_tables.rs` only carries a "Generated from Biopython 1.88" comment).
  It emits the amino-acid tables, and each table's start-codon set and
  stop-codon set, from Biopython 1.88. Check that the regenerated amino-acid
  tables are identical to the current ones.
- [x] 1.3 Kernels.
  - `Translator`: `stop_symbol`, `cds` and the ambiguity mode, as in design
    D2. `Translator::new(frame, table, to_stop)` keeps working.
  - A CDS check function returning the first failing check, used by both
    `translate(cds=True)` and `cds_status` (design D3). Add
    `KernelError::InvalidCds`.
  - `transcribe_into`, `back_transcribe_into`.
  - `expected_errors`, and the Phred minimum and maximum (design D4).
  - Unit tests for every probe in [oracles.md](oracles.md), and property
    tests: `cds_status` is null exactly when `translate(cds=True)` succeeds;
    `back_transcribe(transcribe(s)) == s` for sequences without `U`.
- [x] 1.4 SQL functions.
  - Generalise the return type of `SeqUdf` beyond String and Float64 (Int32
    is needed here; phases 2 and 3 add more).
  - Add `seq_cds_status`, `seq_transcribe`, `seq_back_transcribe`,
    `seq_expected_errors`, `seq_min_quality` and `seq_max_quality`.
  - Add `stop_symbol`, `cds` and `ambiguous` to `seq_translate` after its
    existing arguments.
  - Named arguments (design open question): declare parameter names with
    `Signature::with_parameter_names` and test a call that names a late
    argument and omits earlier ones, such as
    `seq_translate(sequence, cds => true)`. Adopt named arguments if such
    calls work; record the outcome in the PR.
  - SQL tests in `tests/sql.rs`.
- [x] 1.5 Golden tests in `tests/golden.rs` against the corpus from 1.1.
- [ ] 1.6 Tag a functions release.

polars-bio:
  *Not done:* the upstream branch `feat/seq-biopython-parity` is committed locally, not pushed or tagged.

- [ ] 1.7 Pin the tag from 1.6 for all four functions crates.
  *Not done:* polars-bio builds against the local upstream worktree through an uncommitted `[patch]`.
- [x] 1.8 `src/seq_plugin.rs`: extend `seq_translate`'s kwargs; add
  `seq_cds_status`, `seq_transcribe`, `seq_back_transcribe`,
  `seq_expected_errors`, `seq_min_quality` and `seq_max_quality`.
- [x] 1.9 `polars_bio/seq.py`: the new keyword arguments on `translate`
  (keyword-only) and the six new methods, with argument validation and
  docstrings.
- [x] 1.10 `tests/test_seq_expressions.py`:
  - every row of the CDS, stop-symbol and ambiguity tables in
    [oracles.md](oracles.md);
  - Biopython parity for `translate(cds=True, ambiguous="biopython")` on
    generated CDSs, with `pytest.importorskip("Bio")`;
  - `cds_status` reasons, and the filter-then-translate pattern of design D3;
  - `ComputeError` from `cds=True` names the reason and `cds_status`, and
    later expressions still evaluate;
  - existing `translate` results unchanged with default arguments;
  - transcription, expected errors, minimum and maximum quality, including
    nulls, empty strings and a byte below the offset;
  - expression versus SQL parity for every new function;
  - `LazyFrame.serialize()` round trip.
- [x] 1.11 Docs: add the new functions and options to
  `docs/features/sequences.md` (function table and semantics) and check that
  `docs/api/sequences.md` renders them. Add a changelog entry.

Gate: golden and parity tests green; the test suite green on Rust 1.95; the
wheel-size check green.

## 2. ORFs and motif search

- [ ] 2.1 Spike (polars-bio, before any kernel work). Add a throwaway plugin
  function returning `List[Struct]` and one returning `List[Int64]`, with the
  `polars` crate's `dtype-struct` feature enabled.
  - Run it through the `seq_plugin_polars_matrix` job (Polars `1.40.0`, the
    newest `1.x`, `2.0.0` and the newest `2.1.x`), under the in-memory and
    streaming engines, including `explode()` and `unnest()` of the result.
  - Record the size change of every platform wheel from CI.
  - If a supported Polars version fails, stop and bring the fallback in the
    design's risk table back to the maintainer.

Upstream:
  *Partly done:* list and struct outputs pass `tests/test_seq_biopython.py` under both engines on Polars 2.1.0 only; the four-version matrix runs in CI once the branch is pushed. Wheel sizes not measured yet.

- [x] 2.2 Oracles.
  *Done differently:* the ORF oracle is the Biopython tutorial recipe, recorded in [oracles.md](oracles.md); orfipy and `getorf` were not used.
  - Choose the ORF oracle (orfipy, EMBOSS `getorf`, or both) and pin its
    version. Fill in the "ORF finding" section of [oracles.md](oracles.md),
    listing every difference from the definition in design D5.
  - Extend `make_golden.py` with an ORF corpus (both strands, several
    tables including 27, 28 and 31, alternative starts, sequences with `N`,
    sequences with no ORF, sequences that start or end inside an ORF) and a
    motif corpus generated with `nt_search` (ambiguous patterns, overlapping
    hits, palindromes).
- [x] 2.3 Kernels.
  - ORF finder as defined in design D5, in 0-based half-open coordinates,
    writing into a caller-owned buffer or callback.
  - A compiled motif pattern and a search function as defined in design D7.
  - Unit tests, and the property tests from design D10 (the ORF/CDS
    invariant; motif search against a brute-force scan).
- [x] 2.4 SQL functions `seq_orfs` and `seq_find_motif`, with `List<Struct>`
  and `List<Int64>` return types and the trailing `zero_based` argument
  (design D6). SQL tests.
- [x] 2.5 Golden tests against the corpus from 2.2.
- [ ] 2.6 Tag a functions release.

polars-bio:
  *Not done:* see 1.6.

- [x] 2.7 Pin the tag from 2.6. Enable `dtype-struct`. Add `seq_orfs` and
  `seq_find_motif` to `src/seq_plugin.rs`, with the output type computed from
  the arguments (`include_protein`).
  *Done except the pin:* see 1.7.
- [x] 2.8 `polars_bio/seq.py`: `orfs` and `find_motif`. Resolve
  `use_zero_based=None` from the session option when the expression is built
  (design D6). Validate the pattern, `strands`, `start_codons` and `min_len`.
- [x] 2.9 Frame-level `pb.orfs(source, ...)` as in design D5: accept a FASTA
  path or a frame, drop the sequence column, explode and unnest, and tag the
  result with its coordinate system through the existing metadata helper.
  Export it from `polars_bio/__init__.py`.
- [x] 2.10 Tests:
  - `pb.orfs` from a FASTA path and from a frame: one row per ORF, the same
    ORFs as the expression, no sequence column, no rows for a sequence
    without ORFs;
  - `pb.orfs` output in `pb.overlap` next to a tagged annotation frame runs,
    and a coordinate-system mismatch raises `CoordinateSystemMismatchError`;
  - ORFs against the brute-force Biopython reference on generated sequences,
    for both strands and several tables;
  - the ORF/CDS invariant from design D5;
  - `allow_partial` (its output contains every ORF of the default output),
    `include_protein`, `start_codons="ATG"`, `min_len`;
  - dual-meaning codons end ORFs in tables 27, 28 and 31;
  - the same ORFs under 0-based and 1-based settings, differing only in the
    documented way;
  - `find_motif` against `nt_search` on uppercase DNA; the two documented
    differences (lowercase sequence, `U`); `both_strands` with a palindromic
    and a non-palindromic pattern; `overlapping=False`, alone and with
    `both_strands`; invalid patterns;
  - the expression captures the coordinate setting when it is built;
  - expression versus SQL parity with the same `zero_based` value;
  - streaming engine and `serialize()` round trip.
- [x] 2.11 Docs: ORF definition with a worked example, `pb.orfs` into a
  range operation, the coordinate rule, and the motif matching rule, in
  `docs/features/sequences.md`. Add `pb.orfs` to the API reference. Changelog
  entry.

Gate: the Polars matrix green with nested outputs; ORF and motif parity
tests green; wheel sizes within budget.

## 3. Windows, weight, hash and trimming

Upstream:

- [x] 3.1 Oracles.
  - Golden files for `GC_skew` and `molecular_weight` (DNA, RNA, protein;
    single and double stranded; linear and circular; average and
    monoisotopic; inputs Biopython rejects).
  - Probe `seqkit rmdup -s` and Trimmomatic `LEADING`, `TRAILING` and
    `SLIDINGWINDOW`, and fill in the two remaining sections of
    [oracles.md](oracles.md). Pin the Trimmomatic version.
  - Golden files for trimming (several thresholds and window sizes, reads
    shorter than the window, reads trimmed to nothing) and for the kept set of
    `seqkit rmdup -s`.
- [x] 3.2 Weight tables generated from Biopython 1.88 (average and
  monoisotopic, for DNA, RNA and protein).
- [x] 3.3 Kernels: windowed GC content and GC skew, molecular weight, hash
  with `canonical`, and quality trimming, as in design D8. Unit tests and the
  property tests from design D10.
- [x] 3.4 SQL functions `seq_gc_content_windows`, `seq_gc_skew`,
  `seq_molecular_weight`, `seq_hash` and `seq_trim_quality`. This adds the
  `List<Float64>`, `UInt64` and `Struct` return types, a second column
  argument, and optional arguments that accept `NULL`. SQL tests.
- [x] 3.5 Golden tests against the corpus from 3.1.
- [ ] 3.6 Tag a functions release.

polars-bio:
  *Not done:* see 1.6.

- [x] 3.7 Pin the tag from 3.6. Add the five functions to
  `src/seq_plugin.rs`; `seq_trim_quality` takes two input columns.
  *Done except the pin:* see 1.7.
- [x] 3.8 `polars_bio/seq.py`: `gc_content_windows`, `gc_skew`,
  `molecular_weight`, `hash` and `trim_quality`.
- [x] 3.9 Tests:
  - `gc_skew` against Biopython `GC_skew`; windows with a step smaller than
    the window; a window with no G or C; empty input;
  - `molecular_weight` against Biopython for every option combination, and
    null where Biopython raises;
  - `hash`: fixed expected values for known inputs (they must never change),
    `canonical` equality for a sequence and its reverse complement, and
    deduplication with `unique(subset=...)` matching `seqkit rmdup -s`;
  - `trim_quality` against the Trimmomatic golden files; sequence and quality
    stay the same length; length mismatch raises; a fully trimmed read gives
    empty strings; `unnest` restores the `scan_fastq` column names;
  - expression versus SQL parity, streaming, `serialize()` round trip.
- [x] 3.10 Docs: the five functions in `docs/features/sequences.md`,
  including the hash stability statement and collision bound, and a
  quality-control recipe (`expected_errors` filter, trimming, length filter,
  deduplication). Changelog entry.

Gate: parity tests green; hash values pinned by tests.

## 4. Migration page and benchmarks

- [x] 4.1 Docs page "Coming from Biopython": a table mapping Biopython calls
  (`Seq.translate` and its options, `reverse_complement`, `transcribe`,
  `gc_fraction`, `GC_skew`, `nt_search`, `molecular_weight`, the ORF loop from
  the Biopython tutorial, `SeqIO.parse` filters) to polars-bio expressions and
  SQL. List every deliberate difference in one place, with the 1-based
  default for positions first. Link it from the docs navigation and from
  `docs/features/sequences.md`.
- [ ] 4.2 polars-bio-bench (after `add-sequence-operations` 6.1–6.2): add
  Biopython-loop baselines to the `seqops` suite for CDS translation, ORF
  finding and motif search; vsearch for the expected-errors filter; and
  Trimmomatic for trimming. Same fairness rules and correctness gate as design
  D8 of `add-sequence-operations`.
- [ ] 4.3 Add the results summary to `docs/performance.md`.

Gate: every published number comes from a run whose output passed the
correctness gate.

## Verification commands

```sh
# datafusion-bio-functions
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -p datafusion-bio-function-seq

# polars-bio
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
python -m pytest tests/test_seq_expressions.py -v
make test
openspec validate add-sequence-ops-biopython-parity --strict
```
