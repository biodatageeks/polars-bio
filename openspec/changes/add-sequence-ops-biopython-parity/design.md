# Design: Biopython-parity sequence operations

## Context

`add-sequence-operations` established the pattern this change extends:

- kernels are pure `&[u8]` functions in `datafusion-bio-function-seq`
  (`datafusion-bio-functions` repository), with no Arrow, DataFusion or Polars
  types;
- the same crate wraps them as `seq_*` DataFusion scalar functions;
- polars-bio's `src/seq_plugin.rs` wraps them as Polars plugin functions, and
  `polars_bio/seq.py` exposes them as the `seq` expression namespace.

What is new here is the shape of the functions. Every function shipped so far
takes one String column and returns String or Float64. This change adds:

- outputs of type `List[Float64]`, `List[Int64]`, `List[Struct]`, `Struct`,
  `Int32` and `UInt64`;
- one function with two column inputs (sequence and quality);
- outputs that carry coordinates, which have to follow polars-bio's
  coordinate-system setting;
- kernels that can reject a row for a reason other than a strict-mode byte
  (an invalid CDS).

The intended users are people migrating from Biopython loops. Success is that
their results match Biopython where polars-bio claims parity, that every
deliberate difference is documented, and that the operations stream in
Polars without the GIL like the existing ones.

## Goals / Non-Goals

**Goals**
- `translate` can reproduce `Seq.translate` exactly for every input Biopython
  accepts, including `cds=True`, `stop_symbol` and the `B`/`Z`/`J` letters.
- A user can find invalid CDS rows without leaving the frame.
- ORF results reach the range operations without manual coordinate tagging.
- One kernel per operation, shared by the plugin and the SQL function.
- Every parity claim is pinned by a golden test against the named tool.

**Non-Goals**
- `Binary`/`BinaryView` input. No reader emits sequences as bytes.
- Reproducing Biopython's exceptions for inputs it rejects. See D3 and D8.
- PWM motif scanning, regular-expression motifs and mismatch-tolerant search.
- A 128-bit hash.
- Paired-end trimming, adapter trimming and `MINLEN` (a Polars filter).
- Reading the coordinate-system setting inside SQL functions. See D6.

**Anti-goals** (would make the change a failure even if it works)
- Changing the result of any existing `seq.translate` or `seq_translate` call.
- A parity claim in the docs that no test backs.
- A function that silently returns a plausible value for an input it cannot
  handle (for example a molecular weight that skips `N`).

## Approaches

**How the operations are built**

| | A. Kernel + SQL function + plugin expression | B. Biopython behind `map_batches` | C. Pure Polars expressions in Python |
|---|---|---|---|
| Streaming, no GIL | yes | no | yes |
| Exact Biopython results | by golden tests | by construction | only for trivial cases |
| Covers ORFs, IUPAC search, CDS checks | yes | yes | no |
| Cost | upstream release per phase | Biopython becomes a runtime dependency | none |

**Chosen: A.** The issue prescribes it, and it is the only option that keeps
the properties the `seq` namespace already has. B is what users do today and
is the problem being solved. C only covers `transcribe` and single-pattern
searches.

**How the work is packaged**

One OpenSpec change with three phases, each ending in an upstream tag and a
polars-bio release, as the issue's suggested split and
`add-sequence-operations` both do. Three separate changes were considered and
rejected: the phases share the UDF and plugin plumbing, the oracle corpus and
the docs page, and splitting them would repeat that context three times.

**What `translate(cds=True)` does with an invalid row**

| Option | Behaviour | Trade-off |
|---|---|---|
| Raise only | `ComputeError`, as Biopython | Smallest API. A file with one partial CDS fails the query, and the user has no way to find the row. |
| Null on invalid | Never raises | Easy to drop, but silent about the reason and unlike Biopython. |
| `on_invalid="raise"\|"null"` | Raises by default, nulls on request | Polars-like, but still no reason. |
| **Raise + `cds_status()`** | Raises, plus a separate expression that returns the reason | One extra expression. |

**Chosen by the maintainer (2026-11-23): raise + `cds_status()`.**

**How ORF results reach the range operations**

The issue wants an exploded `seq.orfs()` column to work with the range
operations. As the code stands it does not:
- an expression cannot attach coordinate-system metadata to a frame;
- `unnest` is not one of the metadata-preserving frame operations;
- `validate_coordinate_systems` raises `MissingCoordinateSystemError` in
  every mode when one input is tagged (a GFF scan, say) and the other is not.

| Option | Behaviour | Trade-off |
|---|---|---|
| Docs recipe | Explode, unnest, then tag by hand | No new API. A wrong tag shifts results by one position. |
| Keep tags through `unnest` | Tag the frame once before exploding | No new function, but the user still tags a frame that has no coordinates yet. |
| **Frame helper + expression** | `pb.orfs(source, ...)` returns one tagged row per ORF | One more top-level function. |

**Chosen by the maintainer (2026-11-24): frame helper + expression.**

## Decisions

Decisions marked *(maintainer)* were made by the maintainer. The others are
the author's proposals; the maintainer can overrule any of them at review.

### D1. Layering and plumbing

- **Kernels** stay pure `&[u8]` functions. Kernels with list output take a
  caller-owned `Vec` or a callback, so neither adapter needs an intermediate
  allocation per row.
- **Tables generated from Biopython.** Start-codon and stop-codon sets per
  NCBI table, and the DNA, RNA and protein weight tables (average and
  monoisotopic), are generated from Biopython 1.88 by a checked-in script.
  The script is new: today `codon_tables.rs` only carries a "Generated from
  Biopython 1.88" comment, and `make_golden.py` writes golden files only. The
  script also emits the existing amino-acid tables, and regenerating them
  must reproduce the current file.
- **UDF layer.** `udf.rs` today assumes "one string argument, then constant
  options" and a return type of String or Float64. It is extended to:
  - the return types listed in Context;
  - a second column argument (`seq_trim_quality`);
  - optional arguments that accept SQL `NULL` to mean "not set"
    (`seq_trim_quality` steps).
- **Plugin layer.** List and struct outputs use
  `#[polars_expr(output_type_func = ...)]` or
  `output_type_func_with_kwargs` where the type depends on an argument
  (pyo3-polars-derive 0.22 supports both). The `polars` crate gains the
  `dtype-struct` feature.
- **Integer types.** Positions and counts are `Int64`; `frame` and the Phred
  minimum and maximum are `Int32`; the hash is `UInt64`. `UInt8` and other
  narrow integers are avoided because they are feature-gated in the `polars`
  crate.
- **Argument validation** happens twice: in Python when the expression is
  built (`ValueError`), and in the kernel constructor (`ComputeError` or a
  DataFusion error), as today.

### D2. Translation options

Python signature:
`translate(frame=1, table=1, to_stop=False, *, stop_symbol="*", cds=False,
ambiguous="x")`.

- **`stop_symbol`** is any string, including empty and multi-character
  strings, as in Biopython. It replaces every emitted stop. It does not change
  what counts as a stop, so `to_stop` behaves the same.
- **`ambiguous`**
  - `"x"` (default) is today's rule: an ambiguous codon whose expansions do
    not all agree gives `X`.
  - `"biopython"`: the same, except that expansions covering exactly {D, N}
    give `B`, exactly {E, Q} give `Z`, and exactly {I, L} give `J`.
- **`cds=True`** applies Biopython's four checks to the sequence in the
  requested frame, in Biopython's order:
  1. the first codon is a start codon of the table;
  2. the length is a multiple of three;
  3. the final codon is a stop codon of the table;
  4. no other codon is a stop.

  On success the first codon is emitted as `M`, the final stop is not emitted,
  and the codons between translate as usual. Details, all taken from the
  probes in [oracles.md](oracles.md):
  - An ambiguous first (or final) codon passes only if every expansion is a
    start (or stop) codon.
  - An interior codon is a stop only if it translates to a stop. A
    possibly-stop codon such as `TAN` gives `X` and is not an error.
  - In tables 27, 28 and 31 a dual-meaning codon is accepted as the final
    stop and is an amino acid in the interior.
  - `frame` is applied first (offset, and reverse complement for negative
    frames); the checks run on the result. So `frame=-1` validates a CDS given
    on the opposite strand.
  - `to_stop` and `stop_symbol` have no effect on a valid CDS.
- **The kernel's existing leniency is kept in every mode.** A codon with a
  partial gap or an unknown byte still gives `X`, and `...` still gives `-`.
  Biopython raises on these. `cds=True` adds exactly the four checks above.

### D3. Invalid CDS rows *(maintainer)*

- `translate(cds=True)` raises `ComputeError` on the first invalid row. The
  message states the failed check and the codon, in Biopython's wording where
  it has one, and names `seq.cds_status` as the way to find invalid rows.
- `seq.cds_status(frame=1, table=1)` returns a String. The arguments are in
  the order of `translate`, so the positional SQL forms
  `seq_cds_status(s, frame, table)` and `seq_translate(s, frame, table)`
  agree.
  - null when `translate(cds=True)` with the same `frame` and `table` would
    not raise, which includes a null input;
  - otherwise the first failing check, as one of `"no_start_codon"`,
    `"length_not_multiple_of_3"`, `"no_stop_codon"` or
    `"internal_stop_codon"`.
- Both use one kernel function for the checks, so they cannot disagree.

Typical use:

```python
cds = pb.scan_fasta("cds.fa").with_columns(
    status=pl.col("sequence").seq.cds_status(table=11)
)
proteins = cds.filter(pl.col("status").is_null()).with_columns(
    protein=pl.col("sequence").seq.translate(table=11, cds=True)
)
rejected = cds.filter(pl.col("status").is_not_null())
```

### D4. Small operations (phase 1)

- **`transcribe()` / `back_transcribe()`** replace `T`→`U`, `t`→`u` and the
  reverse. Every other byte is unchanged.
- **`expected_errors(offset=33)`** returns the sum of `10^(-Q/10)` as
  Float64, reusing the lookup table behind `mean_quality`. An empty string
  gives `0.0` (a sum, not a ratio). A byte below `offset` raises
  `ComputeError`, as in `mean_quality`.
- **`min_quality(offset=33)` / `max_quality(offset=33)`** return the lowest
  and highest Phred score as Int32. An empty string gives null. A byte below
  `offset` raises `ComputeError`.

### D5. ORF finding

`orfs(table=1, min_len=30, strands="both", start_codons="table",
allow_partial=False, include_protein=False, use_zero_based=None)`

**Definition.** In each reading frame, stop codons split the codons into
segments.
- A **complete ORF** runs from the first start codon in a segment to the stop
  that ends the segment. Later start codons in the same segment do not produce
  additional ORFs: one ORF per stop, the longest.
- `allow_partial=True` **only adds** ORFs; every ORF of the default output is
  still reported unchanged. The additions are:
  - **no start:** the first segment of a frame (no stop before it) contains
    no start codon. An ORF is reported from the frame's first complete codon
    to the stop, with `has_start=false`. If that segment does contain a start
    codon, the complete ORF from that codon is reported as usual and nothing
    is added.
  - **no stop:** the last segment of a frame (no stop after it) contains a
    start codon. An ORF is reported from the first such codon to the frame's
    last complete codon, with `has_stop=false`.
  - **neither:** a frame with no stop codon and no start codon at all yields
    one ORF spanning its complete codons, with both flags false.
- A codon is a start (or stop) only if every expansion of its IUPAC codes is
  one, the same rule as `cds=True`. A codon containing `N` is neither, so an
  ORF runs through it.
- **Every codon in the table's stop-codon set ends an ORF**, including the
  dual-meaning codons of tables 27, 28 and 31. An ORF finder has no context
  to decide between the two meanings, and without this rule table 28 would
  have no stops at all. This differs from `translate`, which reads a
  dual-meaning codon as the amino acid; the invariant below still holds,
  because `cds=True` accepts such a codon as the final stop.
- `start_codons="table"` uses the table's start-codon set; `"ATG"` uses only
  `ATG`/`AUG`.
- `strands` is `"both"`, `"+"` or `"-"`.
- `min_len` is the minimum `aa_len`. The default of 30 amino acids (90
  nucleotides) is a proposal; no single convention exists across ORF finders.

**Output.** `List[Struct]`, sorted by `start`, then `end`, then `strand`. An
input with no ORF gives an empty list; a null input gives null.

| Field | Type | Meaning |
|---|---|---|
| `start`, `end` | Int64 | Interval on the input sequence as given (the `+` strand), for ORFs on either strand. It includes the stop codon when the ORF has one. |
| `strand` | String | `"+"` or `"-"` |
| `frame` | Int32 | 1, 2, 3 or -1, -2, -3, with the meaning of `translate(frame=...)` |
| `aa_len` | Int64 | Amino acids, excluding the stop |
| `has_start`, `has_stop` | Boolean | Always true unless `allow_partial=True` |
| `protein` | String | Only with `include_protein=True`. The first codon is `M` when `has_start`; ambiguity uses the `"x"` rule. |

`has_start` and `has_stop` are an addition to the issue's struct. Without
them a caller cannot tell a partial ORF from a complete one, and adding a
struct field later would change the output type.

**Invariant** (property test): for every ORF with `has_start` and `has_stop`,
the slice of the input at `[start, end)`, reverse-complemented for `-`,
passes `cds_status(table)` when `start_codons="table"`, and its
`translate(cds=True)` equals `protein`.

**Oracle.** Biopython has no ORF finder and the issue names none. Task 2.2
chooses orfipy, EMBOSS `getorf` or both, and records how their definitions
differ from this one. The polars-bio tests also check a brute-force reference
built on Biopython's `Seq.translate`.

**Frame-level `pb.orfs`** *(maintainer)*

`pb.orfs(source, *, sequence_col="sequence", table=1, min_len=30,
strands="both", start_codons="table", allow_partial=False,
include_protein=False, use_zero_based=None) -> pl.LazyFrame`

- `source` is a FASTA path (read with `scan_fasta`) or a Polars DataFrame or
  LazyFrame with a sequence column.
- The result has one row per ORF: every input column except the sequence
  column (for `scan_fasta` input, `name` and `description`), followed by the
  struct fields of `seq.orfs` as columns. A sequence without ORFs contributes
  no rows.
- The sequence column is dropped before exploding. Keeping it would copy a
  whole chromosome into every ORF row.
- The result carries coordinate-system metadata for the system it used, so a
  range operation accepts it next to a tagged input and rejects a mismatch
  with `CoordinateSystemMismatchError`. `use_zero_based=None` reads the
  session option when the function is called.
- It is a thin Python wrapper over the `seq.orfs` expression (`with_columns`,
  `drop`, `explode`, `unnest`, then the existing metadata helper), so it
  streams like any other lazy query. There is no SQL table function for it.
- The `seq.orfs` docstring points to `pb.orfs` for use with range operations.

### D6. Coordinates

`orfs` and `find_motif` return positions, so they need a coordinate system.
An expression has no access to frame metadata, so:

- Both take `use_zero_based: Optional[bool] = None`, the name the readers use.
  - `None` reads the session option
    `datafusion.bio.coordinate_system_zero_based` **when the expression is
    built**, not when it is collected.
  - `True` gives 0-based half-open intervals; `False` gives 1-based closed
    intervals.
- The kernels always work in 0-based half-open coordinates. The adapters
  convert.
- **The session default is 1-based.** With default settings `find_motif`
  therefore returns Biopython's `nt_search` positions plus one. The docs and
  the migration page state this next to the function.
- **SQL.** `seq_orfs` and `seq_find_motif` take an explicit trailing
  `zero_based` boolean, default `false` (1-based, the stack's default). They
  do not read the session option: it is kept only for the Python layer and is
  not a DataFusion configuration field. SQL and expression results are equal
  when the same value is used.

### D7. Motif search

`find_motif(pattern, both_strands=False, overlapping=True,
use_zero_based=None)` returns `List[Int64]`.

- **Pattern.** IUPAC nucleotide codes in either case; `U` means `T`. An empty
  pattern, or one with any other character, raises `ValueError` when the
  expression is built. (Biopython loops forever on an empty pattern.)
- **Matching.** A pattern position matches a sequence base when the base is
  an unambiguous `A`, `C`, `G`, `T` or `U` contained in the pattern code's
  set. An ambiguous or unknown base in the sequence matches nothing, as in
  `nt_search`.
- **Two deliberate differences from `nt_search`:** matching ignores the case
  of the sequence (soft-masked genomes), and `U` in the sequence matches `T`.
  On uppercase DNA the positions are identical to `nt_search`.
- **`overlapping=False`** resumes the search after the end of each hit.
- **`both_strands=True`** also searches the reverse complement of the pattern
  and returns the sorted union of start positions on the given strand. A
  palindromic site is reported once. The strand of a hit is not reported; a
  caller who needs it searches the pattern and its reverse complement
  separately.
- **The two options combine as two independent scans.** The pattern and its
  reverse complement are each searched with the `overlapping` rule, and the
  results are merged. With `overlapping=False`, hits of the same orientation
  never overlap; a hit of the pattern may still overlap a hit of its reverse
  complement.
- Positions are the start of each hit, sorted ascending. No hit gives an
  empty list; a null input gives null.

### D8. Windows, weight, hash and trimming (phase 3)

- **`gc_content_windows(window, step=None, denominator="length")`** and
  **`gc_skew(window=100, step=None)`** return `List[Float64]`.
  - `step=None` means `step = window` (non-overlapping).
  - A window starts at every multiple of `step` below the sequence length and
    is cut at the sequence end, so the last windows can be shorter. With
    `step = window` this is exactly Biopython's `GC_skew` windowing.
  - `gc_skew` is `(G − C) / (G + C)`, case-insensitive. A window with no G or
    C gives `0.0`, as in Biopython. This is a deliberate exception to the
    "zero denominator gives null" rule: zero is the neutral value of a skew,
    and cumulative-skew analyses sum the values.
  - `gc_content_windows` follows `gc_content`: a zero denominator (possible
    only with `denominator="acgt"`) gives a null element.
  - An empty sequence gives an empty list.
- **`molecular_weight(seq_type="DNA", double_stranded=False, circular=False,
  monoisotopic=False)`** returns Float64.
  - The contract: for every input Biopython's `molecular_weight` accepts, the
    same value (golden tests use a relative tolerance of 1e-12); for every
    input Biopython rejects with `ValueError` (an ambiguous or unknown
    letter), **null**. The empty string therefore gives 18.0153, as in
    Biopython.
  - Null instead of an error because `N` in a read is ordinary data, not a
    corrupt file, and a query should not fail on it.
  - An invalid option combination raises `ValueError` when the expression is
    built.
- **`hash(canonical=False)`** returns UInt64.
  - XXH3-64 with seed 0 over the raw bytes. The values are part of the API:
    they stay the same across releases and platforms.
  - `canonical=True` returns `min(hash(s), hash(revcomp(s)))` with the DNA
    alphabet, so a sequence and its reverse complement hash equally.
  - Case is not folded; callers who want that apply `str.to_uppercase()`
    first.
  - The docs state the collision bound of a 64-bit hash (about n²/2⁶⁵ for n
    distinct sequences).
- **`trim_quality(quality, *, leading=None, trailing=None,
  sliding_window=None, offset=33)`** is called on the sequence expression and
  takes the quality expression or column name.
  - Returns `Struct{sequence: String, quality_scores: String}`, the column
    names `scan_fastq` uses, so `unnest` restores the reader's schema.
  - `sliding_window=(size, quality)`. At least one step is required.
  - Steps always run in the order `leading`, `trailing`, `sliding_window`,
    Trimmomatic's customary order. A caller who needs another order calls the
    function twice.
  - A read trimmed to nothing gives two empty strings, not null. Trimmomatic
    drops such reads; the equivalent is a filter on `str.len_bytes()`.
  - A sequence and quality of different lengths raise `ComputeError`. A null
    in either input gives null.
  - The exact cut rules are Trimmomatic's, pinned by task 3.1.

### D9. SQL functions

New: `seq_cds_status`, `seq_transcribe`, `seq_back_transcribe`,
`seq_expected_errors`, `seq_min_quality`, `seq_max_quality`, `seq_orfs`,
`seq_find_motif`, `seq_gc_content_windows`, `seq_gc_skew`,
`seq_molecular_weight`, `seq_hash` and `seq_trim_quality`. `seq_translate`
gains `stop_symbol`, `cds` and `ambiguous` after its existing arguments.

Arguments stay positional and constant, in the order of the Python
signature, as today. `seq_trim_quality` takes
`(seq, qual, leading, trailing, window_size, window_quality, offset)` and
accepts `NULL` for a step that is not wanted.

### D10. Verification

- **Golden tests upstream.** `tests/data/make_golden.py` is extended per
  phase. Strings compare exactly; floats to the oracle's printed precision
  (molecular weight: relative 1e-12).
- **Property tests upstream.** The ORF invariant in D5; `find_motif` against
  a brute-force scan; `back_transcribe(transcribe(s)) == s` for sequences
  without `U`; `canonical` hash equal for `s` and `revcomp(s)`;
  `gc_content_windows` with one window equal to `gc_content`.
- **polars-bio tests.** Expression/SQL parity, null handling, Biopython
  parity through `pytest.importorskip("Bio")`, streaming, `pb.orfs` output in
  a range operation next to a tagged frame, `LazyFrame.serialize()` round
  trip of the new nodes.
- **Packaging gates.** The existing wheel-size check and the Polars
  compatibility matrix. Phase 2 starts with a spike on the matrix because it
  adds the first nested plugin outputs.

## Risks / Trade-offs

| Risk | Mitigation |
|---|---|
| List or struct plugin outputs fail on one of the supported Polars versions | Task 2.1 is a spike on the full matrix (1.40.0 to newest 2.1.x) before any ORF code is written. Fallback: return a JSON string and decode in Python, which keeps the API. |
| The `dtype-struct` feature pushes a wheel over the size budget | The CI size check already fails the build. Task 2.1 measures the size change of every platform wheel before any ORF code is written. |
| Biopython changes a rule in a later release | Parity is claimed for Biopython 1.88, the version the tables and golden files are generated from. The generator script makes a re-check cheap. |
| The ORF definition differs from the tool a user knows | The definition is written in the docs with a worked example, and `oracles.md` records how the oracle tools differ. |
| Users read 1-based motif positions as Biopython's 0-based ones | Stated in the docstring, the docs table and the migration page; `use_zero_based=True` is one argument. |
| `cds=True` fails a long streaming job late | The error message names `cds_status`; the docs show the filter-first pattern from D3. |
| Hash values become a compatibility burden | The algorithm and seed are fixed in the spec. A different algorithm would be a new argument, never a silent change. |
| Long positional SQL signatures are hard to read | Accepted for consistency. See the open question on named arguments. |

## Migration Plan

Additive; no migration. Each phase:
1. merge and tag `datafusion-bio-functions`;
2. pin the tag in polars-bio, add the plugin adapters, Python API, tests and
   docs;
3. release polars-bio.

Rollback is a revert of the polars-bio PR; the upstream tag is harmless on its
own.

## Left to the implementer

- Kernel internals: the search algorithm for `find_motif`, the scanning
  strategy for ORFs, buffer reuse.
- How `Translator` exposes the new options (builder methods or an options
  struct), as long as `Translator::new(frame, table, to_stop)` keeps working.
- Which XXH3-64 implementation to use, preferring a crate already in the
  dependency tree.
- The wording of error messages beyond what D3 requires.
- The file layout of the new kernels and the structure of the golden files.
- Which Python module holds `pb.orfs`.
- Where the migration page sits in the docs navigation.

## Open Questions

- **Named SQL arguments.** `seq_translate(sequence, cds => true)` would read
  better than seven positional arguments. DataFusion 55.3 has
  `Signature::with_parameter_names`, and its source allows it for the
  user-defined signature type `SeqUdf` uses. What is not yet known is how a
  call that names a late argument and omits the ones before it reaches the
  function. Task 1.4 tests this and adopts named arguments if such calls
  work; positional calls keep working either way.
- **Motif hits as intervals.** `find_motif(both_strands=True)` returns
  positions only, as the issue specifies. A strand-aware
  `List[Struct{start, end, strand}]` variant, and a frame-level `pb` helper
  like `pb.orfs`, would take motif hits into the range operations the same
  way. Both can be added later without changing `find_motif`.
- **SQL default coordinates.** Adding the coordinate-system option to the
  shared `BioConfig` would let SQL functions default to the session setting.
  That is a change to the ranges crate and is out of scope here.
