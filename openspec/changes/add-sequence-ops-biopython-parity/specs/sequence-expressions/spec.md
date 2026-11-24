## ADDED Requirements

### Requirement: Translation Output Options

`seq.translate` SHALL accept the keyword arguments `stop_symbol="*"` and
`ambiguous="x"` in addition to `frame`, `table` and `to_stop`. With the
default values every result SHALL be identical to the result without these
arguments.
- `stop_symbol` SHALL be any string, including the empty string and strings
  longer than one character, and SHALL be emitted in place of every stop.
- `ambiguous="x"` SHALL emit `X` for an ambiguous codon whose expansions do
  not all encode the same amino acid.
- `ambiguous="biopython"` SHALL do the same, except that it SHALL emit `B`
  when the expansions encode exactly D and N, `Z` for exactly E and Q, and `J`
  for exactly I and L.

#### Scenario: Custom stop symbol
- **WHEN** `translate(stop_symbol="#")` is evaluated on `"ATGTAAGCCTGA"`
- **THEN** the result SHALL be `"M#A#"`, and `stop_symbol=""` SHALL give `"MA"`

#### Scenario: Biopython ambiguity letters
- **WHEN** `translate(ambiguous="biopython")` is evaluated on `"RATSARMTTWTT"`
- **THEN** the result SHALL be `"BZJX"`, and the default `ambiguous="x"` SHALL give `"XXXX"`

#### Scenario: Exact parity with Biopython
- **WHEN** `translate(ambiguous="biopython")` is evaluated with each supported frame and table on the test corpus
- **THEN** every result SHALL equal Biopython's `Seq.translate` output for the same frame and table, with the trailing partial codon removed and without mapping any letter

#### Scenario: Defaults are unchanged
- **WHEN** `translate()` is evaluated with only `frame`, `table` and `to_stop` on the existing test corpus
- **THEN** every result SHALL equal the result produced before this change

### Requirement: CDS Translation and Validation

`seq.translate` SHALL accept `cds=False`. With `cds=True` it SHALL validate
each sequence, in the requested frame, as a complete coding sequence, using
these checks in this order:
1. the first codon is a start codon of the table;
2. the length is a multiple of three;
3. the final codon is a stop codon of the table;
4. no other codon translates to a stop.

An ambiguous first or final codon SHALL pass only if every expansion is a
start or stop codon respectively. For a valid sequence the first codon SHALL
be emitted as `M`, the final stop SHALL NOT be emitted, and the remaining
codons SHALL translate as without `cds`. For an invalid sequence evaluation
SHALL raise a `ComputeError` that states the failed check and names
`seq.cds_status`.

polars-bio SHALL provide `seq.cds_status(frame=1, table=1)`, returning a
String that is null when `translate(cds=True)` with the same `frame` and
`table` would not raise, and otherwise the first failing check as one of
`"no_start_codon"`, `"length_not_multiple_of_3"`, `"no_stop_codon"` or
`"internal_stop_codon"`.

#### Scenario: Alternative start codon
- **WHEN** `translate(table=11, cds=True)` is evaluated on `"GTGGCCTAA"`
- **THEN** the result SHALL be `"MA"`

#### Scenario: Invalid CDS raises
- **WHEN** `translate(cds=True)` is evaluated on a column containing `"ATGTAAGCCTAA"`
- **THEN** evaluation SHALL raise `polars.exceptions.ComputeError` mentioning the in-frame stop and `cds_status`, and later expressions in the same process SHALL still evaluate normally

#### Scenario: Status reports the first failing check
- **WHEN** `cds_status()` is evaluated on `["ATGGCCTAA", "GTGGCCTAA", "ATGGCCTA", "ATGGCCGCC", "ATGTAAGCCTAA", None]`
- **THEN** the result SHALL be `[None, "no_start_codon", "length_not_multiple_of_3", "no_stop_codon", "internal_stop_codon", None]`

#### Scenario: Filter then translate
- **WHEN** a frame is filtered to rows where `cds_status(table=t)` is null and `translate(table=t, cds=True)` is evaluated on the result
- **THEN** evaluation SHALL NOT raise

#### Scenario: Dual-meaning codons
- **WHEN** `translate(table=27, cds=True)` is evaluated on `"ATGTGAGCCTGA"`
- **THEN** the result SHALL be `"MWA"`

#### Scenario: CDS parity with Biopython
- **WHEN** `translate(cds=True, ambiguous="biopython")` and `cds_status()` are evaluated on the CDS test corpus for every table
- **THEN** every sequence Biopython's `Seq.translate(cds=True)` accepts SHALL give the same protein and a null status, and every sequence Biopython rejects with one of the four checks SHALL give the status for that check

### Requirement: Transcription

polars-bio SHALL provide `seq.transcribe()`, which replaces `T` with `U` and
`t` with `u`, and `seq.back_transcribe()`, which replaces `U` with `T` and `u`
with `t`. Every other byte SHALL be copied unchanged, and a null input SHALL
give null.

#### Scenario: Transcribe and back
- **WHEN** `transcribe()` is evaluated on `["ACGTacgtN-U", None]`
- **THEN** the result SHALL be `["ACGUacguN-U", None]`, and `back_transcribe()` on `"ACGUacgu"` SHALL give `"ACGTacgt"`

### Requirement: Read Quality Summaries

polars-bio SHALL provide, for a quality string:
- `seq.expected_errors(offset=33)`, the sum of `10^(-Q/10)` over the read as
  Float64, which SHALL be `0.0` for an empty string;
- `seq.min_quality(offset=33)` and `seq.max_quality(offset=33)`, the lowest
  and highest Phred score as Int32, which SHALL be null for an empty string.

A null input SHALL give null. A quality byte below `offset` SHALL raise a
`ComputeError`.

#### Scenario: Expected errors
- **WHEN** `expected_errors()` is evaluated on `["I#", "", None]`
- **THEN** the result SHALL be `[10^-4 + 10^-0.2, 0.0, None]`

#### Scenario: Expected-errors filter matches vsearch
- **WHEN** a FASTQ test corpus is filtered with `expected_errors() <= 1.0`
- **THEN** the kept reads SHALL be the reads `vsearch --fastq_maxee 1.0` keeps

#### Scenario: Minimum and maximum quality
- **WHEN** `min_quality()` and `max_quality()` are evaluated on `"I5#"`
- **THEN** the results SHALL be `2` and `40`

### Requirement: ORF Finding

polars-bio SHALL provide `seq.orfs(table=1, min_len=30, strands="both",
start_codons="table", allow_partial=False, include_protein=False,
use_zero_based=None)`, returning for each sequence a list of structs, one per
open reading frame, sorted by `start`, then `end`, then `strand`.
- In each reading frame, a complete ORF SHALL run from the first start codon
  after the previous in-frame stop to the next in-frame stop. A stop SHALL
  produce at most one ORF.
- A codon SHALL count as a start or stop only if every expansion of its
  IUPAC codes is one. Every codon in the table's stop-codon set SHALL end an
  ORF, including the codons that tables 27, 28 and 31 also read as an amino
  acid.
- `start_codons="table"` SHALL use the table's start codons; `"ATG"` SHALL
  use only `ATG`.
- ORFs with fewer than `min_len` amino acids SHALL be omitted.
- With `allow_partial=True`, every ORF of the default output SHALL still be
  reported unchanged, and these SHALL be added: an ORF from the first
  complete codon of a frame to its first stop when no start codon precedes
  that stop (`has_start = false`); an ORF from the first start codon after a
  frame's last stop to its last complete codon (`has_stop = false`); and one
  ORF spanning a frame that has neither a start nor a stop codon.
- Each struct SHALL contain `start` and `end` (Int64) on the input sequence
  as given, including the stop codon when present; `strand` (`"+"` or `"-"`);
  `frame` (Int32, with the meaning of `translate(frame=...)`); `aa_len`
  (Int64, excluding the stop); `has_start` and `has_stop` (Boolean); and,
  only with `include_protein=True`, `protein` (String).
- A sequence without ORFs SHALL give an empty list, and a null input null.

#### Scenario: One ORF per stop
- **WHEN** `orfs(min_len=1)` is evaluated on a sequence in which two start codons precede the same in-frame stop
- **THEN** exactly one ORF SHALL be reported for that stop, beginning at the first of the two start codons

#### Scenario: ORF on the reverse strand
- **WHEN** `orfs(min_len=1, strands="both")` is evaluated on the reverse complement of a sequence that has one forward-strand ORF
- **THEN** the same ORF SHALL be reported with `strand = "-"`, a negative `frame`, and `start`/`end` on the given sequence

#### Scenario: An ORF is a valid CDS
- **WHEN** an ORF is reported with `has_start` and `has_stop` true and `start_codons="table"`
- **THEN** the input sliced to that interval, reverse-complemented for `strand = "-"`, SHALL have a null `cds_status` for the same table, and its `translate(cds=True)` SHALL equal the ORF's `protein`

#### Scenario: Partial ORFs
- **WHEN** `orfs(allow_partial=True)` is evaluated on a sequence that begins inside an ORF, with no start codon before its first in-frame stop
- **THEN** that ORF SHALL be reported with `has_start = false`, and it SHALL NOT be reported with the default `allow_partial=False`

#### Scenario: Partial output contains the default output
- **WHEN** `orfs()` is evaluated on the test corpus with `allow_partial=False` and with `allow_partial=True`
- **THEN** every ORF of the first result SHALL appear unchanged in the second

#### Scenario: Dual-meaning stop codons end ORFs
- **WHEN** `orfs(table=27, min_len=1)` is evaluated on `"ATGGCCTGAGCC"`
- **THEN** one ORF SHALL be reported, ending at the `TGA` codon

### Requirement: ORF Table

polars-bio SHALL provide `pb.orfs(source, *, sequence_col="sequence",
table=1, min_len=30, strands="both", start_codons="table",
allow_partial=False, include_protein=False, use_zero_based=None)`, returning
a LazyFrame with one row per ORF.
- `source` SHALL be a FASTA path or a Polars DataFrame or LazyFrame that has
  the sequence column.
- The result SHALL contain every input column except the sequence column,
  and the fields of the `seq.orfs` struct as columns. The ORFs SHALL be the
  ones `seq.orfs` reports with the same arguments.
- The result SHALL carry coordinate-system metadata for the system it used.
  With `use_zero_based=None` that SHALL be the session option at the time of
  the call.

#### Scenario: One row per ORF
- **WHEN** `pb.orfs` is called on a FASTA file with three sequences, one of which has no ORF
- **THEN** the result SHALL have one row per ORF of the other two sequences, with the `name` column identifying the sequence and no `sequence` column

#### Scenario: Use with a range operation
- **WHEN** the result of `pb.orfs` and an annotation frame tagged with the same coordinate system are passed to `pb.overlap`
- **THEN** the operation SHALL run without a coordinate-system error or warning

#### Scenario: Coordinate-system mismatch is detected
- **WHEN** the result of `pb.orfs(..., use_zero_based=True)` and a frame tagged as 1-based are passed to `pb.overlap`
- **THEN** `CoordinateSystemMismatchError` SHALL be raised

### Requirement: IUPAC Motif Search

polars-bio SHALL provide `seq.find_motif(pattern, both_strands=False,
overlapping=True, use_zero_based=None)`, returning the sorted start positions
of every match as `List[Int64]`.
- `pattern` SHALL consist of IUPAC nucleotide codes. An empty pattern or any
  other character SHALL raise `ValueError` when the expression is built.
- A pattern code SHALL match a sequence base that is an unambiguous `A`, `C`,
  `G`, `T` or `U` contained in the code's set, in either case, with `U`
  treated as `T`. An ambiguous or unknown base in the sequence SHALL match
  nothing.
- `overlapping=False` SHALL resume the search after the end of each match.
- `both_strands=True` SHALL also search for the reverse complement of the
  pattern and SHALL return the sorted, de-duplicated start positions on the
  given sequence. The pattern and its reverse complement SHALL be searched
  independently, each with the `overlapping` rule.
- A sequence without matches SHALL give an empty list, and a null input null.

#### Scenario: Parity with Biopython nt_search
- **WHEN** `find_motif(pattern, use_zero_based=True)` is evaluated on uppercase DNA sequences of the test corpus
- **THEN** every result SHALL equal the positions returned by `Bio.SeqUtils.nt_search` for the same sequence and pattern

#### Scenario: Ambiguity applies to the pattern only
- **WHEN** `find_motif("N", use_zero_based=True)` is evaluated on `"ACGTNACGT"`
- **THEN** the result SHALL be `[0, 1, 2, 3, 5, 6, 7, 8]`

#### Scenario: Overlapping and non-overlapping matches
- **WHEN** `find_motif("AA", use_zero_based=True)` is evaluated on `"AAAAA"`
- **THEN** the result SHALL be `[0, 1, 2, 3]`, and `[0, 2]` with `overlapping=False`

#### Scenario: Both strands
- **WHEN** `find_motif("GAATTC", both_strands=True)` is evaluated on a sequence containing that palindromic site once
- **THEN** the site SHALL be reported once

#### Scenario: Invalid pattern
- **WHEN** a user calls `find_motif("")` or `find_motif("AC!")`
- **THEN** a `ValueError` SHALL be raised before any data is read

### Requirement: Sequence Coordinates

`seq.orfs`, `seq.find_motif` and `pb.orfs` SHALL report positions in the
coordinate system selected by `use_zero_based`: 0-based half-open for `True`,
1-based closed for `False`. With `use_zero_based=None` they SHALL use the
value of the session option `datafusion.bio.coordinate_system_zero_based` at
the time the expression is built or the function is called.

#### Scenario: Default follows the session option
- **WHEN** the session option is 1-based and `find_motif("ACG")` is evaluated on `"ACGTACGT"`
- **THEN** the result SHALL be `[1, 5]`, and `[0, 4]` with `use_zero_based=True`

#### Scenario: ORF interval in both systems
- **WHEN** the same ORF is reported with `use_zero_based=True` and with `use_zero_based=False`
- **THEN** `end` SHALL be equal in both and `start` SHALL differ by one

#### Scenario: Setting is captured at build time
- **WHEN** an expression is built with `use_zero_based=None` and the session option is changed before the frame is collected
- **THEN** the result SHALL use the value the option had when the expression was built

### Requirement: Windowed GC Content and GC Skew

polars-bio SHALL provide `seq.gc_content_windows(window, step=None,
denominator="length")` and `seq.gc_skew(window=100, step=None)`, each
returning `List[Float64]` with one value per window.
- `step=None` SHALL mean `step = window`.
- A window SHALL start at every multiple of `step` below the sequence length
  and SHALL be cut at the end of the sequence.
- `gc_content_windows` SHALL compute each value as `gc_content` does for the
  window, including a null element for a zero denominator.
- `gc_skew` SHALL compute `(G − C) / (G + C)`, ignoring case, and SHALL give
  `0.0` for a window with no G or C.
- An empty sequence SHALL give an empty list, and a null input null.

#### Scenario: GC skew parity with Biopython
- **WHEN** `gc_skew(window=w)` is evaluated on the test corpus
- **THEN** every result SHALL equal `Bio.SeqUtils.GC_skew(seq, w)`

#### Scenario: Last window is partial
- **WHEN** `gc_skew(window=4)` is evaluated on `"AAAAGG"`
- **THEN** the result SHALL be `[0.0, 1.0]`

#### Scenario: One window equals the whole-sequence value
- **WHEN** `gc_content_windows(window=n)` is evaluated on a non-empty sequence of length `n`
- **THEN** the result SHALL be a one-element list equal to `gc_content()`

### Requirement: Molecular Weight

polars-bio SHALL provide `seq.molecular_weight(seq_type="DNA",
double_stranded=False, circular=False, monoisotopic=False)`, returning
Float64. For every input that Biopython's `Bio.SeqUtils.molecular_weight`
accepts with the same arguments, the result SHALL equal Biopython's. For
every input Biopython rejects because of an ambiguous or unknown letter, the
result SHALL be null. A null input SHALL give null.

#### Scenario: Parity with Biopython
- **WHEN** `molecular_weight` is evaluated with each combination of `seq_type`, `double_stranded`, `circular` and `monoisotopic` that Biopython accepts, on the test corpus
- **THEN** every result SHALL equal Biopython's to a relative tolerance of 1e-12

#### Scenario: Ambiguous letter gives null
- **WHEN** `molecular_weight()` is evaluated on `["ACGT", "ACGN"]`
- **THEN** the result SHALL be `[1253.8027, None]`

### Requirement: Sequence Hash

polars-bio SHALL provide `seq.hash(canonical=False)`, returning the XXH3
64-bit hash (seed 0) of the sequence bytes as UInt64. With `canonical=True`
it SHALL return the smaller of the hash of the sequence and the hash of its
DNA reverse complement. Hash values SHALL NOT change between releases or
platforms. A null input SHALL give null.

#### Scenario: Stable values
- **WHEN** `hash()` is evaluated on a fixed set of sequences
- **THEN** the results SHALL equal the values recorded in the test suite

#### Scenario: Canonical hash ignores strand
- **WHEN** `hash(canonical=True)` is evaluated on a sequence and on its reverse complement
- **THEN** the two results SHALL be equal

#### Scenario: Deduplication
- **WHEN** a FASTQ test corpus is de-duplicated with `unique(subset=...)` on `hash()`, keeping the first record
- **THEN** the kept records SHALL be the records `seqkit rmdup -s` keeps when restricted to the given strand

### Requirement: Quality Trimming

polars-bio SHALL provide `seq.trim_quality(quality, *, leading=None,
trailing=None, sliding_window=None, offset=33)` on a sequence expression,
where `quality` is the quality-string expression or column name. It SHALL
return `Struct{sequence: String, quality_scores: String}` holding the trimmed
sequence and quality.
- The steps SHALL be applied in the order `leading`, `trailing`,
  `sliding_window`, each with Trimmomatic's `LEADING`, `TRAILING` and
  `SLIDINGWINDOW` rule. At least one step SHALL be required.
- The two output strings SHALL always have equal length. A read trimmed
  completely SHALL give two empty strings.
- A sequence and quality of different lengths SHALL raise a `ComputeError`,
  as SHALL a quality byte below `offset`. A null in either input SHALL give
  null.

#### Scenario: Parity with Trimmomatic
- **WHEN** `trim_quality(leading=l, trailing=t, sliding_window=(w, q))` is evaluated on the FASTQ test corpus
- **THEN** for every read Trimmomatic keeps with `LEADING:l TRAILING:t SLIDINGWINDOW:w:q`, the trimmed sequence and quality SHALL equal Trimmomatic's, and every read Trimmomatic drops SHALL give two empty strings

#### Scenario: Unnest restores the reader's columns
- **WHEN** the result is unnested after dropping the original `sequence` and `quality_scores` columns of a `scan_fastq` frame
- **THEN** the frame SHALL have `sequence` and `quality_scores` columns of equal length per row

#### Scenario: Length mismatch
- **WHEN** `trim_quality` is evaluated on a row whose sequence and quality differ in length
- **THEN** evaluation SHALL raise `polars.exceptions.ComputeError`

### Requirement: Follow-up Sequence SQL Functions

polars-bio SHALL register the DataFusion scalar functions `seq_cds_status`,
`seq_transcribe`, `seq_back_transcribe`, `seq_expected_errors`,
`seq_min_quality`, `seq_max_quality`, `seq_orfs`, `seq_find_motif`,
`seq_gc_content_windows`, `seq_gc_skew`, `seq_molecular_weight`, `seq_hash`
and `seq_trim_quality` in its session context, and `seq_translate` SHALL
accept `stop_symbol`, `cds` and `ambiguous` after its existing arguments.
Each function SHALL take the parameters of the corresponding `seq`
expression as positional SQL arguments and SHALL return the same results,
because both use the same kernels. `seq_orfs` and `seq_find_motif` SHALL take
the coordinate system as an explicit `zero_based` argument defaulting to
`false`, and SHALL NOT read the session option.

#### Scenario: SQL and expression parity
- **WHEN** a new `seq_*` function and the corresponding `seq` expression are evaluated on the same registered FASTQ or FASTA table with the same arguments
- **THEN** the results SHALL be equal

#### Scenario: Existing SQL calls are unchanged
- **WHEN** `seq_translate` is called with one to four arguments
- **THEN** the result SHALL equal the result produced before this change

#### Scenario: Explicit coordinates in SQL
- **WHEN** `seq_find_motif(sequence, 'ACG')` is evaluated on `'ACGTACGT'`
- **THEN** the result SHALL be `[1, 5]`, and passing `zero_based = true` SHALL give `[0, 4]`
