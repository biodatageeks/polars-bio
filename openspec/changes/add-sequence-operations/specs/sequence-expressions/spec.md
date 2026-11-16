## ADDED Requirements

### Requirement: Sequence Expression Namespace

polars-bio SHALL register a `seq` Polars expression namespace when
`polars_bio` is imported. It SHALL provide
`revcomp(alphabet="dna", strict=False)`, `reverse()`,
`complement(alphabet="dna", strict=False)`,
`gc_content(denominator="length")`,
`translate(frame=1, table=1, to_stop=False)` and
`mean_quality(offset=33, method="error_prob")`. Each function SHALL be backed
by a native Polars expression plugin compiled into the polars-bio extension
module, SHALL accept a String column, and SHALL work on any Polars DataFrame or
LazyFrame, not only on frames produced by polars-bio readers.

#### Scenario: Reverse complement on an arbitrary frame
- **WHEN** a user evaluates `pl.DataFrame({"s": ["ACGTN", None, "aacc"]}).select(pl.col("s").seq.revcomp())`
- **THEN** the result SHALL be `["NACGT", None, "ggtt"]`

#### Scenario: Reverse a quality string
- **WHEN** a user evaluates `pl.col("quality_scores").seq.reverse()` on `["II5#", None]`
- **THEN** the result SHALL be `["#5II", None]`, and SHALL equal `str.reverse()` for every ASCII or multi-byte input without combining characters

#### Scenario: Composition with a polars-bio scan
- **WHEN** a user evaluates `pb.scan_fastq(path).with_columns(gc=pl.col("sequence").seq.gc_content()).filter(pl.col("gc") > 0.5)`
- **THEN** the result SHALL contain exactly the reads whose GC fraction exceeds 0.5

#### Scenario: Non-string input
- **WHEN** a `seq` function is applied to a non-String column
- **THEN** evaluation SHALL raise a Polars error that names the expected String type

### Requirement: Sequence Kernel Semantics

The sequence kernels SHALL implement the following semantics:
- A null input SHALL give a null output.
- A ratio with a zero denominator SHALL give null, never NaN.
- `revcomp` and `complement` SHALL complement the case-preserved IUPAC
  nucleotide codes `ACGTURYSWKMBDHVN`. The complement pairs SHALL be A↔T, C↔G,
  R↔Y, K↔M, B↔V and D↔H, with S, W and N mapping to themselves.
  `alphabet="dna"` SHALL map A to T and U to A; `alphabet="rna"` SHALL map A
  to U and U to A. Any other byte, including the gap characters `-` and `.`,
  SHALL be copied unchanged, unless `strict=True`, in which case the first
  such byte SHALL raise a `ComputeError` that names it.
- `gc_content` SHALL return the case-insensitive count of G and C divided by
  the sequence length for `denominator="length"`, or by the count of A, C, G
  and T for `denominator="acgt"`.
- `translate` SHALL accept frames ±1, ±2 and ±3 and NCBI translation-table ids,
  SHALL drop a trailing partial codon, SHALL emit `*` for stops, SHALL emit `X`
  for an ambiguous codon whose expansions do not all encode the same amino
  acid, SHALL emit `-` for a codon of three gap characters (`-` or `.`),
  and SHALL emit `X` for any other codon containing a gap or a byte outside
  the IUPAC nucleotide codes. A codon that a table lists as both a stop and
  an amino acid SHALL translate to the amino acid.
  `to_stop=True` SHALL truncate before the first stop.
- `mean_quality` SHALL raise a `ComputeError` for a quality byte below
  `offset`. It SHALL return `-10·log10(mean(10^(-Q/10)))` for
  `method="error_prob"` and the mean Phred score for `method="arithmetic"`.

#### Scenario: Empty sequence GC is null
- **WHEN** `gc_content` is evaluated on `""`, or on `"NNN"` with `denominator="acgt"`
- **THEN** the result SHALL be null, and a filter `gc > 0.3` SHALL exclude that row

#### Scenario: Unknown bytes pass through
- **WHEN** `revcomp` is evaluated on `"ACGZ"` with the default `strict=False`
- **THEN** the result SHALL be `"ZCGT"`, the same as seqfu `rc`, seqtk `seq -r` and Biopython

#### Scenario: Strict alphabet
- **WHEN** `revcomp(strict=True)` is evaluated on `"ACGZ"`
- **THEN** evaluation SHALL raise `polars.exceptions.ComputeError` mentioning `Z`, and later expressions in the same process SHALL still evaluate normally

#### Scenario: GC parity with seqkit
- **WHEN** `gc_content(denominator="acgt")` and `gc_content(denominator="length")` are evaluated on the test corpus
- **THEN** the results multiplied by 100 SHALL equal `seqkit fx2tab -n -g` and `seqkit fx2tab -n -B GC` respectively, to seqkit's printed precision

#### Scenario: Mean quality parity with seqkit
- **WHEN** `mean_quality()` is evaluated on the FASTQ test corpus with the default method
- **THEN** every result SHALL equal `seqkit fx2tab -n -q` to seqkit's printed precision

#### Scenario: Translation parity with Biopython
- **WHEN** `translate` is evaluated with each supported frame, table and `to_stop` combination on the test corpus
- **THEN** every result SHALL equal Biopython's `Seq.translate` output for the same frame, with the same table and the trailing partial codon removed, after mapping Biopython's ambiguity letters `B`, `Z` and `J` to `X`

#### Scenario: Translation parity with seqkit
- **WHEN** `translate` is evaluated with a positive frame and a table seqkit supports on the test corpus
- **THEN** every result SHALL equal `seqkit translate` for the same frame and table

#### Scenario: Reverse complement is an involution
- **WHEN** `revcomp` is applied twice with the default `alphabet="dna"` to any sequence that contains no `U` or `u`
- **THEN** the result SHALL equal the input

### Requirement: Streaming Sequence Expressions

Every element-wise `seq` function SHALL be registered as element-wise, SHALL
run inside Polars' streaming engine one chunk at a time, and SHALL NOT acquire
the Python GIL during execution.

#### Scenario: Parallel streaming execution
- **WHEN** a `seq` function runs over a multi-million-row LazyFrame with `collect(engine="streaming")`
- **THEN** the work SHALL be split across several streaming chunks, and the process SHALL use more than one CPU core

#### Scenario: Plan serialization
- **WHEN** a LazyFrame containing a `seq` expression is serialized with `LazyFrame.serialize()` and deserialized in the same environment
- **THEN** the deserialized plan SHALL produce the same result, and the serialized expression SHALL name the plugin function

### Requirement: Sequence SQL Functions

polars-bio SHALL register DataFusion scalar functions `seq_revcomp`,
`seq_reverse`, `seq_complement`, `seq_gc_content`, `seq_translate` and `seq_mean_quality` in
its session context. They SHALL take the same parameters, as positional SQL
arguments, and SHALL return the same results as the corresponding `seq`
expressions, because both use the same kernels.

#### Scenario: SQL and expression parity
- **WHEN** a user runs `pb.sql("SELECT seq_revcomp(sequence) AS rc FROM reads")` against a registered FASTQ table
- **THEN** the `rc` column SHALL equal `pb.scan_fastq(path).select(pl.col("sequence").seq.revcomp())`

#### Scenario: No collision with DataFusion built-ins
- **WHEN** a user calls DataFusion's built-in `translate(str, from, to)` in `pb.sql`
- **THEN** the built-in string function SHALL still be used
