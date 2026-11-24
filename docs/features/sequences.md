# Sequence operations

polars-bio adds a `seq` namespace to Polars expressions for common sequence
operations, inspired by [seqkit](https://github.com/shenwei356/seqkit) and
[seqfu](https://github.com/telatin/seqfu2). The functions are native Polars
expression plugins: they run in parallel without the GIL and stream with
`collect(engine="streaming")`. They work on any String column, whether it came
from a polars-bio reader or from anywhere else.

```python
import polars as pl
import polars_bio as pb  # registers pl.Expr.seq

reads = (
    pb.scan_fastq("reads.fastq.gz")
    .with_columns(
        gc=pl.col("sequence").seq.gc_content(),
        qual=pl.col("quality_scores").seq.mean_quality(),
        rc=pl.col("sequence").seq.revcomp(),
        rq=pl.col("quality_scores").seq.reverse(),
    )
    .filter((pl.col("gc") > 0.4) & (pl.col("qual") >= 20))
    .collect(engine="streaming")
)
```

!!! tip
    Prefer `collect(engine="streaming")` (or `sink_*`) for large files. The
    streaming engine processes the file in chunks on all cores, so memory stays
    bounded by the chunk size rather than the file size.

## Functions

| Expression | What it computes | Matches |
|---|---|---|
| `.seq.revcomp(alphabet="dna", strict=False)` | reverse complement | seqkit `seq -r -p`, seqfu `rc`, seqtk `seq -r`, Biopython |
| `.seq.reverse()` | reversed string, e.g. a FASTQ quality alongside `revcomp` | Polars `str.reverse()`, much faster on ASCII |
| `.seq.complement(alphabet="dna", strict=False)` | complement, not reversed | Biopython `complement` |
| `.seq.gc_content(denominator="length")` | GC fraction, 0 to 1 | seqkit `fx2tab -B GC`; with `"acgt"`, seqkit `fx2tab -g` |
| `.seq.translate(frame=1, table=1, to_stop=False)` | protein translation | Biopython `Seq.translate`, seqkit `translate` |
| `.seq.translate(..., stop_symbol="*", cds=False, ambiguous="x")` | translation options | Biopython `Seq.translate(stop_symbol=, cds=)`; `ambiguous="biopython"` gives `B`, `Z`, `J` |
| `.seq.cds_status(frame=1, table=1)` | why a sequence is not a complete CDS (null if it is) | the four checks of Biopython's `cds=True` |
| `.seq.transcribe()`, `.seq.back_transcribe()` | T↔U | Biopython `Seq.transcribe`, `Seq.back_transcribe` |
| `.seq.mean_quality(offset=33, method="error_prob")` | mean Phred quality | seqkit `fx2tab -q`; `"arithmetic"` is FastQC's mean |
| `.seq.expected_errors(offset=33)` | sum of error probabilities | vsearch/USEARCH `--fastq_maxee` |
| `.seq.min_quality()`, `.seq.max_quality()` | lowest and highest Phred score | — |
| `.seq.orfs(table=1, min_len=30, ...)` | open reading frames, `List[Struct]` | Biopython tutorial ORF recipe |
| `.seq.find_motif(pattern, both_strands=False, overlapping=True)` | IUPAC motif positions, `List[Int64]` | Biopython `nt_search` |
| `.seq.gc_content_windows(window, step=None)`, `.seq.gc_skew(window=100, step=None)` | per-window GC and GC skew, `List[Float64]` | Biopython `GC_skew` |
| `.seq.molecular_weight(seq_type="DNA", ...)` | molecular weight | Biopython `molecular_weight` |
| `.seq.hash(canonical=False)` | stable 64-bit hash, for deduplication | seqkit `rmdup -s` |
| `.seq.trim_quality(quality, leading=, trailing=, sliding_window=)` | trimmed sequence and quality | Trimmomatic `LEADING`, `TRAILING`, `SLIDINGWINDOW` |

## Semantics

- **Nulls.** A null input gives null. A ratio with a zero denominator (an empty
  read, or a read with no A/C/G/T for `denominator="acgt"`) gives **null**, not
  NaN, so `gc > 0.3` drops it. (Polars orders NaN above every number.)
- **Alphabet.** `revcomp` and `complement` complement the IUPAC codes
  `ACGTURYSWKMBDHVN` in either case and keep the case. With `alphabet="dna"`,
  A pairs with T and U with A; with `"rna"`, A pairs with U. Any other byte,
  including the gaps `-` and `.`, is copied unchanged, like seqfu, seqtk and
  Biopython. `strict=True` raises `ComputeError` on the first such byte instead.
- **GC.** G and C in either case. No reference tool counts S.
- **Translation.** Frames ±1, ±2, ±3 (negative frames read the reverse
  complement) and every NCBI table. The trailing partial codon is dropped and
  stops are `*`. An ambiguous codon gives an amino acid only when every
  expansion agrees, otherwise `X`, as in seqkit (Biopython returns `B`, `Z`
  or `J` for a few of them). `---` gives `-`; any other unreadable codon gives
  `X`.
- **Mean quality.** The default `error_prob` method converts each Phred score
  to an error probability, averages, and converts back:
  `-10·log10(mean(10^(-Q/10)))`. A read with one Q40 and one Q2 base scores
  5.01, not 21, which reflects its nearly useless base. A quality byte below
  `offset` raises `ComputeError`.

- **Complete CDSs.** `translate(cds=True)` applies Biopython's checks in
  Biopython's order: a start codon of the table first (emitted as `M`, also
  for alternative starts such as `GTG` in table 11), a length that is a
  multiple of three, one final stop (not emitted) and no other stop. An
  ambiguous first or final codon passes only if every expansion qualifies. An
  invalid row raises `ComputeError`; `cds_status()` returns the failing check
  (`no_start_codon`, `length_not_multiple_of_3`, `no_stop_codon`,
  `internal_stop_codon`) so invalid rows can be filtered first:

    ```python
    cds = pb.scan_fasta("cds.fa").with_columns(
        status=pl.col("sequence").seq.cds_status(table=11)
    )
    proteins = cds.filter(pl.col("status").is_null()).with_columns(
        protein=pl.col("sequence").seq.translate(table=11, cds=True)
    )
    ```

- **ORFs.** In each of the six frames, an ORF runs from the first start
  codon after a stop to the next stop, so a stop yields one ORF, the longest.
  `start` and `end` are on the sequence as given for both strands and include
  the stop codon. For `ATGAAAATGCCCTAA` there is one ORF, `MKMP`, at 0-based
  `[0, 15)`; the second `ATG` does not start another. `allow_partial=True`
  adds ORFs cut by a sequence end and never changes the complete ones.
  `pb.orfs(...)` returns one row per ORF, tagged with its coordinate system,
  so it can go straight into the range operations:

    ```python
    found = pb.orfs("genome.fa", min_len=100)
    hits = pb.overlap(found, genes, cols1=["name", "start", "end"])
    ```

- **Positions follow the coordinate-system setting.** `orfs` and
  `find_motif` report 1-based closed coordinates with the default setting and
  0-based half-open ones with `use_zero_based=True` (or the 0-based session
  option). With the default setting `find_motif` positions are therefore
  Biopython's `nt_search` positions **plus one**.
- **Motifs.** Ambiguity codes are expanded in the pattern only: `N` in the
  sequence matches nothing, as in `nt_search`. Unlike `nt_search`, the
  sequence's case is ignored and `U` matches `T`.
- **Molecular weight** is null where Biopython raises (an `N` or another
  ambiguous letter), so a query does not fail on one read.
- **Hash** values are XXH3-64 with seed 0 and never change between releases.
  `canonical=True` hashes a sequence and its reverse complement equally.
- **Trimming** follows Trimmomatic 0.39 exactly, including two details: an
  `N` base counts as quality 0, and `TRAILING` never examines the first base.
  A read Trimmomatic would drop gives two empty strings; drop them with a
  filter on `str.len_bytes()`.

See [Coming from Biopython](biopython.md) for a side-by-side mapping and
measured speed-ups.

## SQL

The same kernels are registered as `seq_revcomp`, `seq_reverse`, `seq_complement`,
`seq_gc_content`, `seq_translate` and `seq_mean_quality`. DataFusion's own
`translate(str, from, to)` string function is unaffected.

```python
pb.register_fastq("reads.fastq.gz", "reads")
pb.sql(
    "SELECT name, seq_gc_content(sequence) AS gc, "
    "seq_translate(sequence, -1) AS aa FROM reads"
).collect()
```
