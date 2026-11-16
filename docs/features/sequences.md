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
| `.seq.mean_quality(offset=33, method="error_prob")` | mean Phred quality | seqkit `fx2tab -q`; `"arithmetic"` is FastQC's mean |

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
