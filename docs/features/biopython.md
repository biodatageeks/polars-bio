# Coming from Biopython

Code that loops over `SeqIO.parse(...)` records usually becomes one lazy
polars-bio query: the reader produces a frame, and each per-record Biopython
call becomes a `.seq` expression. The query runs in Rust, in parallel, without
the GIL, and streams with `collect(engine="streaming")` or `sink_*`.

```python
# Biopython
from Bio import SeqIO

kept = []
for rec in SeqIO.parse("cds.fa", "fasta"):
    try:
        kept.append((rec.id, str(rec.seq.translate(table=11, cds=True))))
    except Exception:
        pass

# polars-bio
import polars as pl
import polars_bio as pb

kept = (
    pb.scan_fasta("cds.fa")
    .filter(pl.col("sequence").seq.cds_status(table=11).is_null())
    .select("name", pl.col("sequence").seq.translate(table=11, cds=True))
    .collect(engine="streaming")
)
```

## Mapping

| Biopython | polars-bio |
|---|---|
| `SeqIO.parse(path, "fasta")` / `"fastq"` | `pb.scan_fasta(path)` / `pb.scan_fastq(path)` (columns `name`, `description`, `sequence`, and `quality_scores` for FASTQ) |
| `rec.seq.reverse_complement()` | `pl.col("sequence").seq.revcomp()` |
| `rec.seq.complement()` | `.seq.complement()` |
| `rec.seq.transcribe()` / `.back_transcribe()` | `.seq.transcribe()` / `.seq.back_transcribe()` |
| `rec.seq.translate(table=t, to_stop=s)` | `.seq.translate(table=t, to_stop=s, ambiguous="biopython")` |
| `rec.seq.translate(stop_symbol="#")` | `.seq.translate(stop_symbol="#")` |
| `rec.seq.translate(cds=True)` | `.seq.translate(cds=True)`, after filtering on `.seq.cds_status()` |
| `try: ... except TranslationError` | `.seq.cds_status()` (null for a valid CDS, otherwise the failed check) |
| ORF loop over the six translated frames, each ORF from its first `M` to the next `*` | `.seq.orfs(start_codons="ATG")`, or `pb.orfs(path)` for one row per ORF (the tutorial's loop, which splits on `*` without requiring a start, reports different intervals) |
| `Bio.SeqUtils.nt_search(seq, pattern)[1:]` | `.seq.find_motif(pattern, use_zero_based=True)` |
| `Bio.SeqUtils.gc_fraction(seq)` | `.seq.gc_content()` (see the GC note in [Sequence operations](sequences.md)) |
| `Bio.SeqUtils.GC_skew(seq, window)` | `.seq.gc_skew(window)` |
| `Bio.SeqUtils.molecular_weight(seq, ...)` | `.seq.molecular_weight(...)` |
| `rec.letter_annotations["phred_quality"]` | `.seq.mean_quality()`, `.seq.expected_errors()`, `.seq.min_quality()`, `.seq.max_quality()` on `quality_scores` |
| `if len(rec) >= n` | `.filter(pl.col("sequence").str.len_bytes() >= n)` |
| deduplicate with a `set` of sequences | `.with_columns(h=pl.col("sequence").seq.hash(canonical=True)).unique(subset="h")` |

## Differences to know about

1. **Positions are 1-based by default, and count bytes.** `find_motif` and `orfs` follow
   polars-bio's coordinate-system setting, which is 1-based closed unless you
   set it to 0-based. With the default, `find_motif` returns `nt_search`'s
   positions plus one; pass `use_zero_based=True` to get Biopython's. They
   are byte offsets, the same as character offsets for ASCII sequences.
2. **`B`, `Z` and `J` are opt-in.** By default an ambiguous codon whose
   expansions disagree gives `X`, as in seqkit. Pass
   `ambiguous="biopython"` for Biopython's letters.
3. **Invalid codons do not raise.** Biopython raises on a codon with a partial
   gap or an unknown byte; polars-bio translates it to `X` (and `---` or `...`
   to `-`). `cds=True` only adds Biopython's four CDS checks.
4. **`molecular_weight` gives null instead of raising** on an ambiguous or
   unknown letter, so one read with an `N` does not fail the query.
5. **Motif matching ignores the sequence's case and reads `U` as `T`.**
   `nt_search` is case-sensitive and finds nothing in that case.
6. **ORFs count every stop of the table**, including the dual-meaning codons
   of tables 27, 28 and 31, which `translate` reads as amino acids.

## Speed

Measured with `benchmarks/seq/bench_vs_biopython.py`. Before any timing is
reported, the script checks that polars-bio's output equals Biopython's on
the same input.

Apple M3 Max (16 cores), polars-bio built from this change, Polars 2.1.0,
Biopython 1.88, Python 3.11. Best of 3 runs; the end-to-end Biopython loops
ran once. Inputs: the Ensembl GRCh38 CDS set (`Homo_sapiens.GRCh38.cds.all`,
382,428 sequences, 471 Mbp) and ERR5897746 (126 bp Illumina reads). In the
operation rows the data is already in memory (Python lists for Biopython, a
Polars frame for polars-bio), so only the operation is timed.

| Operation | Input | Biopython | polars-bio, 1 thread | polars-bio, 16 threads |
|---|---|---:|---:|---:|
| `translate(cds=True)` + `cds_status` | 382,428 CDS | 17.5 s | 0.69 s (25×) | 0.064 s (273×) |
| `translate`, B/Z/J letters | 382,428 CDS | 17.6 s | 0.40 s (44×) | 0.042 s (424×) |
| ORFs, both strands, ≥ 30 aa | 80,000 CDS, 96 Mbp | 23.5 s | 0.74 s (32×) | 0.071 s (329×) |
| motif `GANTC` (`nt_search`) | 1M reads | 1.21 s | 0.32 s (4×) | 0.029 s (41×) |
| `molecular_weight` | 1M reads | 3.31 s | 0.11 s (31×) | 0.014 s (243×) |
| `GC_skew`, window 100 | 382,428 CDS | 2.45 s | 0.76 s (3×) | 0.074 s (33×) |
| `transcribe` | 1M reads | 0.77 s | 0.019 s (40×) | 0.004 s (200×) |
| expected errors per read | 1M reads | 2.87 s | 0.065 s (44×) | 0.009 s (327×) |
| FASTQ.gz → EE ≤ 1 filter, `SeqIO.parse` loop | ERR5897746_1.fastq.gz (336 MB) | 43.3 s | 1.89 s (23×) | 1.90 s (23×) |
| FASTQ.gz → EE ≤ 1 filter, `FastqGeneralIterator` loop | same | 17.3 s | 1.89 s (9×) | 1.90 s (9×) |

- `nt_search` and `GC_skew` already do their inner work in C (a regular
  expression and `str.count`), so the single-threaded gain is small there.
- The end-to-end rows read a plain gzip file, which decompresses as one
  stream, so extra threads do not help. A BGZF file with a `.gzi` index can
  be read in parallel partitions (see [Parallel processing](parallel.md)).
- The expected-errors filter keeps exactly the reads `vsearch --fastq_filter
  --fastq_maxee 1.0` keeps (334,366 of 719,828 reads of DRR609229), and
  `hash(canonical=True)` deduplication keeps exactly the reads `seqkit rmdup
  -s` keeps (77,751; with `hash()`, the 81,910 of `rmdup -s -P`).
