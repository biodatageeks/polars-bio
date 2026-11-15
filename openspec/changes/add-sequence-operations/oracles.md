# Oracle definitions (task 2.1)

These are the exact definitions used by the tools that serve as parity oracles
and benchmark competitors. They were established on 2026-11-14 by probing each
tool with handcrafted inputs where competing definitions give different
answers, and by reading the seqkit source where the output alone was ambiguous.

## Tool versions

| Tool | Version | Source |
|---|---|---|
| seqkit | 2.14.0 (`shenwei356/bio` v0.15.2) | bioconda |
| seqfu | 1.30.3 | bioconda |
| seqtk | 1.5-r133 | bioconda |
| samtools | 1.25 | bioconda |
| bedtools | 2.31.1 | bioconda |
| Biopython | 1.88 | conda-forge |
| pyfaidx | 0.9.2.2 | bioconda |
| pyfastx | 2.3.1 | bioconda |

## GC content

| Tool / command | Numerator | Denominator | S counted | Lowercase |
|---|---|---|---|---|
| `seqkit fx2tab -g` (per sequence) | G+C | A+C+G+T only (N, gaps and other IUPAC codes excluded) | no | yes |
| `seqkit fx2tab -B GC` (per sequence) | G+C | full length | no | yes |
| `seqkit stats -a` `GC(%)` (per file) | G+C | `sum_len` (full length, gaps included) | no | yes |
| `seqfu bases` `%GC` (per file only) | G+C | total length | no | yes |
| `seqfu stats --gc` (per file) | G+C | total length; printed as a fraction (`0.09`), not a percentage | no | yes |
| `seqtk comp` (per sequence) | reports counts only: A, C, G, T, 2-fold (`#2`: M R W S Y K), 3-fold, N, … | — | S lands in `#2` | yes |

Probe results (`seqkit fx2tab -g` / `-B GC`): `GGNN` 100 / 50, `GG--` 100 / 50,
`GGRR` 100 / 50, `SSAA` 0 / 0, `ggaa` 50 / 50.

**Consequence for D5.** No tool counts S, so the numerator is G+C only. There
are two denominators in use, so `gc_content` takes
`denominator="length"` (default; matches seqkit `-B GC`, seqkit `stats`,
seqfu `bases`/`stats`, and seqtk `comp` with (C+G)/length) or
`denominator="acgt"` (matches `seqkit fx2tab -g`). This replaces the
`ignore_n` flag, which matched no tool.

## Average quality

- `seqkit fx2tab -q` (per read) and `seqkit stats -a` `AvgQual` (per file) use
  the **error-probability** method:
  `-10·log10(mean(10^(-Q/10)))`. The `stats` help text states this is
  deliberate (seqkit issue #448). The per-file value averages over every base
  in the file (`errSum / sum_len`); it is `0` when there are no qualities.
- Probes: `I#` (Q40, Q2) → 5.01 (the arithmetic mean would be 21); `5I`
  (Q20, Q40) → 22.97; `????` → 30.00.
- `seqfu qual` is not comparable: it samples the first 5,000 reads (`-m`) and
  reports an encoding guess and a per-position profile.
- FastQC's per-sequence quality module uses the arithmetic mean.

**Consequence for D5.** `method="error_prob"` is the seqkit parity oracle;
`method="arithmetic"` is checked against a Python reference.

## Q20 / Q30

- seqkit counts bases with quality **≥** 20 / 30 (the code uses `>=`,
  although the help text says "greater than").
- The denominator is `sum_len`, gaps included.
- The value is rounded to 2 decimals and then printed with `%.0f` in `-T`
  output (probe: 7/8 bases → `88`).

## Length statistics (`seqkit stats -a`)

- **Lengths** count every byte of the sequence, including gaps, spaces and
  `*`.
- **`sum_gap`** counts the gap letters `-`, `.` and space (default of
  `-G/--gap-letters`). N is not a gap.
- **`sum_n`** counts `N`/`n` for nucleotide input and `X`/`x` for protein
  input (alphabet detected by seqkit).
- **`avg_len`** is printed with `%.1f`.
- **Quartiles** (`shenwei356/bio` `LengthStats`, a length histogram):
  - Q2 is the median, taking the mean of the two middle values when the count
    is even.
  - With `n` values, each half holds `h = n/2` values (n even) or
    `h = (n+1)/2` values (n odd; both halves include the middle value).
  - Q1 is the median of the first `h` sorted values and Q3 the median of the
    last `h`.
  - In `-T` output Q1–Q3 are printed with Go's `%.0f`, which rounds half to
    even. For lengths 1, 2, 3, 4 the values are 1.5, 2.5, 3.5, printed
    `2 2 4`. Parity tests compare our exact value after the same rounding.
  - A file with a single distinct length returns that length for all three.
- **N50 / L50:** walk the lengths from longest to shortest, accumulating
  bases; N50 is the first length at which the running total reaches
  `sum_len / 2` (float, `>=`). L50 (`N50_num`) counts **records**, not
  distinct lengths, using `ceil` for the partially needed group. seqfu agrees
  on N50 and L50 in every probe.
- **auN** (seqfu only): `sum(L²) / sum(L)`. seqfu notes that versions before
  v1.22.1 computed it incorrectly.

## Reverse complement

| Input | seqkit `seq -r -p` | seqfu `rc` | seqtk `seq -r` | Biopython `reverse_complement` |
|---|---|---|---|---|
| `ACGTRYSWKMBDHVN` | `NBDHVKMWSRYACGT` | same | same | same |
| `acgtrysw` | `wsryacgt` | `WSRYACGT` (uppercases) | `wsryacgt` | `wsryacgt` |
| `AC-G.T` | `A.C-GT` | same | same | same |
| `ACGU` | `UCGT` without `-t`; with `-t rna`, `ACGU` | `ACGT` | `ACGT` | `ACGT`; `reverse_complement_rna` gives `ACGU` |
| `ACGZ` (invalid) | `ZGCA`: no complement at all, only reversed, with a warning | `ZCGT` (passed through) | `ZCGT` | `ZCGT` |

The IUPAC complement table in D5 matches every tool. `alphabet="dna"` matches
seqfu, seqtk and Biopython; `alphabet="rna"` matches Biopython's
`reverse_complement_rna` and `seqkit -t rna`. No tool raises an error on an
invalid byte.

## Translation

seqkit `translate` and Biopython `Seq.translate` agree on every probe:

| Input | Result | Rule |
|---|---|---|
| `ATGGCCTAAGC` | `MA*` | trailing partial codon dropped |
| `GCNAARTARNNNAAN` | `AK*XX` | ambiguous codon → the amino acid if every expansion agrees (`GCN`→A, `AAR`→K, `TAR`→`*`), else `X` |
| `atggcc` | `MA` | lowercase accepted |
| `AGATGA`, table 2 | `*W` | NCBI tables |
| `ATG---GCC` | `M-A` | a codon of three gaps → `-` |
| `ATGGCCTAAGC`, frame −1 | `A*A` | negative frames translate the reverse complement |
| `ATGGCCTAAGC`, to_stop | `MA` | truncate before the first stop |

**Consequence for D5.** Add the gap-codon rule (`---` → `-`).

The golden corpus in `datafusion-bio-function-seq` (40 random coding
sequences, 25 tables, every frame) found where the two tools part ways:

| Input | Biopython | seqkit | Kernel |
|---|---|---|---|
| Ambiguous codon covering {D,N}, {E,Q} or {I,L}: `RAT`, `SAR`, `MTT` | `B`, `Z`, `J` | `X` | `X` |
| Codon with a partial gap, `A-G`, `AT-` | error | `X` | `X` |
| Codon with an unknown byte, `ZZZ` | error | error (whole sequence) | `X` |
| Codon of dots, `...` | error | error | `-` (both `-` and `.` are gaps) |
| `U` mixed with `T`, `ATGUUU` | `MF` | error | `MF` |
| Tables 15, 32, 33 | supported | not supported | supported |
| Tables 27, 28, 31: codons that are both a stop and an amino acid | amino acid; `to_stop` refused | amino acid | amino acid; never a stop |
| Ambiguity codes in negative frames, `RTT` at frame −1 | `N` | `K` | `N` |

The last row is a seqkit 2.14.0 bug: `seqkit translate` reverses but does not
complement IUPAC ambiguity codes in negative frames, so `RTT` is read as `AAR`
instead of `AAY`. `seqkit seq -r -p` complements them correctly. Golden
tests compare negative frames with Biopython only; positive frames match both
tools on every corpus row.

GC: seqkit prints `NaN` for `fx2tab -g` on a sequence with no A, C, G or T;
the kernel returns null.

## Region extraction

`samtools faidx -r`, `bedtools getfasta` and pyfaidx are probed in phase 5,
when the indexed reader exists. No assumptions are recorded here yet.
