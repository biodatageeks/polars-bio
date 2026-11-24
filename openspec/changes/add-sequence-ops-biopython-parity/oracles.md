# Oracle definitions

The rules this change takes from other tools. The Biopython sections were
established on 2026-11-23 by probing Biopython 1.88 with handcrafted inputs.
The sections marked **to probe** are filled in by the oracle task of the phase
that needs them; nothing in them is assumed yet.

## Tool versions

| Tool | Version | Status |
|---|---|---|
| Biopython | 1.88 | probed 2026-11-23 |
| seqkit | 2.14.0 (GitHub release binary) | probed 2026-11-24 (`rmdup -s`) |
| vsearch | 2.32.0 (GitHub release binary) | probed 2026-11-24 |
| Trimmomatic | 0.39 (0.41 needs Java 25; its trimmer sources read the same) | probed 2026-11-24 |
| ORF finder | Biopython tutorial recipe (`Seq.translate` + split on `*`) | 2026-11-24, see below |

Biopython, seqkit and the other versions already pinned in
`add-sequence-operations/oracles.md` stay the same.

## Translation: `Seq.translate(cds=True)`

Signature in 1.88: `translate(table="Standard", stop_symbol="*",
to_stop=False, cds=False, gap="-")`.

| Input | Table | Result |
|---|---|---|
| `ATGGCCTAA` | 1 | `MA` (the final stop is not emitted) |
| `GTGGCCTAA`, `TTGGCCTAA` | 11 | `MA` (alternative start → `M`) |
| `CTGGCCTAA` | 1 | `MA` (table 1 starts are ATG, CTG, TTG) |
| `ATTGCCTAA` | 2 | `MA` |
| `GTGGCCTAA` | 1 | error: `First codon 'GTG' is not a start codon` |
| `ATGGCCTA` | 1 | error: `Sequence length 8 is not a multiple of three` |
| `ATGGCCGCC` | 1 | error: `Final codon 'GCC' is not a stop codon` |
| `ATGTAAGCCTAA`, `ATGGCCTAATAA` | 1 | error: `Extra in frame stop codon 'TAA' found.` |
| `ATGTAA` | 1 | `M` |
| `ATG` | 1 | error: `Final codon 'ATG' is not a stop codon` |
| empty string | 1 | error: `First codon '' is not a start codon` |
| `atggcctaa`, `AUGGCCUAA` | 1 | `MA` (lowercase and RNA accepted) |

Rules derived from the probes:
- **Check order:** start codon, then length, then final stop, then in-frame
  stops. The first failing check is the one reported.
- **Ambiguous first codon:** valid only if every expansion is a start codon.
  `RTGGCCTAA` and `NTGGCCTAA` are valid in table 11.
- **Ambiguous final codon:** valid only if every expansion is a stop.
  `ATGGCCTAR` is valid; `ATGGCCTAN` is an error.
- **Interior codons** translate as usual:
  - `ATGRATTAA` → `MB`, `ATGNNNTAA` → `MX`, `ATG---TAA` → `M-`.
  - `ATGTANTAA` → `MX`: a codon that is only possibly a stop is not an error.
- **Tables with dual-meaning codons (27, 28, 31):**
  - `ATGGCCTGA`, table 27 → `MA`. `TGA` is accepted as the final stop.
  - `ATGTGAGCCTGA`, table 27 → `MWA`. An interior `TGA` is the amino acid, not
    an extra stop.
- `cds=True` with `to_stop=True` or a custom `stop_symbol` gives the same
  result as `cds=True` alone.

Start and stop codon sets per table (Biopython
`CodonTable.unambiguous_dna_by_id`):

| Table | Start codons | Stop codons |
|---|---|---|
| 1 | ATG CTG TTG | TAA TAG TGA |
| 2 | ATA ATC ATG ATT GTG | AGA AGG TAA TAG |
| 3 | ATA ATG GTG | TAA TAG |
| 4 | ATA ATC ATG ATT CTG GTG TTA TTG | TAA TAG |
| 5 | ATA ATC ATG ATT GTG TTG | TAA TAG |
| 6 | ATG | TGA |
| 9 | ATG GTG | TAA TAG |
| 10 | ATG | TAA TAG |
| 11 | ATA ATC ATG ATT CTG GTG TTG | TAA TAG TGA |
| 12 | ATG CTG | TAA TAG TGA |
| 13 | ATA ATG GTG TTG | TAA TAG |
| 14 | ATG | TAG |
| 15 | ATG | TAA TGA |
| 16 | ATG | TAA TGA |
| 21 | ATG GTG | TAA TAG |
| 22 | ATG | TAA TCA TGA |
| 23 | ATG ATT GTG | TAA TAG TGA TTA |
| 24 | ATG CTG GTG TTG | TAA TAG |
| 25 | ATG GTG TTG | TAA TAG |
| 26 | ATG CTG | TAA TAG TGA |
| 27 | ATG | TGA |
| 28 | ATG | TAA TAG TGA |
| 29 | ATG | TGA |
| 30 | ATG | TGA |
| 31 | ATG | TAA TAG |
| 32 | ATA ATC ATG ATT CTG GTG TTG | TAA TGA |
| 33 | ATG CTG GTG TTG | TAG |

The kernel tables are generated from Biopython, not copied from this page.

## Translation: `stop_symbol`

| Input | `stop_symbol` | Result |
|---|---|---|
| `ATGTAAGCCTGA` | `#` | `M#A#` |
| `ATGTAAGCCTGA` | empty string | `MA` |
| `ATGTAAGCCTGA` | `**` | `M**A**` |
| `ATGTARGCC` | `#` | `M#A` (an all-stop ambiguous codon is a stop) |

Any string is accepted, including the empty string and more than one
character.

## Translation: ambiguity letters

| Input | Table | Biopython | Kernel today |
|---|---|---|---|
| `RAT SAR MTT` | 1 | `BZJ` | `XXX` |
| `RAY SAG MTY` | 1 | `BZJ` | `XXX` |
| `RAT SAR MTT` | 2 | `BZJ` | `XXX` |
| `ratsarmtt` | 1 | `BZJ` | `XXX` |
| `YTA`, `ATH` | 1 | `L`, `I` | same |
| `WTT`, `HTT` | 1 | `X`, `X` | same |
| `TAN`, `NNN` | 1 | `X`, `X` | same |
| `TRA` | 1 | `*` | same |

Rule: for an ambiguous codon whose expansions do not all agree, Biopython
returns `B` when the amino acids are exactly {D, N}, `Z` for exactly {E, Q},
`J` for exactly {I, L}, and `X` otherwise. A mix of a stop and an amino acid
is `X`. The golden corpus checks this across all 27 tables.

Unchanged differences, recorded in `add-sequence-operations/oracles.md`:
Biopython raises on a codon with a partial gap, an unknown byte or dots
(`...`); the kernel returns `X`, `X` and `-`. Biopython refuses
`to_stop=True` for tables 27, 28 and 31.

## Transcription

| Input | `transcribe()` | `back_transcribe()` |
|---|---|---|
| `ACGTacgtN-U` | `ACGUacguN-U` | `ACGTacgtN-T` |
| `ACGUacgu` | `ACGUacgu` | `ACGTacgt` |

`transcribe` replaces `T`→`U` and `t`→`u`; `back_transcribe` replaces `U`→`T`
and `u`→`t`. Every other byte is unchanged.

## Motif search: `Bio.SeqUtils.nt_search`

| Sequence | Pattern | Result |
|---|---|---|
| `ACGTACGTAAAA` | `ACG` | `['ACG', 0, 4]` |
| `AAAAA` | `AA` | `['AA', 0, 1, 2, 3]` (overlapping matches) |
| `ACGTNACGT` | `N` | `['[GATC]', 0, 1, 2, 3, 5, 6, 7, 8]` |
| `ACGTNACGT` | `ACGTN` | `['ACGT[GATC]']` (no match) |
| `ACGTRYACGT` | `R` | `['[AG]', 0, 2, 6, 8]` |
| `acgtacgt` | `ACG` | `['ACG']` (no match) |
| `ACGUACGU` | `ACGT` | `['ACGT']` (no match) |
| `ACGT` | `acg` | `KeyError: 'a'` |
| `ACGT` | empty string | never returns |

Rules derived from the probes:
- Positions are 0-based starts on the given strand. Matches may overlap.
- Ambiguity codes are expanded in the **pattern only**. An ambiguous base in
  the sequence matches nothing: sequence `N` does not match pattern `N`.
- Matching is case-sensitive, and `U` in the sequence does not match `T`.
- An empty pattern loops forever in Biopython 1.88 (the probe had to be
  killed).

## Molecular weight: `Bio.SeqUtils.molecular_weight`

Signature in 1.88: `molecular_weight(seq, seq_type="DNA",
double_stranded=False, circular=False, monoisotopic=False)`.

| Input | Options | Result |
|---|---|---|
| `ACGT` | default | 1253.8027 |
| `ACGT` | `double_stranded=True` | 2507.6054 |
| `ACGT` | `circular=True` | 1235.7874 |
| `ACGT` | `monoisotopic=True` | 1253.213097 |
| `ACGU` | `seq_type="RNA"` | 1303.7737 |
| `MKV` | `seq_type="protein"` | 376.51460000000003 |
| `acgt` | default | 1253.8027 (case-insensitive) |
| empty string | default | 18.0153 (one water) |
| `ACGN` | default | `ValueError`: `'N'` is not a valid unambiguous letter for DNA |
| `MKV*`, `MKX` | `seq_type="protein"` | `ValueError` |

## GC skew: `Bio.SeqUtils.GC_skew`

Signature in 1.88: `GC_skew(seq, window=100)`.

| Input | Window | Result |
|---|---|---|
| `GGGGCCCCGGGCAAAA` | 4 | `[1.0, -1.0, 0.5, 0.0]` |
| `GGCC` | 100 | `[0.0]` |
| `ggcc` | 2 | `[1.0, -1.0]` (case-insensitive) |
| `AAAAGG` | 4 | `[0.0, 1.0]` (the last window is partial) |
| `GGNNSS` | 3 | `[1.0, 0.0]` (S is not counted) |
| empty string | 4 | `[]` |

Rules: windows do not overlap; the last, shorter window is included; a window
with no G or C gives `0.0`.

## Expected errors (vsearch 2.32.0)

`vsearch --fastq_filter in.fq --fastq_maxee 1.0 --fastq_qmax 93`:
- EE is `sum(10^(-Q/10))`; the filter keeps a read when **EE ≤ maxee**: a
  single `!` base (EE exactly 1.0) is kept, `!!` (EE 2.0) is dropped.
- `N` bases are not special (`--fastq_maxns` is unset by default).
- `--fastq_qmax` must be raised above its default of 41 for Illumina 1.8+
  files with higher scores; it does not change EE.
- On DRR609229_1 (719,828 reads) vsearch keeps 334,366 reads; polars-bio's
  `filter(seq.expected_errors() <= 1.0)` keeps the same reads in the same
  order (checked 2026-11-24).

## ORF finding

There is no ORF finder in Biopython. The oracle is the recipe from the
Biopython tutorial (translate each of the six frames, split on `*`, take
each segment from its first `M`), restricted to complete ORFs with ATG
starts in tables where only ATG encodes M (1, 4, 11). `seq.orfs(start_codons=
"ATG")` returns exactly those ORFs on 60 generated sequences per table and on
the first 80,000 Ensembl GRCh38 CDSs. The recipe has no notion of alternative
start codons or partial ORFs; those are covered by the kernel's property
tests (an ORF with a start and a stop is a valid CDS; partial output contains
the complete output).

orfipy and EMBOSS `getorf` were not used: `getorf` reports ORFs between stops
by default (not from a start codon), and orfipy is not on the machine's
package channels without conda; both remain possible extra oracles.

## Sequence deduplication (seqkit 2.14.0)

`seqkit rmdup -s` on `r1 ACGTTT, r2 AAACGT, r3 ACGTTT, r4 GGGG, r5 acgttt`:
- `rmdup -s` keeps `r1 r4 r5`: a sequence and its reverse complement are
  duplicates **by default** (r2 is the reverse complement of r1), and case
  matters (r5 is kept).
- `rmdup -s -P` (positive strand only) keeps `r1 r2 r4 r5`.
- `rmdup -s -i` (ignore case) keeps `r1 r4`.
- The first record of a group is kept.

On DRR609229_1, `rmdup -s` keeps 77,751 reads and `rmdup -s -P` 81,910;
polars-bio keeps the same reads in both modes. So
`unique(subset="h", keep="first", maintain_order=True)` reproduces
`rmdup -s` with `h = seq.hash(canonical=True)` and `rmdup -s -P` with
`h = seq.hash()`. For `-i`, upper-case the sequence first.

## Quality trimming (Trimmomatic 0.39)

From the 0.39 jar (`SE -phred33`) and the trimmer sources on `main`:
- `LEADING:q` keeps from the first base with quality **≥ q**; `TRAILING:q`
  keeps up to the last such base, but never examines the first base, so a
  read whose only passing base is its first is dropped.
- `SLIDINGWINDOW:w:q` drops a read shorter than `w` or whose first window
  averages below `q` (the test is `sum < q·w` in 32-bit float). Otherwise it
  cuts at the end of the first failing window, then removes bases below `q`
  from the new end, keeping at least one base.
- An uppercase `N` base counts as quality 0 whatever its quality character;
  a lowercase `n` keeps its quality.
- Steps run in command-line order; a read trimmed to nothing is dropped.
- The golden file covers 9 step combinations on 300 reads (1,615 kept reads).
