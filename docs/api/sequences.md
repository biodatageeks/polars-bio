# Sequence operations

Importing `polars_bio` registers a `seq` namespace on every Polars expression.
See [Sequence operations](../features/sequences.md) for usage and the reference
tools each function matches.

::: polars_bio.seq.SequenceExpr
    handler: python
    options:
        show_root_heading: false
        show_root_toc_entry: false
        heading_level: 2
        docstring_section_style: table
        members:
            - revcomp
            - reverse
            - complement
            - gc_content
            - translate
            - cds_status
            - transcribe
            - back_transcribe
            - mean_quality
            - expected_errors
            - min_quality
            - max_quality
            - orfs
            - find_motif
            - gc_content_windows
            - gc_skew
            - molecular_weight
            - hash
            - trim_quality

## ORF table

::: polars_bio.orfs
    handler: python
    options:
        show_root_heading: true
        heading_level: 3
        docstring_section_style: table

## SQL functions

The same kernels are available in [`pb.sql`](sql.md) as scalar functions.
Optional arguments are positional constants.

| Function | Optional arguments | Returns |
|---|---|---|
| `seq_revcomp(seq)` | `alphabet = 'dna'`, `strict = false` | the input's string type |
| `seq_reverse(seq)` | — | the input's string type |
| `seq_complement(seq)` | `alphabet = 'dna'`, `strict = false` | the input's string type |
| `seq_gc_content(seq)` | `denominator = 'length'` | `DOUBLE` |
| `seq_translate(seq)` | `frame = 1`, `table = 1`, `to_stop = false`, `stop_symbol = '*'`, `cds = false`, `ambiguous = 'x'` | the input's string type |
| `seq_cds_status(seq)` | `frame = 1`, `table = 1` | `VARCHAR` |
| `seq_transcribe(seq)`, `seq_back_transcribe(seq)` | — | the input's string type |
| `seq_mean_quality(qual)` | `offset = 33`, `method = 'error_prob'` | `DOUBLE` |
| `seq_expected_errors(qual)` | `offset = 33` | `DOUBLE` |
| `seq_min_quality(qual)`, `seq_max_quality(qual)` | `offset = 33` | `INT` |
| `seq_orfs(seq)` | `table = 1`, `min_len = 30`, `strands = 'both'`, `start_codons = 'table'`, `allow_partial = false`, `include_protein = false`, `zero_based = false` | list of structs |
| `seq_find_motif(seq, pattern)` | `both_strands = false`, `overlapping = true`, `zero_based = false` | list of `BIGINT` |
| `seq_gc_content_windows(seq, window)` | `step = NULL` (= window), `denominator = 'length'` | list of `DOUBLE` |
| `seq_gc_skew(seq)` | `window = 100`, `step = NULL` (= window) | list of `DOUBLE` |
| `seq_molecular_weight(seq)` | `seq_type = 'DNA'`, `double_stranded = false`, `circular = false`, `monoisotopic = false` | `DOUBLE` |
| `seq_hash(seq)` | `canonical = false` | `BIGINT UNSIGNED` |
| `seq_trim_quality(seq, qual)` | `leading`, `trailing`, `window_size`, `window_quality` (each `NULL` = not used), `offset = 33` | struct `{sequence, quality_scores}` |

`seq_orfs` and `seq_find_motif` do not read the session coordinate-system
option: they report 1-based closed coordinates unless `zero_based` is `true`.
