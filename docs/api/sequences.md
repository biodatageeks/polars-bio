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
            - complement
            - gc_content
            - translate
            - mean_quality

## SQL functions

The same kernels are available in [`pb.sql`](sql.md) as scalar functions.
Optional arguments are positional constants.

| Function | Optional arguments | Returns |
|---|---|---|
| `seq_revcomp(seq)` | `alphabet = 'dna'`, `strict = false` | the input's string type |
| `seq_complement(seq)` | `alphabet = 'dna'`, `strict = false` | the input's string type |
| `seq_gc_content(seq)` | `denominator = 'length'` | `DOUBLE` |
| `seq_translate(seq)` | `frame = 1`, `table = 1`, `to_stop = false` | the input's string type |
| `seq_mean_quality(qual)` | `offset = 33`, `method = 'error_prob'` | `DOUBLE` |
