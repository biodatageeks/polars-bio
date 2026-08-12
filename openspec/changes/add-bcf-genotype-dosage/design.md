## Context

BCF stores FORMAT fields as contiguous typed series. The upstream provider can
now materialize biallelic GT dosage directly without genotype strings.

## Goals / Non-Goals

- Goals: explicit typed dosage, lazy execution, bounded Arrow batches, output
  equivalence with snputils, and lower one-thread median wall time.
- Non-goals: changing default VCF/BCF schema, collapsing multiallelic alleles,
  or materializing a whole-file dense matrix inside the reader.

## Decisions

- `genotype_output` accepts `"string"` (default) or `"dosage"`.
- Dosage requires BCF with only `GT` selected and counts allele index 1.
- Missing GT is null; phase does not affect the count; output is `Int8`.
- The benchmark consumes typed dosage directly and normalizes null to `-1` only
  for cross-tool equality checks.

## Performance Gate

Use fresh one-thread processes and a release build with
`RUSTFLAGS="-C target-cpu=native"`. Run at least three interleaved repetitions
on the same BCF and samples, compare all normalized cells and rows, and require
the polars-bio median to be lower than the pinned snputils median. Report peak
RSS for both.

## Benchmark Evidence

The immutable companion
[benchmark report](https://github.com/biodatageeks/bioformats-benchmark/blob/924ef37e3816681f82ce750d7ea9133fa819d2bb/BCF_BENCHMARK.md)
records the exact inputs, revisions, release/native build flags, complete-output
hashes, three raw rounds, wall-time medians, and peak RSS. At one thread,
polars-bio took 5.248 s with 2,658.7 MB peak RSS versus 8.513 s and 10,067.4 MB
for pinned snputils; all 2,532,408,788 normalized dosage cells matched.
