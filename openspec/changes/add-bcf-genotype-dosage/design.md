## Context

BCF stores FORMAT fields as contiguous typed series. The upstream provider can
now materialize biallelic GT dosage directly without genotype strings.

## Goals / Non-Goals

- Goals: dedicated VCF/BCF eager, lazy, schema-description, and SQL-registration
  entry points, explicit typed BCF dosage, lazy execution, bounded Arrow
  batches, output equivalence with snputils, and lower one-thread median wall
  time.
- Non-goals: changing default VCF/BCF schema, collapsing multiallelic alleles,
  or materializing a whole-file dense matrix inside the reader.

## Decisions

- `read_vcf` and `scan_vcf` accept text VCF only and expose neither
  `genotype_output` nor `genotype_encoding_raw`.
- `read_bcf` and `scan_bcf` accept BCF only; their `genotype_output` accepts
  `"string"` (default) or `"dosage"`.
- `describe_vcf` and `register_vcf` accept text VCF only; `describe_bcf` and
  `register_bcf` accept BCF only. Each pair shares its native implementation
  internally, and BCF SQL registration exposes string or dosage GT output.
- The VCF and BCF entry points continue to share the internal DataFusion table
  provider; the public method validates the physical input format before using
  that provider.
- `genotype_encoding_raw` remains on `read_vcf_zarr` and `scan_vcf_zarr`, where
  it is functional rather than a deprecated ignored compatibility argument.
- Dosage requires BCF with only `GT` selected and counts allele index 1.
- Missing GT is null; phase does not affect the count; output is `Int8`.
- The benchmark consumes typed dosage directly and normalizes null to `-1` only
  for cross-tool equality checks.

## Migration

- Replace `read_vcf("input.bcf", ...)` with `read_bcf("input.bcf", ...)`.
- Replace `scan_vcf("input.bcf", ...)` with `scan_bcf("input.bcf", ...)`.
- Replace `describe_vcf("input.bcf")` with `describe_bcf("input.bcf")`.
- Replace `register_vcf("input.bcf", ...)` with `register_bcf("input.bcf", ...)`.
- Remove `genotype_encoding_raw` from VCF/BCF calls. VCF Zarr calls are
  unchanged.
- Move BCF `genotype_output` requests to the corresponding BCF method. Text VCF
  continues to return its existing string genotype representation.

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
