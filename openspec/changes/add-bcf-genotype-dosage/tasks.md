## 1. API and execution

- [x] 1.1 Add and validate the explicit Python/Rust genotype output option.
- [x] 1.2 Pass dosage mode to the upstream BCF table provider.
- [x] 1.3 Preserve lazy scan and default string compatibility.

## 2. Validation

- [x] 2.1 Test typed schema, dosage values, missingness, and invalid options.
- [x] 2.2 Verify all normalized rows and cells against snputils.
- [x] 2.3 Run release/native one-thread timing and peak-RSS repetitions until the
  polars-bio median is lower than snputils.
- [x] 2.4 Convert the reusable VCF test corpus to BCF and assert exact rows,
  columns, data types, eager/lazy output, and schema descriptions.
- [x] 2.5 Test CSI range pushdown, 1/2/4/8 input partitions, unindexed fallback,
  projection, SQL registration, coordinates, samples, INFO, and FORMAT parity.

Benchmark evidence for 2.2–2.3 is recorded in the immutable companion
[BCF report](https://github.com/biodatageeks/bioformats-benchmark/blob/06d5ffe75172a7a9687ae7f102385b29214f15af/BCF_BENCHMARK.md).
It benchmarks this repository at `dc4101fb5ba9231d4d1ef57df16651db20c84e0e`
against datafusion-bio-formats `36dad82c09f7291c0147d8560117d8c242806f9b`:
all 993,881 rows and 2,532,408,788 dosage cells matched, and the three-run
single-thread medians were 4.837 s / 2,640.5 MB peak RSS for polars-bio versus
8.190 s / 10,068.2 MB for pinned snputils. The report contains raw rounds,
hashes, build flags, environment metadata, and reproduction commands.

## 3. Documentation

- [x] 3.1 Document BCF auto-detection, lazy usage, CSI indexing, parallelism,
  typed dosage constraints, schema compatibility, and input-only status.
