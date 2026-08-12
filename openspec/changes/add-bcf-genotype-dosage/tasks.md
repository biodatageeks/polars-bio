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

## 3. Documentation

- [x] 3.1 Document BCF auto-detection, lazy usage, CSI indexing, parallelism,
  typed dosage constraints, schema compatibility, and input-only status.
