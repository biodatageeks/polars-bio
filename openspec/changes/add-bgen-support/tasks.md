## 1. API and execution

- [x] 1.1 Add `BgenReadOptions` and an `InputFormat::Bgen` registration arm.
- [x] 1.2 Add `read_bgen`, `scan_bgen`, `describe_bgen`, and `register_bgen`.
- [x] 1.3 Validate the input path suffix, the genotype output mode, and the
  probability layout.
- [x] 1.5 Pass the probability layout to the provider and keep it inert for
  dosage output.
- [x] 1.4 Return registration errors rather than panicking in the extension.

## 2. Metadata

- [x] 2.1 Extract the `bio.bgen.*` schema metadata and the emitted sample order.
- [x] 2.2 Report the genotype representation from the emitted struct field.

## 3. Validation

- [x] 3.1 Test variant metadata, both genotype outputs, sample selection and
  reordering, projection, and predicate pushdown.
- [x] 3.2 Test that content is independent of `target_partitions`.
- [x] 3.6 Test that the fixed probability layout declares its width, returns the
  same probabilities as the nested layout, and rejects an unknown layout name.
- [x] 3.3 Test that `describe_bgen` neither replaces nor leaves behind a table.
- [x] 3.4 Verify output against the independent `bgen` package element by
  element, with no tolerance, at every partition count. The comparison needs
  cohort-scale fixtures and the `bgen`/`snputils` readers, so it lives in
  `biodatageeks/bioformats-benchmark` (`benchmarks/bgen_verify.py`), not in this
  repository's test suite. Evidence: `BGEN_BENCHMARK.md`, "Zero mismatches".
- [x] 3.5 Benchmark against snputils and record the workloads where polars-bio
  is slower. Also in `biodatageeks/bioformats-benchmark`
  (`run_bgen_benchmarks.py`); the probability workload is recorded as slower.

## 4. Documentation

- [x] 4.1 Document the BGEN APIs, options, the probability layouts, and the
  row-order caveat.
- [x] 4.2 Add BGEN to the feature matrix and the API reference.
