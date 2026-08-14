## 1. API and execution

- [x] 1.1 Add `BgenReadOptions` and an `InputFormat::Bgen` registration arm.
- [x] 1.2 Add `read_bgen`, `scan_bgen`, `describe_bgen`, and `register_bgen`.
- [x] 1.3 Validate the input path suffix and the genotype output mode.
- [x] 1.4 Return registration errors rather than panicking in the extension.

## 2. Metadata

- [x] 2.1 Extract the `bio.bgen.*` schema metadata and the emitted sample order.
- [x] 2.2 Report the genotype representation from the emitted struct field.

## 3. Validation

- [x] 3.1 Test variant metadata, both genotype outputs, sample selection and
  reordering, projection, and predicate pushdown.
- [x] 3.2 Test that content is independent of `target_partitions`.
- [x] 3.3 Test that `describe_bgen` neither replaces nor leaves behind a table.
- [x] 3.4 Verify output against the independent `bgen` package element by
  element, with no tolerance, at every partition count.
- [x] 3.5 Benchmark against snputils and record the workloads where polars-bio
  is slower.

## 4. Documentation

- [x] 4.1 Document the BGEN APIs, options, and the row-order caveat.
- [x] 4.2 Add BGEN to the feature matrix and the API reference.
