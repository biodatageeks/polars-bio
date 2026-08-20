# Tasks

## 1. Provider
- [x] 1.1 Add `matrix::read_genotype_matrix` to the BGEN provider, decoding each
      variant at its final address (datafusion-bio-formats#237)
- [x] 1.2 Make the `genotypes` struct children projectable
      (datafusion-bio-formats#236)
- [x] 1.3 Label matrix rows with the coordinate-adjusted `start`

## 2. Binding
- [x] 2.1 `OpenBgenMatrix` and the `BgenMatrixReader` pyclass
- [x] 2.2 `BgenMatrixReader.read_into` taking the destination array itself and
      validating its type, dtype, C-contiguity, writability, alignment and
      length in Rust, then decoding with the GIL held so no Python thread can
      resize the array out from under it
- [x] 2.3 `genotype_fields` on `scan_bgen`, `read_bgen`, and `register_bgen`
- [x] 2.4 Export from `polars_bio`

## 3. Tests
- [x] 3.1 Matrix against the DataFrame path, compared as bit patterns
- [x] 3.2 Thread counts produce identical values
- [x] 3.3 Sample selection reorders columns
- [x] 3.4 The missing-call sentinel, against a fixture carrying `./.` calls,
      compared with the nulls the DataFrame path reports
- [x] 3.5 `genotype_fields` selection, ordering, and rejection cases
- [x] 3.6 Positions under a zero-based read, compared against the scan under both
      systems — the gap that let a row-mislabelling bug through provider review

## 4. Documentation
- [x] 4.1 API docstrings covering the dosage-only limit and the PLOIDY cost
- [x] 4.2 Benchmark writeup and blog post re-measured
