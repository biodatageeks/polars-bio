## 1. Coordinate-aware readers

- [x] 1.1 Add eager and lazy CSV and Parquet readers with explicit source coordinate declarations.
- [x] 1.2 Normalize interval columns and support point data with `end=None`.
- [x] 1.3 Add optional coordinate conversion and preserve metadata through eager collection.
- [x] 1.4 Add `zero_based` and `use_zero_based` support to `scan_table` and `read_table`.
- [x] 1.5 Validate chromosome strings, integer coordinate widths, and shift overflow.

## 2. Safe range-operation resolution

- [x] 2.1 Resolve tagged frames, native VCF/BCF paths, and unknown inputs consistently.
- [x] 2.2 Reject mismatched tagged inputs and tagged/unknown pairs before executing range operations.
- [x] 2.3 Validate supported path formats before coordinate resolution and provide actionable errors.
- [x] 2.4 Document the coordinate resolution behavior and add regression coverage.

## 3. OpenSpec review

- [ ] 3.1 Review and approve this proposal before merging the implementation.
