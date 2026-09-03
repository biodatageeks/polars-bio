## 1. Binding
- [ ] 1.1 `PyPgenMatrixReader` accepts an optional selection (index range or `int64` NumPy array) and a `row_range(chrom, start, end)` lookup; `shape`, `positions_into`, and `read_into` follow it.

## 2. Python API
- [ ] 2.1 Add `region` and `rows` to `read_pgen_matrix`; parse `chrom:start-end` in the configured coordinate system; reject both at once and unsorted or out-of-range `rows`.
- [ ] 2.2 Document in `reading.md` with a chunked example; CHANGELOG entry.

## 3. Tests
- [ ] 3.1 `region` and `rows` selections equal the corresponding slice of the full matrix on the oracle fixtures; empty region gives a `(0, samples)` matrix.
- [ ] 3.2 Windowed loop over `rows=range(...)` reassembles the full matrix.
- [ ] 3.3 Real-panel check: a chr22 region of `GRCh38_1000G_ALL.pgen` matches `pgenlib` ALT counts.
