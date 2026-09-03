## 1. Upstream dependency
- [x] 1.1 Land `refactor-pgen-companion-memory-model` in datafusion-bio-formats and cut a release tag.
- [x] 1.2 Bump every `datafusion-bio-format-*` tag in `Cargo.toml` and regenerate `Cargo.lock`.

## 2. Options plumbing
- [x] 2.1 Add `max_companion_bytes`, `max_decompressed_companion_bytes`, `max_variants` (`Option<usize>`) to `PgenReadOptions` in `src/option.rs` (fields, constructor signature, `default()`).
- [x] 2.2 Forward them in `native_pgen_options` (`src/scan.rs`) with `unwrap_or(defaults.x)`.
- [x] 2.3 Replace `PyPgenMatrixReader::positions` with `positions_into(destination: int64 NumPy array)` and have `read_pgen_matrix` allocate the array; drop the list round trip.

## 3. Python entry points
- [x] 3.1 Add the three keyword arguments, documented in the same style as `max_range_bytes`, to `read_pgen`, `scan_pgen`, `read_pgen_matrix`, `describe_pgen` in `polars_bio/io.py` and `register_pgen` in `polars_bio/sql.py`.
- [x] 3.2 Document the caps and the panel-scale memory expectation in `docs/features/reading.md`; add a CHANGELOG entry referencing #453.

## 4. Tests
- [x] 4.1 Forwarding test through `_captured_pgen_options` for each entry point.
- [x] 4.2 A cap lowered below the oracle fixture raises an error naming the argument; a raised cap leaves content identical to the default read.
- [x] 4.3a Positions test: `PgenMatrix.positions` is `int64`, one per row, equal to the scan's `start` column, and the reader exposes no list-returning positions method.
- [x] 4.3 Full `tests/test_pgen_io.py` green; `cargo clippy` and `cargo fmt` clean.

## 5. Real-panel verification
- [x] 5.1 `describe_pgen`, metadata scan, and `read_pgen_matrix(field="ALT_COUNT")` on a region of `pgsc_1000G_v1/GRCh38_1000G_ALL.pgen`; record open time and peak RSS.
- [x] 5.2 ALT-count parity with `pgenlib` on that region; GRCh37 companions open as well.
- [ ] 5.3 Report the numbers on #453 and in the PR.
