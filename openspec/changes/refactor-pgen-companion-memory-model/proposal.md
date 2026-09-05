# Change: Open production PLINK 2 filesets by fixing the PGEN companion memory model

## Why

`describe_pgen`, `scan_pgen`, `read_pgen`, and `read_pgen_matrix` refuse the
published PGS Catalog 1000 Genomes panel (`pgsc_1000G_v1`, #453): its
`.pvar.zst` is 541–592 MiB against a 512 MiB provider cap, decodes to
2.3–2.5 GiB against a 1 GiB cap, holds 75.2M (GRCh38) and 84.8M (GRCh37)
variants against a 100M cap, and no polars-bio entry point exposes any of
those options. Raising the caps alone is not a fix: the provider holds the compressed
bytes, the decoded text, and six heap objects per variant at once, which
measures at ~600 B per variant on a chr22 slice of the same panel, or ~45 GB
to describe and ~85 GB to scan the full panel.

## What Changes

- Upstream (datafusion-bio-formats, mirrored change of the same id): stream
  the companion decode in bounded blocks, store variants in a columnar table
  at a pinned per-variant cost, raise the companion and variant-count defaults so
  the standard panels open untuned, and name the option in every limit error.
- polars-bio: expose `max_companion_bytes`, `max_decompressed_companion_bytes`,
  and `max_variants` on `PgenReadOptions` and on `read_pgen`, `scan_pgen`,
  `read_pgen_matrix`, `describe_pgen`, and `register_pgen`, forwarded like the
  existing range controls and defaulting to the provider values.
- Return `read_pgen_matrix` row positions as a NumPy array built in Rust
  instead of a Python list of integers; on the 75M-variant panel the list
  costs ~2.7 GB before NumPy sees it.
- Pin every `datafusion-bio-format-*` crate to the exact upstream revision
  carrying the change until a release is available; the release-tag transition
  remains open in polars-bio #457. Document the caps in
  `docs/features/reading.md` and the changelog.
- Verify on the real `GRCh38_1000G_ALL` trio: open time, peak RSS, and ALT
  count parity with `pgenlib` on a region.

## Impact

- Affected specs: `pgen` (PGEN Companion Discovery, PGEN Read Coalescing
  Control gain companion-limit requirements).
- Affected code: `src/option.rs` (`PgenReadOptions`), `src/scan.rs`
  (`native_pgen_options`), `src/lib.rs` (`PyPgenMatrixReader::positions`), `polars_bio/io.py`, `polars_bio/sql.py`,
  `docs/features/reading.md`, `tests/test_pgen_io.py`, `Cargo.toml`,
  `CHANGELOG.md`.
- No schema or coordinate change. Existing calls behave the same except that
  filesets previously rejected by size now open.
