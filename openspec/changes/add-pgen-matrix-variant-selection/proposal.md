# Change: Variant selection for `read_pgen_matrix`

## Why

`read_pgen_matrix` decodes every variant in the fileset. On the PGS Catalog
1000 Genomes panel (75.2M variants, 3,202 samples) the dense output is
224 GiB for `ALT_COUNT` and 896 GiB for `DS`, so after the companion cap is
lifted (`refactor-pgen-companion-memory-model`, #453) the call still cannot
complete. Scoring needs the rows that match a score file or a region, and
callers who want to stream need a way to take the matrix in row windows.

## What Changes

- `read_pgen_matrix` gains `region="chrom:start-end"` (or `chrom` alone) and
  `rows=` (a `slice`/`range` or a sorted integer array of PVAR row indices).
  At most one of the two may be given. Shape, `positions`, and
  `sample_names` follow the selection.
- Row windows are the caller's loop over `rows=range(...)`; documented in
  `reading.md` with a chunked-scoring example.
- Mirrors the upstream change of the same id, which adds the selection to the
  matrix reader and a region-to-rows lookup on the variant table.

## Impact

- Affected specs: `pgen` (Dense PGEN Genotype Matrix gains a selection
  clause).
- Affected code: `polars_bio/io.py` (`read_pgen_matrix`), `src/lib.rs`
  (`PyPgenMatrixReader`), `src/scan.rs` (`OpenPgenMatrix`),
  `docs/features/reading.md`, `tests/test_pgen_io.py`.
- Depends on the upstream release carrying `add-pgen-matrix-variant-selection`.
