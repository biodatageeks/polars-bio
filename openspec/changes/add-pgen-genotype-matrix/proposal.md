# Change: Add a dense PGEN genotype matrix reader

## Why

Association testing, PCA and relatedness all consume the whole-cohort genotype
matrix, not a DataFrame. `read_pgen` can produce the values, but getting a dense
array out of it costs a second full copy of every genotype: the scan builds
Arrow record batches, and the caller must then consolidate them into one
contiguous array. On chromosome 22 of 1000 Genomes that copy moves 10 GB twice.

Worse, that copy does not parallelize. It saturates memory bandwidth at roughly
2.8x however many threads it is given, while the decoder behind it scales 5x, so
it became the ceiling on a multi-partition read — 1.85x on eight partitions
against the decoder's own 5.05x.

## What Changes

- Add `read_pgen_matrix`, returning a dense NumPy matrix rather than a
  DataFrame, with the variant positions and sample names that label its axes.
- Decode straight into the destination array. The provider is given the
  caller's buffer and writes genotypes at their final address, so the values are
  written once and no copy stage exists.
- Support the fields that have one value per sample: `ALT_COUNT` as `int8` and
  `DS` as `float32`. Reject the others with a message pointing at `read_pgen`.
- Write a caller-chosen sentinel where a genotype is missing, defaulting to
  `-9` for `ALT_COUNT` and NaN for `DS`, since a matrix has no validity bitmap.
- Add `copy_threads` to bound the decoders, defaulting to
  `datafusion.execution.target_partitions` so a single-partition read stays
  single-threaded end to end.
- Guarantee PVAR row order at every partition count, which `read_pgen` does not:
  each variant is written at its own row index rather than in completion order.

## Impact

- Affected specs: `pgen`
- Affected code: `polars_bio/io.py`, `src/scan.rs`, `src/lib.rs`
- New public API only. `read_pgen`, `scan_pgen`, `register_pgen` and
  `describe_pgen` are unchanged, and still build Arrow.
- Requires provider support for decoding into a caller-owned buffer
  (`datafusion-bio-format-pgen`'s `matrix` module).
- NumPy becomes required for this function alone. It is imported inside it, so
  `import polars_bio` still works without NumPy installed.
