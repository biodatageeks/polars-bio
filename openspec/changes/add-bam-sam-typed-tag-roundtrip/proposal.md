# Change: Add typed BAM/SAM optional tag roundtrip support

GitHub issues:
- https://github.com/biodatageeks/polars-bio/issues/362
- https://github.com/biodatageeks/polars-bio/issues/363
- https://github.com/biodatageeks/polars-bio/issues/364
- https://github.com/biodatageeks/polars-bio/issues/365

Upstream dependency:
- https://github.com/biodatageeks/datafusion-bio-formats/pull/170

## Why

`polars-bio` already threads BAM/SAM read options such as `tag_fields`, `infer_tag_types`, `infer_tag_sample_size`, and `tag_type_hints` into `BamTableProvider::new`, but the repo is still pinned to `datafusion-bio-formats` revision `33b2a93e5b1842a308da00360e646bd73dde5f09`, which predates the backend work needed for full typed optional-field support.

That leaves four user-visible gaps:

1. `tag_type_hints` validation rejects `B` array hints and subtype forms such as `ML:B:C` or `pa:B:i`.
2. Exact SAM tag typing for `A`, `H`, and `B` subtypes is lost after normal Polars transforms because Arrow field metadata does not survive those operations.
3. Write APIs do not have an explicit escape hatch for ambiguous new tags whose Arrow dtype alone cannot distinguish `A`, `H`, and `Z`.
4. The test suite does not cover full read -> add tag(s) -> write -> read-back workflows for eager and lazy BAM/SAM APIs.

## What Changes

- Update `datafusion-bio-format-*` dependencies to a revision that includes the backend work from `datafusion-bio-formats#170` that closes the serializer and hint parsing gaps tracked by upstream issues `#168` and `#169`.
- Extend Python read-side hint validation and docstrings to accept `TAG:B` and `TAG:B:SUBTYPE` grammar in addition to existing scalar forms.
- Extract exact BAM/SAM tag typing into `source_header` metadata so transformed `DataFrame` and `LazyFrame` objects retain the original SAM type information.
- Add optional write-time `tag_type_overrides` support for BAM/SAM sinks so callers can explicitly declare ambiguous or newly created tag columns.
- Update the Rust BAM/SAM write path to resolve exact tag types from overrides, preserved metadata, existing field metadata, and Arrow dtype inference in that order.
- Add regression coverage for typed scalar tags, typed array tags, transformed existing tags, newly injected tags, eager write paths, and lazy sink paths.
- Document the read/write semantics for typed custom tags, including standard tags such as `ML` and `FZ`.

## Impact

- Affected specs: `bam-tags` (new capability spec).
- Affected code:
  - `Cargo.toml`, `Cargo.lock`
  - `polars_bio/io.py`
  - `polars_bio/sql.py`
  - `polars_bio/_metadata.py`
  - `polars_bio/metadata_extractors.py`
  - `src/option.rs`
  - `src/write.rs`
  - `tests/test_custom_tag_inference.py`
  - `tests/test_io_bam.py`
  - `tests/test_source_metadata.py`
- User-visible behavior:
  - `read_bam` / `scan_bam` / `read_sam` / `scan_sam` accept exact `B`-type hints.
  - `write_bam` / `sink_bam` / `write_sam` / `sink_sam` preserve or explicitly apply exact SAM tag types instead of degrading `A`/`H` to `Z` or collapsing array subtypes to bare `B`.

