# Implementation Tasks

## 1. Dependency alignment

- [x] 1.1 Update all `datafusion-bio-format-*` git dependencies in `Cargo.toml` to a revision that includes `datafusion-bio-formats#170`.
- [x] 1.2 Refresh `Cargo.lock` after the dependency update.
- [x] 1.3 Record in the implementation notes whether the final pin is the merged upstream commit or the temporary PR head SHA `95787c8be9cbb8137a72b094161978f3213ac0e4`.

## 2. Python read-side validation and metadata plumbing

- [x] 2.1 Extend `_VALID_SAM_TYPE_CODES` and `_validate_tag_type_hints()` in `polars_bio/io.py` to accept `B`-type hints and exact array subtype grammar.
- [x] 2.2 Apply the same validation behavior to BAM/SAM SQL entry points in `polars_bio/sql.py`.
- [x] 2.3 Update BAM/SAM metadata extraction in `polars_bio/metadata_extractors.py` so exact tag type strings are captured from field metadata into BAM/SAM `source_header`.
- [x] 2.4 Extend `polars_bio/_metadata.py` and related helpers as needed so BAM/SAM `tag_types` metadata survives representative `DataFrame` and `LazyFrame` transformations.
- [x] 2.5 Update BAM/SAM read docstrings to document scalar hints, `B` hints, and standard examples such as `ML:B:C` and `FZ:B:S`.

## 3. Write API and Rust write-path changes

- [x] 3.1 Add optional `tag_type_overrides` parameters to `write_bam`, `sink_bam`, `write_sam`, and `sink_sam` in `polars_bio/io.py`.
- [x] 3.2 Validate write-time overrides in Python using the same exact type grammar accepted by the read APIs.
- [x] 3.3 Extend Rust write options in `src/option.rs` and the Python/Rust bridge so serialized override maps reach the BAM/SAM write path.
- [x] 3.4 Update `_write_bam_file()` to pass preserved BAM/SAM tag type metadata and explicit overrides into the writer.
- [x] 3.5 Update `src/write.rs` so `add_bam_tag_metadata()` resolves exact tag types in the order: explicit overrides -> preserved source metadata -> existing field metadata -> Arrow dtype inference.
- [x] 3.6 Infer exact `B` subtype strings from list element dtypes when no explicit metadata exists.
- [x] 3.7 Keep default behavior for unannotated string tags as `Z`, while allowing `A` and `H` only when metadata or overrides specify them.

## 4. Unit tests

- [x] 4.1 Add validator tests for accepted and rejected `tag_type_hints` / `tag_type_overrides` grammar.
- [x] 4.2 Add metadata tests confirming BAM/SAM `tag_types` are present on reads and survive `with_columns`, `filter`, and `select` on both eager and lazy objects.
- [x] 4.3 Add focused tests for write-time precedence between explicit overrides, preserved metadata, and dtype inference where practical.

## 5. End-to-end integration tests

- [x] 5.1 Extend `tests/test_custom_tag_inference.py` with read-side coverage for exact `B` hints and expected Polars dtypes for custom scalar and array tags.
- [x] 5.2 Add eager BAM roundtrip tests in `tests/test_io_bam.py` for `read_bam -> add custom tags -> write_bam -> read_bam`.
- [x] 5.3 Add lazy BAM roundtrip tests in `tests/test_io_bam.py` for `scan_bam -> add custom tags -> sink_bam -> read_bam`.
- [x] 5.4 Cover mixed scenarios that preserve existing tags while adding new scalar and array tags.
- [x] 5.5 Cover wide Arrow numeric widths (`Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`) on write for supported SAM integer/float tags.
- [x] 5.6 Add regression coverage for transformed existing `A` and `H` tags so they do not silently degrade to `Z` on write.
- [x] 5.7 Add regression coverage for newly created `A` and `H` tags written via `tag_type_overrides`.
- [x] 5.8 Add BAM-only assertions for `CG` when a fixture is available, and keep SAM roundtrip assertions independent of `CG`.
- [x] 5.9 Add tests for standard typed array tags `ML -> list[u8]` and `FZ -> list[u16]`, generating a temporary fixture with `pysam` if existing assets do not contain them.

## 6. Documentation and validation

- [x] 6.1 Update user-facing BAM/SAM docs and docstrings with examples for typed custom tag reads and writes.
- [x] 6.2 Document the semantics and precedence of `tag_type_overrides`.
- [x] 6.3 Run targeted test coverage for BAM/SAM tag reads and writes.
- [x] 6.4 Run `openspec validate add-bam-sam-typed-tag-roundtrip --strict`.

## Implementation Notes

- Final `datafusion-bio-format-*` pin: temporary upstream PR head SHA `95787c8be9cbb8137a72b094161978f3213ac0e4` from `datafusion-bio-formats#170`.
- No `CG` fixture was available locally, so BAM-only `CG` assertions remain deferred; SAM roundtrip coverage was kept independent of `CG`.
