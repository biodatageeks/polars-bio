## Context

The current BAM/SAM tag path in `polars-bio` is split across three layers:

1. Python read APIs validate user hints in `polars_bio/io.py` and `polars_bio/sql.py`.
2. Rust scan registration in `src/scan.rs` forwards those options to `BamTableProvider::new`.
3. Rust write logic in `src/write.rs` reconstructs BAM tag metadata before serialization.

The scan path is already wired for typed tag support, but two concrete limitations remain in `polars-bio` itself:

- `_validate_tag_type_hints()` only accepts scalar type codes `{i, f, Z, A, H}`.
- `add_bam_tag_metadata()` preserves existing `BAM_TAG_TYPE_KEY` when it survives, otherwise it falls back to broad Arrow inference (`Utf8 -> Z`, any list -> `B`).

As of April 9, 2026, upstream `datafusion-bio-formats#170` contains the backend work needed for full typed BAM/SAM roundtrips, but that PR is still open. Its head SHA is `95787c8be9cbb8137a72b094161978f3213ac0e4`, and the current `polars-bio` pin still points at `33b2a93e5b1842a308da00360e646bd73dde5f09`.

## Goals / Non-Goals

### Goals

- Accept explicit `B`-type read hints including exact array subtypes.
- Preserve exact SAM tag typing across ordinary Polars transforms.
- Allow callers to declare exact types for newly created ambiguous write-time tags.
- Keep typed scalar and array tags stable across eager and lazy BAM/SAM roundtrips.
- Add focused regression coverage for the workflows described in issues `#362` to `#365`.

### Non-Goals

- Re-implement BAM/SAM optional-field parsing or serialization logic that already exists upstream.
- Change the semantics of non-tag BAM/SAM columns.
- Add CRAM-specific behavior beyond what falls out naturally from the updated backend dependency.

## Decisions

### Decision 1: Align to the backend revision that contains `datafusion-bio-formats#170`

`polars-bio` should not add local decoding shims for features that upstream now supports. The change should pin all `datafusion-bio-format-*` crates to a revision that includes the full typed optional-field roundtrip work from PR `#170`.

Implementation guidance:

- Preferred target: the merged commit on `master` once upstream PR `#170` lands.
- Temporary fallback if implementation starts before merge: pin to head SHA `95787c8be9cbb8137a72b094161978f3213ac0e4` and replace it with the merged revision before release.

Reasoning:

- PR `#170` explicitly closes the upstream gaps referenced by issues `#362` and `#363`.
- It also adds exact `B` subtype handling, wide numeric coercions, and hex handling that `polars-bio` should consume rather than duplicate.

### Decision 2: Expand read-side hint grammar instead of inventing a new syntax

Read APIs should continue using SAM-style hint strings, but the validator must accept the full grammar:

- Scalar: `TAG:i`, `TAG:f`, `TAG:Z`, `TAG:A`, `TAG:H`
- Array, default subtype: `TAG:B`
- Array, explicit subtype: `TAG:B:c`, `TAG:B:C`, `TAG:B:s`, `TAG:B:S`, `TAG:B:i`, `TAG:B:I`, `TAG:B:f`

Validation rules:

- `TAG` remains exactly two characters.
- `B` hints may have either two segments (`TAG:B`) or three segments (`TAG:B:SUBTYPE`).
- Only the SAM array subtype codes above are accepted in the third segment.

This keeps read-side user input aligned with upstream parsing and with the examples already referenced in issue `#365`.

### Decision 3: Preserve tag typing in `source_header` metadata

Arrow field metadata is not reliable after `with_columns`, `filter`, `select`, and similar Polars operations, but the existing `config_meta` / `source_header` channel does survive those transforms.

The change should extend BAM/SAM metadata extraction to populate a header-level map such as:

```json
{
  "tag_types": {
    "tp": "A",
    "XH": "H",
    "ML": "B:C",
    "FZ": "B:S"
  }
}
```

Data flow:

1. On read, inspect field-level `BAM_TAG_TYPE_KEY` metadata and copy exact tag type strings into BAM/SAM `source_header`.
2. Preserve that header metadata through Polars transformations using the existing `set_source_metadata()` / `get_metadata()` path.
3. On write, consult preserved tag type metadata before falling back to Arrow dtype inference.

This solves the transformed-existing-tag case from issue `#364` without depending on Arrow field metadata surviving a DataFrame rewrite.

### Decision 4: Add explicit write-time overrides for ambiguous new tags

Metadata preservation solves roundtrips for existing tags, but it does not help when a caller creates a new `Utf8` tag column from scratch. For those cases the write API needs an explicit override surface.

Add optional `tag_type_overrides: dict[str, str] | None` to:

- `write_bam`
- `sink_bam`
- `write_sam`
- `sink_sam`

Accepted values use the same exact type strings as preserved metadata:

- `A`, `H`, `Z`, `i`, `f`
- `B`
- `B:c`, `B:C`, `B:s`, `B:S`, `B:i`, `B:I`, `B:f`

Resolution order in the write path:

1. `tag_type_overrides`
2. Preserved `source_header["tag_types"]`
3. Existing Arrow field metadata (`BAM_TAG_TYPE_KEY`)
4. Arrow dtype inference

Consequences:

- Existing transformed `A` / `H` tags retain fidelity automatically.
- Newly added ambiguous string tags can be written correctly without requiring users to manage internal metadata helpers.
- Exact list subtypes can still be forced when needed, even though most array cases can be inferred from the list element dtype.

### Decision 5: Keep the Rust writer responsible for the final schema metadata

The Python layer should collect and pass override / preserved type information, but `src/write.rs` should remain the single place that materializes BAM tag metadata on the outgoing Arrow schema.

Required changes:

- Extend `BamWriteOptions` and the shared write plumbing so Python can pass serialized `tag_type_overrides`.
- Teach `add_bam_tag_metadata()` to work with exact type strings instead of single-character fallbacks.
- Infer exact array subtype strings from list element dtypes when no explicit metadata exists.

This keeps the serialization contract close to the code that already knows how to decorate BAM/SAM schemas before upstream serialization.

## Test Strategy

### Unit tests

- `polars_bio/io.py` hint validation:
  - accepts valid scalar and `B` subtype hints
  - rejects malformed `TAG:B:X`, missing subtype tokens, and non-2-character tag names
- metadata plumbing:
  - BAM/SAM source metadata includes exact `tag_types`
  - `tag_types` survive representative `DataFrame` and `LazyFrame` transforms
- write override validation:
  - accepted override strings mirror read-side grammar
  - malformed overrides fail before entering Rust

### End-to-end integration tests

- Read-side typed tag coverage using existing nanopore BAM plus temporary SAM conversion:
  - scalar custom tags: `A`, `i`, `f`, `Z`, `H`
  - array tags: `B:c`, `B:C`, `B:s`, `B:S`, `B:i`, `B:I`, `B:f`
  - standard tags: `ML -> list[u8]`, `FZ -> list[u16]`
- Write-side roundtrips for both eager and lazy APIs:
  - `read_bam -> with_columns -> write_bam -> read_bam`
  - `scan_bam -> with_columns -> sink_bam -> read_bam`
  - matching SAM paths, excluding `CG` as a required assertion
- Ambiguous tag fidelity:
  - transformed existing `A` / `H` tags write back with the same exact type
  - newly created `A` / `H` tags roundtrip correctly when `tag_type_overrides` is supplied
- Mixed-tag scenarios:
  - preserve existing tags while adding new scalar and array tags
  - cover wide numeric inputs (`Int64`, `UInt64`, `Float64`) that the updated backend is expected to narrow safely

### Fixture strategy

- Reuse `tests/data/io/bam/nanopore_custom_tags.bam` and SAM converted from it for most custom tag coverage.
- Keep `CG` assertions BAM-only.
- If no existing fixture exposes `ML`, `FZ`, or `H`, generate a minimal temporary BAM fixture with `pysam` inside the tests instead of adding a large binary asset.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Upstream PR `#170` is still open | Medium | Use the PR head SHA temporarily or wait for merge before implementation starts |
| Metadata model adds another BAM-specific header key | Low | Store it under existing `source_header` plumbing rather than creating a parallel metadata system |
| Override API overlaps with preserved metadata | Low | Document and test a strict precedence order |
| SAM cannot require BAM-only `CG` behavior | Low | Keep `CG` assertions restricted to BAM fixtures and exclude it from required SAM roundtrips |
