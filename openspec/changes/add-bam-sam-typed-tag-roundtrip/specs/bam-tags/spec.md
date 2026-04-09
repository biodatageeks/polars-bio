## ADDED Requirements

### Requirement: BAM and SAM read APIs accept exact optional tag type hints

`read_bam`, `scan_bam`, `read_sam`, and `scan_sam` SHALL accept explicit optional tag hints for both scalar SAM tag types and `B` array types.

#### Scenario: Explicit `B` subtype hint is accepted
- **WHEN** a user reads BAM or SAM with `tag_type_hints=["ML:B:C", "pa:B:i", "pt:i"]`
- **THEN** the call succeeds without client-side validation errors
- **AND** the resulting columns use the corresponding logical Polars dtypes when the backend returns those tag values.

#### Scenario: Bare `B` hint is accepted
- **WHEN** a user reads BAM or SAM with `tag_type_hints=["ML:B"]`
- **THEN** the call succeeds without client-side validation errors.

### Requirement: Typed optional BAM/SAM tags preserve concrete logical dtypes at the Polars boundary

The system SHALL expose typed optional BAM/SAM tags with concrete logical dtypes instead of collapsing them to generic strings or generic integer lists.

#### Scenario: Standard typed array tags keep their concrete list dtype
- **WHEN** a user reads BAM or SAM requesting standard tags `ML` and `FZ`
- **THEN** `ML` is exposed as `list[u8]`
- **AND** `FZ` is exposed as `list[u16]`.

#### Scenario: Custom scalar and array tags keep their logical types
- **WHEN** a user reads BAM or SAM requesting typed custom tags such as integer, float, character, hex, string, and `B` array tags
- **THEN** the resulting Polars columns reflect those logical types rather than falling back to `Utf8` or an undifferentiated integer list.

### Requirement: BAM and SAM write APIs preserve exact tag types for transformed existing tags

`write_bam`, `sink_bam`, `write_sam`, and `sink_sam` SHALL preserve exact SAM tag types for tags that were read from BAM or SAM and then transformed through ordinary Polars operations.

#### Scenario: Existing character tag survives transform and write
- **WHEN** a user reads a BAM or SAM containing a tag of SAM type `A`, applies a Polars transform such as `filter` or `with_columns`, and writes the result back out
- **THEN** the written tag keeps SAM type `A`
- **AND** it is not silently rewritten as `Z`.

#### Scenario: Existing exact array subtype survives transform and write
- **WHEN** a user reads a BAM or SAM containing a `B`-type tag with a concrete subtype such as `B:C` or `B:S`, applies a Polars transform, and writes the result back out
- **THEN** the written tag keeps the same concrete array subtype.

### Requirement: BAM and SAM write APIs allow explicit typing for new ambiguous tag columns

`write_bam`, `sink_bam`, `write_sam`, and `sink_sam` SHALL allow callers to supply explicit tag type overrides for new tag columns whose exact SAM type cannot be recovered from Arrow dtype alone.

#### Scenario: New `A` and `H` tags are written with explicit overrides
- **WHEN** a user creates new BAM/SAM tag columns and calls the write API with `tag_type_overrides={"tp": "A", "XH": "H"}`
- **THEN** the writer serializes `tp` as SAM type `A`
- **AND** the writer serializes `XH` as SAM type `H`.

### Requirement: BAM and SAM roundtrips preserve newly added typed tag columns

The system SHALL preserve newly added optional BAM/SAM tag columns across read -> transform -> write -> read-back workflows for both eager and lazy APIs.

#### Scenario: Eager BAM roundtrip with new scalar and array tags
- **WHEN** a user reads a BAM, adds new scalar and array tag columns, writes the result with `write_bam`, and reads the output back with matching `tag_fields`
- **THEN** the new tag columns are present in the read-back result
- **AND** their values and logical dtypes match the written data.

#### Scenario: Lazy BAM sink roundtrip with preserved and new tags
- **WHEN** a user scans a BAM, preserves existing tag columns, adds new tag columns, writes the result with `sink_bam`, and reads the output back
- **THEN** both existing and newly added tag columns are present in the read-back result
- **AND** the tag typing matches the preserved metadata or explicit overrides.
