## Context

Coordinate metadata is available for native readers and can be attached to Polars frames, but plain tabular inputs may have no recorded system. Guessing the system for one side of a range operation can silently shift overlaps. The implementation keeps the existing 1-based global default and adds explicit metadata for tabular sources.

## Goals / Non-Goals

- Goals: expose coordinate-aware CSV, Parquet, and table readers; reject ambiguous tagged/untagged pairs; validate interval column types and checked shifts.
- Non-Goals: change the global coordinate-system default; convert coordinates inside range operations; add BED path support.

## Decisions

- Readers require `zero_based` to declare the source system. `use_zero_based` optionally selects the output system; `None` preserves the source system.
- Readers normalize interval names, cast chromosome identifiers to strings, and tag the resulting Polars frame with the output coordinate system.
- Start and end columns must be integer types no wider than 64 bits. Shifts use a wider intermediate type and a strict cast back to the source type, so overflow raises instead of wrapping.
- Range-operation resolution distinguishes tagged frames, native VCF/BCF paths, and unknown inputs. A tagged/unknown pair always raises. Two unknown inputs follow the existing strict/lenient setting; VCF/BCF paths use the native reader's coordinate option.

## Risks / Trade-offs

- Existing callers that combine a tagged frame with an untagged frame must tag or explicitly normalize both inputs.
- Integer-only interval validation rejects data that would fail later in the range engine, with the error reported at reader time instead.

## Migration Plan

Use `pb.scan_csv` / `pb.scan_parquet` (or their eager counterparts) with `zero_based=True` or `False`. For existing frames, set coordinate metadata before using them with a tagged frame, or convert both inputs to the same system.

## Open Questions

- None.
