# Change: Add coordinate-safe tabular readers

## Why

Range operations can silently shift results when a tagged frame is paired with an input whose coordinate system is unknown. Plain CSV and Parquet readers also provide no public way to record the coordinate system used by their interval data.

## What Changes

- Add lazy and eager CSV and Parquet readers that require an explicit source coordinate system, normalize interval columns, and record coordinate metadata.
- Let `scan_table` and `read_table` optionally record and convert coordinate systems.
- **BREAKING:** Range operations reject a tagged input paired with an untagged input in every validation mode; matching tagged inputs continue to use their recorded system.
- Validate chromosome and coordinate column types before tagging data, and detect overflow during coordinate conversion.
- Preserve the existing global default for inputs that are both untagged.

## Impact

- Affected specs: `coordinate-system`
- Affected code: `polars_bio/_tabular_io.py`, `polars_bio/_metadata.py`, `polars_bio/range_op.py`, `polars_bio/range_op_helpers.py`, `polars_bio/range_op_io.py`, and `polars_bio/io.py`
- Related design: `docs/superpowers/specs/2026-10-16-coordinate-safe-tabular-readers-design.md`
