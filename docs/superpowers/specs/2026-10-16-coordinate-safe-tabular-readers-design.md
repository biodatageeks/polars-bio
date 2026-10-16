# Coordinate-safe range operations and tabular readers

- **Date:** 2026-10-16
- **Issue:** [#486](https://github.com/biodatageeks/polars-bio/issues/486) — range ops silently assume 1-based when only one input has coordinate-system metadata
- **Related:** #483 (fixed in #487), #278
- **Status:** design approved; spec under review

## Problem

Range operations (`overlap`, `nearest`, `count_overlaps`, `coverage`, `merge`) read the
coordinate system of each input from metadata. In the default lenient mode
(`datafusion.bio.coordinate_system_check = "false"`), an input without metadata is assumed to
use the global `datafusion.bio.coordinate_system_zero_based` setting (default 1-based), with a
`UserWarning`. When that input actually holds 0-based half-open coordinates — typically a BED
file read with `pl.read_csv`, or a CSV/Parquet path passed directly — results are shifted by one
position and nothing fails.

Verified on 0.36.2 with a variant at 1-based position 101 and BED interval `[101, 200)`
(1-based `102..200`, so the correct overlap is 0 rows): the untagged BED returns **1 row**
whenever it is paired with a tagged 1-based frame (vepyr, `scan_vcf`), as a frame or as a
CSV/Parquet path.

There is no public way to load an arbitrary CSV or Parquet table with its coordinate system
recorded; the test suite carries its own helpers for this
(`tests/test_lazyframe_partitioning.py::_scan_csv_with_metadata`, `_read_csv_with_metadata`).

## Goals

1. When one input's coordinate system is known and the other's is not, range operations raise
   instead of guessing.
2. Users can load CSV and Parquet data with the coordinate system declared, renamed to the
   standard interval columns, and optionally converted.
3. Every missing-metadata error and warning points to the fix.
4. The default stays lenient: code that pairs two untagged inputs keeps working.

## Non-goals

- Automatic coordinate conversion inside range operations (a mismatch stays an error).
- Changing the default of `coordinate_system_check`.
- Support for BED paths as range-op inputs (currently `ValueError: Only CSV, Parquet, VCF, and
  BCF files are supported`); filed separately.
- Preserving the tag through `group_by().agg()` and `pl.concat` (both drop it; with this design
  that surfaces as an error rather than a silent guess).

## Design

### 1. Coordinate-system resolution (`polars_bio/_metadata.py`)

Each input resolves to one of three states:

| Input | State |
|---|---|
| Polars/pandas frame with `coordinate_system_zero_based` metadata; registered table whose Arrow schema carries it | **tagged** (its value) |
| VCF/BCF path (read by the native reader, which applies the global setting) | **native path** (global setting) |
| Untagged frame; CSV/Parquet path (read by plain `pl.read_*`, no coordinate handling); registered table without metadata | **unknown** |

A new private `_resolve_coordinate_system(df, ctx) -> tuple[Optional[bool], Optional[str]]`
returns the value and the evidence (`"tag"`, `"native-path"`, or `None`). `get_coordinate_system`
is unchanged (it reports metadata only).

`validate_coordinate_systems(df1, df2, ctx)` applies:

| Inputs | Lenient (default) | Strict |
|---|---|---|
| both known, same system | that system | that system |
| both known, different | `CoordinateSystemMismatchError` | same |
| one **tagged**, other unknown | **`MissingCoordinateSystemError`** (new) | same |
| one **native path**, other unknown | `UserWarning`, global setting | `MissingCoordinateSystemError` |
| both unknown | `UserWarning`, global setting | `MissingCoordinateSystemError` |

A native path is known but carries no evidence beyond the global setting, so it does not trigger
the mixed-case error. In strict mode a native path is no longer reported as missing (today
`pb.overlap("a.vcf", "a.vcf")` raises in strict mode).

`validate_coordinate_system_single(df, ctx)` uses the same resolution: tagged or native path →
its value; unknown → warning (lenient) or error (strict).

**Ordering.** Input-format validation in the range-op path (unsupported extensions such as
`.bed`, frame combined with a VCF path) must run before coordinate validation, so the reported
error is the real cause. Today `_get_filter_op_from_metadata` runs first; the format checks that
currently live in `range_op_helpers.py` / `range_op_io.py` are moved or duplicated ahead of it.

### 2. Tabular readers (new `polars_bio/_tabular_io.py`)

```python
pb.scan_csv(source, *, zero_based: bool, use_zero_based: bool | None = None,
            chrom: str = "chrom", start: str = "start", end: str | None = "end",
            **polars_kwargs) -> pl.LazyFrame
pb.scan_parquet(source, *, zero_based: bool, use_zero_based: bool | None = None,
                chrom: str = "chrom", start: str = "start", end: str | None = "end",
                **polars_kwargs) -> pl.LazyFrame
pb.read_csv(...)      # same signature as scan_csv  -> pl.DataFrame
pb.read_parquet(...)  # same signature as scan_parquet -> pl.DataFrame
```

Behaviour, in order:

1. Delegate to `pl.scan_csv` / `pl.scan_parquet` with `**polars_kwargs` unchanged.
2. `zero_based` is keyword-only and required; it **declares** the coordinate system of the
   source. Omitting it raises Python's `TypeError`; a non-`bool` raises `TypeError` explaining
   `True` = 0-based half-open (e.g. BED), `False` = 1-based closed (e.g. VCF/GFF).
3. Validate the named columns against `collect_schema()`. Missing columns raise `ValueError`
   listing the available columns; if only the default `end` is missing, the message suggests
   `end=None`.
4. Rename `chrom`/`start`/`end` sources to `chrom`/`start`/`end`. Renaming onto a name that
   already exists as another column raises `ValueError`.
5. `end=None` (point data): derive `end` in the **source** system — `end = start` (1-based) or
   `end = start + 1` (0-based). If an `end` column already exists, raise `ValueError`.
6. Conversion: `use_zero_based=None` keeps the source system. Otherwise convert to the requested
   output system: 0-based → 1-based `start + 1`; 1-based → 0-based `start - 1`; `end` unchanged.
7. Tag the frame with the **output** system (`set_coordinate_system`) and record source metadata
   (`set_source_metadata(format="csv"|"parquet", path=str(source))`).
8. `read_*` = `scan_*(...).collect()`, keeping the metadata on the `DataFrame`.

`zero_based`/`use_zero_based` follow the existing convention: `use_zero_based` selects the
output representation, as in `scan_bed`/`scan_vcf`; `zero_based` is the declaration that those
readers do not need because their formats define it.

### 3. `scan_table` / `read_table`

Gain the same optional `zero_based: bool | None = None` and `use_zero_based: bool | None = None`
keywords. `zero_based=None` keeps today's behaviour (untagged result), so existing callers are
not broken; untagged results fall under the resolution rules above. Supplying `zero_based`
applies steps 6–7 of the readers (no column renaming: the bioframe schema already names them).

### 4. Messages

**`MissingCoordinateSystemError`** names the position and type of the untagged input and, in
the mixed case, the other input's system:

```
Second input (Polars LazyFrame) is missing coordinate system metadata, while the other input is
1-based. polars-bio cannot tell whether this input is 0-based or 1-based, and guessing would
shift overlaps by one position.

Load the data with a polars-bio reader that records the coordinate system:
  pb.scan_parquet(path, zero_based=True)   # or pb.scan_csv / pb.read_parquet / pb.read_csv
  (zero_based=True: 0-based half-open, e.g. BED; False: 1-based closed, e.g. VCF/GFF)
or tag an existing frame:
  df.config_meta.set(coordinate_system_zero_based=True)
```

Hints by input type (`_get_metadata_hint`):

| Input | Hint |
|---|---|
| Polars frame | `pb.scan_parquet`/`scan_csv`/`read_*` with `zero_based=`, or `df.config_meta.set(...)` |
| pandas frame | `pb.read_csv`/`read_parquet` with `zero_based=`, or `df.attrs["coordinate_system_zero_based"]` |
| CSV/Parquet path | pass `pb.scan_parquet(path, zero_based=...)` / `pb.scan_csv(...)` instead of the path |
| registered table | unchanged |

**Lenient `UserWarning`:**

```
Coordinate system metadata is missing for: <inputs>. Assuming 1-based (global
POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED). If that is wrong, overlaps are shifted by one
position. Load the data with pb.scan_csv/pb.scan_parquet(..., zero_based=True|False) (or
read_csv/read_parquet) to record the coordinate system and silence this warning.
```

**`CoordinateSystemMismatchError`:** the hint names the conversion route that exists for every
input kind: "Re-read one of the inputs with `use_zero_based=True|False` (`scan_*`/`read_*`,
including `pb.scan_csv`/`pb.scan_parquet(..., zero_based=..., use_zero_based=...)`)."

### 5. Documentation

- `docs/features/reading.md`, "Coordinate systems support": replace the lenient/strict
  description with the resolution table; add a "Reading CSV and Parquet" subsection with
  `scan_csv`/`scan_parquet` examples (declaration, renaming, `end=None`, conversion); update
  "Error handling" with the new messages.
- Docstrings of `_get_filter_op_from_metadata*` (`range_op.py`), `validate_coordinate_systems`,
  `validate_coordinate_system_single` (`_metadata.py`): state that lenient is the default and
  describe the mixed-case rule (they currently claim strict is the default).
- API reference: add the four readers to `docs/api` alongside `scan_table`.
- `CHANGELOG.md` `[Unreleased]`: **Added** the readers and `scan_table`/`read_table` keywords;
  **Changed** (behaviour) mixed tagged/untagged inputs and tagged frame + CSV/Parquet path now
  raise `MissingCoordinateSystemError` in lenient mode; VCF/BCF paths no longer warn and no
  longer fail in strict mode.

### 6. Testing

New `tests/test_coordinate_system_resolution.py`:

- **Case matrix**, parametrised over lenient and strict, using a variant at 1-based 101 and BED
  `[101, 200)`; expected outcomes:

  | # | left | right | lenient | strict |
  |---|---|---|---|---|
  | 1 | T1 | T1 | 0 rows | 0 rows |
  | 2 | T0 | T0 | 0 rows | 0 rows |
  | 3 | T1 | T0 | Mismatch | Mismatch |
  | 4 | T1 | U (polars) | Missing | Missing |
  | 4b | T1 | U (pandas) | Missing | Missing |
  | 5 | U | T0 | Missing | Missing |
  | 6 | U | U | warning, 1 row | Missing |
  | 7 | T1 | VCF path | ValueError (unsupported combination) | same |
  | 8 | T0 | VCF path | Mismatch | Mismatch |
  | 9 | VCF path | VCF path | 1 row, no warning | 1 row |
  | 10 | T1 | Parquet path | Missing | Missing |
  | 10b | T1 | CSV path | Missing | Missing |
  | 11 | Parquet path | CSV path | warning, 1 row | Missing |
  | 11b | U | Parquet path | warning, 1 row | Missing |
  | 12 | U | VCF path | ValueError (unsupported combination) | same |
  | 13 | T1 | BED path | ValueError (unsupported format) | same |

  Error tests also assert the message names the untagged input and contains the reader hint.
- **Readers:** required `zero_based`; non-`bool` rejected; renaming; missing column (with the
  `end=None` hint); rename clash; `end=None` in both systems; conversion 0→1 and 1→0 (values and
  tag); `use_zero_based` equal to the source is a no-op; `read_*` keeps the tag; `polars_kwargs`
  pass through; CSV and Parquet.
- **End-to-end:** a 1-based tagged frame (as produced by vepyr) overlapped with
  `pb.scan_csv(bed, zero_based=True, use_zero_based=False)` returns 0 rows; the same BED via
  `pl.read_csv` raises Missing.
- **`scan_table`/`read_table`:** untagged by default; tagged and converted with the keywords.
- **Existing tests updated for intended changes:**
  - `tests/test_bcf_io.py::test_bcf_path_is_supported_by_range_operations` — no warning is
    expected for VCF/BCF paths any more.
  - `tests/test_lazyframe_partitioning.py::test_partitioned_lazy_stream_supports_mixed_lazy_and_path_inputs`
    — tagged lazy frame + CSV path now raises. The streaming test keeps its coverage by using an
    untagged lazy side (both inputs unknown, lenient fallback), and a new test asserts that the
    tagged combination raises. The local `_scan_csv_with_metadata`/`_read_csv_with_metadata`
    helpers stay: they keep the files' `contig`/`pos_start`/`pos_end` names, which the public
    readers would rename.
  - Format pre-checks are not applied to `count_overlaps`, whose non-naive branch reads inputs
    through `read_df_to_datafusion` with different rules.

A throwaway prototype of sections 1, 2 (without `use_zero_based`) and 4 reproduced the matrix
above and left the rest of the suite passing (1,908 passed; the four failures were the
intended changes listed here).

## Release

Ships as a `feat:` PR closing #486, in the next minor release. The behaviour change is limited
to inputs whose result was ambiguous (rows 4, 4b, 5, 10, 10b) and is called out in the changelog.
