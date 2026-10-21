# Coordinate-safe Range Operations and Tabular Readers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Range operations raise instead of guessing when only one input's coordinate system is known, and users get `pb.scan_csv`/`scan_parquet`/`read_csv`/`read_parquet` readers that record (and optionally convert) the coordinate system.

**Architecture:** Coordinate resolution in `polars_bio/_metadata.py` classifies every range-op input as *tagged*, *native path* (VCF/BCF, read with the global setting) or *unknown*, and `validate_coordinate_systems` raises on tagged + unknown. A format pre-check in `range_op_helpers.py` runs before coordinate validation so unsupported inputs report their real error. A new module `polars_bio/_tabular_io.py` wraps `pl.scan_csv`/`pl.scan_parquet`, renames interval columns, converts coordinates on request and tags the result; `scan_table`/`read_table` reuse its tagging helper.

**Tech Stack:** Python 3.10+, Polars 1.x, polars-config-meta (`df.config_meta`), pandas (optional), pytest, maturin (Rust extension, unchanged).

**Spec:** `docs/superpowers/specs/2026-10-16-coordinate-safe-tabular-readers-design.md`

## Global Constraints

- Default stays lenient: `datafusion.bio.coordinate_system_check` default `"false"` is not changed.
- No automatic coordinate conversion inside range operations; a mismatch stays `CoordinateSystemMismatchError`.
- Readers: `zero_based` is keyword-only and required; `use_zero_based: bool | None = None` selects the output system; interval columns are renamed to `chrom`/`start`/`end`.
- Conversion: 0-based → 1-based `start + 1`; 1-based → 0-based `start - 1`; `end` unchanged. The tag records the **output** system.
- `end=None` derives `end` in the **source** system before conversion: `end = start` (1-based) or `end = start + 1` (0-based).
- `scan_table`/`read_table`: `zero_based=None` (default) keeps today's untagged result.
- Native-path suffixes: `.vcf`, `.bcf` (compressed variants such as `.vcf.gz` included). CSV/Parquet paths are *unknown*.
- Existing message fragments that tests rely on must stay: `"missing coordinate system metadata"`, `"Coordinate system metadata is missing"`, `"POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED"`, `"config_meta.set"`, `"coordinate_system_zero_based"`, `"df.attrs"`, input type names (`"Polars DataFrame"`, `"Polars LazyFrame"`, `"Pandas DataFrame"`), `"mismatch"`, `"0-based"`, `"1-based"`.
- Commits follow Conventional Commits and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Pre-commit runs isort and black on commit.

## Review Focus

1. **Compressed and remote paths** (`a.vcf.gz`, `s3://bucket/a.vcf.gz?versionId=1`, `a.csv.gz`) — a user expects VCFs to count as native and CSVs as unknown regardless of compression or URL parameters. Pinned in Task 1 (`test_path_classification`).
2. **Tag lost by `pl.concat` / `group_by().agg()`** — a user who tagged both inputs and then concatenated expects a clear error naming the fix, not a silent guess. Pinned in Task 1 (`test_concat_drops_tag_and_raises_with_hint`).
3. **Integer dtypes survive conversion and `end` derivation** — an `Int32` `start` must stay `Int32`; a 1-based `start = 0` converted to 0-based must fail loudly, not wrap or become `-1` silently in an unsigned column. Pinned in Task 2 (`test_conversion_preserves_dtype`, `test_unsigned_underflow_raises`).
4. **Non-integer `start` column** (CSV inferred as string, e.g. `"1,000"`) — a user expects an error at read time, not a confusing failure inside the range op. Pinned in Task 2 (`test_non_integer_start_rejected`).
5. **Multi-file Parquet sources** (glob or list) — `pl.scan_parquet` accepts both; the reader must not break on them and must still tag. Pinned in Task 2 (`test_parquet_glob_and_list_sources`).

---

## File structure

| File | Responsibility | Task |
|---|---|---|
| `polars_bio/_metadata.py` (modify) | input classification, validation rules, error/warning text, hints | 1 |
| `polars_bio/range_op.py` (modify) | docstrings of `_get_filter_op_from_metadata*`; call format pre-check | 1, 3 |
| `polars_bio/range_op_helpers.py` (modify) | `_validate_input_formats` pre-check | 3 |
| `polars_bio/_tabular_io.py` (create) | readers + `_apply_coordinate_system` tagging/conversion helper | 2 |
| `polars_bio/__init__.py` (modify) | export the four readers | 2 |
| `polars_bio/io.py` (modify) | `scan_table`/`read_table` keywords | 4 |
| `tests/test_coordinate_system_resolution.py` (create) | case matrix, messages, classification, end-to-end | 1, 3, 5 |
| `tests/test_tabular_readers.py` (create) | reader behaviour | 2, 4 |
| `tests/test_bcf_io.py`, `tests/test_lazyframe_partitioning.py` (modify) | intended behaviour changes | 1 |
| `docs/features/reading.md`, `docs/api/reading.md`, `CHANGELOG.md` (modify) | documentation | 5 |

## Task 0: Build the worktree environment (setup, no commit)

The Rust extension must be built for this worktree; tests import `polars_bio.polars_bio`.

- [ ] **Step 1: Create the venv and build**

```bash
cd ~/research/git/_wt/pb-coord-readers
make install          # = uv sync --all-extras && uv run maturin develop (debug build; several minutes)
```

- [ ] **Step 2: Baseline**

Run: `uv run pytest tests/test_coordinate_system_metadata.py tests/test_warnings.py tests/test_bcf_io.py tests/test_lazyframe_partitioning.py -q`
Expected: all pass. Record the count; later tasks must not lose tests.

---

### Task 1: Coordinate resolution rules and messages

**Files:**
- Modify: `polars_bio/_metadata.py` (imports near line 11–29; `_get_metadata_hint`; `validate_coordinate_systems` ~267–362; `validate_coordinate_system_single` ~365–410)
- Modify: `polars_bio/range_op.py:56-111` (docstrings only)
- Modify: `tests/test_bcf_io.py:438-447`, `tests/test_lazyframe_partitioning.py:157-175` and add a test after line 459
- Create: `tests/test_coordinate_system_resolution.py`

**Interfaces:**
- Produces: `_resolve_coordinate_system(df, ctx=None) -> tuple[Optional[bool], Optional[str]]` (evidence `"tag"`, `"native-path"`, `None`); `_NATIVE_PATH_SUFFIXES: frozenset[str]`; unchanged public signatures `validate_coordinate_systems(df1, df2, ctx=None) -> bool`, `validate_coordinate_system_single(df, ctx=None) -> bool`.
- Produces for tests: fixtures `data_files` (dict of paths) and `check_mode` in `tests/test_coordinate_system_resolution.py`, reused by Tasks 3 and 5.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_coordinate_system_resolution.py`:

```python
"""Coordinate-system resolution for range operations (issue #486).

Every case overlaps a variant at 1-based position 101 with the BED interval
[101, 200) (1-based 102..200), so the correct result is always 0 rows; a
1-row result means the coordinates were shifted by one position.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl
import pytest

import polars_bio as pb
from polars_bio import CoordinateSystemMismatchError, MissingCoordinateSystemError
from polars_bio._metadata import _resolve_coordinate_system, set_coordinate_system

SCHEMA = {"chrom": pl.Utf8, "start": pl.Int64, "end": pl.Int64}
VAR1 = {"chrom": ["1"], "start": [101], "end": [101]}  # 1-based closed
VAR0 = {"chrom": ["1"], "start": [100], "end": [101]}  # same variant, 0-based half-open
BEDRAW = {"chrom": ["1"], "start": [101], "end": [200]}  # BED values, 0-based half-open
BED1 = {"chrom": ["1"], "start": [102], "end": [200]}  # same interval, 1-based closed

VCF_TEXT = (
    "##fileformat=VCFv4.2\n##contig=<ID=1,length=1000>\n"
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n1\t101\t.\tA\tG\t.\tPASS\t.\n"
)

MISSING, MISMATCH = MissingCoordinateSystemError, CoordinateSystemMismatchError
WARN1 = ("warn", 1)  # lenient fallback: warning + 1 row


@pytest.fixture(scope="module")
def data_files(tmp_path_factory):
    d = tmp_path_factory.mktemp("coords")
    pl.DataFrame(BEDRAW).write_parquet(d / "bed.parquet")
    pl.DataFrame(BEDRAW).write_csv(d / "bed.csv")
    (d / "r.bed").write_text("1\t101\t200\n")
    (d / "variant.vcf").write_text(VCF_TEXT)
    return {
        "Pparquet": str(d / "bed.parquet"),
        "Pcsv": str(d / "bed.csv"),
        "Pbed": str(d / "r.bed"),
        "Pvcf": str(d / "variant.vcf"),
    }


@pytest.fixture(params=["lenient", "strict"])
def check_mode(request):
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, request.param == "strict")
    yield request.param
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)


def _lf(rows, zero_based=None):
    lf = pl.LazyFrame(rows, schema=SCHEMA)
    if zero_based is not None:
        set_coordinate_system(lf, zero_based)
    return lf


def make_input(name, data_files):
    frames = {
        "T1var": lambda: _lf(VAR1, False),
        "T0var": lambda: _lf(VAR0, True),
        "T1bed": lambda: _lf(BED1, False),
        "T0bed": lambda: _lf(BEDRAW, True),
        "Ubed": lambda: _lf(BEDRAW),
        "Uvar": lambda: _lf(VAR1),
        "Ubed_pd": lambda: pd.DataFrame(BEDRAW),
    }
    return frames[name]() if name in frames else data_files[name]


def run_overlap(left, right):
    """Return (outcome, coordinate warnings, error message)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = pb.overlap(left, right)
            result = result.collect() if isinstance(result, pl.LazyFrame) else result
            outcome, message = len(result), ""
        except Exception as e:  # noqa: BLE001 - the outcome is the exception type
            outcome, message = type(e), str(e)
    coord_warnings = [str(w.message) for w in caught if "oordinate system" in str(w.message)]
    return outcome, coord_warnings, message


# id, left, right, lenient, strict
MATRIX = [
    ("1", "T1var", "T1bed", 0, 0),
    ("2", "T0var", "T0bed", 0, 0),
    ("3", "T1var", "T0bed", MISMATCH, MISMATCH),
    ("4", "T1var", "Ubed", MISSING, MISSING),
    ("4b", "T1var", "Ubed_pd", MISSING, MISSING),
    ("5", "Ubed", "T0var", MISSING, MISSING),
    ("6", "Uvar", "Ubed", WARN1, MISSING),
    ("8", "T0bed", "Pvcf", MISMATCH, MISMATCH),
    ("9", "Pvcf", "Pvcf", 1, 1),
    ("10", "T1var", "Pparquet", MISSING, MISSING),
    ("10b", "T1var", "Pcsv", MISSING, MISSING),
    ("11", "Pparquet", "Pcsv", WARN1, MISSING),
    ("11b", "Ubed", "Pparquet", WARN1, MISSING),
]


@pytest.mark.parametrize("case_id, left, right, lenient, strict", MATRIX, ids=[c[0] for c in MATRIX])
def test_case_matrix(check_mode, data_files, case_id, left, right, lenient, strict):
    expected = lenient if check_mode == "lenient" else strict
    outcome, coord_warnings, message = run_overlap(
        make_input(left, data_files), make_input(right, data_files)
    )
    if expected == WARN1:
        assert outcome == 1, message
        assert coord_warnings, "expected a coordinate-system warning"
    elif isinstance(expected, int):
        assert outcome == expected, message
        assert not coord_warnings, coord_warnings
    else:
        assert outcome is expected, f"got {outcome}: {message}"


class TestMessages:
    def test_mixed_error_names_input_and_other_system(self, check_mode, data_files):
        _, _, message = run_overlap(make_input("T1var", data_files), make_input("Ubed", data_files))
        assert message.startswith("Second input (Polars LazyFrame) is missing coordinate system metadata")
        assert "the other input is 1-based" in message
        assert "shift overlaps by one position" in message
        assert "pb.scan_parquet(path, zero_based=True)" in message
        assert "config_meta.set" in message

    def test_pandas_hint(self, check_mode, data_files):
        _, _, message = run_overlap(make_input("T1var", data_files), make_input("Ubed_pd", data_files))
        assert "Pandas DataFrame" in message
        assert "pb.read_csv" in message
        assert 'df.attrs["coordinate_system_zero_based"]' in message

    def test_path_hint(self, check_mode, data_files):
        _, _, message = run_overlap(make_input("T1var", data_files), make_input("Pparquet", data_files))
        assert "file path" in message
        assert "instead of the path" in message

    def test_lenient_warning_points_to_readers(self, data_files):
        pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
        pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)
        _, coord_warnings, _ = run_overlap(make_input("Uvar", data_files), make_input("Ubed", data_files))
        (warning,) = coord_warnings
        assert warning.startswith("Coordinate system metadata is missing for: Polars LazyFrame, Polars LazyFrame.")
        assert "Assuming 1-based" in warning
        assert "POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED" in warning
        assert "shifted by one position" in warning
        assert "pb.scan_csv/pb.scan_parquet(..., zero_based=True|False)" in warning

    def test_mismatch_hint_names_readers(self, check_mode, data_files):
        _, _, message = run_overlap(make_input("T1var", data_files), make_input("T0bed", data_files))
        assert "first input uses 1-based" in message
        assert "use_zero_based=True|False" in message
        assert "pb.scan_csv/pb.scan_parquet" in message


@pytest.mark.parametrize(
    "path, evidence",
    [
        ("/d/a.vcf", "native-path"),
        ("/d/a.vcf.gz", "native-path"),
        ("/d/A.BCF", "native-path"),
        ("s3://bucket/a.vcf.gz?versionId=1", "native-path"),
        ("/d/a.csv", None),
        ("/d/a.csv.gz", None),
        ("/d/a.parquet", None),
        ("gs://bucket/x.parquet", None),
    ],
)
def test_path_classification(path, evidence):
    pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED, False)
    value, got = _resolve_coordinate_system(path)
    assert got == evidence
    assert value == (False if evidence else None)


def test_concat_drops_tag_and_raises_with_hint(check_mode, data_files):
    merged = pl.concat([_lf(BED1, False), _lf(BED1, False)])
    _, _, message = run_overlap(make_input("T1var", data_files), merged)
    assert "Second input (Polars LazyFrame) is missing coordinate system metadata" in message
    assert "pb.scan_parquet" in message


def test_single_input_rules(check_mode, data_files):
    """merge() uses validate_coordinate_system_single: tagged runs, untagged follows the mode."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert len(pb.merge(_lf(BED1, False)).collect()) == 1
    assert not [w for w in caught if "oordinate system" in str(w.message)]
    if check_mode == "strict":
        with pytest.raises(MissingCoordinateSystemError, match="config_meta.set"):
            pb.merge(_lf(BEDRAW)).collect()
    else:
        with pytest.warns(UserWarning, match="Assuming 1-based"):
            pb.merge(_lf(BEDRAW)).collect()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_coordinate_system_resolution.py -q`
Expected: collection error `ImportError: cannot import name '_resolve_coordinate_system'`.

- [ ] **Step 3: Implement the rules in `polars_bio/_metadata.py`**

Add the import after the existing `from .exceptions import ...` line (~29):

```python
from ._path_utils import path_suffixes
```

Replace the whole `_get_metadata_hint` function with:

```python
_READER_HINT = (
    "  pb.scan_parquet(path, zero_based=True)   # or pb.scan_csv / pb.read_parquet / pb.read_csv\n"
    "  (zero_based=True: 0-based half-open, e.g. BED; False: 1-based closed, e.g. VCF/GFF)"
)


def _get_metadata_hint(df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str]) -> str:
    """Get a hint on how to record the coordinate system for the given input type."""
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)) or _has_config_meta(df):
        return (
            "Load the data with a polars-bio reader that records the coordinate system:\n"
            f"{_READER_HINT}\n"
            "or tag an existing frame:\n"
            "  df.config_meta.set(coordinate_system_zero_based=True)"
        )
    if _is_pandas_dataframe(df):
        return (
            "Load the data with pb.read_csv(..., zero_based=...) / "
            "pb.read_parquet(..., zero_based=...), or tag the existing DataFrame:\n"
            '  df.attrs["coordinate_system_zero_based"] = True   # 0-based half-open (e.g. BED)\n'
            '  df.attrs["coordinate_system_zero_based"] = False  # 1-based closed (e.g. VCF/GFF)'
        )
    if isinstance(df, str) and _is_file_path(df):
        return (
            "This file is read without coordinate handling. Load it with a polars-bio reader "
            "that records the coordinate system and pass the frame instead of the path:\n"
            f"{_READER_HINT}"
        )
    if isinstance(df, str):
        return (
            "For registered tables, ensure the table was registered with coordinate system "
            "metadata. Use polars-bio I/O functions (scan_*, read_*) to load data first."
        )
    return "Use polars-bio I/O functions to ensure metadata is set correctly."
```

Replace the whole `validate_coordinate_systems` and `validate_coordinate_system_single` functions with:

```python
# Paths read by polars-bio's native readers, which apply the global
# datafusion.bio.coordinate_system_zero_based setting.
_NATIVE_PATH_SUFFIXES = frozenset({".vcf", ".bcf"})


def _resolve_coordinate_system(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str], ctx=None
) -> tuple[Optional[bool], Optional[str]]:
    """Resolve an input's coordinate system for a range operation.

    Returns:
        ``(zero_based, evidence)``. ``evidence`` is ``"tag"`` for inputs carrying
        coordinate-system metadata, ``"native-path"`` for VCF/BCF paths (read with
        the global setting), or ``None`` when the system is unknown (untagged
        frames, CSV/Parquet paths, tables without metadata); ``zero_based`` is
        ``None`` exactly when ``evidence`` is ``None``.
    """
    if isinstance(df, str) and _is_file_path(df):
        if _NATIVE_PATH_SUFFIXES.intersection(path_suffixes(df)):
            return _get_global_zero_based(), "native-path"
        return None, None
    zero_based = get_coordinate_system(df, ctx)
    return zero_based, ("tag" if zero_based is not None else None)


def _cs_name(zero_based: bool) -> str:
    return "0-based" if zero_based else "1-based"


def _missing_error(df, other_zero_based=None, position=None) -> MissingCoordinateSystemError:
    subject = f"{position} input ({_get_input_type_name(df)})" if position else _get_input_type_name(df)
    message = f"{subject} is missing coordinate system metadata"
    if other_zero_based is not None:
        message += (
            f", while the other input is {_cs_name(other_zero_based)}. polars-bio cannot "
            f"tell whether this input is 0-based or 1-based, and guessing would shift "
            f"overlaps by one position"
        )
    message = message[0].upper() + message[1:]
    return MissingCoordinateSystemError(f"{message}.\n\n{_get_metadata_hint(df)}")


def _warn_assumed(inputs) -> bool:
    global_zero_based = _get_global_zero_based()
    names = ", ".join(_get_input_type_name(d) for d in inputs)
    warnings.warn(
        f"Coordinate system metadata is missing for: {names}. Assuming "
        f"{_cs_name(global_zero_based)} (global POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED). "
        f"If that is wrong, overlaps are shifted by one position. Load the data with "
        f"pb.scan_csv/pb.scan_parquet(..., zero_based=True|False) (or read_csv/read_parquet) "
        f"to record the coordinate system and silence this warning.",
        UserWarning,
        stacklevel=5,
    )
    return global_zero_based


def validate_coordinate_systems(
    df1: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    df2: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
) -> bool:
    """Resolve the shared coordinate system of two range-operation inputs.

    Each input is *tagged* (coordinate-system metadata), a *native path*
    (VCF/BCF file, read with the global ``datafusion.bio.coordinate_system_zero_based``
    setting) or *unknown* (untagged frame, CSV/Parquet path, table without metadata).

    - Both known: they must agree, otherwise ``CoordinateSystemMismatchError``.
    - One tagged, the other unknown: ``MissingCoordinateSystemError`` in every mode,
      because guessing could shift results by one position.
    - Otherwise, with an unknown input: when ``datafusion.bio.coordinate_system_check``
      is ``"false"`` (the default, lenient) the global setting is used and a
      ``UserWarning`` is emitted; when ``"true"`` (strict)
      ``MissingCoordinateSystemError`` is raised.

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.

    Raises:
        MissingCoordinateSystemError: See the rules above.
        CoordinateSystemMismatchError: If the inputs use different coordinate systems.
    """
    cs1, evidence1 = _resolve_coordinate_system(df1, ctx)
    cs2, evidence2 = _resolve_coordinate_system(df2, ctx)
    strict = _get_coordinate_system_check()

    if cs1 is None and cs2 is None:
        if strict:
            raise _missing_error(df1, position="first")
        cs1 = cs2 = _warn_assumed([df1, df2])
    elif cs1 is None or cs2 is None:
        known_cs, known_evidence = (cs2, evidence2) if cs1 is None else (cs1, evidence1)
        missing_df, position = (df1, "first") if cs1 is None else (df2, "second")
        if strict or known_evidence == "tag":
            raise _missing_error(missing_df, known_cs, position)
        # The known side is only a native path: no evidence beyond the global setting.
        cs1 = cs2 = _warn_assumed([missing_df])

    if cs1 != cs2:
        raise CoordinateSystemMismatchError(
            f"Coordinate system mismatch: first input uses {_cs_name(cs1)} coordinates, "
            f"second input uses {_cs_name(cs2)} coordinates. Re-read one of the inputs with "
            f"use_zero_based=True|False (scan_*/read_*, including "
            f"pb.scan_csv/pb.scan_parquet(..., zero_based=..., use_zero_based=...))."
        )
    return cs1


def validate_coordinate_system_single(
    df: Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, str],
    ctx=None,
) -> bool:
    """Resolve the coordinate system of a single range-operation input.

    Tagged inputs and native (VCF/BCF) paths resolve to their system. An unknown
    input uses the global setting with a ``UserWarning`` in lenient mode (the
    default) and raises ``MissingCoordinateSystemError`` in strict mode.

    Returns:
        True if 0-based coordinates, False if 1-based coordinates.
    """
    zero_based, _ = _resolve_coordinate_system(df, ctx)
    if zero_based is None:
        if _get_coordinate_system_check():
            raise _missing_error(df)
        zero_based = _warn_assumed([df])
    return zero_based
```

- [ ] **Step 4: Fix the docstrings in `polars_bio/range_op.py:56-111`**

In both `_get_filter_op_from_metadata` and `_get_filter_op_from_metadata_single`, replace the block

```
    The behavior when metadata is missing is controlled by the session parameter
    `datafusion.bio.coordinate_system_check`:
    - When "true" (default): Raises MissingCoordinateSystemError
    - When "false": Falls back to `datafusion.bio.coordinate_system_zero_based` and emits a warning
```

with

```
    Missing metadata is resolved by ``validate_coordinate_systems`` /
    ``validate_coordinate_system_single``: lenient mode (the default,
    ``datafusion.bio.coordinate_system_check = "false"``) falls back to
    ``datafusion.bio.coordinate_system_zero_based`` with a warning, except that a
    tagged input paired with an unknown one always raises; strict mode raises.
```

and in their `Raises:` sections replace `and datafusion.bio.coordinate_system_check is "true".` with `(see validate_coordinate_systems).`

- [ ] **Step 5: Update the two existing tests whose behaviour changes on purpose**

`tests/test_bcf_io.py`: add `import warnings` to the imports, and replace lines 442–445

```python
    with pytest.warns(UserWarning, match="Coordinate system metadata is missing"):
        expected = pb.overlap(vcf_path, vcf_path).collect()
    with pytest.warns(UserWarning, match="Coordinate system metadata is missing"):
        actual = pb.overlap(str(bcf_path), str(bcf_path)).collect()
```

with

```python
    # VCF/BCF paths are read with the global setting, so no warning is expected.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        expected = pb.overlap(vcf_path, vcf_path).collect()
        actual = pb.overlap(str(bcf_path), str(bcf_path)).collect()
    assert not [w for w in caught if "Coordinate system metadata is missing" in str(w.message)]
```

`tests/test_lazyframe_partitioning.py`: in `_build_mixed_lazy_path_overlap_case.factory` (lines 157–167) the lazy side is tagged and the other side is a CSV path, which now raises. To keep covering the mixed lazy + path streaming path, make the lazy side untagged (both inputs unknown → lenient fallback, warning already filtered by the test):

```python
    def factory() -> pl.LazyFrame:
        # Untagged lazy side: a tagged frame paired with a CSV path raises
        # MissingCoordinateSystemError (see test below), so both sides are unknown
        # and the lenient fallback applies.
        left = pl.scan_csv(DF_OVER_PATH1) if left_lazy else DF_OVER_PATH1
        right = DF_OVER_PATH2 if left_lazy else pl.scan_csv(DF_OVER_PATH2)
```

Then add after `test_partitioned_lazy_stream_supports_mixed_lazy_and_path_inputs`:

```python
@pytest.mark.parametrize("left_lazy", [True, False], ids=["lazy+path", "path+lazy"])
def test_tagged_lazy_with_csv_path_raises(left_lazy):
    tagged = _scan_csv_with_metadata(DF_OVER_PATH1 if left_lazy else DF_OVER_PATH2, zero_based=False)
    left, right = (tagged, DF_OVER_PATH2) if left_lazy else (DF_OVER_PATH1, tagged)
    with pytest.raises(pb.MissingCoordinateSystemError, match="instead of the path"):
        pb.overlap(left, right, cols1=COLUMNS, cols2=COLUMNS, output_type="polars.LazyFrame")
```

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/test_coordinate_system_resolution.py tests/test_coordinate_system_metadata.py tests/test_warnings.py tests/test_bcf_io.py tests/test_lazyframe_partitioning.py -q`
Expected: all pass.

- [ ] **Step 7: Full suite**

Run: `uv run pytest tests -q -x --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py && uv run pytest tests/test_overlap_algorithms.py tests/test_streaming.py -q`
Expected: all pass. A failure that pairs a tagged input with an untagged one or a CSV/Parquet path is an intended change only if it matches spec rows 4–5/10; otherwise stop and report.

- [ ] **Step 8: Commit**

```bash
git add polars_bio/_metadata.py polars_bio/range_op.py tests/test_coordinate_system_resolution.py tests/test_bcf_io.py tests/test_lazyframe_partitioning.py
git commit -m "feat: raise when only one range-op input has coordinate metadata (#486)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Tabular readers

**Files:**
- Create: `polars_bio/_tabular_io.py`
- Modify: `polars_bio/__init__.py` (imports near line 24; `__all__` near `"read_table"` line 227 and `"scan_table"` line 255)
- Create: `tests/test_tabular_readers.py`

**Interfaces:**
- Consumes: `set_coordinate_system(df, zero_based)`, `set_source_metadata(df, format, path)`, `get_coordinate_system(df)` from `polars_bio/_metadata.py`.
- Produces: `pb.scan_csv`, `pb.scan_parquet` (→ `pl.LazyFrame`), `pb.read_csv`, `pb.read_parquet` (→ `pl.DataFrame`), all `(source, *, zero_based: bool, use_zero_based: bool | None = None, chrom: str = "chrom", start: str = "start", end: str | None = "end", **polars_kwargs)`; and for Task 4: `_apply_coordinate_system(lf: pl.LazyFrame, *, zero_based: bool, use_zero_based: bool | None, fmt: str, source) -> pl.LazyFrame` and `_collect_with_metadata(lf: pl.LazyFrame) -> pl.DataFrame`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_tabular_readers.py`:

```python
"""pb.scan_csv / scan_parquet / read_csv / read_parquet (issue #486)."""

from __future__ import annotations

import polars as pl
import pytest

import polars_bio as pb
from polars_bio._metadata import get_coordinate_system, get_metadata


@pytest.fixture
def files(tmp_path):
    bed = pl.DataFrame({"chrom": ["1", "1"], "start": [101, 300], "end": [200, 400], "name": ["a", "b"]})
    bed.write_csv(tmp_path / "bed.csv")
    bed.write_parquet(tmp_path / "bed.parquet")
    pl.DataFrame({"contig": ["1"], "pos": [101], "ref": ["A"]}).write_parquet(tmp_path / "calls.parquet")
    pl.DataFrame({"chrom": ["1"], "chromStart": [101], "chromEnd": [200], "start": [0]}).write_csv(
        tmp_path / "clash.csv"
    )
    pl.DataFrame({"chrom": ["1"], "start": ["1,000"], "end": [2000]}).write_csv(tmp_path / "text_start.csv")
    for i in range(2):
        bed.write_parquet(tmp_path / f"part{i}.parquet")
    return tmp_path


READERS = [
    ("scan_csv", "bed.csv"),
    ("read_csv", "bed.csv"),
    ("scan_parquet", "bed.parquet"),
    ("read_parquet", "bed.parquet"),
]


def _rows(frame):
    return (frame.collect() if isinstance(frame, pl.LazyFrame) else frame).sort("start")


@pytest.mark.parametrize("reader, name", READERS)
def test_zero_based_is_required(files, reader, name):
    with pytest.raises(TypeError, match="zero_based"):
        getattr(pb, reader)(str(files / name))


@pytest.mark.parametrize("reader, name", READERS)
@pytest.mark.parametrize("bad", ["yes", 1, None])
def test_zero_based_must_be_bool(files, reader, name, bad):
    with pytest.raises(TypeError, match="zero_based must be True"):
        getattr(pb, reader)(str(files / name), zero_based=bad)


@pytest.mark.parametrize("reader, name", READERS)
@pytest.mark.parametrize("zero_based", [True, False])
def test_declares_and_tags_without_conversion(files, reader, name, zero_based):
    frame = getattr(pb, reader)(str(files / name), zero_based=zero_based)
    assert isinstance(frame, pl.LazyFrame if reader.startswith("scan") else pl.DataFrame)
    assert get_coordinate_system(frame) is zero_based
    assert _rows(frame)["start"].to_list() == [101, 300]
    meta = get_metadata(frame)
    assert meta["format"] == name.split(".")[1]
    assert meta["path"] == str(files / name)


@pytest.mark.parametrize(
    "zero_based, use_zero_based, starts",
    [(True, False, [102, 301]), (False, True, [100, 299]), (True, True, [101, 300]), (False, False, [101, 300])],
)
def test_conversion(files, zero_based, use_zero_based, starts):
    lf = pb.scan_csv(str(files / "bed.csv"), zero_based=zero_based, use_zero_based=use_zero_based)
    rows = _rows(lf)
    assert rows["start"].to_list() == starts
    assert rows["end"].to_list() == [200, 400]
    assert get_coordinate_system(lf) is use_zero_based


def test_use_zero_based_must_be_bool_or_none(files):
    with pytest.raises(TypeError, match="use_zero_based must be True"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, use_zero_based="no")


def test_rename_and_point_data(files):
    lf = pb.scan_parquet(str(files / "calls.parquet"), zero_based=False, chrom="contig", start="pos", end=None)
    rows = lf.collect()
    assert rows.columns == ["chrom", "start", "ref", "end"]
    assert rows.row(0) == ("1", 101, "A", 101)
    assert get_coordinate_system(lf) is False


@pytest.mark.parametrize("zero_based, use_zero_based, expected", [(True, None, (101, 102)), (False, True, (100, 101))])
def test_point_end_derived_in_source_system(files, zero_based, use_zero_based, expected):
    rows = pb.read_parquet(
        str(files / "calls.parquet"),
        zero_based=zero_based,
        use_zero_based=use_zero_based,
        chrom="contig",
        start="pos",
        end=None,
    )
    assert (rows["start"][0], rows["end"][0]) == expected


def test_missing_default_end_suggests_end_none(files):
    with pytest.raises(ValueError, match="pass end=None"):
        pb.scan_parquet(str(files / "calls.parquet"), zero_based=False, chrom="contig", start="pos")


def test_missing_named_column_lists_available(files):
    with pytest.raises(ValueError, match=r"\['nope'\] not found.*Available columns"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, chrom="nope")


def test_rename_clash(files):
    with pytest.raises(ValueError, match=r"Cannot rename interval columns to \['start'\]"):
        pb.scan_csv(str(files / "clash.csv"), zero_based=True, start="chromStart", end="chromEnd")


def test_end_none_with_existing_end_column(files):
    with pytest.raises(ValueError, match="already has one"):
        pb.scan_csv(str(files / "bed.csv"), zero_based=True, end=None)


def test_polars_kwargs_pass_through(files):
    assert len(pb.read_csv(str(files / "bed.csv"), zero_based=True, n_rows=1)) == 1
    lf = pb.scan_parquet(str(files / "bed.parquet"), zero_based=True, n_rows=1)
    assert len(lf.collect()) == 1


def test_conversion_preserves_dtype(files):
    path = files / "int32.parquet"
    pl.DataFrame(
        {"chrom": ["1"], "start": [101], "end": [200]},
        schema={"chrom": pl.Utf8, "start": pl.Int32, "end": pl.Int32},
    ).write_parquet(path)
    rows = pb.read_parquet(str(path), zero_based=True, use_zero_based=False)
    assert rows.schema["start"] == pl.Int32
    assert rows["start"][0] == 102


def test_unsigned_underflow_raises(files):
    path = files / "uint.parquet"
    pl.DataFrame(
        {"chrom": ["1"], "start": [0], "end": [5]}, schema={"chrom": pl.Utf8, "start": pl.UInt32, "end": pl.UInt32}
    ).write_parquet(path)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pb.read_parquet(str(path), zero_based=False, use_zero_based=True)


def test_non_integer_start_rejected(files):
    with pytest.raises(TypeError, match="'start' must be an integer column"):
        pb.scan_csv(str(files / "text_start.csv"), zero_based=True)


def test_parquet_glob_and_list_sources(files):
    glob_lf = pb.scan_parquet(str(files / "part*.parquet"), zero_based=True)
    list_lf = pb.scan_parquet([str(files / "part0.parquet"), str(files / "part1.parquet")], zero_based=True)
    for lf in (glob_lf, list_lf):
        assert len(lf.collect()) == 4
        assert get_coordinate_system(lf) is True


def test_read_keeps_tag_after_collect(files):
    df = pb.read_csv(str(files / "bed.csv"), zero_based=True)
    assert get_coordinate_system(df) is True
    assert get_coordinate_system(df.lazy()) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tabular_readers.py -q`
Expected: FAIL with `AttributeError: module 'polars_bio' has no attribute 'scan_csv'`.

- [ ] **Step 3: Create `polars_bio/_tabular_io.py`**

```python
"""Coordinate-aware CSV and Parquet readers.

Thin wrappers over ``pl.scan_csv`` / ``pl.scan_parquet`` that record the coordinate
system of the data, so range operations never have to guess it (issue #486).
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from ._metadata import set_coordinate_system, set_source_metadata

_BOOL_MESSAGE = (
    "{name} must be True (0-based half-open, e.g. BED) or False (1-based closed, e.g. VCF/GFF)"
)


def _check_bool(name: str, value, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not isinstance(value, bool):
        suffix = ", or None to keep the source system" if allow_none else ""
        raise TypeError(_BOOL_MESSAGE.format(name=name) + suffix + f"; got {value!r}")


def _rename_interval_columns(lf: pl.LazyFrame, *, zero_based: bool, chrom: str, start: str, end, source):
    names = lf.collect_schema().names()
    wanted = [("chrom", chrom), ("start", start)] + ([("end", end)] if end is not None else [])
    missing = [src for _, src in wanted if src not in names]
    if missing:
        message = f"Interval column(s) {missing} not found in {source!r}. Available columns: {names}."
        if end == "end" and missing == ["end"]:
            message += " For point data without an end column, pass end=None to derive it from start."
        raise ValueError(message)
    renames = {src: dst for dst, src in wanted if src != dst}
    clash = sorted(dst for dst in renames.values() if dst in names and dst not in renames)
    if clash:
        raise ValueError(
            f"Cannot rename interval columns to {clash}: {source!r} already has column(s) "
            f"with that name. Rename or drop them first."
        )
    if end is None and "end" in names and "end" not in renames:
        raise ValueError(f"end=None derives an 'end' column, but {source!r} already has one; pass end='end' to use it.")
    if renames:
        lf = lf.rename(renames)
    if end is None:
        start_dtype = lf.collect_schema()["start"]
        derived = pl.col("start") + 1 if zero_based else pl.col("start")
        lf = lf.with_columns(derived.cast(start_dtype).alias("end"))
    return lf


def _apply_coordinate_system(
    lf: pl.LazyFrame, *, zero_based: bool, use_zero_based: Optional[bool], fmt: str, source
) -> pl.LazyFrame:
    """Convert ``start`` to the requested output system and tag the frame.

    ``zero_based`` declares the source system; ``use_zero_based`` selects the output
    system (``None`` keeps the source system). The tag records the output system.
    """
    _check_bool("zero_based", zero_based)
    _check_bool("use_zero_based", use_zero_based, allow_none=True)
    schema = lf.collect_schema()
    if "start" not in schema:
        raise ValueError(
            f"Recording a coordinate system needs a 'start' column; {source!r} has {schema.names()}."
        )
    if not schema["start"].is_integer():
        raise TypeError(f"Column 'start' must be an integer column, got {schema['start']} in {source!r}.")
    out_zero_based = zero_based if use_zero_based is None else use_zero_based
    if out_zero_based != zero_based:
        shift = 1 if zero_based else -1  # 0-based -> 1-based: +1; 1-based -> 0-based: -1
        lf = lf.with_columns((pl.col("start") + shift).cast(schema["start"], strict=True))
    set_source_metadata(lf, format=fmt, path=str(source))
    set_coordinate_system(lf, out_zero_based)
    return lf


def _collect_with_metadata(lf: pl.LazyFrame) -> pl.DataFrame:
    df = lf.collect()
    df.config_meta.set(**lf.config_meta.get_metadata())
    return df


def _scan(reader, fmt, source, zero_based, use_zero_based, chrom, start, end, polars_kwargs) -> pl.LazyFrame:
    _check_bool("zero_based", zero_based)
    _check_bool("use_zero_based", use_zero_based, allow_none=True)
    lf = reader(source, **polars_kwargs)
    lf = _rename_interval_columns(lf, zero_based=zero_based, chrom=chrom, start=start, end=end, source=source)
    return _apply_coordinate_system(lf, zero_based=zero_based, use_zero_based=use_zero_based, fmt=fmt, source=source)


def scan_csv(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.LazyFrame:
    """Lazily read a CSV file with its coordinate system recorded.

    Parameters:
        source: Path(s) accepted by ``pl.scan_csv``.
        zero_based: Coordinate system of the data in the file: True for 0-based
            half-open (e.g. BED), False for 1-based closed (e.g. VCF/GFF). Required.
        use_zero_based: Coordinate system of the result. None (default) keeps the
            source system; otherwise ``start`` is converted.
        chrom: Name of the chromosome column; renamed to ``chrom``.
        start: Name of the start column; renamed to ``start``.
        end: Name of the end column; renamed to ``end``. None for point data:
            ``end`` is derived from ``start`` in the source system.
        **polars_kwargs: Passed to ``pl.scan_csv`` unchanged.
    """
    return _scan(pl.scan_csv, "csv", source, zero_based, use_zero_based, chrom, start, end, polars_kwargs)


def scan_parquet(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.LazyFrame:
    """Lazily read Parquet data with its coordinate system recorded.

    Parameters are as for [scan_csv][polars_bio._tabular_io.scan_csv];
    ``source`` and ``**polars_kwargs`` are passed to ``pl.scan_parquet``.
    """
    return _scan(pl.scan_parquet, "parquet", source, zero_based, use_zero_based, chrom, start, end, polars_kwargs)


def read_csv(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.DataFrame:
    """Read a CSV file into a DataFrame with its coordinate system recorded.

    Parameters are as for [scan_csv][polars_bio._tabular_io.scan_csv].
    """
    return _collect_with_metadata(
        scan_csv(source, zero_based=zero_based, use_zero_based=use_zero_based, chrom=chrom, start=start, end=end, **polars_kwargs)
    )


def read_parquet(
    source,
    *,
    zero_based: bool,
    use_zero_based: Optional[bool] = None,
    chrom: str = "chrom",
    start: str = "start",
    end: Optional[str] = "end",
    **polars_kwargs,
) -> pl.DataFrame:
    """Read Parquet data into a DataFrame with its coordinate system recorded.

    Parameters are as for [scan_parquet][polars_bio._tabular_io.scan_parquet].
    """
    return _collect_with_metadata(
        scan_parquet(
            source, zero_based=zero_based, use_zero_based=use_zero_based, chrom=chrom, start=start, end=end, **polars_kwargs
        )
    )
```

`get_metadata` returns the source format and path under the keys `"format"` and `"path"` (asserted in `test_declares_and_tags_without_conversion`).

- [ ] **Step 4: Export the readers in `polars_bio/__init__.py`**

After `from .io import PgenMatrix` (line 25) add:

```python
from ._tabular_io import read_csv, read_parquet, scan_csv, scan_parquet
```

In `__all__`, add `"read_csv",` and `"read_parquet",` next to `"read_table",`, and `"scan_csv",` and `"scan_parquet",` next to `"scan_table",`.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/test_tabular_readers.py -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add polars_bio/_tabular_io.py polars_bio/__init__.py tests/test_tabular_readers.py
git commit -m "feat: add pb.scan_csv/scan_parquet/read_csv/read_parquet with recorded coordinate system (#486)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Format checks before coordinate checks

**Files:**
- Modify: `polars_bio/range_op_helpers.py` (add `_validate_input_formats` after `_validate_overlap_input`, ~line 379)
- Modify: `polars_bio/range_op.py` (import line 22; call sites after each `_validate_overlap_input(...)` in `overlap` ~228, `nearest` ~315, `coverage` ~393, `merge` ~636, `cluster` ~696, `complement` ~758, `subtract` ~835 — **not** `count_overlaps`, whose non-naive branch reads inputs through `read_df_to_datafusion` with different rules)
- Modify: `tests/test_coordinate_system_resolution.py` (append)

**Interfaces:**
- Consumes: `make_input`, `run_overlap`, `data_files`, `check_mode`, `MISSING`, `_lf`, `BEDRAW` from Task 1's test module.
- Produces: `_validate_input_formats(df1, df2, output_type: str) -> None` (`df2=None` for single-input operations).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_coordinate_system_resolution.py`)

```python
FORMAT_CASES = [
    ("7", "T1bed", "Pvcf", "only supported for CSV and Parquet"),
    ("12", "Ubed", "Pvcf", "only supported for CSV and Parquet"),
    ("13", "T1var", "Pbed", "Only CSV, Parquet, VCF, and BCF files are supported"),
]


@pytest.mark.parametrize("case_id, left, right, match", FORMAT_CASES, ids=[c[0] for c in FORMAT_CASES])
def test_unsupported_inputs_report_format_error_first(check_mode, data_files, case_id, left, right, match):
    outcome, coord_warnings, message = run_overlap(make_input(left, data_files), make_input(right, data_files))
    assert outcome is ValueError, f"got {outcome}: {message}"
    assert match in message
    assert not coord_warnings, coord_warnings


def test_single_input_bed_path_reports_format_error(check_mode, data_files):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(ValueError, match="Only CSV, Parquet, VCF, and BCF files are supported"):
            pb.merge(data_files["Pbed"]).collect()
    assert not [w for w in caught if "oordinate system" in str(w.message)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_coordinate_system_resolution.py -q -k "format_error"`
Expected: FAIL — row 12 lenient emits a coordinate warning, rows 12/13 strict and row 13 lenient raise `MissingCoordinateSystemError`.

- [ ] **Step 3: Implement the pre-check in `polars_bio/range_op_helpers.py`**

Extend two existing imports — `from ._metadata import _is_file_path, set_coordinate_system` and `from .range_op_io import _df_to_reader, _get_schema, _is_lazyframe_like, range_lazy_scan` — and, after `_validate_overlap_input`, add:

```python
_SUPPORTED_PATH_SUFFIXES = frozenset({".parquet", ".csv", ".vcf", ".bcf"})
_STREAMABLE_PATH_SUFFIXES = frozenset({".parquet", ".csv"})


def _validate_input_formats(df1, df2, output_type: str) -> None:
    """Reject unsupported input combinations before coordinate validation.

    Mirrors the checks that would otherwise fail later in ``range_op_io`` so that
    the real cause is reported instead of a coordinate-system error or warning.
    ``df2`` is None for single-input operations.
    """
    inputs = [df for df in (df1, df2) if df is not None]
    for df in inputs:
        if isinstance(df, str) and _is_file_path(df):
            suffixes = set(path_suffixes(df))
            if suffixes and not suffixes & _SUPPORTED_PATH_SUFFIXES:
                raise ValueError("Only CSV, Parquet, VCF, and BCF files are supported")
    paths = [df for df in inputs if isinstance(df, str)]
    frames = [df for df in inputs if not isinstance(df, str)]
    if output_type == "polars.LazyFrame" and len(paths) == 1 and len(frames) == 1:
        if _is_lazyframe_like(frames[0]) and not set(path_suffixes(paths[0])) & _STREAMABLE_PATH_SUFFIXES:
            raise ValueError("Mixed LazyFrame + path streaming is only supported for CSV and Parquet inputs.")
```

Note: a bare string without suffix (a registered table name) is left to the existing code path, as today.

- [ ] **Step 4: Call it from `polars_bio/range_op.py`**

Change line 22 to `from .range_op_helpers import _validate_input_formats, _validate_overlap_input, range_operation`. Immediately after the `_validate_overlap_input(...)` line in each operation add:

- `overlap`, `nearest`, `coverage`, `subtract`: `_validate_input_formats(df1, df2, output_type)`
- `merge`, `cluster`, `complement`: `_validate_input_formats(df, None, output_type)`

Do not add it to `count_overlaps`.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/test_coordinate_system_resolution.py tests/test_lazyframe_partitioning.py tests/test_bcf_io.py -q`
Expected: all pass.

- [ ] **Step 6: Full suite**

Run: `uv run pytest tests -q --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py && uv run pytest tests/test_overlap_algorithms.py tests/test_streaming.py -q`
Expected: all pass. If a previously passing test now fails with one of the two `ValueError`s, the pre-check is stricter than the real code path for that case: narrow the condition to match the case and add a test for it; do not delete the test.

- [ ] **Step 7: Commit**

```bash
git add polars_bio/range_op_helpers.py polars_bio/range_op.py tests/test_coordinate_system_resolution.py
git commit -m "fix: report unsupported range-op inputs before coordinate-system checks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: `scan_table` / `read_table` keywords

**Files:**
- Modify: `polars_bio/io.py:3584-3617` (`read_table`, `scan_table`)
- Modify: `tests/test_tabular_readers.py` (append)

**Interfaces:**
- Consumes: `_apply_coordinate_system(lf, *, zero_based, use_zero_based, fmt, source)`, `_collect_with_metadata(lf)` from Task 2.
- Produces: `read_table(path, schema=None, *, zero_based=None, use_zero_based=None, **kwargs)`, `scan_table(path, schema=None, *, zero_based=None, use_zero_based=None, **kwargs)`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_tabular_readers.py`)

```python
@pytest.fixture
def bed_file(tmp_path):
    path = tmp_path / "r.bed"
    path.write_text("1\t101\t200\n1\t300\t400\n")
    return str(path)


def test_table_untagged_by_default(bed_file):
    assert get_coordinate_system(pb.scan_table(bed_file, schema="bed3")) is None
    assert get_coordinate_system(pb.read_table(bed_file, schema="bed3")) is None


@pytest.mark.parametrize("reader", ["scan_table", "read_table"])
def test_table_tagged_and_converted(bed_file, reader):
    frame = getattr(pb, reader)(bed_file, schema="bed3", zero_based=True, use_zero_based=False)
    assert get_coordinate_system(frame) is False
    assert _rows(frame)["start"].to_list() == [102, 301]


def test_table_tagged_without_conversion(bed_file):
    lf = pb.scan_table(bed_file, schema="bed3", zero_based=True)
    assert get_coordinate_system(lf) is True
    assert _rows(lf)["start"].to_list() == [101, 300]


def test_table_use_zero_based_requires_zero_based(bed_file):
    with pytest.raises(ValueError, match="use_zero_based requires zero_based"):
        pb.scan_table(bed_file, schema="bed3", use_zero_based=False)


def test_table_zero_based_without_schema(bed_file):
    with pytest.raises(ValueError, match="needs a 'start' column"):
        pb.scan_table(bed_file, zero_based=True)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tabular_readers.py -q -k table`
Expected: FAIL with `TypeError: ... got an unexpected keyword argument 'zero_based'` (passed through to `pl.scan_csv`).

- [ ] **Step 3: Implement in `polars_bio/io.py`**

Replace `read_table` and `scan_table` (lines 3584–3617) with:

```python
    @staticmethod
    def read_table(
        path: str,
        schema: Dict = None,
        *,
        zero_based: Optional[bool] = None,
        use_zero_based: Optional[bool] = None,
        **kwargs,
    ) -> pl.DataFrame:
        """
         Read a tab-delimited (i.e. BED) file into a Polars DataFrame.
         Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
         but faster. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

        Parameters:
            path: The path to the file.
            schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).
            zero_based: Coordinate system of the data in the file (True: 0-based half-open, e.g. BED; False: 1-based closed). None (default) leaves the result untagged.
            use_zero_based: Coordinate system of the result when `zero_based` is given. None keeps the source system; otherwise `start` is converted.
        """
        from ._tabular_io import _collect_with_metadata

        return _collect_with_metadata(
            IOOperations.scan_table(
                path, schema, zero_based=zero_based, use_zero_based=use_zero_based, **kwargs
            )
        )

    @staticmethod
    def scan_table(
        path: str,
        schema: Dict = None,
        *,
        zero_based: Optional[bool] = None,
        use_zero_based: Optional[bool] = None,
        **kwargs,
    ) -> pl.LazyFrame:
        """
         Lazily read a tab-delimited (i.e. BED) file into a Polars LazyFrame.
         Tries to be compatible with Bioframe's [read_table](https://bioframe.readthedocs.io/en/latest/guide-io.html)
         but faster and lazy. Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).

        Parameters:
            path: The path to the file.
            schema: Schema should follow the Bioframe's schema [format](https://github.com/open2c/bioframe/blob/2b685eebef393c2c9e6220dcf550b3630d87518e/bioframe/io/schemas.py#L174).
            zero_based: Coordinate system of the data in the file (True: 0-based half-open, e.g. BED; False: 1-based closed). None (default) leaves the result untagged.
            use_zero_based: Coordinate system of the result when `zero_based` is given. None keeps the source system; otherwise `start` is converted.
        """
        df = pl.scan_csv(path, separator="\t", has_header=False, **kwargs)
        if schema is not None:
            columns = SCHEMAS[schema]
            if len(columns) != len(df.collect_schema()):
                raise ValueError(
                    f"Schema incompatible with the input. Expected {len(columns)} columns in a schema, got {len(df.collect_schema())} in the input data file. Please provide a valid schema."
                )
            for i, c in enumerate(columns):
                df = df.rename({f"column_{i + 1}": c})
        if zero_based is None:
            if use_zero_based is not None:
                raise ValueError(
                    "use_zero_based requires zero_based: declare the coordinate system of the file first."
                )
            return df
        from ._tabular_io import _apply_coordinate_system

        return _apply_coordinate_system(
            df, zero_based=zero_based, use_zero_based=use_zero_based, fmt="table", source=path
        )
```

Before editing, copy the existing schema-handling lines verbatim from the file (the block above reproduces lines 3608–3616 as of master `d8dc382`); if they differ, keep the file's version and only add the `zero_based` handling. Confirm `Optional` is imported at the top of `io.py` (`from typing import ...`); add it if missing.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_tabular_readers.py -q && uv run pytest tests -q -k "read_table or scan_table"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add polars_bio/io.py tests/test_tabular_readers.py
git commit -m "feat: scan_table/read_table accept zero_based/use_zero_based (#486)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: End-to-end flow, documentation and changelog

**Files:**
- Modify: `tests/test_coordinate_system_resolution.py` (append)
- Modify: `docs/features/reading.md` (sections "Error handling" ~1014 to "Strict mode" ~1080), `docs/api/reading.md` (append a section), `CHANGELOG.md` (`[Unreleased]`)

**Interfaces:**
- Consumes: `pb.scan_csv` (Task 2), `_lf`, `VAR1`, `BEDRAW`, `check_mode`, `tmp_path` fixture.

- [ ] **Step 1: Write the end-to-end test** (append to `tests/test_coordinate_system_resolution.py`)

```python
def test_vepyr_style_flow(check_mode, tmp_path):
    """A 1-based tagged frame (as vepyr produces) against a BED-like CSV."""
    bed_csv = tmp_path / "regions.csv"
    pl.DataFrame(BEDRAW).write_csv(bed_csv)
    variants = _lf(VAR1, False)

    with pytest.raises(MissingCoordinateSystemError, match="pb.scan_parquet"):
        pb.overlap(variants, pl.read_csv(bed_csv)).collect()
    with pytest.raises(CoordinateSystemMismatchError, match="use_zero_based"):
        pb.overlap(variants, pb.scan_csv(str(bed_csv), zero_based=True)).collect()
    regions = pb.scan_csv(str(bed_csv), zero_based=True, use_zero_based=False)
    assert len(pb.overlap(variants, regions).collect()) == 0
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/test_coordinate_system_resolution.py -q -k vepyr`
Expected: PASS (Tasks 1–2 already implement the behaviour; this pins the user flow).

- [ ] **Step 3: Update `docs/features/reading.md`**

Replace everything from `### Error handling` up to (not including) `### Migration from previous versions` with:

````markdown
### Reading CSV and Parquet with a coordinate system

`pb.scan_csv`, `pb.scan_parquet`, `pb.read_csv` and `pb.read_parquet` wrap the Polars readers and record the coordinate system, which polars-bio cannot infer from these formats. `zero_based` is required and declares what the file contains; `use_zero_based` optionally converts the result.

```python
import polars_bio as pb

# BED-like CSV (0-based half-open), converted to 1-based to match VCF/vepyr frames
regions = pb.scan_csv("regions.csv", zero_based=True, use_zero_based=False)

# Differently named columns are renamed to chrom/start/end; end=None for point data
calls = pb.scan_parquet("calls.parquet", zero_based=False, chrom="contig", start="pos", end=None)

hits = pb.overlap(pb.scan_vcf("variants.vcf"), regions)
```

`scan_table` and `read_table` accept the same `zero_based` / `use_zero_based` keywords (the result stays untagged when `zero_based` is omitted).

### How range operations resolve coordinate systems

Each input of a range operation is **tagged** (it carries coordinate-system metadata: polars-bio readers, results of range operations, frames tagged by hand), a **VCF/BCF path** (read with the global `datafusion.bio.coordinate_system_zero_based` setting) or **unknown** (an untagged frame, a CSV/Parquet path, a table without metadata).

| Inputs | Lenient (default) | Strict |
|---|---|---|
| both known, same system | runs | runs |
| both known, different systems | `CoordinateSystemMismatchError` | `CoordinateSystemMismatchError` |
| one tagged, one unknown | `MissingCoordinateSystemError` | `MissingCoordinateSystemError` |
| one VCF/BCF path, one unknown | warning, global setting | `MissingCoordinateSystemError` |
| both unknown | warning, global setting | `MissingCoordinateSystemError` |

A tagged input paired with an unknown one always raises: polars-bio cannot tell whether the unknown input is 0-based or 1-based, and guessing would shift overlaps by one position. Unsupported inputs (for example a BED path, or a LazyFrame combined with a VCF path) are reported before any coordinate check.

### Error handling

#### MissingCoordinateSystemError

```python
import polars as pl
import polars_bio as pb

variants = pb.scan_vcf("variants.vcf")                       # tagged, 1-based
regions = pl.read_csv("regions.csv")                         # untagged

pb.overlap(variants, regions)
# MissingCoordinateSystemError: Second input (Polars DataFrame) is missing coordinate system
# metadata, while the other input is 1-based. ...
```

**How to fix:** load the data with `pb.scan_csv` / `pb.scan_parquet` (or `read_*`) and `zero_based=...`, or tag the frame (see [Setting metadata on DataFrames](#setting-metadata-on-dataframes)).

#### CoordinateSystemMismatchError

```python
df1 = pb.scan_vcf("file.vcf")                        # 1-based (default)
df2 = pb.scan_bed("file.bed", use_zero_based=True)   # 0-based

pb.overlap(df1, df2)  # CoordinateSystemMismatchError
```

**How to fix:** re-read one input with `use_zero_based=...` so both use the same system; for CSV/Parquet use `pb.scan_csv(..., zero_based=..., use_zero_based=...)`.

### Default behavior (lenient validation)

By default (`coordinate_system_check=false`), when **neither** input carries metadata, polars-bio uses the global `coordinate_system_zero_based` setting and emits a warning that names the assumed system:

```python
a = pl.DataFrame({"chrom": ["chr1"], "start": [100], "end": [200]})
b = pl.DataFrame({"chrom": ["chr1"], "start": [150], "end": [250]})
pb.overlap(a, b)
# UserWarning: Coordinate system metadata is missing for: Polars DataFrame, Polars DataFrame.
# Assuming 1-based (global POLARS_BIO_COORDINATE_SYSTEM_ZERO_BASED). ...
```

### Strict mode

Strict mode raises `MissingCoordinateSystemError` for every unknown input:

```python
pb.set_option(pb.POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)
```

!!! tip
    Enable strict mode in production pipelines so that no input is ever interpreted with an assumed coordinate system.

````

Also change the `check` node label in the mermaid diagram (~line 876) only if it no longer matches; it already reads `default: false = lenient`, so leave it.

- [ ] **Step 4: Update `docs/api/reading.md`**

Append at the end of the file:

```markdown
## CSV and Parquet with a coordinate system

::: polars_bio._tabular_io
    handler: python
    options:
        show_root_heading: false
        show_root_toc_entry: false
        heading_level: 3
        docstring_section_style: table
        members:
            - scan_csv
            - scan_parquet
            - read_csv
            - read_parquet
```

- [ ] **Step 5: Update `CHANGELOG.md`**

Under `## [Unreleased]` add:

```markdown
### Added
- `pb.scan_csv`, `pb.scan_parquet`, `pb.read_csv` and `pb.read_parquet`: wrap the Polars readers and record the coordinate system (`zero_based=` required), rename interval columns to `chrom`/`start`/`end`, derive `end` for point data (`end=None`) and optionally convert (`use_zero_based=`) (#486).
- `scan_table` / `read_table` accept `zero_based=` and `use_zero_based=` (#486).

### Changed
- Range operations raise `MissingCoordinateSystemError` when one input carries coordinate-system metadata and the other does not (an untagged frame or a CSV/Parquet path), also in the default lenient mode. Previously the untagged input was assumed to use the global setting, which shifted results by one position for 0-based data (#486).
- VCF/BCF paths passed to range operations are treated as using the global coordinate-system setting: no missing-metadata warning, and no error in strict mode.
- Missing-metadata errors and warnings name the input, the assumed or conflicting system and the readers to use; unsupported inputs (BED paths, a LazyFrame combined with a VCF path) are reported before coordinate checks.
```

- [ ] **Step 6: Build the docs**

Run: `uv run mkdocs build 2>&1 | tail -20`
Expected: build succeeds; no new warnings mentioning `reading.md` or `_tabular_io`. If mkdocs is not in the dev extras, skip and note it in the PR body.

- [ ] **Step 7: Full suite and commit**

Run: `uv run pytest tests -q --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py && uv run pytest tests/test_overlap_algorithms.py tests/test_streaming.py -q`
Expected: all pass.

```bash
git add tests/test_coordinate_system_resolution.py docs/features/reading.md docs/api/reading.md CHANGELOG.md
git commit -m "docs: coordinate-system resolution rules and tabular readers (#486)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## After all tasks

Open a PR from `feat/coordinate-safe-readers` to `master` titled `feat: coordinate-safe range ops and CSV/Parquet readers` with `Closes #486`, listing the behaviour changes from the changelog and the full-suite result. Remove or keep `docs/superpowers/` per the maintainer's choice before merging.
