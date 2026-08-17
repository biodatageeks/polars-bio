# PGEN Input Binding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose PLINK 2 PGEN/PVAR/PSAM filesets through polars-bio as `read_pgen`, `scan_pgen`, `register_pgen`, and `describe_pgen`.

**Architecture:** Mirror the existing BGEN binding exactly. A `PgenReadOptions` pyclass in `src/option.rs` carries options from Python; `src/scan.rs` converts them to the upstream `datafusion_bio_format_pgen::PgenReadOptions` and builds a `PgenTableProvider`; `polars_bio/io.py` and `polars_bio/sql.py` provide the four Python entry points, all routing through the shared `_read_file` / `py_register_table` path used by every other format.

**Tech Stack:** Rust + pyo3 (maturin), DataFusion 53.0.0, `datafusion-bio-format-pgen` at rev `e029e08`, Python 3 + polars + pyarrow, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-08-17-pgen-polars-bio-binding-design.md`

## Global Constraints

- Branch is `feat/bgen-pr220-bench`. Do not create a new branch.
- The provider dependency rev is `e029e08` — identical to the other twelve `datafusion-bio-format-*` deps already in `Cargo.toml`. Do not bump any of them.
- Rebuild after every Rust change: `make install` (which runs `unset CONDA_PREFIX && uv run maturin develop`). Python-only changes need no rebuild.
- Run tests with `uv run pytest`, never bare `pytest`.
- `make pre-commit` runs `cargo fmt --all`, `cargo clippy --all-features`, `ruff check --fix`, and `ruff format`. Run it before each commit.
- Enum-valued options cross the pyo3 boundary as lowercase strings, never as integers or Python enums. This matches BGEN's `genotype_output` / `probability_layout`.
- Never use `.unwrap()` or `.expect()` on `PgenTableProvider::try_new` in the registration arm. It fails on ordinary user input and must return a `Result`.
- **Verify every edit landed.** A clean `cargo check` is not evidence a string replacement matched. After each edit to `src/option.rs`, `grep` for the new text and confirm a hit.
- **Every new test must fail before the implementation exists**, and fail for the intended reason. A test that passes against unwired code is measuring something else.

---

## File Structure

**Created:**
- `tests/data/io/pgen/{oracle,dosage,phase,unused_alt}.{pgen,pvar,psam}` — 12 fixture files copied from upstream
- `tests/test_pgen_io.py` — the whole Python test suite for this feature
- `openspec/changes/add-pgen-support/{proposal.md,tasks.md,design.md}` and `specs/pgen/spec.md`

**Modified:**
- `Cargo.toml` — one dependency line
- `src/option.rs` — `InputFormat::Pgen`, its display arm, the `PgenReadOptions` pyclass, the `ReadOptions.pgen_read_options` field, and the positional `#[pyo3(signature = ...)]` at line 215
- `src/lib.rs` — provider imports (line ~23) and `m.add_class::<PgenReadOptions>()` (line ~958)
- `src/scan.rs` — `.pgen` suffix detection (line ~490), two string→enum helpers (near line ~497), and the `InputFormat::Pgen` registration arm (near line ~819)
- `polars_bio/io.py` — four validators near line 296, plus `read_pgen`, `scan_pgen`, and `describe_pgen`
- `polars_bio/sql.py` — `register_pgen` and validator imports
- `polars_bio/metadata_extractors.py` — `_extract_pgen_specific_metadata` and its dispatch line
- `polars_bio/__init__.py` — four exports and four `__all__` entries

---

## Task 1: Fixtures and the end-to-end read path

Delivers `pb.read_pgen(path)` and `pb.scan_pgen(path)` returning variant metadata with a GT-only `genotypes` struct, plus coverage of all five genotype fields.

**Files:**
- Create: `tests/data/io/pgen/` (12 fixture files)
- Create: `tests/test_pgen_io.py`
- Modify: `Cargo.toml`, `src/option.rs`, `src/lib.rs`, `src/scan.rs`, `polars_bio/io.py`, `polars_bio/__init__.py`

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  - Rust: `option::InputFormat::Pgen`; `option::PgenReadOptions { object_storage_options: Option<ObjectStorageOptions>, genotype_fields: Option<Vec<String>>, zero_based: bool }` with `#[staticmethod] default()`; `ReadOptions.pgen_read_options: Option<PgenReadOptions>`
  - Python: `polars_bio.read_pgen(path, genotype_fields=["GT"], ...) -> pl.DataFrame`, `polars_bio.scan_pgen(...) -> pl.LazyFrame`, `polars_bio.io._validate_pgen_input_path(path, operation="read")`, `polars_bio.io._validate_pgen_genotype_fields(genotype_fields)`

- [ ] **Step 1: Copy the fixtures from upstream**

The upstream repo is a sibling checkout. The fixtures are 23–58 bytes each and are *not* generated with `plink2` — do not install `plink2` for this task.

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
mkdir -p tests/data/io/pgen
UPSTREAM=/Users/mwiewior/CLionProjects/datafusion-bio-formats
for name in oracle dosage phase unused_alt; do
  for ext in pgen pvar psam; do
    git -C "$UPSTREAM" show "e029e08:datafusion/bio-format-pgen/tests/data/pgenlib/$name.$ext" \
      > "tests/data/io/pgen/$name.$ext"
  done
done
ls -l tests/data/io/pgen/
```

Expected: 12 files. `oracle.pgen` is 32 bytes, `oracle.pvar` 58, `oracle.psam` 29, `dosage.pgen` 28, `phase.pgen` 24, `unused_alt.pgen` 23.

- [ ] **Step 2: Confirm the fixture contents match what the tests will assert**

```bash
head -4 tests/data/io/pgen/oracle.pvar tests/data/io/pgen/oracle.psam
```

Expected — `oracle.pvar` has a `#CHROM POS ID REF ALT` header then `1 10 v1 A C`, `1 20 v2 G T`, `2 30 v3 C G`; `oracle.psam` has a `#IID` header then `s1`..`s8`. The other three filesets each hold one variant and four samples `s1`..`s4`.

Note for later steps: the PSAM files carry **only** an `#IID` column. The provider defaults a missing FID and SID to the literal string `"0"`, so `psam_id_mode="fid_iid"` yields `0:s1` and `"fid_iid_sid"` yields `0:s1:0`. That is what Task 2 asserts.

- [ ] **Step 3: Write the failing tests**

Create `tests/test_pgen_io.py`. The expected genotype values are taken from upstream `datafusion/bio-format-pgen/tests/provider_test.rs`, which asserts them against the same fixtures.

```python
"""PGEN read, scan, register, and describe tests."""

import numpy as np
import polars as pl
import pytest
from _expected import DATA_DIR

import polars_bio as pb
from polars_bio.context import ctx

PGEN_DIR = DATA_DIR / "io" / "pgen"
ORACLE_PATH = PGEN_DIR / "oracle.pgen"
DOSAGE_PATH = PGEN_DIR / "dosage.pgen"
PHASE_PATH = PGEN_DIR / "phase.pgen"
UNUSED_ALT_PATH = PGEN_DIR / "unused_alt.pgen"
TARGET_PARTITIONS = "datafusion.execution.target_partitions"

# oracle: 3 variants over 8 samples s1..s8, written by pgenlib.
ORACLE_SAMPLES = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]
ORACLE_IDS = ["v1", "v2", "v3"]
ORACLE_CHROMS = ["1", "1", "2"]
ORACLE_REFS = ["A", "G", "C"]
ORACLE_ALTS = [["C"], ["T"], ["G"]]
# The single-variant filesets each hold four samples, the fourth missing.
SMALL_SAMPLES = ["s1", "s2", "s3", "s4"]
DOSAGE_DS = [0.125, 1.0, 1.875, None]
PHASE_GT = [[0, 1], [1, 0], [0, 0], None]
PHASE_PHASED = [True, True, False, None]
UNUSED_ALT_GT = [[0, 0], [0, 1], [1, 1], None]


def _genotype_field(schema, name: str) -> pl.DataType:
    """Look the genotype struct field up by name rather than by position."""
    fields = {field.name: field.dtype for field in schema["genotypes"].fields}
    return fields[name]


def _genotype_names(frame: pl.DataFrame) -> list[str]:
    return [field.name for field in frame.schema["genotypes"].fields]


def _child(frame: pl.DataFrame, name: str, row: int = 0) -> list:
    """Return one genotype child's per-sample values for a single row."""
    return frame["genotypes"][row][name].to_list()


@pytest.fixture(autouse=True)
def single_partition():
    """Pin partitions for these tests without leaking the setting.

    The option is process-global, so leaving it at 1 would silently make every
    later test module run single-partition.
    """
    previous = pb.get_option(TARGET_PARTITIONS)
    pb.set_option(TARGET_PARTITIONS, "1")
    try:
        yield
    finally:
        pb.set_option(TARGET_PARTITIONS, previous)


class TestPgenRead:
    def test_read_pgen_returns_variant_metadata(self):
        frame = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert frame["id"].to_list() == ORACLE_IDS
        assert frame["chrom"].to_list() == ORACLE_CHROMS
        assert frame["ref"].to_list() == ORACLE_REFS
        assert [row.to_list() for row in frame["alt"]] == ORACLE_ALTS
        assert frame.height == 3

    def test_default_emits_only_the_gt_field(self):
        # The provider default selects all five children; the Python default
        # narrows to GT so read_pgen(path) is not the expensive call.
        frame = pb.read_pgen(str(ORACLE_PATH))
        assert _genotype_names(frame) == ["GT"]

    def test_scan_pgen_is_lazy(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        assert isinstance(lazy, pl.LazyFrame)
        assert lazy.collect().height == 3

    def test_dosage_field_reports_stored_dosages(self):
        frame = pb.read_pgen(str(DOSAGE_PATH), genotype_fields=["DS"])
        assert _genotype_names(frame) == ["DS"]
        values = _child(frame, "DS")
        assert values[3] is None
        assert values[:3] == pytest.approx(DOSAGE_DS[:3])

    def test_phase_field_accompanies_gt(self):
        frame = pb.read_pgen(str(PHASE_PATH), genotype_fields=["GT", "PHASED"])
        assert _genotype_names(frame) == ["GT", "PHASED"]
        assert _child(frame, "GT") == PHASE_GT
        assert _child(frame, "PHASED") == PHASE_PHASED

    def test_unused_alt_allele_keeps_allele_indices(self):
        frame = pb.read_pgen(str(UNUSED_ALT_PATH))
        # The PVAR declares ALT=C,G but only C is observed; indices still refer
        # to the declared allele list.
        assert [row.to_list() for row in frame["alt"]] == [["C", "G"]]
        assert _child(frame, "GT") == UNUSED_ALT_GT

    def test_requested_field_order_is_preserved(self):
        frame = pb.read_pgen(str(PHASE_PATH), genotype_fields=["PHASED", "GT"])
        assert _genotype_names(frame) == ["PHASED", "GT"]

    def test_all_five_fields_can_be_selected(self):
        frame = pb.read_pgen(
            str(DOSAGE_PATH),
            genotype_fields=["GT", "PHASED", "DS", "DS_STORED", "HDS"],
        )
        assert _genotype_names(frame) == [
            "GT",
            "PHASED",
            "DS",
            "DS_STORED",
            "HDS",
        ]


class TestPgenValidation:
    @pytest.mark.parametrize("call", [pb.read_pgen, pb.scan_pgen])
    def test_non_pgen_paths_are_rejected(self, call):
        with pytest.raises(ValueError, match=r"\.pgen"):
            call("cohort.bgen")

    def test_unknown_genotype_field_is_rejected(self):
        with pytest.raises(ValueError, match="GQ"):
            pb.read_pgen(str(ORACLE_PATH), genotype_fields=["GQ"])

    def test_empty_genotype_fields_is_rejected(self):
        with pytest.raises(ValueError, match="at least one"):
            pb.read_pgen(str(ORACLE_PATH), genotype_fields=[])
```

- [ ] **Step 4: Run the tests to verify they fail**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v
```

Expected: collection succeeds, every test FAILS with `AttributeError: module 'polars_bio' has no attribute 'read_pgen'`. If any test passes, stop — something else is satisfying it.

- [ ] **Step 5: Add the provider dependency**

In `Cargo.toml`, after the `datafusion-bio-format-bgen` line (line 62):

```toml
datafusion-bio-format-pgen = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", rev = "e029e08" }
```

Then add `datafusion-bio-format-pgen` to the `[dependencies]` section the same way `datafusion-bio-format-bgen` appears there, and refresh the lock:

```bash
cargo check --workspace 2>&1 | tail -20
grep -c 'name = "datafusion-bio-format-pgen"' Cargo.lock
```

Expected: `cargo check` clean; the `grep` prints `1`.

- [ ] **Step 6: Add `InputFormat::Pgen` and the options pyclass**

In `src/option.rs`, add `Pgen,` to the `InputFormat` enum after `Bgen,` (line ~147), and `InputFormat::Pgen => "PGEN",` to the display match after the `Bgen` arm (line ~176).

Add the pyclass after `BgenReadOptions` (after line ~910):

```rust
#[pyclass(name = "PgenReadOptions", from_py_object)]
#[derive(Clone, Debug)]
pub struct PgenReadOptions {
    pub object_storage_options: Option<ObjectStorageOptions>,
    /// Genotype children to emit, from `GT`, `PHASED`, `DS`, `DS_STORED`, `HDS`.
    #[pyo3(get, set)]
    pub genotype_fields: Option<Vec<String>>,
    /// If true, output 0-based half-open coordinates; if false, 1-based closed.
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl PgenReadOptions {
    #[new]
    #[pyo3(signature = (object_storage_options=None, genotype_fields=None, zero_based=false))]
    pub fn new(
        object_storage_options: Option<PyObjectStorageOptions>,
        genotype_fields: Option<Vec<String>>,
        zero_based: bool,
    ) -> Self {
        PgenReadOptions {
            object_storage_options: pyobject_storage_options_to_object_storage_options(
                object_storage_options,
            ),
            genotype_fields,
            zero_based,
        }
    }

    #[staticmethod]
    pub fn default() -> Self {
        PgenReadOptions {
            object_storage_options: Some(ObjectStorageOptions {
                chunk_size: Some(1024 * 1024), // 1MB
                concurrent_fetches: Some(4),
                allow_anonymous: false,
                enable_request_payer: false,
                max_retries: Some(5),
                timeout: Some(300), // 300 seconds
                compression_type: Some(CompressionType::AUTO),
            }),
            genotype_fields: None,
            zero_based: false,
        }
    }
}
```

- [ ] **Step 7: Extend `ReadOptions` — the fragile edit**

Three separate places in `src/option.rs`, all of which must change together. The `#[pyo3(signature = ...)]` list is positional: adding the struct field without extending the signature is a silent binding mismatch, not a compile error.

1. After `pub bgen_read_options: Option<BgenReadOptions>,` (line ~209):

```rust
    #[pyo3(get, set)]
    pub pgen_read_options: Option<PgenReadOptions>,
```

2. In the `#[pyo3(signature = ...)]` attribute (line ~215), append `, pgen_read_options=None` immediately before the closing `))]`.

3. In `pub fn new(...)`, add the parameter `pgen_read_options: Option<PgenReadOptions>,` after `bgen_read_options`, and `pgen_read_options,` to the struct literal.

Verify all three landed:

```bash
grep -n "pgen_read_options" src/option.rs
```

Expected: exactly four lines — the struct field declaration, the entry inside the `#[pyo3(signature = ...)]` attribute, the `new` parameter, and the struct-literal entry. Fewer than four means an edit silently did not apply. This will still compile, and the mismatch will only surface as a wrong-argument error at runtime.

- [ ] **Step 8: Register the class and import the provider**

In `src/lib.rs`, after the `datafusion_bio_format_bgen` import block (line ~23-25):

```rust
use datafusion_bio_format_pgen::{
    PgenReadOptions as NativePgenReadOptions, PgenTableProvider,
};
```

Add `PgenReadOptions` to the `use crate::option::{...}` list at line ~46, and after `m.add_class::<BgenReadOptions>()?;` (line ~958):

```rust
    m.add_class::<PgenReadOptions>()?;
```

- [ ] **Step 9: Add path detection and the registration arm**

In `src/scan.rs`, in the format-inference chain, after the `.bgen` arm (line ~490):

```rust
    } else if path.ends_with(".pgen") {
        InputFormat::Pgen
```

Then the registration arm, after the `InputFormat::Bgen` arm (line ~849):

```rust
        InputFormat::Pgen => {
            let pgen_read_options = match &read_options {
                Some(options) => match options.clone().pgen_read_options {
                    Some(pgen_read_options) => pgen_read_options,
                    _ => PgenReadOptions::default(),
                },
                _ => PgenReadOptions::default(),
            };
            info!(
                "Registering PGEN table {} with options: {:?}",
                table_name, pgen_read_options
            );
            let native_options = NativePgenReadOptions {
                genotype_fields: pgen_read_options.genotype_fields.clone(),
                coordinate_system: CoordinateSystem::from_zero_based(pgen_read_options.zero_based),
                object_storage_options: pgen_read_options.object_storage_options.clone(),
                ..Default::default()
            };
            // Opening a PGEN fileset can fail on user input, such as an absent
            // PVAR companion or an unknown genotype field, so the error is
            // returned instead of panicking inside the extension.
            let table_provider =
                PgenTableProvider::try_new(path.to_string(), native_options).await?;
            ctx.register_table(table_name, Arc::new(table_provider))?;
        },
```

Add `PgenReadOptions` to the `use crate::option::{...}` list at the top of `scan.rs`.

- [ ] **Step 10: Build and confirm the Rust side compiles**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && make install 2>&1 | tail -20
```

Expected: `maturin develop` succeeds and installs the extension.

- [ ] **Step 11: Add the Python validators**

In `polars_bio/io.py`, after `_validate_bgen_genotype_output` (line ~318):

```python
PGEN_GENOTYPE_FIELDS = ("GT", "PHASED", "DS", "DS_STORED", "HDS")


def _validate_pgen_input_path(path: str, operation: str = "read") -> None:
    """Keep the PGEN entry points format-specific."""
    if not strip_url_parameters(path).lower().endswith(".pgen"):
        raise ValueError(
            f"PGEN {operation} requires a path ending in '.pgen', got {path!r}"
        )


def _validate_pgen_genotype_fields(genotype_fields: Sequence[str]) -> None:
    if not genotype_fields:
        raise ValueError(
            "genotype_fields must name at least one of "
            f"{', '.join(PGEN_GENOTYPE_FIELDS)}"
        )
    unknown = [name for name in genotype_fields if name not in PGEN_GENOTYPE_FIELDS]
    if unknown:
        raise ValueError(
            f"unsupported PGEN genotype field(s) {unknown!r}; "
            f"available fields: {', '.join(PGEN_GENOTYPE_FIELDS)}"
        )
```

If `Sequence` is not already imported at the top of `io.py`, add it to the `typing` import.

- [ ] **Step 12: Add `read_pgen` and `scan_pgen`**

In `polars_bio/io.py`, inside `class IOOperations`, after `scan_bgen` (line ~2065):

```python
    @staticmethod
    def read_pgen(
        path: str,
        genotype_fields: Sequence[str] = ("GT",),
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.DataFrame:
        """
        Read a PLINK 2 PGEN fileset into a DataFrame.

        One row is one PVAR variant. The `.pvar` and `.psam` companions are
        discovered from the `.pgen` basename.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`. A neighbouring `.pvar` (or `.pvar.zst`) and `.psam` are discovered automatically.
            genotype_fields: Genotype children to emit, from `"GT"`, `"PHASED"`, `"DS"`, `"DS_STORED"`, and `"HDS"`, in the requested order. Defaults to `("GT",)`. Note this narrows the provider default, which emits all five.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            compression_type: The compression override. PGEN record compression is read from the file header.
            projection_pushdown: Enable column projection pushdown. Metadata-only scans do not read genotype records.
            predicate_pushdown: Push `chrom`, `id`, `start`, and `end` predicates into variant selection.
            use_zero_based: If True, output 0-based half-open coordinates. If False, output 1-based closed coordinates. If None (default), uses the global configuration.

        !!! note
            PGEN is input-only.
        """
        lf = IOOperations.scan_pgen(
            path=path,
            genotype_fields=genotype_fields,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_pgen(
        path: str,
        genotype_fields: Sequence[str] = ("GT",),
        chunk_size: int = 8,
        concurrent_fetches: int = 1,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        max_retries: int = 5,
        timeout: int = 300,
        compression_type: str = "auto",
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
    ) -> pl.LazyFrame:
        """
        Lazily read a PLINK 2 PGEN fileset into a LazyFrame.

        Projection pushdown and configured input partition parallelism are
        preserved. See `read_pgen` for the parameters.
        """
        _validate_pgen_input_path(path)
        _validate_pgen_genotype_fields(genotype_fields)
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        zero_based = _resolve_zero_based(use_zero_based)
        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=list(genotype_fields),
            zero_based=zero_based,
        )
        read_options = ReadOptions(pgen_read_options=pgen_read_options)
        return _read_file(
            path,
            InputFormat.Pgen,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )
```

Add `PgenReadOptions` to the extension-module import at the top of `io.py`, beside `BgenReadOptions`.

- [ ] **Step 13: Export the new functions**

In `polars_bio/__init__.py`, beside each corresponding `bgen` line:

```python
read_pgen = data_input.read_pgen
scan_pgen = data_input.scan_pgen
```

and add `"read_pgen"` and `"scan_pgen"` to `__all__`.

- [ ] **Step 14: Run the tests to verify they pass**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v
```

Expected: all tests in `TestPgenRead` and `TestPgenValidation` PASS.

If `test_dosage_field_reports_stored_dosages` fails on values rather than on wiring, print the actuals before changing the assertion — the expected values come from upstream's `provider_test.rs` against these exact bytes, so a mismatch means the fixture copy is wrong, not the assertion.

- [ ] **Step 15: Commit**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
make pre-commit
git add tests/data/io/pgen tests/test_pgen_io.py Cargo.toml Cargo.lock \
        src/option.rs src/lib.rs src/scan.rs polars_bio/io.py polars_bio/__init__.py
git commit -m "feat: read PGEN filesets through read_pgen and scan_pgen

genotype_fields defaults to GT alone rather than the provider default of
all five children, so read_pgen(path) is not the expensive call."
```

---

## Task 2: Sample selection and PSAM identity

Delivers `samples`, `missing_sample_policy`, and `psam_id_mode`.

**Files:**
- Modify: `src/option.rs`, `src/scan.rs`, `polars_bio/io.py`, `tests/test_pgen_io.py`

**Interfaces:**
- Consumes: `PgenReadOptions` and `scan_pgen` from Task 1
- Produces: three new `PgenReadOptions` fields — `samples: Option<Vec<String>>`, `missing_sample_policy: String`, `psam_id_mode: String`; Python parameters `samples=None`, `missing_sample_policy="error"`, `psam_id_mode="iid"`; validators `_validate_pgen_psam_id_mode`, `_validate_pgen_missing_sample_policy`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pgen_io.py`:

```python
class TestPgenSampleSelection:
    def test_samples_subset_and_reorder(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH), samples=["s8", "s2", "s1"]
        ).sort("start")
        assert len(_child(frame, "GT", 0)) == 3
        # Upstream provider_test.rs asserts s8 = [[0,0],[0,1],[0,0]] and
        # s2 = [[0,0],[0,0],[0,0]] across variants v1, v2, v3 for this
        # exact requested order.
        s8 = [_child(frame, "GT", row)[0] for row in range(3)]
        s2 = [_child(frame, "GT", row)[1] for row in range(3)]
        assert s8 == [[0, 0], [0, 1], [0, 0]]
        assert s2 == [[0, 0], [0, 0], [0, 0]]

    def test_sample_names_are_exposed_as_metadata(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        metadata = pb.get_metadata(lazy)
        assert metadata["pgen"]["sample_names"] == ORACLE_SAMPLES

    def test_selected_sample_names_follow_the_request(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH), samples=["s8", "s2", "s1"])
        metadata = pb.get_metadata(lazy)
        assert metadata["pgen"]["sample_names"] == ["s8", "s2", "s1"]

    def test_missing_sample_errors_by_default(self):
        with pytest.raises(Exception, match="nope"):
            pb.read_pgen(str(ORACLE_PATH), samples=["s1", "nope"])

    def test_missing_sample_can_be_ignored(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH),
            samples=["s1", "nope"],
            missing_sample_policy="ignore",
        )
        assert len(_child(frame, "GT", 0)) == 1


class TestPgenPsamIdMode:
    def test_default_mode_uses_iid_alone(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        assert pb.get_metadata(lazy)["pgen"]["sample_names"] == ORACLE_SAMPLES

    def test_fid_iid_mode_prefixes_the_default_fid(self):
        # These PSAM files declare only #IID, so FID defaults to "0".
        lazy = pb.scan_pgen(str(ORACLE_PATH), psam_id_mode="fid_iid")
        assert pb.get_metadata(lazy)["pgen"]["sample_names"] == [
            f"0:{name}" for name in ORACLE_SAMPLES
        ]

    def test_fid_iid_sid_mode_appends_the_default_sid(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH), psam_id_mode="fid_iid_sid")
        assert pb.get_metadata(lazy)["pgen"]["sample_names"] == [
            f"0:{name}:0" for name in ORACLE_SAMPLES
        ]

    def test_selection_uses_the_constructed_names(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH), psam_id_mode="fid_iid", samples=["0:s2"]
        )
        assert len(_child(frame, "GT", 0)) == 1

    def test_unknown_id_mode_is_rejected(self):
        with pytest.raises(ValueError, match="psam_id_mode"):
            pb.read_pgen(str(ORACLE_PATH), psam_id_mode="fid")

    def test_unknown_missing_sample_policy_is_rejected(self):
        with pytest.raises(ValueError, match="missing_sample_policy"):
            pb.read_pgen(str(ORACLE_PATH), missing_sample_policy="skip")
```

Note this task's tests depend on `metadata["pgen"]["sample_names"]`, which Task 4 implements. Until then, run only the tests that do not call `pb.get_metadata`:

```bash
uv run pytest tests/test_pgen_io.py -v -k "samples_subset or missing_sample or unknown_id_mode or unknown_missing or selection_uses"
```

The `get_metadata` tests are expected to fail until Task 4 and are re-run there.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v \
  -k "samples_subset or missing_sample or unknown_id_mode or unknown_missing or selection_uses"
```

Expected: FAIL with `TypeError: scan_pgen() got an unexpected keyword argument 'samples'`.

- [ ] **Step 3: Add the three fields to the pyclass**

In `src/option.rs`, in `struct PgenReadOptions`:

```rust
    /// Requested sample names in output order, or all samples when absent.
    #[pyo3(get, set)]
    pub samples: Option<Vec<String>>,
    /// `"error"` rejects an absent requested sample, `"ignore"` omits it.
    #[pyo3(get, set)]
    pub missing_sample_policy: String,
    /// `"iid"`, `"fid_iid"`, or `"fid_iid_sid"`.
    #[pyo3(get, set)]
    pub psam_id_mode: String,
```

Extend the `#[pyo3(signature = ...)]`, the `new` parameters, the struct literal, and `default()`:

```rust
    #[pyo3(signature = (object_storage_options=None, genotype_fields=None, zero_based=false, samples=None, missing_sample_policy="error".to_string(), psam_id_mode="iid".to_string()))]
```

with `samples: None`, `missing_sample_policy: "error".to_string()`, and `psam_id_mode: "iid".to_string()` in `default()`.

Verify:

```bash
grep -c "psam_id_mode" src/option.rs
```

Expected: at least `5`.

- [ ] **Step 4: Add the string→enum helpers**

In `src/scan.rs`, beside `bgen_output_mode` (line ~507):

```rust
fn pgen_psam_id_mode(mode: &str) -> datafusion::common::Result<PsamIdMode> {
    match mode.to_ascii_lowercase().as_str() {
        "iid" => Ok(PsamIdMode::Iid),
        "fid_iid" => Ok(PsamIdMode::FidIid),
        "fid_iid_sid" => Ok(PsamIdMode::FidIidSid),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported PGEN psam_id_mode '{mode}'. Expected 'iid', 'fid_iid', or 'fid_iid_sid'."
        ))),
    }
}

fn pgen_missing_sample_policy(
    policy: &str,
) -> datafusion::common::Result<MissingSamplePolicy> {
    match policy.to_ascii_lowercase().as_str() {
        "error" => Ok(MissingSamplePolicy::Error),
        "ignore" => Ok(MissingSamplePolicy::Ignore),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported PGEN missing_sample_policy '{policy}'. Expected 'error' or 'ignore'."
        ))),
    }
}
```

Import `PsamIdMode` from `datafusion_bio_format_pgen` and `MissingSamplePolicy` from `datafusion_bio_format_core::genotype` in `src/lib.rs` / `src/scan.rs` as the existing imports are organised.

In the `InputFormat::Pgen` arm, add to `native_options`:

```rust
                samples: pgen_read_options.samples.clone(),
                missing_sample_policy: pgen_missing_sample_policy(
                    &pgen_read_options.missing_sample_policy,
                )?,
                psam_id_mode: pgen_psam_id_mode(&pgen_read_options.psam_id_mode)?,
```

- [ ] **Step 5: Add the Python validators and parameters**

In `polars_bio/io.py`, after `_validate_pgen_genotype_fields`:

```python
PGEN_PSAM_ID_MODES = ("iid", "fid_iid", "fid_iid_sid")
PGEN_MISSING_SAMPLE_POLICIES = ("error", "ignore")


def _validate_pgen_psam_id_mode(psam_id_mode: str) -> None:
    if psam_id_mode not in PGEN_PSAM_ID_MODES:
        raise ValueError(
            "psam_id_mode must be one of "
            f"{', '.join(repr(mode) for mode in PGEN_PSAM_ID_MODES)}, "
            f"got {psam_id_mode!r}"
        )


def _validate_pgen_missing_sample_policy(missing_sample_policy: str) -> None:
    if missing_sample_policy not in PGEN_MISSING_SAMPLE_POLICIES:
        raise ValueError(
            "missing_sample_policy must be either 'error' or 'ignore', "
            f"got {missing_sample_policy!r}"
        )
```

Add to both `read_pgen` and `scan_pgen` signatures, after `genotype_fields`:

```python
        samples: Union[list[str], None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
```

with docstring entries:

```
            samples: Sample identifiers to emit, in requested order. If *None*, all samples are emitted in PSAM order.
            missing_sample_policy: `"error"` (default) rejects a requested sample name absent from the PSAM; `"ignore"` omits it from the selection.
            psam_id_mode: How selectable sample names are built from PSAM identifiers. `"iid"` (default) uses IID alone and rejects duplicates; `"fid_iid"` uses `FID:IID`; `"fid_iid_sid"` uses `FID:IID:SID`. A PSAM without FID or SID columns defaults those parts to `"0"`.
```

In `scan_pgen`, call the two new validators beside the existing ones, pass the three values through to `PgenReadOptions(...)`, and forward all three from `read_pgen` to `scan_pgen`.

- [ ] **Step 6: Rebuild and run the tests**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && make install 2>&1 | tail -5 \
  && uv run pytest tests/test_pgen_io.py -v \
     -k "samples_subset or missing_sample or unknown_id_mode or unknown_missing or selection_uses"
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
make pre-commit
git add src/option.rs src/scan.rs polars_bio/io.py tests/test_pgen_io.py
git commit -m "feat: select and reorder PGEN samples by PSAM identity

Adds samples, missing_sample_policy, and psam_id_mode. Enum options cross
the pyo3 boundary as lowercase strings, matching the BGEN precedent."
```

---

## Task 3: Companion paths and range tuning

Delivers `pvar_path`, `psam_path`, `pgi_path`, `max_range_gap`, `max_range_bytes`, and `batch_soft_byte_limit`.

**Files:**
- Modify: `src/option.rs`, `src/scan.rs`, `polars_bio/io.py`, `tests/test_pgen_io.py`

**Interfaces:**
- Consumes: `PgenReadOptions` from Tasks 1–2
- Produces: six new `PgenReadOptions` fields — `pvar_path`, `psam_path`, `pgi_path` as `Option<String>`; `max_range_gap`, `max_range_bytes` as `Option<u64>`; `batch_soft_byte_limit` as `Option<usize>`. `None` means the provider default.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pgen_io.py`:

```python
class TestPgenCompanionPaths:
    def test_explicit_companions_are_used(self):
        frame = pb.read_pgen(
            str(ORACLE_PATH),
            pvar_path=str(PGEN_DIR / "oracle.pvar"),
            psam_path=str(PGEN_DIR / "oracle.psam"),
        ).sort("start")
        assert frame["id"].to_list() == ORACLE_IDS

    def test_a_missing_companion_reports_the_path(self, tmp_path):
        orphan = tmp_path / "orphan.pgen"
        orphan.write_bytes((PGEN_DIR / "oracle.pgen").read_bytes())
        with pytest.raises(Exception, match="orphan"):
            pb.read_pgen(str(orphan))


class TestPgenRangeTuning:
    """The tuning knobs bound I/O, so they must not change what is emitted.

    Each test compares against the default read rather than against a
    hardcoded genotype matrix: the property under test is invariance, and a
    literal would only restate what Task 1 already pins down.
    """

    @pytest.mark.parametrize("gap", [0, 4096])
    def test_range_gap_does_not_change_content(self, gap):
        tuned = pb.read_pgen(str(ORACLE_PATH), max_range_gap=gap).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert tuned.equals(default)
        assert tuned["id"].to_list() == ORACLE_IDS

    def test_batch_soft_byte_limit_does_not_change_content(self):
        small = pb.read_pgen(str(ORACLE_PATH), batch_soft_byte_limit=1).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert small.equals(default)

    def test_max_range_bytes_does_not_change_content(self):
        tuned = pb.read_pgen(str(ORACLE_PATH), max_range_bytes=1).sort("start")
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        assert tuned.equals(default)
        assert tuned["id"].to_list() == ORACLE_IDS

    def test_the_invariance_check_can_actually_fail(self):
        # A comparison that cannot fail proves nothing. Confirm `.equals`
        # distinguishes two genuinely different reads before trusting the
        # three tests above.
        default = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        subset = pb.read_pgen(str(ORACLE_PATH), samples=["s1"]).sort("start")
        assert not default.equals(subset)


class TestPgenPartitions:
    @pytest.mark.parametrize("partitions", ["1", "2", "4"])
    def test_content_is_independent_of_partition_count(self, partitions):
        previous = pb.get_option(TARGET_PARTITIONS)
        pb.set_option(TARGET_PARTITIONS, partitions)
        try:
            frame = pb.read_pgen(str(ORACLE_PATH)).sort("start")
        finally:
            pb.set_option(TARGET_PARTITIONS, previous)
        assert frame["id"].to_list() == ORACLE_IDS
        assert frame.height == 3
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v \
  -k "TestPgenCompanionPaths or TestPgenRangeTuning or TestPgenPartitions"
```

Expected: FAIL with `TypeError: scan_pgen() got an unexpected keyword argument 'pvar_path'`, except `test_content_is_independent_of_partition_count`, which should already PASS from Task 1 — that one is a regression guard, not a new behavior.

- [ ] **Step 3: Add the six fields to the pyclass**

In `src/option.rs`, in `struct PgenReadOptions`:

```rust
    /// Explicit PVAR location; `.pvar` then `.pvar.zst` are tried when absent.
    #[pyo3(get, set)]
    pub pvar_path: Option<String>,
    /// Explicit PSAM location; the shared-basename `.psam` is used when absent.
    #[pyo3(get, set)]
    pub psam_path: Option<String>,
    /// Explicit external PGEN index location.
    #[pyo3(get, set)]
    pub pgi_path: Option<String>,
    /// Maximum unselected byte gap bridged by one PGEN range.
    #[pyo3(get, set)]
    pub max_range_gap: Option<u64>,
    /// Maximum size of a coalesced PGEN range.
    #[pyo3(get, set)]
    pub max_range_bytes: Option<u64>,
    /// Soft target for genotype bytes in one RecordBatch.
    #[pyo3(get, set)]
    pub batch_soft_byte_limit: Option<usize>,
```

Extend the `#[pyo3(signature = ...)]` with `, pvar_path=None, psam_path=None, pgi_path=None, max_range_gap=None, max_range_bytes=None, batch_soft_byte_limit=None`, add the matching `new` parameters and struct-literal entries, and set all six to `None` in `default()`.

- [ ] **Step 4: Map them in the registration arm**

In `src/scan.rs`, in the `InputFormat::Pgen` arm, build the native options so a `None` falls back to the provider default rather than to zero:

```rust
            let defaults = NativePgenReadOptions::default();
            let native_options = NativePgenReadOptions {
                genotype_fields: pgen_read_options.genotype_fields.clone(),
                coordinate_system: CoordinateSystem::from_zero_based(pgen_read_options.zero_based),
                object_storage_options: pgen_read_options.object_storage_options.clone(),
                samples: pgen_read_options.samples.clone(),
                missing_sample_policy: pgen_missing_sample_policy(
                    &pgen_read_options.missing_sample_policy,
                )?,
                psam_id_mode: pgen_psam_id_mode(&pgen_read_options.psam_id_mode)?,
                pvar_path: pgen_read_options.pvar_path.clone(),
                psam_path: pgen_read_options.psam_path.clone(),
                pgi_path: pgen_read_options.pgi_path.clone(),
                max_range_gap: pgen_read_options
                    .max_range_gap
                    .unwrap_or(defaults.max_range_gap),
                max_range_bytes: pgen_read_options
                    .max_range_bytes
                    .unwrap_or(defaults.max_range_bytes),
                batch_soft_byte_limit: pgen_read_options
                    .batch_soft_byte_limit
                    .unwrap_or(defaults.batch_soft_byte_limit),
                ..defaults
            };
```

Note `..defaults` replaces the earlier `..Default::default()` and reuses the same value, so the six size caps stay at their provider defaults.

- [ ] **Step 5: Add the Python parameters**

Add to both `read_pgen` and `scan_pgen` signatures, after `psam_id_mode`:

```python
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        max_range_gap: Union[int, None] = None,
        max_range_bytes: Union[int, None] = None,
        batch_soft_byte_limit: Union[int, None] = None,
```

with docstring entries:

```
            pvar_path: An explicit `.pvar` companion. A neighbouring `.pvar` then `.pvar.zst` is discovered otherwise.
            psam_path: An explicit `.psam` companion. The shared-basename `.psam` is used otherwise.
            pgi_path: An explicit `.pgi` index, for a PGEN that uses an external index.
            max_range_gap: The largest run of unselected bytes bridged when coalescing reads, in bytes. The provider default is 0, which never bridges a gap and issues one read per contiguous run of selected variants. Raising it trades wasted bytes for fewer requests, which matters most on object storage. If *None*, the provider default is used.
            max_range_bytes: The largest coalesced read, in bytes. If *None*, the provider default is used.
            batch_soft_byte_limit: A soft target for genotype bytes in one RecordBatch. If *None*, the provider default is used.
```

Pass all six through to `PgenReadOptions(...)` in `scan_pgen` and forward them from `read_pgen`.

- [ ] **Step 6: Rebuild and run the tests**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && make install 2>&1 | tail -5 \
  && uv run pytest tests/test_pgen_io.py -v \
     -k "TestPgenCompanionPaths or TestPgenRangeTuning or TestPgenPartitions"
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
make pre-commit
git add src/option.rs src/scan.rs polars_bio/io.py tests/test_pgen_io.py
git commit -m "feat: accept explicit PGEN companions and range tuning

max_range_gap defaults to 0 in the provider, so a subset scan issues one
read per contiguous run. Exposing it lets a caller trade wasted bytes for
fewer requests."
```

---

## Task 4: Registration, description, and metadata

Delivers `register_pgen`, `describe_pgen`, and `bio.pgen.*` metadata through `get_metadata`.

**Files:**
- Modify: `polars_bio/sql.py`, `polars_bio/io.py`, `polars_bio/metadata_extractors.py`, `polars_bio/__init__.py`, `tests/test_pgen_io.py`

**Interfaces:**
- Consumes: `PgenReadOptions`, `InputFormat.Pgen`, and the validators from Tasks 1–3
- Produces: `polars_bio.register_pgen(path, name=None, ...) -> None`; `polars_bio.describe_pgen(path, ...) -> pl.DataFrame`; `metadata_extractors._extract_pgen_specific_metadata` returning a dict with keys `storage_mode`, `index`, `specification_baseline`, `sample_names`, `sample_identities`, `genotype_fields`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pgen_io.py`:

```python
class TestPgenRegister:
    def test_register_pgen_creates_a_queryable_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_oracle")
        frame = pb.sql("SELECT id FROM pgen_oracle ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_oracle")

    def test_failed_registration_keeps_the_existing_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_keep")
        with pytest.raises(Exception):
            pb.register_pgen(str(ORACLE_PATH), "pgen_keep", samples=["nope"])
        frame = pb.sql("SELECT id FROM pgen_keep ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_keep")

    def test_register_pgen_rejects_an_unknown_genotype_field(self):
        with pytest.raises(ValueError, match="GQ"):
            pb.register_pgen(str(ORACLE_PATH), "pgen_bad", genotype_fields=["GQ"])

    def test_register_pgen_rejects_a_non_pgen_path(self):
        with pytest.raises(ValueError, match=r"\.pgen"):
            pb.register_pgen("cohort.bgen", "pgen_bad")


class TestPgenDescribe:
    def test_describe_pgen_reports_schema_and_properties(self):
        described = pb.describe_pgen(str(ORACLE_PATH))
        assert described["name"].to_list() == [
            "chrom",
            "start",
            "end",
            "id",
            "ref",
            "alt",
            "genotypes",
        ]
        assert described["index"][0] == "embedded"
        assert described["storage_mode"][0].startswith("0x")
        assert described["specification_baseline"][0] is not None

    def test_describe_leaves_no_table_behind(self):
        before = set(pb.list_tables()) if hasattr(pb, "list_tables") else None
        pb.describe_pgen(str(ORACLE_PATH))
        if before is not None:
            assert set(pb.list_tables()) == before

    def test_describe_does_not_disturb_a_registered_table(self):
        pb.register_pgen(str(ORACLE_PATH), "pgen_live")
        pb.describe_pgen(str(ORACLE_PATH))
        frame = pb.sql("SELECT id FROM pgen_live ORDER BY start").collect()
        assert frame["id"].to_list() == ORACLE_IDS
        ctx.deregister_table("pgen_live")


class TestPgenMetadata:
    def test_metadata_reports_index_and_storage_mode(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        pgen = pb.get_metadata(lazy)["pgen"]
        assert pgen["index"] == "embedded"
        assert pgen["storage_mode"].startswith("0x")
        assert pgen["specification_baseline"] is not None

    def test_metadata_reports_selected_genotype_fields(self):
        lazy = pb.scan_pgen(str(PHASE_PATH), genotype_fields=["GT", "PHASED"])
        assert pb.get_metadata(lazy)["pgen"]["genotype_fields"] == ["GT", "PHASED"]

    def test_metadata_reports_psam_identities(self):
        lazy = pb.scan_pgen(str(ORACLE_PATH))
        identities = pb.get_metadata(lazy)["pgen"]["sample_identities"]
        assert [identity["iid"] for identity in identities] == ORACLE_SAMPLES
```

If `pb.list_tables` does not exist, delete `test_describe_leaves_no_table_behind` rather than leaving a conditional that always passes.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v \
  -k "TestPgenRegister or TestPgenDescribe or TestPgenMetadata or sample_names or fid_iid"
```

Expected: FAIL with `AttributeError: module 'polars_bio' has no attribute 'register_pgen'` and `KeyError: 'pgen'`.

- [ ] **Step 3: Add the metadata extractor**

In `polars_bio/metadata_extractors.py`, after the `bio.bgen` dispatch (line ~117):

```python
    if any(key.startswith("bio.pgen") for key in schema_meta.keys()):
        result["pgen"] = _extract_pgen_specific_metadata(
            schema, schema_meta, field_meta
        )
```

and after `_bgen_genotype_output`:

```python
def _extract_pgen_specific_metadata(
    schema: pa.Schema, schema_meta: dict, field_meta: dict
) -> Dict[str, Any]:
    """
    Extract PGEN-specific metadata.

    PGEN has no embedded header. The provider records the storage mode, index
    provenance, and specification baseline in the schema metadata, and the
    emitted sample order and full PSAM identities in the ``genotypes`` field
    metadata.
    """
    genotypes_meta = field_meta.get("genotypes", {})

    def _json(key: str):
        raw = genotypes_meta.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    return {
        "storage_mode": schema_meta.get("bio.pgen.storage_mode"),
        "index": schema_meta.get("bio.pgen.index"),
        "specification_baseline": schema_meta.get("bio.pgen.specification_baseline"),
        "sample_names": _json("bio.genotype.sample_names"),
        "sample_identities": _json("bio.pgen.sample_identities"),
        "genotype_fields": _pgen_genotype_fields(schema),
    }


def _pgen_genotype_fields(schema: pa.Schema) -> Optional[list]:
    """Report the emitted genotype children in schema order."""
    index = schema.get_field_index("genotypes")
    if index < 0:
        return None
    genotypes = schema.field(index).type
    if not pa.types.is_struct(genotypes):
        return None
    return [
        genotypes.field(position).name for position in range(genotypes.num_fields)
    ]
```

- [ ] **Step 4: Add `register_pgen`**

In `polars_bio/sql.py`, after `register_bgen` (line ~1013):

```python
    @staticmethod
    def register_pgen(
        path: str,
        name: Union[str, None] = None,
        genotype_fields: Sequence[str] = ("GT",),
        samples: Union[list[str], None] = None,
        missing_sample_policy: str = "error",
        psam_id_mode: str = "iid",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
        pgi_path: Union[str, None] = None,
        chunk_size: int = 64,
        concurrent_fetches: int = 8,
        allow_anonymous: bool = True,
        max_retries: int = 5,
        timeout: int = 300,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        use_zero_based: Optional[bool] = None,
    ) -> None:
        """
        Register a PLINK 2 PGEN fileset as a DataFusion table.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`. Neighbouring `.pvar` and `.psam` companions are auto-discovered.
            name: The name of the table. If *None*, the name will be generated automatically from the path.
            genotype_fields: Genotype children to emit, from `"GT"`, `"PHASED"`, `"DS"`, `"DS_STORED"`, and `"HDS"`. Defaults to `("GT",)`.
            samples: Sample identifiers to register, in requested order.
            missing_sample_policy: `"error"` (default) rejects an absent requested sample; `"ignore"` omits it.
            psam_id_mode: `"iid"` (default), `"fid_iid"`, or `"fid_iid_sid"`.
            pvar_path: An explicit `.pvar` companion.
            psam_path: An explicit `.psam` companion.
            pgi_path: An explicit `.pgi` index.
            chunk_size: The size in MB of a chunk when reading from an object store.
            concurrent_fetches: The number of concurrent fetches when reading from an object store.
            allow_anonymous: Whether to allow anonymous access to object storage.
            max_retries: The maximum number of retries for reading the file from object storage.
            timeout: The timeout in seconds for reading the file from object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override.
            use_zero_based: If True, register 0-based half-open coordinates. If False, 1-based closed. If None (default), uses the global configuration.

        !!! Example
            ```python
            import polars_bio as pb
            pb.register_pgen("cohort.pgen", "cohort", genotype_fields=["DS"])
            pb.sql("SELECT id, genotypes FROM cohort WHERE chrom = '1'").collect()
            ```
        """
        _validate_pgen_input_path(path, operation="register")
        _validate_pgen_genotype_fields(genotype_fields)
        _validate_pgen_psam_id_mode(psam_id_mode)
        _validate_pgen_missing_sample_policy(missing_sample_policy)
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=chunk_size,
            concurrent_fetches=concurrent_fetches,
            max_retries=max_retries,
            timeout=timeout,
            compression_type=compression_type,
        )

        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=list(genotype_fields),
            samples=samples,
            missing_sample_policy=missing_sample_policy,
            psam_id_mode=psam_id_mode,
            pvar_path=pvar_path,
            psam_path=psam_path,
            pgi_path=pgi_path,
            zero_based=_resolve_zero_based(use_zero_based),
        )
        read_options = ReadOptions(pgen_read_options=pgen_read_options)
        py_register_table(ctx, path, name, InputFormat.Pgen, read_options)
```

Add `PgenReadOptions` to the extension import at the top of `sql.py` (line ~8) and the four `_validate_pgen_*` names to the `polars_bio.io` import (line ~36).

- [ ] **Step 5: Add `describe_pgen`**

In `polars_bio/io.py`, after `describe_bgen`:

```python
    @staticmethod
    def describe_pgen(
        path: str,
        allow_anonymous: bool = True,
        enable_request_payer: bool = False,
        compression_type: str = "auto",
        pvar_path: Union[str, None] = None,
        psam_path: Union[str, None] = None,
    ) -> pl.DataFrame:
        """
        Describe the schema a PLINK 2 PGEN fileset produces.

        PGEN has no embedded header, so instead of a field dictionary this
        returns one row per emitted column, plus the file-level properties the
        provider records in the Arrow schema metadata: the storage mode,
        whether the index is embedded or external, the specification baseline,
        and the coordinate system.

        Parameters:
            path: The path to the PGEN file. The path must end in `.pgen`.
            allow_anonymous: Whether to allow anonymous access to object storage.
            enable_request_payer: Whether to enable request payer for object storage.
            compression_type: The compression override.
            pvar_path: An explicit `.pvar` companion.
            psam_path: An explicit `.psam` companion.

        !!! note
            The reported schema is the one the default `genotype_fields=("GT",)`
            produces. Selecting other genotype fields changes the children of
            the `genotypes` struct.
        """
        _validate_pgen_input_path(path, operation="describe")
        object_storage_options = PyObjectStorageOptions(
            allow_anonymous=allow_anonymous,
            enable_request_payer=enable_request_payer,
            chunk_size=8,
            concurrent_fetches=1,
            max_retries=1,
            timeout=10,
            compression_type=compression_type,
        )
        pgen_read_options = PgenReadOptions(
            object_storage_options=object_storage_options,
            genotype_fields=["GT"],
            pvar_path=pvar_path,
            psam_path=psam_path,
            zero_based=_resolve_zero_based(None),
        )
        # Registering under the derived name would deregister and replace a
        # table the caller already registered for the same file, so describe
        # uses a private name and removes it again.
        describe_name = f"_pb_pgen_describe_{uuid4().hex}"
        table = py_register_table(
            ctx,
            path,
            describe_name,
            InputFormat.Pgen,
            ReadOptions(pgen_read_options=pgen_read_options),
        )
        try:
            schema = py_get_table_schema(ctx, table.name)
        finally:
            ctx.deregister_table(table.name)
        metadata = {
            (key.decode() if isinstance(key, bytes) else key): (
                value.decode() if isinstance(value, bytes) else value
            )
            for key, value in (schema.metadata or {}).items()
        }
        described = pl.DataFrame(
            {
                "name": [field.name for field in schema],
                "type": [str(field.type) for field in schema],
            }
        )
        properties = {
            "storage_mode": metadata.get("bio.pgen.storage_mode"),
            "index": metadata.get("bio.pgen.index"),
            "specification_baseline": metadata.get(
                "bio.pgen.specification_baseline"
            ),
            "coordinate_system_zero_based": metadata.get(
                "bio.coordinate_system_zero_based"
            ),
        }
        return described.with_columns(
            [pl.lit(value).alias(name) for name, value in properties.items()]
        )
```

- [ ] **Step 6: Export the new functions**

In `polars_bio/__init__.py`, beside the corresponding `bgen` lines:

```python
register_pgen = data_processing.register_pgen
describe_pgen = data_input.describe_pgen
```

and add `"register_pgen"` and `"describe_pgen"` to `__all__`.

- [ ] **Step 7: Run the full suite**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/test_pgen_io.py -v
```

Expected: every test PASSES, including the Task 2 `get_metadata` tests that were deferred.

- [ ] **Step 8: Run the whole test suite for regressions**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && uv run pytest tests/ \
  --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py -q 2>&1 | tail -20
```

Expected: no new failures. The `ReadOptions` positional signature change in Task 1 is the risk here — a BGEN or VCF test failing with a `TypeError` about argument count means the signature edit was wrong.

- [ ] **Step 9: Commit**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
make pre-commit
git add polars_bio/sql.py polars_bio/io.py polars_bio/metadata_extractors.py \
        polars_bio/__init__.py tests/test_pgen_io.py
git commit -m "feat: register, describe, and surface metadata for PGEN"
```

---

## Task 5: OpenSpec change and documentation

**Files:**
- Create: `openspec/changes/add-pgen-support/proposal.md`, `tasks.md`, `design.md`, `specs/pgen/spec.md`
- Modify: the reading documentation page that lists supported formats

- [ ] **Step 1: Find where formats are documented**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
grep -rln "read_bgen" docs/ mkdocs.yml 2>/dev/null
```

Note every hit — each is a place PGEN must appear too.

- [ ] **Step 2: Write the proposal**

Create `openspec/changes/add-pgen-support/proposal.md`:

```markdown
# Change: Add PGEN genotype input

## Why

PLINK 2 filesets are the working format for genome-wide association analysis
and the storage format of large biobank genotype releases. polars-bio reads
every other common variant format, so a user holding a `.pgen` must convert the
cohort before any polars-bio query, which costs a full rewrite.

## What Changes

- Add dedicated `read_pgen`, `scan_pgen`, `describe_pgen`, and `register_pgen`
  APIs backed by the upstream `datafusion-bio-format-pgen` provider.
- Emit one row per PVAR variant, with `chrom`, `start`, `end`, `id`, `ref`, and
  a list-typed `alt`, alongside a `genotypes` struct.
- Select genotype children by name through `genotype_fields`, from `GT`,
  `PHASED`, `DS`, `DS_STORED`, and `HDS`, in the requested order.
- Default `genotype_fields` to `("GT",)`, narrowing the provider default of
  every available child, so the default read is not the expensive one.
- Discover the `.pvar` (then `.pvar.zst`) and `.psam` companions from the
  `.pgen` basename, and accept explicit `pvar_path`, `psam_path`, and
  `pgi_path`.
- Build selectable sample names from PSAM identifiers under `psam_id_mode`,
  one of `iid`, `fid_iid`, or `fid_iid_sid`, and allow `samples=[...]` to
  select and reorder emitted samples under `missing_sample_policy`.
- Expose `max_range_gap`, `max_range_bytes`, and `batch_soft_byte_limit`, so a
  caller can trade wasted bytes for fewer object-storage requests. The
  provider's `max_range_gap` default of 0 never bridges a gap.
- Report storage mode, index provenance, specification baseline, emitted sample
  order, and full PSAM identities through `get_metadata`.
- Return registration errors instead of panicking, so an absent sample name
  raises rather than aborting the interpreter.

## Impact

- Affected specs: `pgen`
- Affected code: `polars_bio/io.py`, `polars_bio/sql.py`,
  `polars_bio/metadata_extractors.py`, package exports, `src/option.rs`,
  `src/scan.rs`, `src/lib.rs`, PGEN tests, and reading documentation
```

- [ ] **Step 3: Write the delta spec**

Create `openspec/changes/add-pgen-support/specs/pgen/spec.md`:

```markdown
## ADDED Requirements

### Requirement: PGEN Genotype Input

The system SHALL read PLINK 2 PGEN/PVAR/PSAM filesets through dedicated eager,
lazy, registration, and description methods, emitting one row per PVAR variant.

#### Scenario: Lazy scan with the default genotype field
- **WHEN** a user calls `scan_pgen` without a genotype field selection
- **THEN** the `genotypes` struct has exactly one child, `GT`
- **AND** each `GT` entry holds two allele indices per selected sample
- **AND** the scan remains lazy until collection.

#### Scenario: Selecting several genotype fields
- **WHEN** a user calls `read_pgen` with `genotype_fields=["GT", "PHASED"]`
- **THEN** the `genotypes` struct has those children in that order
- **AND** `PHASED` distinguishes a missing call from an unphased one.

#### Scenario: Unsupported genotype field
- **WHEN** a genotype field outside `GT`, `PHASED`, `DS`, `DS_STORED`, and
  `HDS` is given
- **THEN** the call raises `ValueError` before any file is opened.

#### Scenario: Empty genotype field selection
- **WHEN** `genotype_fields` is an empty sequence
- **THEN** the call raises `ValueError` before any file is opened.

### Requirement: PGEN Companion Discovery

The system SHALL locate the PVAR and PSAM companions from the `.pgen` basename
and SHALL accept explicit locations.

#### Scenario: Automatic discovery
- **WHEN** a `.pgen` is read with no companion paths given
- **THEN** a neighbouring `.pvar`, or `.pvar.zst` when the former is absent, and
  the shared-basename `.psam` are used.

#### Scenario: Explicit companions
- **WHEN** `pvar_path` or `psam_path` is given
- **THEN** that location is used in place of discovery.

#### Scenario: Absent companion
- **WHEN** a required companion cannot be opened
- **THEN** the error names the location that was tried.

### Requirement: PGEN Sample Selection

The system SHALL construct selectable sample names from PSAM identifiers and
SHALL emit a requested subset in the requested order.

#### Scenario: Subset and reorder
- **WHEN** `samples=["s8", "s2", "s1"]` is given
- **THEN** each genotype child holds three per-sample entries in that order.

#### Scenario: Identifier mode
- **WHEN** `psam_id_mode="fid_iid"` is given for a PSAM declaring only IID
- **THEN** selectable names take the form `0:IID`, the FID defaulting to `0`.

#### Scenario: Absent requested sample under the default policy
- **WHEN** a requested sample name is not in the PSAM
- **THEN** the call raises rather than silently emitting fewer samples.

#### Scenario: Absent requested sample under the ignore policy
- **WHEN** `missing_sample_policy="ignore"` is given
- **THEN** absent requested names are omitted from the selection
- **AND** the remaining requested names are emitted in order.

#### Scenario: Unsupported identifier mode
- **WHEN** an identifier mode outside `iid`, `fid_iid`, and `fid_iid_sid` is
  given
- **THEN** the call raises `ValueError` before any file is opened.

### Requirement: PGEN Read Coalescing Control

The system SHALL let a caller bound how PGEN byte ranges are coalesced, without
changing the emitted content.

#### Scenario: Range gap does not change content
- **WHEN** the same fileset is read with different `max_range_gap` values
- **THEN** the emitted rows and genotypes are identical.

#### Scenario: Provider defaults are preserved
- **WHEN** a tuning option is left unset
- **THEN** the provider default is used rather than a zero value.

### Requirement: PGEN Metadata Reporting

The system SHALL report file-level PGEN properties and sample identity through
schema metadata.

#### Scenario: Metadata after a scan
- **WHEN** `get_metadata` is called on a PGEN scan
- **THEN** the `pgen` entry reports storage mode, index provenance,
  specification baseline, emitted sample names, full PSAM identities, and the
  selected genotype fields.

#### Scenario: Description without disturbing a registered table
- **WHEN** `describe_pgen` is called for a file already registered under
  another name
- **THEN** the emitted columns and file properties are returned
- **AND** the previously registered table remains queryable.

### Requirement: PGEN Registration Errors

The system SHALL surface fileset and selection errors as exceptions.

#### Scenario: Failed registration preserves the existing table
- **WHEN** a registration under an existing table name fails on an absent
  sample name
- **THEN** an exception is raised
- **AND** the table previously registered under that name remains queryable.
```

- [ ] **Step 4: Write the design note and tasks**

Create `openspec/changes/add-pgen-support/design.md`:

```markdown
# Design: PGEN genotype input

Full reasoning is in
`docs/superpowers/specs/2026-08-17-pgen-polars-bio-binding-design.md`. Recorded
here are the three decisions that are not evident from the API alone.

## The Python default narrows the provider default

`resolve_genotype_fields` returns every available field when the requested list
is absent, so `PgenReadOptions::default()` emits `GT`, `PHASED`, `DS`,
`DS_STORED`, and `HDS` together. That is a defensible Rust default: explicit,
with no hidden narrowing.

It is a poor Python default. `read_pgen(path)` would decode five
representations of the same genotypes, and a benchmark run at default arguments
would report a figure no comparison corresponds to. The Python default is
therefore `("GT",)`, and the divergence is documented on the function and the
option class.

## Three range knobs are exposed; six size caps are not

`max_range_gap` defaults to 0, so no unselected gap is ever bridged when
coalescing PGEN byte ranges and a subset scan issues one read per contiguous
run. That, `max_range_bytes`, and `batch_soft_byte_limit` are the options that
move throughput on object storage, so they cross into Python.

The six remaining caps — `max_companion_bytes`,
`max_decompressed_companion_bytes`, `max_header_bytes`, `max_record_bytes`,
`max_variants`, and `max_samples` — are guardrails against malformed input
rather than tuning, and stay at their provider defaults.

An unset knob passes the provider default through, never a zero. A zero
`max_range_bytes` would disable coalescing entirely rather than mean "default".

## Enum options cross as lowercase strings

`missing_sample_policy` and `psam_id_mode` are strings rather than Python
enums, matching how BGEN passes `genotype_output` and `probability_layout`.
Conversion happens in `src/scan.rs` and an unrecognised value returns a
`DataFusionError::Plan`, so a typo surfaces as an exception rather than a
panic inside the extension.
```

Create `openspec/changes/add-pgen-support/tasks.md`:

```markdown
## 1. API and execution

- [x] 1.1 Add `PgenReadOptions` and an `InputFormat::Pgen` registration arm.
- [x] 1.2 Add `read_pgen`, `scan_pgen`, `describe_pgen`, and `register_pgen`.
- [x] 1.3 Validate the input path suffix, the genotype fields, the PSAM
  identifier mode, and the missing-sample policy.
- [x] 1.4 Default `genotype_fields` to `("GT",)`, narrowing the provider
  default of every available child.
- [x] 1.5 Accept explicit `pvar_path`, `psam_path`, and `pgi_path`, falling
  back to basename discovery.
- [x] 1.6 Pass `max_range_gap`, `max_range_bytes`, and
  `batch_soft_byte_limit` through, using the provider default when unset.
- [x] 1.7 Return registration errors rather than panicking in the extension.

## 2. Metadata

- [x] 2.1 Extract the `bio.pgen.*` schema metadata: storage mode, index
  provenance, and specification baseline.
- [x] 2.2 Report the emitted sample order and the full PSAM identities from
  the `genotypes` field metadata.
- [x] 2.3 Report the selected genotype fields from the emitted struct.

## 3. Validation

- [x] 3.1 Test variant metadata, the GT-only default, and each of the five
  genotype fields.
- [x] 3.2 Test sample selection and reordering, both missing-sample policies,
  and all three PSAM identifier modes.
- [x] 3.3 Test that explicit companions are used and that an absent companion
  names the location tried.
- [x] 3.4 Test that the range knobs do not change emitted content, and that
  the invariance check can fail.
- [x] 3.5 Test that content is independent of `target_partitions`.
- [x] 3.6 Test register and describe, including that a failed registration
  preserves the existing table.
```

- [ ] **Step 5: Validate the change**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio && openspec validate add-pgen-support --strict
```

Expected: no errors. If `openspec` is not on PATH, run it via the project's usual invocation and record the result.

- [ ] **Step 6: Update the reading documentation**

Add PGEN to each file found in Step 1, matching how BGEN is presented: the format list, the API reference entries for the four functions, and any supported-formats table.

- [ ] **Step 7: Commit**

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
make pre-commit
git add openspec/changes/add-pgen-support docs mkdocs.yml
git commit -m "docs: add the add-pgen-support OpenSpec change and reading docs"
```

---

## Verification

Before declaring the plan complete, run and paste the output of:

```bash
cd /Users/mwiewior/CLionProjects/polars-bio
uv run pytest tests/test_pgen_io.py -v
uv run pytest tests/ --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py -q
cargo clippy --all-features 2>&1 | tail -5
openspec validate add-pgen-support --strict
git log --oneline feat/bgen-pr220-bench -6
```

All five must succeed. A claim that the binding works is not supportable without the first two.

## Out of scope

Benchmarks, `plink2`, fixture generation, and the `snputils` comparison. Those
are Task 2 of `HANDOVER-pgen-benchmarks.md` and depend on this binding existing.
