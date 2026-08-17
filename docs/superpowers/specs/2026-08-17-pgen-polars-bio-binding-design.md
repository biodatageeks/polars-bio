# PGEN input for polars-bio

Date: 2026-08-17
Branch: `feat/bgen-pr220-bench`
Upstream provider: `datafusion-bio-format-pgen` at `e029e08`

## Why

polars-bio reads every other common variant format. PLINK 2 filesets are absent:
`grep -c pgen Cargo.toml` is 0 while the other twelve providers are pinned. A
user holding a `.pgen` must convert the cohort before any polars-bio query.

PGEN is also the last of the three formats — BCF, BGEN, PGEN — in the pending
benchmark comparison against `snputils`. BCF and BGEN already read; PGEN blocks
that work entirely.

## Approach

Mirror the BGEN binding structurally. The provider is already a DataFusion
`TableProvider`, and every format reaches Python through the same
`InputFormat` → `ReadOptions` → `_read_file` path. No alternative architecture
was considered seriously: deviating would cost consistency and buy nothing.

The one place PGEN does not map onto BGEN is genotype representation. BGEN has
an output *mode* (`genotype_output`) and a storage *layout*
(`probability_layout`). PGEN instead selects children by name: `genotype_fields`
takes any subset of `GT`, `PHASED`, `DS`, `DS_STORED`, and `HDS`, and each
selected name becomes its own child of the `genotypes` column carrying its own
Arrow metadata. The two BGEN options collapse into one PGEN list.

## Option surface

`PgenReadOptions` upstream has eighteen fields. Twelve cross into Python:

- **Selection** — `samples`, `genotype_fields`, `missing_sample_policy`,
  `psam_id_mode`
- **Companion paths** — `pvar_path`, `psam_path`, `pgi_path`, all discovered
  from the `.pgen` basename when absent
- **Coordinates and storage** — `zero_based`, `object_storage_options`
- **Range tuning** — `max_range_gap`, `max_range_bytes`,
  `batch_soft_byte_limit`

The six hard size caps (`max_companion_bytes`, `max_decompressed_companion_bytes`,
`max_header_bytes`, `max_record_bytes`, `max_variants`, `max_samples`) stay at
their provider defaults. They are guardrails against malformed input, not
tuning.

The three range knobs are exposed deliberately, breaking symmetry with BGEN.
`max_range_gap` defaults to `0`, so no unselected gap is ever bridged when
coalescing PGEN byte ranges and a subset scan issues one read per contiguous
run. That is the knob that moves throughput, and the benchmark work this
binding unblocks cannot reach it otherwise.

### Enum options cross as strings

`missing_sample_policy` takes `"error"` or `"ignore"`; `psam_id_mode` takes
`"iid"`, `"fid_iid"`, or `"fid_iid_sid"`. This follows BGEN, which passes
`"probability"` and `"nested"` the same way. Conversion happens in `scan.rs`
helpers beside the existing `bgen_output_mode` and `bgen_probability_layout`,
returning `DataFusionError::Plan` on an unrecognised value rather than
panicking.

### `genotype_fields` defaults to `["GT"]`

`resolve_genotype_fields` in `bio-format-core` returns *every* available field
when passed `None`. A `PgenReadOptions::default()` scan therefore materialises
all five children, decoding five representations of the same genotypes.

That is a defensible Rust default — explicit, no hidden narrowing — and a poor
Python one. `read_pgen(path)` should not be the expensive call, and a benchmark
run at default arguments should correspond to something. `snputils` reads
allele counts, which is `GT` alone.

So the Python default narrows to `["GT"]` and the divergence from the Rust
default is documented on both the function and the option class.

## Components

### Rust

| File | Change |
| --- | --- |
| `Cargo.toml` | `datafusion-bio-format-pgen` pinned at `e029e08`, the thirteenth provider dep |
| `src/option.rs` | `InputFormat::Pgen` variant and `"PGEN"` display arm; `PgenReadOptions` pyclass; `pgen_read_options` field on `ReadOptions`; the positional `#[pyo3(signature = ...)]` list extended |
| `src/lib.rs` | import `PgenReadOptions as NativePgenReadOptions`, `PgenTableProvider`, `PsamIdMode`; `m.add_class::<PgenReadOptions>()` |
| `src/scan.rs` | `.pgen` suffix detection; `psam_id_mode` and `missing_sample_policy` string conversions; the `InputFormat::Pgen` registration arm |

The `#[pyo3(signature = ...)]` list enumerates every format's options
positionally. Adding a field to the struct without extending that list is a
silent binding mismatch, not a compile error.

### Python

`read_pgen`, `scan_pgen`, `register_pgen`, and `describe_pgen` — the same four
entry points BGEN has, in `io.py` and `sql.py`, exported from `__init__.py`.

Signature: `path`, `genotype_fields=["GT"]`, `samples=None`,
`missing_sample_policy="error"`, `psam_id_mode="iid"`, `pvar_path=None`,
`psam_path=None`, `pgi_path=None`, `max_range_gap=None`,
`max_range_bytes=None`, `batch_soft_byte_limit=None`, the seven object-storage
arguments, `projection_pushdown=True`, `predicate_pushdown=True`, and
`use_zero_based=None`. A `None` tuning knob means the provider default.

`_validate_pgen_input_path`, `_validate_pgen_genotype_fields`,
`_validate_pgen_psam_id_mode`, and `_validate_pgen_missing_sample_policy`
follow `_validate_bgen_input_path` at `io.py:296`, so a bad value raises
`ValueError` before any file is opened.

`metadata_extractors.py` gains `_extract_pgen_specific_metadata`, keyed off the
`bio.pgen.*` prefix: `sample_identities` (the `PGEN_SAMPLE_IDENTITIES_KEY`
field metadata), `ploidy_semantics`, `dosage_scale`, and `phase_semantics`.

## Data flow

`read_pgen` → `scan_pgen` → validators → `PgenReadOptions` → `ReadOptions` →
`_read_file(path, InputFormat.Pgen, ...)` → `py_register_table` → the
`InputFormat::Pgen` arm in `scan.rs` → `PgenTableProvider::try_new` → a
registered DataFusion table returned as a `LazyFrame`. `read_pgen` collects and
applies the coordinate system from schema metadata, as `read_bgen` does.

## Error handling

The three-file fileset is the failure mode BGEN does not have. A `.pgen` whose
`.pvar` or `.psam` is missing or unreadable must report the path that was
tried, because discovery is implicit — `.pvar` then `.pvar.zst`, then the
shared-basename `.psam`.

`PgenTableProvider::try_new` is fallible on user input: an unknown sample name,
a duplicate IID under `psam_id_mode="iid"`, an unsupported genotype field. The
registration arm propagates the error rather than unwrapping, following the
BGEN precedent at `scan.rs:844`, so a failed registration leaves any existing
table of that name intact and surfaces as a Python exception.

## Testing

Fixtures come from upstream, not from `plink2`. `datafusion/bio-format-pgen/tests/data/pgenlib/`
holds four triples of 23–58 bytes each — `dosage`, `phase`, `unused_alt`, and a
multi-sample `oracle` — copied to `tests/data/io/pgen/`. This matches BGEN,
whose committed `multisample.bgen` is 245 bytes. `plink2` is needed only for
benchmark fixtures, which are out of scope here.

`tests/test_pgen_io.py`, mirroring `test_bgen_io.py`:

- variant metadata columns and the default `GT`-only genotype child
- each of the five genotype fields, including that `DS` and `DS_STORED` differ
  on the `dosage` fixture
- sample selection and reordering against the `oracle` fixture
- `missing_sample_policy` in both directions
- `psam_id_mode` across all three modes, including duplicate-IID rejection
- phase semantics on the `phase` fixture
- content independence from `target_partitions`
- register and describe, including that a failed register preserves the
  existing table
- validation rejections for a non-`.pgen` path, an unknown genotype field, an
  unknown id mode, and an unknown missing-sample policy

Two disciplines carried from the previous session's handover, where three green
signals were misleading:

1. Every new test runs against the unwired code first and must fail for the
   reason intended. A test that passes before the feature exists is measuring
   something else.
2. Each edit is verified to have landed. A clean `cargo check` is not evidence
   a string replacement matched — the positional `#[pyo3(signature = ...)]`
   list is exactly where a missed edit compiles and misbehaves.

## OpenSpec

An `add-pgen-support` change accompanies the implementation, matching
`add-bgen-support`:

- `openspec/changes/add-pgen-support/proposal.md` — why, what changes, impact
- `openspec/changes/add-pgen-support/tasks.md` — the implementation checklist
- `openspec/changes/add-pgen-support/design.md` — the decisions above that are
  not obvious from the API: the `["GT"]` default, the exposed range knobs, and
  strings for enums
- `openspec/changes/add-pgen-support/specs/pgen/spec.md` — `## ADDED
  Requirements` with a `#### Scenario:` per requirement

`openspec validate add-pgen-support --strict` must pass.

## Out of scope

The benchmark runner, `plink2` installation, fixture generation, and the
`snputils` comparison. Those are Task 2 of the handover and depend on this
binding existing first.
