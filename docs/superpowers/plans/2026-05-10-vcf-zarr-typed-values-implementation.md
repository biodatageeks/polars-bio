# VCF Zarr Typed Values Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make VCZ scans preserve typed INFO/FORMAT values and default `GT` to raw typed allele calls while keeping string `GT` available through `genotype_encoding_raw=False`.

**Architecture:** The upstream DataFusion provider owns schema inference and Arrow array materialization. polars-bio only exposes the new option and passes it into the provider. Tests first lock the provider contract, then Python tests verify the public API and Polars-visible dtypes/values.

**Tech Stack:** Rust, DataFusion Arrow arrays, zarrs V2 local stores, PyO3, Polars, pytest.

---

### Task 1: Upstream Contract Tests

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add failing schema tests**

Add Rust tests proving:
- `variant_DP` is `Int8`, `variant_DB` is `Boolean`, and 2D `variant_AF` is `List<Float32>`.
- `call_DP` is `List<Int32>`.
- default raw `call_genotype` is `List<List<Int32>>` and exposes `GT_phased` as `List<Boolean>` when `call_genotype_phased` exists.
- `genotype_encoding_raw=false` makes `GT` `List<Utf8>` and omits raw sidecar fields.

- [ ] **Step 2: Add failing collection tests**

Add Rust tests proving:
- INFO predicates and selected values use typed arrays.
- non-GT FORMAT scalar sample values use typed list arrays.
- non-GT FORMAT 3D values use nested typed lists.
- default raw `GT` uses nested typed lists.
- string `GT` mode keeps the previous `"1|0"` representation.

- [ ] **Step 3: Run targeted upstream tests and confirm RED**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_schema_preserves_typed_info_fields
```

Expected: fail because the provider still emits UTF-8 string fields and has no `genotype_encoding_raw` option.

### Task 2: Upstream Provider Implementation

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/schema.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/arrays.rs`

- [ ] **Step 1: Add `genotype_encoding_raw` to upstream read options**

Add `pub genotype_encoding_raw: bool` to `VcfZarrReadOptions`, implement `Default` manually so the default is `true`, and preserve it in `normalize_read_options`.

- [ ] **Step 2: Infer typed logical schema**

In `schema.rs`, infer Arrow types from Zarr dtype and dimensions:
- INFO 1D primitive: scalar primitive type.
- INFO 2D primitive: `List<primitive>`.
- FORMAT 2D primitive: `List<primitive>`.
- FORMAT 3D primitive: `List<List<primitive>>`.
- `GT` with `genotype_encoding_raw=false`: `List<Utf8>`.
- raw `GT` from `call_genotype`: `List<List<integer>>`, plus typed `GT_phased`/`GT_mask` sidecars when present.

- [ ] **Step 3: Build typed Arrow arrays**

In `arrays.rs`, replace INFO/FORMAT string builders for typed paths with typed array builders:
- primitive scalar arrays for 1D INFO.
- list arrays for 2D INFO and 2D FORMAT.
- nested list arrays for 3D FORMAT and raw 3D `GT`.
- keep existing string formatting path only for `GT` string mode and source string arrays.

- [ ] **Step 4: Run targeted upstream tests and fix until GREEN**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr
```

Expected: pass.

- [ ] **Step 5: Commit upstream provider changes**

Run:

```bash
git add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git commit -m "Preserve typed VCF Zarr values"
```

### Task 3: polars-bio API and Tests

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/src/option.rs`
- Modify: `/Users/mwiewior/research/git/polars-bio/src/scan.rs`
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/io.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/tests/test_vcf_zarr_io.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/Cargo.toml`
- Modify: `/Users/mwiewior/research/git/polars-bio/Cargo.lock`

- [ ] **Step 1: Add failing Python API tests**

Add pytest coverage proving:
- `scan_vcf_zarr(..., info_fields=["DP"])` returns integer `DP`.
- `scan_vcf_zarr(..., info_fields=["AF"])` returns list-of-float `AF`.
- `scan_vcf_zarr(..., format_fields=["DP"], samples=[...])` returns integer sample lists.
- default `scan_vcf_zarr(..., format_fields=["GT"])` returns raw nested allele lists for spec `call_genotype`.
- `genotype_encoding_raw=False` returns string `GT` and no raw sidecars.

- [ ] **Step 2: Run targeted Python tests and confirm RED**

Run:

```bash
uv run pytest tests/test_vcf_zarr_io.py -q
```

Expected: fail until PyO3/API forwarding and the updated upstream revision are wired in.

- [ ] **Step 3: Expose and forward `genotype_encoding_raw`**

Add `genotype_encoding_raw: bool = True` to `VcfZarrReadOptions`, `read_vcf_zarr`, and `scan_vcf_zarr`. Pass it into `NativeVcfZarrReadOptions`.

- [ ] **Step 4: Point Cargo to the new upstream git revision**

After the upstream commit, replace the `datafusion-bio-formats` dependency rev in `Cargo.toml` and refresh `Cargo.lock`.

- [ ] **Step 5: Run targeted polars-bio tests and fix until GREEN**

Run:

```bash
uv run pytest tests/test_vcf_zarr_io.py -q
cargo check
```

Expected: pass.

- [ ] **Step 6: Commit polars-bio changes**

Run:

```bash
git add src/option.rs src/scan.rs polars_bio/io.py tests/test_vcf_zarr_io.py Cargo.toml Cargo.lock docs/superpowers/plans/2026-05-10-vcf-zarr-typed-values-implementation.md openspec/changes/add-vcf-zarr-support/tasks.md
git commit -m "Expose typed VCF Zarr genotype encoding"
```

### Task 4: Final Verification

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/openspec/changes/add-vcf-zarr-support/tasks.md`

- [ ] **Step 1: Update OpenSpec task status**

Mark the typed VCZ implementation tasks complete once upstream and polars-bio tests pass.

- [ ] **Step 2: Run final validation**

Run:

```bash
openspec validate add-vcf-zarr-support --strict
uv run pytest tests/test_vcf_zarr_io.py -q
cargo check
```

Expected: pass.

- [ ] **Step 3: Commit final task status if changed**

Run:

```bash
git add openspec/changes/add-vcf-zarr-support/tasks.md
git commit -m "Mark typed VCZ tasks complete"
```

Only commit this step if `tasks.md` changed after the implementation commit.
