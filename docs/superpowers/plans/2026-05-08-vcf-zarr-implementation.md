# VCF Zarr Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit local-filesystem `scan_vcf_zarr` and `read_vcf_zarr` support with logical VCF schema output and DataFusion-level projection/predicate pruning.

**Architecture:** Implement the VCF Zarr DataFusion provider in `/Users/mwiewior/CLionProjects/datafusion-bio-formats` under the existing `datafusion-bio-format-vcf` crate, then integrate it into `polars-bio` through explicit Python APIs and a temporary local path dependency. Keep raw VCF Zarr arrays behind the provider; polars-bio sees the same logical VCF columns it already exposes for text/BGZF VCF.

**Tech Stack:** Rust 2024, DataFusion 50.3, Arrow 56, zarrs 0.23.x, PyO3, Polars `register_io_source`, pytest, OpenSpec.

---

## File Structure

### Upstream Checkout: `/Users/mwiewior/CLionProjects/datafusion-bio-formats`

- Modify `Cargo.toml`: add `zarrs` to workspace dependencies if used by more than one crate, otherwise keep it in the VCF crate.
- Modify `datafusion/bio-format-vcf/Cargo.toml`: add `zarrs`, `ndarray`, and test support dependencies if needed.
- Modify `datafusion/bio-format-vcf/src/lib.rs`: expose a new `zarr` module.
- Create `datafusion/bio-format-vcf/src/zarr/mod.rs`: module boundary and public exports.
- Create `datafusion/bio-format-vcf/src/zarr/metadata.rs`: open local stores, validate root metadata, read array metadata, and parse VCF Zarr dimensions.
- Create `datafusion/bio-format-vcf/src/zarr/schema.rs`: build Arrow schema and field metadata matching existing VCF provider conventions.
- Create `datafusion/bio-format-vcf/src/zarr/planning.rs`: convert DataFusion projections/filters into raw-array dependencies and candidate variant chunk ranges.
- Create `datafusion/bio-format-vcf/src/zarr/arrays.rs`: typed Zarr array wrappers, chunk/subset reads, missing/fill conversion, and dictionary lookups.
- Create `datafusion/bio-format-vcf/src/zarr/record_batch.rs`: construct Arrow arrays and `RecordBatch`es for logical VCF output.
- Create `datafusion/bio-format-vcf/src/zarr/physical_exec.rs`: `ExecutionPlan` that streams selected chunks as batches.
- Create `datafusion/bio-format-vcf/src/zarr/table_provider.rs`: `VcfZarrTableProvider` and read options.
- Create `datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`: provider integration tests.
- Create fixture directory `datafusion/bio-format-vcf/tests/data/vcf_zarr/`: generated `.vcz` stores and source VCF notes.

### polars-bio: `/Users/mwiewior/research/git/polars-bio`

- Modify `Cargo.toml`: temporarily point VCF format dependency at `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf`.
- Modify `Cargo.lock`: updated by Cargo.
- Modify `src/option.rs`: add `InputFormat::VcfZarr`, `VcfZarrReadOptions`, and a `ReadOptions` field.
- Modify `src/scan.rs`: register `VcfZarrTableProvider`.
- Modify `src/lib.rs`: export the PyO3 class and keep scan registration usable.
- Modify `polars_bio/io.py`: add `read_vcf_zarr` and `scan_vcf_zarr`.
- Modify `polars_bio/__init__.py`: export the new APIs and read-options type if public.
- Modify `polars_bio/predicate_translator.py`: reuse VCF logical column type sets for VCF Zarr.
- Modify `polars_bio/metadata_extractors.py`: accept VCF Zarr source metadata if the provider emits additional `bio.vcf.zarr.*` keys.
- Create `tests/test_vcf_zarr_io.py`: Python API, schema, projection, predicate, samples, and metadata tests.
- Modify `docs/features.md` and `docs/api.md`: document explicit VCF Zarr read/scan APIs.

---

## Task 1: Establish Branches, Fixture Commands, and Dependency Baseline

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/Cargo.toml`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/README.md`

- [ ] **Step 1: Verify both repositories are on the feature branch**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats branch --show-current
git -C /Users/mwiewior/research/git/polars-bio branch --show-current
```

Expected output:

```text
add-vcf-zarr-support
add-vcf-zarr-support
```

- [ ] **Step 2: Add upstream VCF crate dependencies**

In `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/Cargo.toml`, add these dependencies under `[dependencies]`:

```toml
zarrs = { version = "0.23.10", default-features = true }
ndarray = "0.17"
```

Keep `zarrs` in this crate first. Move it to `[workspace.dependencies]` only if another crate needs it.

- [ ] **Step 3: Add a fixture-generation note**

Append this section to `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/README.md`:

````markdown
## VCF Zarr Test Fixtures

VCF Zarr fixtures in `tests/data/vcf_zarr/` are generated from the small indexed VCF fixtures in this crate.

Example regeneration command:

```bash
python -m pip install "bio2zarr[vcf]"
mkdir -p tests/data/vcf_zarr
python -m bio2zarr vcf2zarr explode tests/multi_chrom.vcf.gz tests/data/vcf_zarr/multi_chrom.icf -p0
python -m bio2zarr vcf2zarr encode tests/data/vcf_zarr/multi_chrom.icf tests/data/vcf_zarr/multi_chrom.vcz -p0
rm -rf tests/data/vcf_zarr/multi_chrom.icf
```
````

- [ ] **Step 4: Run dependency check**

Run:

```bash
cargo check -p datafusion-bio-format-vcf
```

Expected: the crate compiles, or fails only if `zarrs` feature compatibility requires a narrower feature set. If it fails because of optional native codec toolchain requirements, switch to:

```toml
zarrs = { version = "0.23.10", default-features = false, features = ["filesystem", "ndarray", "gzip", "zstd"] }
```

and rerun the same command.

- [ ] **Step 5: Commit dependency baseline**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/Cargo.toml datafusion/bio-format-vcf/README.md Cargo.lock
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): add vcf zarr dependency baseline"
```

---

## Task 2: Create Provider Module Skeleton and Failing Metadata Tests

**Files:**
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/mod.rs`
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/metadata.rs`
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/lib.rs`
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add failing metadata tests**

Create `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`:

```rust
use datafusion_bio_format_vcf::zarr::{VcfZarrReadOptions, VcfZarrTableProvider};

#[test]
fn vcf_zarr_rejects_missing_store() {
    let result = VcfZarrTableProvider::new(
        "/definitely/not/a/store.vcz".to_string(),
        VcfZarrReadOptions::default(),
    );

    let message = result.expect_err("missing store must fail").to_string();
    assert!(
        message.contains("VCF Zarr") && message.contains("not found"),
        "unexpected error: {message}"
    );
}

#[test]
#[ignore = "requires VCF Zarr metadata parsing and fixture"]
fn vcf_zarr_requires_version_0_4() {
    let fixture = "tests/data/vcf_zarr/unsupported_version.vcz";
    let result = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default());

    let message = result
        .expect_err("unsupported vcf_zarr_version must fail")
        .to_string();
    assert!(
        message.contains("unsupported vcf_zarr_version") && message.contains("0.4"),
        "unexpected error: {message}"
    );
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_rejects_missing_store -- --nocapture
```

Expected: compile failure because `datafusion_bio_format_vcf::zarr` does not exist.

- [ ] **Step 3: Expose the `zarr` module**

In `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/lib.rs`, add:

```rust
/// VCF Zarr table provider implementation.
pub mod zarr;
```

- [ ] **Step 4: Add module skeleton**

Create `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/mod.rs`:

```rust
//! VCF Zarr support for Apache DataFusion.
//!
//! This module reads VCF Zarr 0.4 local filesystem stores and exposes them
//! through the same logical schema as the regular VCF table provider.

pub mod metadata;
pub mod table_provider;

pub use table_provider::{VcfZarrReadOptions, VcfZarrTableProvider};
```

- [ ] **Step 5: Add read options and provider constructor skeleton**

Create `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`:

```rust
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use super::metadata::VcfZarrMetadata;

#[derive(Clone, Debug, Default)]
pub struct VcfZarrReadOptions {
    pub info_fields: Option<Vec<String>>,
    pub format_fields: Option<Vec<String>>,
    pub samples: Option<Vec<String>>,
    pub coordinate_system_zero_based: bool,
}

#[derive(Debug)]
pub struct VcfZarrTableProvider {
    path: String,
    options: VcfZarrReadOptions,
    metadata: VcfZarrMetadata,
    schema: SchemaRef,
}

impl VcfZarrTableProvider {
    pub fn new(path: String, options: VcfZarrReadOptions) -> Result<Self> {
        let metadata = VcfZarrMetadata::open_local(&path)?;
        let schema = Arc::new(Schema::empty());

        Ok(Self {
            path,
            options,
            metadata,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for VcfZarrTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "VCF Zarr execution plan is not implemented yet".to_string(),
        ))
    }
}
```

- [ ] **Step 6: Add metadata opener skeleton**

Create `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/metadata.rs`:

```rust
use std::path::Path;

use datafusion::common::{DataFusionError, Result};

pub const SUPPORTED_VCF_ZARR_VERSION: &str = "0.4";

#[derive(Clone, Debug)]
pub struct VcfZarrMetadata {
    pub vcf_zarr_version: String,
}

impl VcfZarrMetadata {
    pub fn open_local(path: &str) -> Result<Self> {
        if !Path::new(path).exists() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr store not found: {path}"
            )));
        }

        Err(DataFusionError::NotImplemented(format!(
            "VCF Zarr metadata reader must validate vcf_zarr_version against supported version {SUPPORTED_VCF_ZARR_VERSION}"
        )))
    }
}
```

- [ ] **Step 7: Run missing-store test**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_rejects_missing_store -- --nocapture
```

Expected: PASS for the missing-store test.

- [ ] **Step 8: Commit skeleton**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/lib.rs datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): scaffold vcf zarr provider"
```

---

## Task 3: Generate Minimal VCF Zarr Fixtures

**Files:**
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/data/vcf_zarr/`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Generate a supported fixture**

Run from `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf`:

```bash
python -m pip install "bio2zarr[vcf]"
mkdir -p tests/data/vcf_zarr
python -m bio2zarr vcf2zarr explode tests/multi_chrom.vcf.gz tests/data/vcf_zarr/multi_chrom.icf -p0
python -m bio2zarr vcf2zarr encode tests/data/vcf_zarr/multi_chrom.icf tests/data/vcf_zarr/multi_chrom.vcz -p0
rm -rf tests/data/vcf_zarr/multi_chrom.icf
```

Expected: `tests/data/vcf_zarr/multi_chrom.vcz` exists and contains Zarr metadata files.

- [ ] **Step 2: Generate an unsupported-version fixture**

Copy the supported fixture and edit only the root group attributes:

```bash
cp -R tests/data/vcf_zarr/multi_chrom.vcz tests/data/vcf_zarr/unsupported_version.vcz
python - <<'PY'
import json
from pathlib import Path

root = Path("tests/data/vcf_zarr/unsupported_version.vcz")
attrs = root / ".zattrs"
data = json.loads(attrs.read_text())
data["vcf_zarr_version"] = "999.0"
attrs.write_text(json.dumps(data, indent=2, sort_keys=True))
PY
```

Expected: `unsupported_version.vcz/.zattrs` contains `"vcf_zarr_version": "999.0"`.

Leave `vcf_zarr_requires_version_0_4` ignored until metadata parsing is implemented.

- [ ] **Step 3: Add a supported-version test**

Append to `tests/vcf_zarr_provider_test.rs`:

```rust
#[test]
#[ignore = "requires VCF Zarr metadata parsing"]
fn vcf_zarr_accepts_version_0_4_fixture() {
    let fixture = "tests/data/vcf_zarr/multi_chrom.vcz";
    let provider = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default())
        .expect("supported fixture should open");

    assert_eq!(provider.schema().fields().len(), 0);
}
```

This assertion is intentionally temporary: it confirms metadata opening before schema construction exists.
Leave this test ignored until metadata parsing is implemented.

- [ ] **Step 4: Run fixture tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr -- --nocapture
```

Expected: unsupported-version and supported-version tests fail until real `.zattrs` parsing is implemented.
The ignored fixture tests may be run with `--ignored` to observe the expected failures, but the default
test binary should pass with the fixture-dependent tests ignored.

- [ ] **Step 5: Commit fixtures**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/tests/data/vcf_zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "test(vcf): add vcf zarr fixtures"
```

---

## Task 4: Implement VCF Zarr Metadata Parsing

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/metadata.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Expand metadata tests**

Replace the temporary supported-version test body with:

Also remove the `#[ignore = "requires VCF Zarr metadata parsing and fixture"]` attribute from
`vcf_zarr_requires_version_0_4` so unsupported-version validation is active.
Remove the `#[ignore = "requires VCF Zarr metadata parsing"]` attribute from
`vcf_zarr_accepts_version_0_4_fixture` when replacing its temporary body.

```rust
#[test]
fn vcf_zarr_accepts_version_0_4_fixture() {
    let fixture = "tests/data/vcf_zarr/multi_chrom.vcz";
    let provider = VcfZarrTableProvider::new(fixture.to_string(), VcfZarrReadOptions::default())
        .expect("supported fixture should open");

    assert!(
        provider.schema().metadata().contains_key("bio.vcf.zarr.version"),
        "schema metadata should include the VCF Zarr version"
    );
}
```

- [ ] **Step 2: Implement root `.zattrs` parsing**

Replace `metadata.rs` with:

```rust
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

pub const SUPPORTED_VCF_ZARR_VERSION: &str = "0.4";

#[derive(Clone, Debug)]
pub struct VcfZarrMetadata {
    pub root_path: PathBuf,
    pub vcf_zarr_version: String,
    pub root_attributes: HashMap<String, Value>,
}

impl VcfZarrMetadata {
    pub fn open_local(path: &str) -> Result<Self> {
        let root_path = PathBuf::from(path);
        if !root_path.exists() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr store not found: {path}"
            )));
        }

        if !root_path.is_dir() {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr path is not a directory store: {path}"
            )));
        }

        let attrs_path = root_path.join(".zattrs");
        let text = fs::read_to_string(&attrs_path).map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to read VCF Zarr root attributes at {}: {error}",
                attrs_path.display()
            ))
        })?;

        let root_attributes: HashMap<String, Value> = serde_json::from_str(&text).map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to parse VCF Zarr root attributes at {}: {error}",
                attrs_path.display()
            ))
        })?;

        let version = root_attributes
            .get("vcf_zarr_version")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "VCF Zarr root attribute 'vcf_zarr_version' is missing at {}",
                    attrs_path.display()
                ))
            })?
            .to_string();

        if version != SUPPORTED_VCF_ZARR_VERSION {
            return Err(DataFusionError::Execution(format!(
                "unsupported vcf_zarr_version '{version}' at {}; expected {SUPPORTED_VCF_ZARR_VERSION}",
                attrs_path.display()
            )));
        }

        Ok(Self {
            root_path,
            vcf_zarr_version: version,
            root_attributes,
        })
    }

    pub fn array_exists(&self, name: &str) -> bool {
        self.root_path.join(name).join(".zarray").exists()
    }
}
```

- [ ] **Step 3: Add schema metadata in provider constructor**

In `table_provider.rs`, replace `let schema = Arc::new(Schema::empty());` with:

```rust
let schema = Arc::new(Schema::new_with_metadata(
    Vec::new(),
    [(
        "bio.vcf.zarr.version".to_string(),
        metadata.vcf_zarr_version.clone(),
    )]
    .into_iter()
    .collect(),
));
```

- [ ] **Step 4: Run metadata tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr -- --nocapture
```

Expected: all metadata tests pass.

- [ ] **Step 5: Commit metadata parsing**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): validate vcf zarr metadata"
```

---

## Task 5: Build Logical Arrow Schema From VCF Zarr Metadata

**Files:**
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/schema.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/mod.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add schema assertions**

Append to `tests/vcf_zarr_provider_test.rs`:

```rust
#[test]
fn vcf_zarr_schema_exposes_logical_vcf_columns() {
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    let schema = provider.schema();
    let names: Vec<&str> = schema.fields().iter().map(|field| field.name().as_str()).collect();

    for expected in ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"] {
        assert!(
            names.contains(&expected),
            "logical schema should contain {expected}; got {names:?}"
        );
    }
}
```

- [ ] **Step 2: Expose schema module**

In `src/zarr/mod.rs`, add:

```rust
pub mod schema;
```

- [ ] **Step 3: Create schema builder**

Create `src/zarr/schema.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion_bio_format_core::COORDINATE_SYSTEM_METADATA_KEY;
use datafusion_bio_format_core::metadata::{
    VCF_FIELD_FIELD_TYPE_KEY, VCF_FIELD_FORMAT_ID_KEY, VCF_FIELD_NUMBER_KEY, VCF_FIELD_TYPE_KEY,
    VCF_FILE_FORMAT_KEY, VCF_SAMPLE_NAMES_KEY, to_json_string,
};

use super::metadata::VcfZarrMetadata;
use super::table_provider::VcfZarrReadOptions;

pub fn build_logical_schema(
    metadata: &VcfZarrMetadata,
    options: &VcfZarrReadOptions,
) -> Result<SchemaRef> {
    for required in ["variant_contig", "variant_position", "contig_id", "variant_allele"] {
        if !metadata.array_exists(required) {
            return Err(DataFusionError::Execution(format!(
                "VCF Zarr store is missing required array '{required}'"
            )));
        }
    }

    let mut fields = vec![
        Field::new("chrom", DataType::Utf8, false),
        Field::new("start", DataType::UInt32, false),
        Field::new("end", DataType::UInt32, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("ref", DataType::Utf8, true),
        Field::new("alt", DataType::Utf8, true),
        Field::new("qual", DataType::Float32, true),
        Field::new("filter", DataType::Utf8, true),
    ];

    if let Some(info_fields) = &options.info_fields {
        for name in info_fields {
            fields.push(Field::new(name, DataType::Utf8, true).with_metadata(
                [
                    (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "INFO".to_string()),
                    (VCF_FIELD_TYPE_KEY.to_string(), "String".to_string()),
                    (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                ]
                .into_iter()
                .collect(),
            ));
        }
    }

    if let Some(format_fields) = &options.format_fields {
        let genotype_children = format_fields
            .iter()
            .map(|name| {
                Field::new(name, DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true)
                    .with_metadata(
                        [
                            (VCF_FIELD_FIELD_TYPE_KEY.to_string(), "FORMAT".to_string()),
                            (VCF_FIELD_FORMAT_ID_KEY.to_string(), name.clone()),
                            (VCF_FIELD_TYPE_KEY.to_string(), "String".to_string()),
                            (VCF_FIELD_NUMBER_KEY.to_string(), ".".to_string()),
                        ]
                        .into_iter()
                        .collect(),
                    )
            })
            .collect();
        fields.push(Field::new("genotypes", DataType::Struct(genotype_children), true));
    }

    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(VCF_FILE_FORMAT_KEY.to_string(), "VCFv4".to_string());
    schema_metadata.insert(
        COORDINATE_SYSTEM_METADATA_KEY.to_string(),
        options.coordinate_system_zero_based.to_string(),
    );
    schema_metadata.insert(
        "bio.vcf.zarr.version".to_string(),
        metadata.vcf_zarr_version.clone(),
    );
    schema_metadata.insert(VCF_SAMPLE_NAMES_KEY.to_string(), to_json_string(&Vec::<String>::new()));

    Ok(Arc::new(Schema::new_with_metadata(fields, schema_metadata)))
}
```

- [ ] **Step 4: Use schema builder in provider**

In `table_provider.rs`, import and use:

```rust
use super::schema::build_logical_schema;
```

Replace the current schema construction with:

```rust
let schema = build_logical_schema(&metadata, &options)?;
```

- [ ] **Step 5: Run schema test**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_schema_exposes_logical_vcf_columns -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit schema builder**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): build vcf zarr logical schema"
```

---

## Task 6: Implement Projection Planning

**Files:**
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/planning.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/mod.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add projection planning test**

Append to `tests/vcf_zarr_provider_test.rs`:

```rust
use datafusion_bio_format_vcf::zarr::planning::ProjectionPlan;

#[test]
fn vcf_zarr_projection_plan_prunes_unneeded_arrays() {
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    let schema = provider.schema();
    let chrom = schema.index_of("chrom").unwrap();
    let start = schema.index_of("start").unwrap();
    let plan = ProjectionPlan::from_projection(&schema, Some(&vec![chrom, start]));

    assert!(plan.raw_arrays.contains("variant_contig"));
    assert!(plan.raw_arrays.contains("contig_id"));
    assert!(plan.raw_arrays.contains("variant_position"));
    assert!(!plan.raw_arrays.contains("variant_allele"));
    assert!(!plan.raw_arrays.iter().any(|name| name.starts_with("call_")));
}
```

- [ ] **Step 2: Expose planning module**

In `src/zarr/mod.rs`, add:

```rust
pub mod planning;
```

- [ ] **Step 3: Create projection planner**

Create `src/zarr/planning.rs`:

```rust
use std::collections::BTreeSet;

use datafusion::arrow::datatypes::SchemaRef;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectionPlan {
    pub projected_indices: Option<Vec<usize>>,
    pub projected_columns: BTreeSet<String>,
    pub raw_arrays: BTreeSet<String>,
}

impl ProjectionPlan {
    pub fn from_projection(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Self {
        let projected_indices = projection.cloned();
        let projected_columns: BTreeSet<String> = match projection {
            Some(indices) => indices
                .iter()
                .map(|index| schema.field(*index).name().clone())
                .collect(),
            None => schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        };

        let mut raw_arrays = BTreeSet::new();

        for column in &projected_columns {
            add_dependencies(column, &mut raw_arrays);
        }

        Self {
            projected_indices,
            projected_columns,
            raw_arrays,
        }
    }
}

fn add_dependencies(column: &str, raw_arrays: &mut BTreeSet<String>) {
    match column {
        "chrom" => {
            raw_arrays.insert("variant_contig".to_string());
            raw_arrays.insert("contig_id".to_string());
        }
        "start" => {
            raw_arrays.insert("variant_position".to_string());
        }
        "end" => {
            raw_arrays.insert("variant_position".to_string());
            raw_arrays.insert("variant_length".to_string());
            raw_arrays.insert("variant_allele".to_string());
        }
        "id" => {
            raw_arrays.insert("variant_id".to_string());
        }
        "ref" | "alt" => {
            raw_arrays.insert("variant_allele".to_string());
        }
        "qual" => {
            raw_arrays.insert("variant_quality".to_string());
        }
        "filter" => {
            raw_arrays.insert("variant_filter".to_string());
            raw_arrays.insert("filter_id".to_string());
        }
        "genotypes" => {}
        info_or_format => {
            raw_arrays.insert(format!("variant_{info_or_format}"));
            raw_arrays.insert(format!("call_{info_or_format}"));
        }
    }
}
```

- [ ] **Step 4: Store projection plan in scan path**

In `table_provider.rs`, add:

```rust
use super::planning::ProjectionPlan;
```

Inside `scan`, before returning the not-implemented error, add:

```rust
let _projection_plan = ProjectionPlan::from_projection(&self.schema, _projection);
```

This compiles the planner into the scan path without changing behavior yet.

- [ ] **Step 5: Run planner tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_projection_plan_prunes_unneeded_arrays -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit projection planner**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): plan vcf zarr array projection"
```

---

## Task 7: Implement Minimal Execution for Core Columns

**Files:**
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/arrays.rs`
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/record_batch.rs`
- Create: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/mod.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add DataFusion collect test for core projection**

Append to `tests/vcf_zarr_provider_test.rs`:

```rust
use std::sync::Arc;

use datafusion::prelude::SessionContext;

#[tokio::test]
async fn vcf_zarr_collects_core_columns() {
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start FROM vcz LIMIT 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5);
    assert_eq!(batches[0].schema().field(0).name(), "chrom");
    assert_eq!(batches[0].schema().field(1).name(), "start");
}
```

- [ ] **Step 2: Expose execution modules**

In `src/zarr/mod.rs`, add:

```rust
pub mod arrays;
pub mod physical_exec;
pub mod record_batch;
```

- [ ] **Step 3: Create array helper with a narrow first implementation**

Create `src/zarr/arrays.rs` with a first implementation that reads the required core arrays from the fixture and returns logical vectors:

```rust
use datafusion::common::{DataFusionError, Result};

use super::metadata::VcfZarrMetadata;

#[derive(Clone, Debug)]
pub struct CoreRows {
    pub chrom: Vec<String>,
    pub start: Vec<u32>,
}

pub fn read_core_rows(
    metadata: &VcfZarrMetadata,
    limit: Option<usize>,
    coordinate_system_zero_based: bool,
) -> Result<CoreRows> {
    let _ = coordinate_system_zero_based;
    let _ = metadata;
    let _ = limit;

    Err(DataFusionError::NotImplemented(
        "read_core_rows must read variant_contig, contig_id, and variant_position with zarrs"
            .to_string(),
    ))
}
```

Replace the error with real `zarrs` reads before completing this task. The required behavior is:

- open `contig_id`, `variant_contig`, and `variant_position`,
- read enough rows to satisfy `limit` when present,
- map integer contig indexes to contig strings,
- convert `variant_position` to `start` using `coordinate_system_zero_based`,
- return `CoreRows` with equal vector lengths.

- [ ] **Step 4: Create record batch builder**

Create `src/zarr/record_batch.rs`:

```rust
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

use super::arrays::CoreRows;

pub fn build_core_record_batch(schema: SchemaRef, rows: CoreRows) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(rows.chrom)),
        Arc::new(UInt32Array::from(rows.start)),
    ];
    Ok(RecordBatch::try_new(schema, arrays)?)
}
```

- [ ] **Step 5: Create physical execution plan**

Create `src/zarr/physical_exec.rs`:

```rust
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use futures::stream;

use super::arrays::read_core_rows;
use super::metadata::VcfZarrMetadata;
use super::record_batch::build_core_record_batch;
use super::table_provider::VcfZarrReadOptions;

pub struct VcfZarrExec {
    schema: SchemaRef,
    metadata: VcfZarrMetadata,
    options: VcfZarrReadOptions,
    limit: Option<usize>,
    cache: PlanProperties,
}

impl VcfZarrExec {
    pub fn new(schema: SchemaRef, metadata: VcfZarrMetadata, options: VcfZarrReadOptions, limit: Option<usize>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            metadata,
            options,
            limit,
            cache,
        }
    }
}

impl Debug for VcfZarrExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VcfZarrExec").finish()
    }
}

impl DisplayAs for VcfZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "VcfZarrExec")
    }
}

impl ExecutionPlan for VcfZarrExec {
    fn name(&self) -> &str {
        "VcfZarrExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let rows = read_core_rows(
            &self.metadata,
            self.limit,
            self.options.coordinate_system_zero_based,
        )?;
        let batch = build_core_record_batch(self.schema.clone(), rows)?;
        let stream = stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    }
}
```

- [ ] **Step 6: Return the execution plan from the provider**

In `table_provider.rs`, import:

```rust
use super::physical_exec::VcfZarrExec;
```

In `scan`, replace the `NotImplemented` return with:

```rust
Ok(Arc::new(VcfZarrExec::new(
    self.schema.clone(),
    self.metadata.clone(),
    self.options.clone(),
    _limit,
)))
```

- [ ] **Step 7: Finish `read_core_rows` with zarrs**

Update `read_core_rows` so the test in Step 1 passes. The completed function must:

- read `contig_id` as strings,
- read `variant_contig` as integer indexes,
- read `variant_position` as integer positions,
- apply the requested row limit,
- return an error if any contig index is out of range.

- [ ] **Step 8: Run core collection test**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_collects_core_columns -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit minimal execution**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): read core columns from vcf zarr"
```

---

## Task 8: Add Full Core Column Mapping and Projection-Aware Batch Construction

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/arrays.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/record_batch.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add full core query test**

Append to `tests/vcf_zarr_provider_test.rs`:

```rust
#[tokio::test]
async fn vcf_zarr_collects_all_core_columns() {
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start, end, id, ref, alt, qual, filter FROM vcz LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 3);
    assert_eq!(batches[0].num_columns(), 8);
}
```

- [ ] **Step 2: Expand row model**

Replace `CoreRows` in `arrays.rs` with:

```rust
#[derive(Clone, Debug)]
pub struct LogicalRows {
    pub chrom: Vec<String>,
    pub start: Vec<u32>,
    pub end: Vec<u32>,
    pub id: Vec<Option<String>>,
    pub ref_allele: Vec<Option<String>>,
    pub alt: Vec<Option<String>>,
    pub qual: Vec<Option<f32>>,
    pub filter: Vec<Option<String>>,
}
```

- [ ] **Step 3: Implement full core dependency reads**

Rename `read_core_rows` to `read_logical_rows` and implement these mappings:

```text
chrom  = contig_id[variant_contig[row]]
start  = variant_position[row] - 1 when zero_based else variant_position[row]
end    = start + variant_length[row] when zero_based and variant_length exists
end    = variant_position[row] + variant_length[row] - 1 when one-based and variant_length exists
id     = variant_id[row], with "." mapped to null
ref    = variant_allele[row, 0]
alt    = comma-joined variant_allele[row, 1..] after removing fill values
qual   = variant_quality[row], BCF-style missing NaN mapped to null
filter = filter_id entries where variant_filter[row, i] is true, comma joined; no true values maps to null or PASS according to existing VCF semantics
```

- [ ] **Step 4: Make batch construction projection-aware**

Change `build_core_record_batch` to:

```rust
pub fn build_logical_record_batch(
    schema: SchemaRef,
    rows: LogicalRows,
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "chrom" => Arc::new(StringArray::from(rows.chrom.clone())),
            "start" => Arc::new(UInt32Array::from(rows.start.clone())),
            "end" => Arc::new(UInt32Array::from(rows.end.clone())),
            "id" => Arc::new(StringArray::from(rows.id.clone())),
            "ref" => Arc::new(StringArray::from(rows.ref_allele.clone())),
            "alt" => Arc::new(StringArray::from(rows.alt.clone())),
            "qual" => Arc::new(datafusion::arrow::array::Float32Array::from(rows.qual.clone())),
            "filter" => Arc::new(StringArray::from(rows.filter.clone())),
            other => {
                return Err(datafusion::common::DataFusionError::Execution(format!(
                    "VCF Zarr logical column '{other}' is not implemented in batch construction"
                )));
            }
        };
        arrays.push(array);
    }

    Ok(RecordBatch::try_new(schema, arrays)?)
}
```

- [ ] **Step 5: Pass projected schema to execution plan**

In `table_provider.rs`, add a helper equivalent to the existing VCF provider:

```rust
fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(indices) if indices.is_empty() => Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
            Vec::new(),
            schema.metadata().clone(),
        )),
        Some(indices) => Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
            indices.iter().map(|index| schema.field(*index).clone()).collect::<Vec<_>>(),
            schema.metadata().clone(),
        )),
        None => schema.clone(),
    }
}
```

Use the projected schema when constructing `VcfZarrExec`.

- [ ] **Step 6: Run full core tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_collects_all_core_columns -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit full core mapping**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): map vcf zarr core columns"
```

---

## Task 9: Add INFO, FORMAT, and Sample Selection

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/schema.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/arrays.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/record_batch.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add INFO projection test**

Append:

```rust
#[tokio::test]
async fn vcf_zarr_projects_requested_info_field() {
    let ctx = SessionContext::new();
    let options = VcfZarrReadOptions {
        info_fields: Some(vec!["DP".to_string()]),
        ..VcfZarrReadOptions::default()
    };
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        options,
    )
    .expect("fixture should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start, DP FROM vcz LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches[0].schema().field(2).name(), "DP");
}
```

- [ ] **Step 2: Add sample subset test**

Append:

```rust
#[tokio::test]
async fn vcf_zarr_respects_sample_subset_in_schema_metadata() {
    let options = VcfZarrReadOptions {
        format_fields: Some(vec!["GT".to_string()]),
        samples: Some(vec!["S2".to_string(), "S1".to_string()]),
        ..VcfZarrReadOptions::default()
    };
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        options,
    )
    .expect("fixture should open");

    let samples = provider
        .schema()
        .metadata()
        .get("bio.vcf.samples")
        .expect("sample metadata should exist");
    assert!(
        samples.contains("S2") && samples.contains("S1"),
        "unexpected sample metadata: {samples}"
    );
}
```

- [ ] **Step 3: Resolve samples from `sample_id`**

In `arrays.rs` or a new helper in `schema.rs`, add:

```rust
pub fn resolve_selected_samples(
    available: &[String],
    requested: &Option<Vec<String>>,
) -> Vec<String> {
    match requested {
        None => available.to_vec(),
        Some(names) => names
            .iter()
            .filter(|name| available.iter().any(|available| available == *name))
            .cloned()
            .collect(),
    }
}
```

Use this when building schema metadata.

- [ ] **Step 4: Read INFO arrays**

Extend `LogicalRows` with:

```rust
pub info_values: std::collections::BTreeMap<String, Vec<Option<String>>>,
```

For each requested INFO field:

- read array `variant_<ID>`,
- convert scalar values to strings for the first implementation,
- map spec missing/fill sentinels to null,
- insert into `info_values` under `<ID>`.

- [ ] **Step 5: Read FORMAT arrays into nested genotypes**

Extend `LogicalRows` with:

```rust
pub format_values: std::collections::BTreeMap<String, Vec<Option<String>>>,
```

For the first implementation, represent selected FORMAT data as list/string values under `genotypes` according to the existing VCF logical schema. Keep exact typed representation aligned with current `VcfTableProvider` output for the same fixture.

- [ ] **Step 6: Extend batch construction for dynamic fields**

In `record_batch.rs`, add match arms:

```rust
other if rows.info_values.contains_key(other) => {
    Arc::new(StringArray::from(rows.info_values[other].clone()))
}
"genotypes" => {
    build_genotypes_struct_array(field, &rows.format_values)?
}
```

Implement `build_genotypes_struct_array` in the same file. It must produce an Arrow array whose data type matches the `genotypes` field created by `schema.rs`.

- [ ] **Step 7: Run dynamic field tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_projects_requested_info_field vcf_zarr_respects_sample_subset_in_schema_metadata -- --nocapture
```

Expected: both tests pass.

- [ ] **Step 8: Commit INFO/FORMAT support**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): read vcf zarr info format and samples"
```

---

## Task 10: Add Predicate and Chunk Pruning

**Files:**
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/planning.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/physical_exec.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/src/zarr/table_provider.rs`
- Modify: `/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs`

- [ ] **Step 1: Add predicate equivalence test**

Append:

```rust
#[tokio::test]
async fn vcf_zarr_filters_by_chrom_and_start() {
    let ctx = SessionContext::new();
    let provider = VcfZarrTableProvider::new(
        "tests/data/vcf_zarr/multi_chrom.vcz".to_string(),
        VcfZarrReadOptions::default(),
    )
    .expect("fixture should open");

    ctx.register_table("vcz", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT chrom, start FROM vcz WHERE chrom = 'chr1' AND start >= 1 LIMIT 10")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for batch in batches {
        assert!(batch.num_rows() <= 10);
    }
}
```

- [ ] **Step 2: Extend planning with candidate chunks**

In `planning.rs`, add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariantChunkSelection {
    pub chunk_indices: Vec<u64>,
    pub used_region_index: bool,
}
```

Add a function:

```rust
pub fn plan_variant_chunks(
    metadata: &super::metadata::VcfZarrMetadata,
    filters: &[datafusion::logical_expr::Expr],
) -> VariantChunkSelection {
    let _ = metadata;
    let _ = filters;
    VariantChunkSelection {
        chunk_indices: Vec::new(),
        used_region_index: false,
    }
}
```

Then replace the body with logic that:

- detects genomic filters using `datafusion_bio_format_core::genomic_filter`,
- reads `region_index` when present,
- otherwise reads `variant_contig` and `variant_position` chunk metadata or chunk values,
- returns candidate chunk indexes.

- [ ] **Step 3: Pass filters into execution plan**

Add `filters: Vec<Expr>` to `VcfZarrExec`, clone filters in `table_provider.rs`, and apply the resulting candidate chunk selection before reading projected arrays.

- [ ] **Step 4: Apply residual filtering**

After each batch is built, apply record-level filters by using DataFusion physical filtering if a convenient helper exists. If not, keep provider pushdown as `Inexact` and rely on DataFusion to apply residual filters above the provider. Confirm with an `EXPLAIN` query that the logical filter remains in the plan when provider-level filtering is inexact.

- [ ] **Step 5: Run predicate tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test vcf_zarr_filters_by_chrom_and_start -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit pruning**

Run:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats add datafusion/bio-format-vcf/src/zarr datafusion/bio-format-vcf/tests/vcf_zarr_provider_test.rs
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats commit -m "feat(vcf): prune vcf zarr chunks with predicates"
```

---

## Task 11: Integrate Local Provider Into polars-bio

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/Cargo.toml`
- Modify: `/Users/mwiewior/research/git/polars-bio/src/option.rs`
- Modify: `/Users/mwiewior/research/git/polars-bio/src/scan.rs`
- Modify: `/Users/mwiewior/research/git/polars-bio/src/lib.rs`
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/io.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/__init__.py`
- Create: `/Users/mwiewior/research/git/polars-bio/tests/test_vcf_zarr_io.py`

- [ ] **Step 1: Add local path dependency**

In `Cargo.toml`, replace the `datafusion-bio-format-vcf` dependency line during development with:

```toml
datafusion-bio-format-vcf = { path = "/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf" }
```

- [ ] **Step 2: Add failing Python API tests**

Create `tests/test_vcf_zarr_io.py`:

```python
from pathlib import Path

import polars as pl
import polars_bio as pb


VCF_ZARR = Path("/Users/mwiewior/CLionProjects/datafusion-bio-formats/datafusion/bio-format-vcf/tests/data/vcf_zarr/multi_chrom.vcz")


def test_scan_vcf_zarr_returns_lazyframe():
    lf = pb.scan_vcf_zarr(str(VCF_ZARR))
    assert isinstance(lf, pl.LazyFrame)


def test_read_vcf_zarr_core_columns():
    df = pb.read_vcf_zarr(str(VCF_ZARR)).select(["chrom", "start"]).head(5)
    assert df.height == 5
    assert df.columns == ["chrom", "start"]
```

- [ ] **Step 3: Run failing Python tests**

Run:

```bash
pytest tests/test_vcf_zarr_io.py -q
```

Expected: FAIL because `scan_vcf_zarr` is not exported.

- [ ] **Step 4: Add PyO3 options**

In `src/option.rs`, add enum variant:

```rust
VcfZarr,
```

Add display arm:

```rust
InputFormat::VcfZarr => "VCFZARR",
```

Add read-options struct:

```rust
#[pyclass(name = "VcfZarrReadOptions")]
#[derive(Clone, Debug)]
pub struct VcfZarrReadOptions {
    #[pyo3(get, set)]
    pub info_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub format_fields: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub samples: Option<Vec<String>>,
    #[pyo3(get, set)]
    pub zero_based: bool,
}

#[pymethods]
impl VcfZarrReadOptions {
    #[new]
    #[pyo3(signature = (info_fields=None, format_fields=None, samples=None, zero_based=true))]
    pub fn new(
        info_fields: Option<Vec<String>>,
        format_fields: Option<Vec<String>>,
        samples: Option<Vec<String>>,
        zero_based: bool,
    ) -> Self {
        Self {
            info_fields,
            format_fields,
            samples,
            zero_based,
        }
    }
}
```

Add `vcf_zarr_read_options: Option<VcfZarrReadOptions>` to `ReadOptions` and its constructor.

- [ ] **Step 5: Register provider in Rust scan code**

In `src/scan.rs`, import:

```rust
use datafusion_bio_format_vcf::zarr::{VcfZarrReadOptions as NativeVcfZarrReadOptions, VcfZarrTableProvider};
```

Add `InputFormat::VcfZarr` match arm:

```rust
InputFormat::VcfZarr => {
    let vcf_zarr_read_options = match &read_options {
        Some(options) => options
            .clone()
            .vcf_zarr_read_options
            .unwrap_or_else(|| VcfZarrReadOptions::new(None, None, None, true)),
        None => VcfZarrReadOptions::new(None, None, None, true),
    };

    let native_options = NativeVcfZarrReadOptions {
        info_fields: vcf_zarr_read_options.info_fields,
        format_fields: vcf_zarr_read_options.format_fields,
        samples: vcf_zarr_read_options.samples,
        coordinate_system_zero_based: vcf_zarr_read_options.zero_based,
    };

    let table_provider = VcfZarrTableProvider::new(path.to_string(), native_options).unwrap();
    ctx.register_table(table_name, Arc::new(table_provider))
        .expect("Failed to register VCF Zarr table");
}
```

- [ ] **Step 6: Export the PyO3 class**

In `src/lib.rs`, add `VcfZarrReadOptions` to the `use crate::option::{...}` list and to the module registration section where other option classes are registered.

- [ ] **Step 7: Add Python APIs**

In `polars_bio/io.py`, import `VcfZarrReadOptions` from `polars_bio.polars_bio`.

Add methods to `IOOperations`:

```python
    @staticmethod
    def read_vcf_zarr(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
    ) -> pl.DataFrame:
        lf = IOOperations.scan_vcf_zarr(
            path=path,
            info_fields=info_fields,
            format_fields=format_fields,
            projection_pushdown=projection_pushdown,
            predicate_pushdown=predicate_pushdown,
            use_zero_based=use_zero_based,
            samples=samples,
        )
        zero_based = lf.config_meta.get_metadata().get("coordinate_system_zero_based")
        df = lf.collect()
        if zero_based is not None:
            set_coordinate_system(df, zero_based)
        return df

    @staticmethod
    def scan_vcf_zarr(
        path: str,
        info_fields: Union[list[str], None] = None,
        format_fields: Union[list[str], None] = None,
        projection_pushdown: bool = True,
        predicate_pushdown: bool = True,
        use_zero_based: Optional[bool] = None,
        samples: Union[list[str], None] = None,
    ) -> pl.LazyFrame:
        zero_based = _resolve_zero_based(use_zero_based)
        read_options = ReadOptions(
            vcf_zarr_read_options=VcfZarrReadOptions(
                info_fields=info_fields,
                format_fields=format_fields,
                samples=samples,
                zero_based=zero_based,
            )
        )
        return _read_file(
            path,
            InputFormat.VcfZarr,
            read_options,
            projection_pushdown,
            predicate_pushdown,
            zero_based=zero_based,
        )
```

- [ ] **Step 8: Export Python APIs**

In `polars_bio/__init__.py`, add:

```python
read_vcf_zarr = data_input.read_vcf_zarr
scan_vcf_zarr = data_input.scan_vcf_zarr
```

Add `"read_vcf_zarr"` and `"scan_vcf_zarr"` to `__all__`.

- [ ] **Step 9: Run Rust build**

Run:

```bash
cargo check
```

Expected: PASS.

- [ ] **Step 10: Run Python tests**

Run:

```bash
pytest tests/test_vcf_zarr_io.py -q
```

Expected: PASS.

- [ ] **Step 11: Commit polars-bio integration**

Run:

```bash
git -C /Users/mwiewior/research/git/polars-bio add Cargo.toml Cargo.lock src/option.rs src/scan.rs src/lib.rs polars_bio/io.py polars_bio/__init__.py tests/test_vcf_zarr_io.py
git -C /Users/mwiewior/research/git/polars-bio commit -m "feat: add explicit vcf zarr scan APIs"
```

---

## Task 12: Add polars-bio Pushdown, Metadata, and Parity Tests

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/predicate_translator.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/io.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/polars_bio/metadata_extractors.py`
- Modify: `/Users/mwiewior/research/git/polars-bio/tests/test_vcf_zarr_io.py`

- [ ] **Step 1: Add parity and pushdown tests**

Append to `tests/test_vcf_zarr_io.py`:

```python
def test_scan_vcf_zarr_projection_pushdown_matches_non_projection():
    projected = pb.scan_vcf_zarr(str(VCF_ZARR), projection_pushdown=True).select(["chrom", "start"]).collect()
    unprojected = pb.scan_vcf_zarr(str(VCF_ZARR), projection_pushdown=False).select(["chrom", "start"]).collect()
    assert projected.equals(unprojected)


def test_scan_vcf_zarr_predicate_pushdown_matches_client_filter():
    predicate = pl.col("start") >= 1
    pushed = pb.scan_vcf_zarr(str(VCF_ZARR), predicate_pushdown=True).filter(predicate).select(["chrom", "start"]).collect()
    fallback = pb.scan_vcf_zarr(str(VCF_ZARR), predicate_pushdown=False).filter(predicate).select(["chrom", "start"]).collect()
    assert pushed.equals(fallback)


def test_read_vcf_zarr_preserves_coordinate_metadata():
    df = pb.read_vcf_zarr(str(VCF_ZARR), use_zero_based=True)
    meta = pb.get_metadata(df)
    assert meta["coordinate_system_zero_based"] is True
```

- [ ] **Step 2: Add VCF Zarr to format column-type map**

In `polars_bio/io.py`, add:

```python
    "VcfZarr": (VCF_STRING_COLUMNS, VCF_UINT32_COLUMNS, None),
```

to `_FORMAT_COLUMN_TYPES`. If `str(InputFormat.VcfZarr)` returns a different suffix, use that suffix.

- [ ] **Step 3: Extend metadata extraction only if needed**

If the provider emits `bio.vcf.*` schema keys, no metadata extraction change is needed. If it emits `bio.vcf.zarr.*` only, update `_extract_format_specific_metadata` so keys starting with `bio.vcf.zarr` still populate `result["vcf"]`.

Use this condition:

```python
    if any(key.startswith("bio.vcf") for key in schema_meta.keys()):
        result["vcf"] = _extract_vcf_specific_metadata(schema, schema_meta, field_meta)
```

This already matches `bio.vcf.zarr.*`; do not change it unless tests prove otherwise.

- [ ] **Step 4: Run targeted tests**

Run:

```bash
pytest tests/test_vcf_zarr_io.py -q
pytest tests/test_vcf_projection_pushdown.py -q
pytest tests/test_vcf_read_options.py -q
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit tests and metadata polish**

Run:

```bash
git -C /Users/mwiewior/research/git/polars-bio add polars_bio/io.py polars_bio/predicate_translator.py polars_bio/metadata_extractors.py tests/test_vcf_zarr_io.py
git -C /Users/mwiewior/research/git/polars-bio commit -m "test: cover vcf zarr pushdown and metadata"
```

---

## Task 13: Add Documentation

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/docs/features.md`
- Modify: `/Users/mwiewior/research/git/polars-bio/docs/api.md`

- [ ] **Step 1: Add feature docs**

In `docs/features.md`, add a VCF Zarr subsection near the existing VCF format support content:

```markdown
### VCF Zarr

polars-bio supports explicit local-filesystem reads of VCF Zarr 0.4 stores through `scan_vcf_zarr` and `read_vcf_zarr`.

```python
import polars as pl
import polars_bio as pb

lf = pb.scan_vcf_zarr("cohort.vcz")
df = (
    lf.filter((pl.col("chrom") == "chr1") & (pl.col("start") >= 1_000_000))
      .select(["chrom", "start", "ref", "alt"])
      .collect()
)
```

The VCF Zarr reader exposes the same logical schema as `scan_vcf`. It supports column projection, genomic predicate pruning, INFO/FORMAT field selection, and sample subset selection. The first implementation is read/scan only and supports local filesystem stores.
```

- [ ] **Step 2: Add API docs**

In `docs/api.md`, add `scan_vcf_zarr` and `read_vcf_zarr` beside the other I/O APIs using the existing mkdocstrings pattern used in the file.

- [ ] **Step 3: Run docs-adjacent tests**

Run:

```bash
pytest tests/test_vcf_zarr_io.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit docs**

Run:

```bash
git -C /Users/mwiewior/research/git/polars-bio add docs/features.md docs/api.md
git -C /Users/mwiewior/research/git/polars-bio commit -m "docs: document vcf zarr scanning"
```

---

## Task 14: Final Validation and Dependency Cleanup

**Files:**
- Modify: `/Users/mwiewior/research/git/polars-bio/Cargo.toml`
- Modify: `/Users/mwiewior/research/git/polars-bio/Cargo.lock`
- Modify: `/Users/mwiewior/research/git/polars-bio/openspec/changes/add-vcf-zarr-support/tasks.md`

- [ ] **Step 1: Run upstream VCF tests**

Run:

```bash
cargo test -p datafusion-bio-format-vcf --test vcf_zarr_provider_test -- --nocapture
cargo test -p datafusion-bio-format-vcf --test projection_pushdown_test -- --nocapture
cargo test -p datafusion-bio-format-vcf --test format_columns_test -- --nocapture
```

Expected: all pass.

- [ ] **Step 2: Run polars-bio Rust checks**

Run from `/Users/mwiewior/research/git/polars-bio`:

```bash
cargo check
```

Expected: PASS.

- [ ] **Step 3: Run polars-bio Python tests**

Run:

```bash
pytest tests/test_vcf_zarr_io.py -q
pytest tests/test_vcf_projection_pushdown.py -q
pytest tests/test_vcf_format_columns.py -q
pytest tests/test_vcf_parsing.py -q
pytest tests/test_vcf_read_options.py -q
```

Expected: all pass.

- [ ] **Step 4: Validate OpenSpec**

Run:

```bash
openspec validate add-vcf-zarr-support --strict
```

Expected:

```text
Change 'add-vcf-zarr-support' is valid
```

- [ ] **Step 5: Replace local path dependency before review**

After the upstream `datafusion-bio-formats` branch is pushed, replace the local path dependency in `polars-bio/Cargo.toml` with a publishable branch dependency:

```toml
datafusion-bio-format-vcf = { git = "https://github.com/biodatageeks/datafusion-bio-formats.git", branch = "add-vcf-zarr-support" }
```

Record the upstream commit SHA in the PR description by running:

```bash
git -C /Users/mwiewior/CLionProjects/datafusion-bio-formats rev-parse HEAD
```

- [ ] **Step 6: Update OpenSpec task checklist**

In `openspec/changes/add-vcf-zarr-support/tasks.md`, mark completed tasks with `- [x]` only after the implementation and validation above have passed.

- [ ] **Step 7: Commit cleanup**

Run:

```bash
git -C /Users/mwiewior/research/git/polars-bio add Cargo.toml Cargo.lock openspec/changes/add-vcf-zarr-support/tasks.md
git -C /Users/mwiewior/research/git/polars-bio commit -m "chore: finalize vcf zarr dependency reference"
```

---

## Self-Review

- Spec coverage: the plan covers explicit APIs, local filesystem scope, VCF Zarr 0.4 validation, logical VCF schema, projection pruning, genomic predicate chunk pruning with region-index and fallback strategy, sample subset selection, metadata, docs, and validation.
- Provider location: the plan implements the provider in `/Users/mwiewior/CLionProjects/datafusion-bio-formats` and integrates it into polars-bio through a local path dependency during development.
- Risk handling: unsupported metadata, missing stores, missing arrays, and unsupported remote paths are covered by metadata/schema/provider tasks.
- Type consistency: `VcfZarrReadOptions`, `VcfZarrTableProvider`, `VcfZarrExec`, `ProjectionPlan`, and `LogicalRows` names are used consistently across tasks.
