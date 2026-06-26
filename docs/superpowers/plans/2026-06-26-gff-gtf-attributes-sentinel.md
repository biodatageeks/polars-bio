# GFF/GTF `attributes` Sentinel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the GFF/GTF readers emit the nested `attributes` `List<Struct>` column *and* flattened attribute fields from a single registration, so polars-bio queries that mix raw `attributes` with parsed fields return correct results instead of raising.

**Architecture:** Treat the literal name `"attributes"` as a sentinel inside the reader's Mode 2 (`attr_fields=Some`). When present, the schema emits a nested `List<Struct{tag,value}>` column for it (instead of `Utf8`), and the flattened loader populates that slot with the fully-parsed attribute struct array. polars-bio then requests `attr_fields = parsed_fields + ["attributes"]` whenever a query needs both representations, and its existing pushdown/fallback pipeline resolves every column by name.

**Tech Stack:** Rust (DataFusion, Arrow) in `datafusion-bio-formats`; Python + PyO3 (`maturin`) in `polars-bio`; pytest; cargo test.

## Global Constraints

- Upstream branch base: rev `94d4d6811eaa8d967ae2ae99485a91ca80d82e6d` (the rev polars-bio pins today), to isolate this feature from unrelated `master` changes.
- Mirror every reader change in BOTH `datafusion/bio-format-gff` and `datafusion/bio-format-gtf`.
- Do NOT change behavior of `attr_fields=None` (Mode 1) or of Mode 2 without the sentinel. The change must be purely additive.
- The sentinel name is exactly the string `"attributes"` (matches polars-bio's `STATIC`/raw column name).
- GFF attribute parsing URL-decodes (`%3B %3D %26 %2C %09`); GTF does not. Reuse each crate's existing `parse_*_attributes_to_vec` — do not reimplement parsing.
- The nested `List<Struct>` produced for the sentinel must be byte-identical to that crate's Mode 1 `attributes` column (same struct fields `tag: Utf8 non-null`, `value: Utf8 nullable`).
- Out of scope: the unimplemented remote-GFF attribute path (`get_remote_gff_stream` appends nulls today — leave as-is); the `object_storage_options.unwrap()` panic; DataFusion pushdown of `List<Struct>` predicates.
- Local clone for dev: `/Users/mwiewior/research/git/datafusion-bio-formats` (workspace containing all `bio-format-*` crates).
- polars-bio build: `maturin develop --release` after any Rust dependency change.

---

## File Structure

**Upstream `datafusion-bio-formats` (branch `feat/attributes-sentinel-in-flattened` off `94d4d68`):**
- `datafusion/bio-format-gff/src/table_provider.rs` — `determine_schema_on_demand` Mode 2 loop (sentinel field type).
- `datafusion/bio-format-gff/src/physical_exec.rs` — `set_attribute_builders` (sentinel builder) + `load_attributes_unnest_from_string` (sentinel slot fill).
- `datafusion/bio-format-gff/tests/attribute_projection_test.rs` — new GFF sentinel tests.
- `datafusion/bio-format-gtf/src/table_provider.rs` — same Mode 2 loop change.
- `datafusion/bio-format-gtf/src/physical_exec.rs` — `set_attribute_builders` + `load_attributes_unnest_from_string` (GTF tuple shape `.1`).
- `datafusion/bio-format-gtf/tests/gtf_read_test.rs` — new GTF sentinel tests.

**`polars-bio` (branch `feat/gff-gtf-attributes-sentinel` off the PR branch `issue-396-gff-gtf-predicate-fallback`):**
- `Cargo.toml` — dependency source for the ten `datafusion-bio-format-*` crates (path override for dev → commit SHA pin at the end).
- `polars_bio/io.py` — `AnnotationLazyFrameWrapper.select()` `_attr` computation (replace the `NotImplementedError`).
- `tests/test_filter_select_attributes_bug_fix.py` — rewrite the two interim "raises" tests; add positive GFF tests.
- `tests/test_io_gtf.py` — add positive GTF test.

---

## PHASE A — Upstream reader (`datafusion-bio-formats`)

### Task 1: GFF — sentinel schema, builder, and loader

**Files:**
- Modify: `datafusion/bio-format-gff/src/table_provider.rs` (`determine_schema_on_demand`, Mode 2 `Some(attrs)` arm ~lines 66–87)
- Modify: `datafusion/bio-format-gff/src/physical_exec.rs` (`set_attribute_builders` lines 167–178; `load_attributes_unnest_from_string` final loop lines 272–289)
- Test: `datafusion/bio-format-gff/tests/attribute_projection_test.rs`

**Interfaces:**
- Consumes: existing `parse_gff_attributes_to_vec(&str) -> Vec<Attribute>` (physical_exec.rs:181), `OptionalField::new(&DataType, usize)`, `OptionalField::append_array_struct(Vec<Attribute>)` (core table_utils.rs:171).
- Produces: a `GffTableProvider` registered with `attr_fields = Some(vec!["ID","attributes"])` whose schema has `ID: Utf8` at index 8 and `attributes: List<Struct{tag,value}>` at index 9, both populated correctly.

- [ ] **Step 1: Create the branch off the pinned rev**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-formats
git fetch origin
git checkout -b feat/attributes-sentinel-in-flattened 94d4d6811eaa8d967ae2ae99485a91ca80d82e6d
```

- [ ] **Step 2: Write the failing GFF test**

Append to `datafusion/bio-format-gff/tests/attribute_projection_test.rs` (reuses `create_test_gff_file_with_attributes`, `create_object_storage_options` already in that file; add any missing `use` for `DataType`):

```rust
#[tokio::test]
async fn test_attributes_sentinel_with_flattened_field(
) -> Result<(), Box<dyn std::error::Error>> {
    use datafusion::arrow::array::{Array, ListArray, StringArray};
    use datafusion::arrow::datatypes::DataType;

    let file_path = create_test_gff_file_with_attributes().await?;
    let table = GffTableProvider::new(
        file_path.clone(),
        Some(vec!["ID".to_string(), "attributes".to_string()]),
        Some(create_object_storage_options()),
        true,
    )?;

    // Schema: 8 static + ID(Utf8) + attributes(List<Struct>)
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 10);
    assert_eq!(schema.field(8).name(), "ID");
    assert_eq!(schema.field(8).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(9).name(), "attributes");
    assert!(matches!(schema.field(9).data_type(), DataType::List(_)));

    let ctx = SessionContext::new();
    ctx.register_table("g", Arc::new(table))?;
    let results = ctx
        .sql("SELECT \"ID\", attributes FROM g")
        .await?
        .collect()
        .await?;
    let batch = &results[0];

    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id.value(0), "gene1");

    let attrs = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    // Row 0 has a non-null, non-empty nested attributes list
    assert!(attrs.is_valid(0));
    assert!(attrs.value(0).len() >= 2);
    Ok(())
}
```

- [ ] **Step 3: Run the test, verify it fails**

Run: `cd /Users/mwiewior/research/git/datafusion-bio-formats && cargo test -p datafusion-bio-format-gff test_attributes_sentinel_with_flattened_field -- --nocapture`
Expected: FAIL — schema field 9 is `Utf8` (not `List`) and/or the `attributes` column is all-null.

- [ ] **Step 4: Implement the schema sentinel**

In `table_provider.rs`, replace the Mode 2 `Some(attrs)` loop body:

```rust
        Some(attrs) => {
            // Mode 2: Projection - add requested attributes as individual columns
            // All GFF attributes are treated as nullable strings for simplicity
            for attr_name in &attrs {
                if attr_name == "attributes" {
                    // Sentinel: emit the nested attributes column alongside
                    // flattened fields (identical to Mode 1's attributes field).
                    fields.push(Field::new(
                        "attributes",
                        DataType::List(FieldRef::from(Box::new(Field::new(
                            "item",
                            DataType::Struct(Fields::from(vec![
                                Field::new("tag", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ])),
                            true,
                        )))),
                        true,
                    ));
                } else {
                    fields.push(Field::new(
                        attr_name,
                        DataType::Utf8, // All GFF attributes are strings
                        true,           // Attributes can be null
                    ));
                }
            }
            debug!(
                "GFF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
```

- [ ] **Step 5: Implement the builder sentinel**

In `physical_exec.rs`, replace `set_attribute_builders`:

```rust
fn set_attribute_builders(
    batch_size: usize,
    attr_fields: &[String],
    attribute_builders: &mut (Vec<String>, Vec<DataType>, Vec<OptionalField>),
) {
    for attr_name in attr_fields {
        if attr_name == "attributes" {
            // Sentinel: a nested List<Struct> builder (matches the Mode 1 builder).
            let dt = DataType::List(FieldRef::new(Field::new(
                "attribute",
                DataType::Struct(Fields::from(vec![
                    Field::new("tag", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                true,
            )));
            let field = OptionalField::new(&dt, batch_size).unwrap();
            attribute_builders.0.push(attr_name.clone());
            attribute_builders.1.push(dt);
            attribute_builders.2.push(field);
        } else {
            let field = OptionalField::new(&DataType::Utf8, batch_size).unwrap();
            attribute_builders.0.push(attr_name.clone());
            attribute_builders.1.push(DataType::Utf8);
            attribute_builders.2.push(field);
        }
    }
}
```

- [ ] **Step 6: Implement the loader sentinel**

In `physical_exec.rs`, replace the final `for i in 0..attribute_builders.2.len()` loop inside `load_attributes_unnest_from_string` (lines 272–289):

```rust
    for i in 0..attribute_builders.2.len() {
        if let Some(indices) = &projected_attribute_indices
            && !indices.contains(&i)
        {
            attribute_builders.2[i].append_null()?;
            continue;
        }

        if attribute_builders.0[i] == "attributes" {
            // Sentinel slot: append the fully-parsed nested attributes struct.
            let attributes = parse_gff_attributes_to_vec(attributes_str);
            attribute_builders.2[i].append_array_struct(attributes)?;
            continue;
        }

        if let Some(value) = attributes_map.get(&attribute_builders.0[i]) {
            attribute_builders.2[i].append_string(value)?;
        } else {
            attribute_builders.2[i].append_null()?;
        }
    }
```

- [ ] **Step 7: Run the test, verify it passes**

Run: `cargo test -p datafusion-bio-format-gff test_attributes_sentinel_with_flattened_field -- --nocapture`
Expected: PASS.

- [ ] **Step 8: Run the full GFF crate test suite (no regressions)**

Run: `cargo test -p datafusion-bio-format-gff`
Expected: PASS (existing Mode 1 / Mode 2 tests unchanged).

- [ ] **Step 9: Commit**

```bash
git add datafusion/bio-format-gff/src/table_provider.rs \
        datafusion/bio-format-gff/src/physical_exec.rs \
        datafusion/bio-format-gff/tests/attribute_projection_test.rs
git commit -m "feat(gff): emit nested attributes alongside flattened fields via 'attributes' sentinel"
```

---

### Task 2: GFF — confirm sentinel on the indexed path

**Files:**
- Test: `datafusion/bio-format-gff/tests/indexed_read_test.rs`

**Interfaces:**
- Consumes: the Task 1 changes (the indexed stream `get_indexed_gff_stream` calls the same `set_attribute_builders` + `load_attributes_unnest_from_string`, so it should work without further edits).
- Produces: proof the sentinel works through the indexed (`.tbi`) reader.

- [ ] **Step 1: Write a failing/guard test on the indexed path**

Append to `datafusion/bio-format-gff/tests/indexed_read_test.rs` a test that registers the bundled `multi_chrom.gff3.gz` provider with `attr_fields = Some(vec!["ID".into(), "attributes".into()])`, runs a query that triggers the indexed stream (the same pattern existing tests in that file use), and asserts `attributes` is a `List` column and non-null for at least one row, while `ID` is a populated `Utf8`. Match the existing file's provider-construction and region-query helpers (read the top of the file first and copy its setup verbatim, substituting the `attr_fields` argument).

- [ ] **Step 2: Run it**

Run: `cargo test -p datafusion-bio-format-gff --test indexed_read_test`
Expected: PASS (Task 1 already routes indexed attribute loading through the shared helper). If it FAILS because the indexed stream parses attributes differently, fix `get_indexed_gff_stream` (physical_exec.rs ~1391–1403) to call the same loader, then re-run.

- [ ] **Step 3: Commit**

```bash
git add datafusion/bio-format-gff/tests/indexed_read_test.rs
git commit -m "test(gff): cover attributes sentinel on the indexed read path"
```

---

### Task 3: GTF — sentinel schema, builder, and loader

**Files:**
- Modify: `datafusion/bio-format-gtf/src/table_provider.rs` (`determine_schema_on_demand`, Mode 2 `Some(attrs)` arm)
- Modify: `datafusion/bio-format-gtf/src/physical_exec.rs` (`set_attribute_builders` lines 366–376; `load_attributes_unnest_from_string` final loop lines 437–450)
- Test: `datafusion/bio-format-gtf/tests/gtf_read_test.rs`

**Interfaces:**
- Consumes: existing `parse_gtf_attributes_to_vec(&str) -> Vec<Attribute>` (physical_exec.rs:378), `OptionalField::append_array_struct`. Note GTF's `attribute_builders` is a **2-tuple** `(Vec<String>, Vec<OptionalField>)` — builders are `.1` (not `.2`).
- Produces: a `GtfTableProvider` registered with `attr_fields = Some(vec!["gene_id","attributes"])` whose schema has `gene_id: Utf8` then `attributes: List<Struct>`, both populated. Works for local + remote + indexed (all share `GtfBatchCollector`).

- [ ] **Step 1: Write the failing GTF test**

Append to `datafusion/bio-format-gtf/tests/gtf_read_test.rs` (reuses `test_gtf_path`, `setup_ctx`; uses `gene_id` which exists in `tests/test.gtf`):

```rust
#[tokio::test]
async fn test_gtf_attributes_sentinel_with_flattened_field() {
    use datafusion::arrow::array::{Array, ListArray, StringArray};
    use datafusion::arrow::datatypes::DataType;

    let table = GtfTableProvider::new(
        test_gtf_path(),
        Some(vec!["gene_id".to_string(), "attributes".to_string()]),
        None,
        true,
    )
    .unwrap();

    let schema = table.schema();
    assert_eq!(schema.fields().len(), 10);
    assert_eq!(schema.field(8).name(), "gene_id");
    assert_eq!(schema.field(8).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(9).name(), "attributes");
    assert!(matches!(schema.field(9).data_type(), DataType::List(_)));

    let ctx = SessionContext::new();
    ctx.register_table("gtf", Arc::new(table)).unwrap();
    let results = ctx
        .sql("SELECT \"gene_id\", attributes FROM gtf LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &results[0];

    let gene_id = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!gene_id.value(0).is_empty());

    let attrs = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(attrs.is_valid(0));
    assert!(attrs.value(0).len() >= 1);
}
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `cargo test -p datafusion-bio-format-gtf test_gtf_attributes_sentinel_with_flattened_field -- --nocapture`
Expected: FAIL — field 9 is `Utf8` / `attributes` is null.

- [ ] **Step 3: Implement the GTF schema sentinel**

In `table_provider.rs`, replace the Mode 2 `Some(attrs)` loop:

```rust
        Some(attrs) => {
            for attr_name in &attrs {
                if attr_name == "attributes" {
                    fields.push(Field::new(
                        "attributes",
                        DataType::List(FieldRef::from(Box::new(Field::new(
                            "item",
                            DataType::Struct(Fields::from(vec![
                                Field::new("tag", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ])),
                            true,
                        )))),
                        true,
                    ));
                } else {
                    fields.push(Field::new(attr_name, DataType::Utf8, true));
                }
            }
            debug!(
                "GTF Schema Mode 2 (Projection): 8 static fields + {} attribute fields = {} columns total",
                attrs.len(),
                8 + attrs.len()
            );
        }
```

- [ ] **Step 4: Implement the GTF builder sentinel**

In `physical_exec.rs`, replace `set_attribute_builders` (2-tuple shape):

```rust
fn set_attribute_builders(
    batch_size: usize,
    attr_fields: &[String],
    attribute_builders: &mut (Vec<String>, Vec<OptionalField>),
) {
    for attr_name in attr_fields {
        let field = if attr_name == "attributes" {
            OptionalField::new(
                &DataType::List(FieldRef::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("tag", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    true,
                ))),
                batch_size,
            )
            .unwrap()
        } else {
            OptionalField::new(&DataType::Utf8, batch_size).unwrap()
        };
        attribute_builders.0.push(attr_name.clone());
        attribute_builders.1.push(field);
    }
}
```

- [ ] **Step 5: Implement the GTF loader sentinel**

In `physical_exec.rs`, replace the final `for i in 0..attribute_builders.1.len()` loop inside `load_attributes_unnest_from_string`:

```rust
    for i in 0..attribute_builders.1.len() {
        if let Some(indices) = &projected_attribute_indices
            && !indices.contains(&i)
        {
            attribute_builders.1[i].append_null()?;
            continue;
        }

        if attribute_builders.0[i] == "attributes" {
            let attributes = parse_gtf_attributes_to_vec(attributes_str);
            attribute_builders.1[i].append_array_struct(attributes)?;
            continue;
        }

        if let Some(value) = attributes_map.get(&attribute_builders.0[i]) {
            attribute_builders.1[i].append_string(value)?;
        } else {
            attribute_builders.1[i].append_null()?;
        }
    }
```

- [ ] **Step 6: Run the GTF test, verify it passes**

Run: `cargo test -p datafusion-bio-format-gtf test_gtf_attributes_sentinel_with_flattened_field -- --nocapture`
Expected: PASS.

- [ ] **Step 7: Run the full GTF crate test suite**

Run: `cargo test -p datafusion-bio-format-gtf`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add datafusion/bio-format-gtf/src/table_provider.rs \
        datafusion/bio-format-gtf/src/physical_exec.rs \
        datafusion/bio-format-gtf/tests/gtf_read_test.rs
git commit -m "feat(gtf): emit nested attributes alongside flattened fields via 'attributes' sentinel"
```

---

## PHASE B — polars-bio integration

### Task 4: Build polars-bio against the local upstream branch

**Files:**
- Modify (dev-only, not committed in this task): `Cargo.toml`

**Interfaces:**
- Consumes: the local clone at `/Users/mwiewior/research/git/datafusion-bio-formats` checked out on `feat/attributes-sentinel-in-flattened`.
- Produces: a polars-bio wheel built against the sentinel-capable reader, used by Tasks 5–6.

- [ ] **Step 1: Create the polars-bio feature branch**

```bash
cd /Users/mwiewior/research/git/polars-bio
git checkout issue-396-gff-gtf-predicate-fallback
git checkout -b feat/gff-gtf-attributes-sentinel
```

- [ ] **Step 2: Point the ten bio-format deps at the local path**

Add a `[patch."https://github.com/biodatageeks/datafusion-bio-formats.git"]` section to the end of `Cargo.toml` (keeps the existing `rev`-pinned lines untouched; patch redirects all of them to the local workspace):

```toml
[patch."https://github.com/biodatageeks/datafusion-bio-formats.git"]
datafusion-bio-format-vcf = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-vcf" }
datafusion-bio-format-core = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-core" }
datafusion-bio-format-gff = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-gff" }
datafusion-bio-format-fastq = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-fastq" }
datafusion-bio-format-bam = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-bam" }
datafusion-bio-format-cram = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-cram" }
datafusion-bio-format-bed = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-bed" }
datafusion-bio-format-fasta = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-fasta" }
datafusion-bio-format-pairs = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-pairs" }
datafusion-bio-format-gtf = { path = "/Users/mwiewior/research/git/datafusion-bio-formats/datafusion/bio-format-gtf" }
```

(If a `[patch]` redirect fails to resolve a crate, fall back to editing each `rev`-pinned line to `path = ...` directly. Verify crate dir names against `ls /Users/mwiewior/research/git/datafusion-bio-formats/datafusion`.)

- [ ] **Step 3: Build**

Run: `maturin develop --release`
Expected: builds successfully against the local path.

- [ ] **Step 4: Sanity-check no behavior change yet**

Run: `python -m pytest tests/test_io_gtf.py tests/test_gff_eager_vs_lazy.py -q`
Expected: PASS (reader change is additive; existing behavior unchanged).

- [ ] **Step 5: Do NOT commit the dev path override.**

The `[patch]` block is replaced by a committed `rev` pin in Task 6. Leave it uncommitted in the working tree for now.

---

### Task 5: polars-bio — request the sentinel and verify correct results

**Files:**
- Modify: `polars_bio/io.py` — `AnnotationLazyFrameWrapper.select()` `_attr` block (the `needs_raw_attributes`/`NotImplementedError` block added in PR #397).
- Modify: `tests/test_filter_select_attributes_bug_fix.py` — rewrite the two "raises" tests; add positive tests.
- Modify: `tests/test_io_gtf.py` — add a positive GTF test.

**Interfaces:**
- Consumes: the sentinel-capable reader (Task 4 build). `scan_columns`, `attr_cols`, `columns` already computed earlier in `select()`.
- Produces: `select()` returns correct rows for mixed raw-`attributes` + parsed-field queries; no `NotImplementedError`.

- [ ] **Step 1: Rewrite the two interim "raises" tests to assert correctness**

In `tests/test_filter_select_attributes_bug_fix.py`, replace `test_raw_attributes_predicate_with_parsed_select_raises` and `test_parsed_predicate_with_raw_attributes_select_raises` with:

```python
    def test_raw_attributes_predicate_with_parsed_select(self, test_gff_file):
        """Predicate on raw nested `attributes` + select a parsed field returns correct rows."""
        # Select rows that carry a "Type" attribute (only gene rows do in the fixture).
        has_type = pl.col("attributes").list.eval(
            pl.element().struct.field("tag")
        ).list.contains("Type")
        lf = pb.scan_gff(test_gff_file).filter(has_type)

        projected = lf.select("ID").collect()
        oracle = lf.collect().select("ID")

        pl.testing.assert_frame_equal(projected, oracle)
        assert projected.height > 0
        assert projected["ID"].null_count() == 0

    def test_parsed_predicate_with_raw_attributes_select(self, test_gff_file):
        """Predicate on a parsed field + select the raw nested `attributes` returns correct rows."""
        flt = pl.col("ID").str.contains("GENE")
        got = (
            pb.scan_gff(test_gff_file, attr_fields=["ID"])
            .filter(flt)
            .select("attributes")
            .collect()
        )
        expected_ids = (
            pb.scan_gff(test_gff_file, attr_fields=["ID"])
            .filter(flt)
            .select("ID")
            .collect()["ID"]
            .to_list()
        )

        assert got.height == len(expected_ids)
        assert got.height > 0
        # Each returned nested attributes row contains its matching ID value.
        recovered = (
            got.select(
                pl.col("attributes")
                .list.eval(
                    pl.element()
                    .filter(pl.element().struct.field("tag") == "ID")
                    .struct.field("value")
                )
                .list.first()
                .alias("ID")
            )["ID"]
            .to_list()
        )
        assert recovered == expected_ids
```

- [ ] **Step 2: Add a projection-only test (the pre-existing crash)**

Add to the same class:

```python
    def test_select_raw_attributes_and_parsed_field_together(self, test_gff_file):
        """Selecting raw `attributes` and a parsed field together (no predicate) works."""
        out = pb.scan_gff(test_gff_file).select(["attributes", "ID"]).collect()
        assert set(out.columns) == {"attributes", "ID"}
        assert out.height > 0
        assert out["ID"].null_count() == 0
        # attributes is the nested List<Struct> representation
        assert out.schema["attributes"] == pl.List(
            pl.Struct({"tag": pl.String, "value": pl.String})
        )
```

- [ ] **Step 3: Add the GTF equivalent**

Append to `tests/test_io_gtf.py` (follow that file's fixture/style; create a small GTF fixture inline if the module lacks one):

```python
def test_gtf_parsed_predicate_with_raw_attributes_select(tmp_path):
    import polars as pl
    import polars_bio as pb

    gtf = tmp_path / "t.gtf"
    gtf.write_text(
        'chr1\ttest\tgene\t1\t9\t.\t+\t.\tgene_id "ENSG1"; gene_name "A";\n'
        'chr1\ttest\tgene\t10\t19\t.\t+\t.\tgene_id "ENSG2"; gene_name "B";\n'
    )
    got = (
        pb.scan_gtf(str(gtf), attr_fields=["gene_id"])
        .filter(pl.col("gene_id") == "ENSG1")
        .select("attributes")
        .collect()
    )
    assert got.height == 1
    recovered = got.select(
        pl.col("attributes")
        .list.eval(
            pl.element()
            .filter(pl.element().struct.field("tag") == "gene_id")
            .struct.field("value")
        )
        .list.first()
        .alias("gene_id")
    )["gene_id"].to_list()
    assert recovered == ["ENSG1"]
```

- [ ] **Step 4: Run the new tests, verify they FAIL (still raising NotImplementedError)**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py -q -k "raw_attributes or parsed_predicate or together"`
Expected: FAIL with `NotImplementedError` (the production code still raises).

- [ ] **Step 5: Replace the `NotImplementedError` block with the sentinel `_attr` request**

In `polars_bio/io.py`, replace the conflict block (the `needs_raw_attributes and attr_cols` `raise NotImplementedError(...)` plus the following `if needs_raw_attributes: _attr = None ...`) with:

```python
            needs_raw_attributes = "attributes" in scan_columns
            if needs_raw_attributes and attr_cols:
                # Request flattened fields AND the nested `attributes` column in a
                # single registration (reader supports the "attributes" sentinel).
                _attr = attr_cols + ["attributes"]
            elif needs_raw_attributes:
                _attr = None
            elif attr_cols:
                _attr = attr_cols
            else:
                _attr = []
```

Delete the explanatory comment block and the `raise NotImplementedError(...)` introduced in PR #397.

- [ ] **Step 6: Run the new tests, verify they PASS**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py -q`
Expected: PASS.

- [ ] **Step 7: Run the full regression suite from the PR**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py tests/test_gff_eager_vs_lazy.py tests/test_optimization_bug_fix.py -q`
Expected: PASS.

- [ ] **Step 8: Commit (code + tests only — not Cargo.toml)**

```bash
git add polars_bio/io.py tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py
git commit -m "feat: return correct results for mixed raw-attributes + parsed-field GFF/GTF queries"
```

---

### Task 6: Pin the upstream commit and final verification

**Files:**
- Modify: `Cargo.toml` (replace dev path override with the new upstream commit SHA on all ten deps)

**Interfaces:**
- Consumes: the pushed upstream branch commit SHA.
- Produces: a reproducible build pinned to the published reader commit; green full suite.

- [ ] **Step 1: Push the upstream branch and capture the SHA**

```bash
cd /Users/mwiewior/research/git/datafusion-bio-formats
git push -u origin feat/attributes-sentinel-in-flattened
NEW_SHA=$(git rev-parse HEAD)
echo "$NEW_SHA"
```

- [ ] **Step 2: Repin all ten deps in polars-bio `Cargo.toml`**

Remove the `[patch...]` block added in Task 4. Update the ten `datafusion-bio-format-*` lines' `rev = "94d4d68..."` to `rev = "<NEW_SHA>"` (leave `datafusion-bio-function-*` lines untouched).

- [ ] **Step 3: Rebuild against the pinned commit**

Run: `cd /Users/mwiewior/research/git/polars-bio && maturin develop --release`
Expected: builds from the git commit (no path override).

- [ ] **Step 4: Full verification**

Run: `python -m pytest tests/test_filter_select_attributes_bug_fix.py tests/test_io_gtf.py tests/test_gff_eager_vs_lazy.py tests/test_optimization_bug_fix.py -q`
Then a broader smoke run: `python -m pytest tests/test_io_gff.py -q` (if present).
Expected: PASS.

- [ ] **Step 5: Commit the pin**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: pin datafusion-bio-formats to attributes-sentinel commit"
```

- [ ] **Step 6: Push and update PR #397 (or open a stacked PR)**

```bash
git push -u origin feat/gff-gtf-attributes-sentinel
```
Then reply on PR #397 summarizing that the `NotImplementedError` is replaced by the real fix, linking the upstream branch/commit. (Confirm with the user whether to retarget PR #397 onto this branch or open a follow-up PR.)

---

## Self-Review

**Spec coverage:**
- Sentinel schema (both crates) → Tasks 1, 3. ✓
- Sentinel execution/loader (both crates, all streams via shared helpers) → Tasks 1, 3; indexed path explicitly checked in Task 2. ✓
- polars-bio `_attr` change replacing `NotImplementedError` → Task 5. ✓
- Cargo.toml dev path override → commit SHA pin → Tasks 4, 6. ✓
- TDD: every code change is preceded by a failing test (Rust Steps 2/3; Python Step 4) → ✓
- Tests for both findings + projection-only + GTF + eager-vs-lazy → Task 5. ✓
- Out-of-scope items (remote GFF stub, unwrap panic, List pushdown) explicitly excluded → Global Constraints. ✓

**Placeholder scan:** Task 2 deliberately defers exact indexed-test setup to "copy the file's existing helpers" because that file's harness wasn't extracted verbatim; this is a read-then-mirror instruction, not a code placeholder. All production code steps contain complete code.

**Type consistency:** GFF `attribute_builders` is the 3-tuple (`.0` names, `.1` types, `.2` builders); GTF is the 2-tuple (`.0` names, `.1` builders) — the loader/builder steps use `.2` for GFF and `.1` for GTF accordingly. Parse helpers: `parse_gff_attributes_to_vec` (GFF) vs `parse_gtf_attributes_to_vec` (GTF). Nested List child name matches each crate's existing Mode 1 (`"item"` in schema; builder `"attribute"` for GFF / `"item"` for GTF — identical to current working Mode 1, so `RecordBatch::try_new` tolerates it as it does today).
