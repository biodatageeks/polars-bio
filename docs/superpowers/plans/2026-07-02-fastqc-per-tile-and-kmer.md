# FastQC per_tile_quality + kmer_content Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add FastQC modules 11 (`per_tile_quality`) and 12 (`kmer_content`) to the streaming FastQC pipeline, each matching FastQC 0.12.1 `--nogroup` golden output exactly.

**Architecture:** QC algorithms live in the upstream crate `datafusion-bio-functions/datafusion/bio-function-fastqc` (local path dep) as `QcModule` impls emitting the fixed tidy schema; polars-bio is thin glue (`src/fastqc.rs` UDTF + `polars_bio/fastqc_op.py`). per_tile needs the read header, so the `QcModule::update` signature gains a `name` parameter (cross-cutting, all 10 existing modules adapted). kmer faithfully replicates FastQC (2% sampling, 500bp truncation, binomial×4^k p-value) for exact golden parity, valid for single-partition plain-`.fastq` scans.

**Tech Stack:** Rust (DataFusion/Arrow, PyO3), Python (polars, pyarrow, pytest), FastQC 0.12.1 as the golden oracle (installed at `/opt/homebrew/bin/fastqc`).

## Global Constraints

- Upstream crate path: `/Users/mwiewior/research/git/datafusion-bio-functions/datafusion/bio-function-fastqc/`
- polars-bio repo: `/Users/mwiewior/research/git/polars-bio/`
- Build the Python wheel after any Rust change: `maturin develop --release` (from polars-bio root; slow, minutes).
- Rust crate unit tests: `cargo test -p datafusion-bio-function-fastqc` (from the datafusion-bio-functions workspace).
- Python tests: `python -m pytest tests/test_fastqc*.py -v` (from polars-bio root).
- Tidy schema is FIXED (`tidy_schema()` in `lib.rs`) — do NOT change it. New modules emit rows into the existing 6 columns (`module,label,position,metric,value,value_str`).
- The two `ALL_MODULES` lists (Rust `lib.rs`, Python `fastqc_op.py`) and `EXPECTED_MODULES` (`tests/test_fastqc.py`) must stay in lockstep.
- FastQC golden regeneration uses `fastqc --nogroup`; kmer additionally needs `--limits <patched>` with `kmer ignore 0`.
- Commit after each task. Do NOT push. Branch is `feat/fastqc-phase1`.

---

### Task 1: Thread the read header into `QcModule::update` (cross-cutting, no behavior change)

**Files:**
- Modify: `datafusion/bio-function-fastqc/src/lib.rs` (trait def ~line 61; `update_batch` ~166-190)
- Modify (add `_name: &[u8]` param to `update`): `adapter_content.rs`, `basic_stats.rs`, `dup_levels.rs`, `overrepresented.rs`, `per_base_content.rs`, `per_base_n.rs`, `per_base_quality.rs`, `per_seq_gc.rs`, `per_seq_quality.rs`, `seq_length.rs` (all in `datafusion/bio-function-fastqc/src/`)
- Modify: `polars-bio/src/fastqc.rs` (scan projection ~line 61)
- Test: existing `cargo test -p datafusion-bio-function-fastqc` is the regression net (behavior must not change).

**Interfaces:**
- Produces: `fn update(&mut self, name: &[u8], seq: &[u8], qual: &[u8])` on `QcModule`; `update_batch` now reads the `name` column (falls back to empty) and passes it. `fastqc.rs` projection includes `"name"` (and `"description"`).

- [ ] **Step 1: Update the trait signature**

In `lib.rs`, change the trait method:

```rust
    /// Fold one read (raw header/name bytes, raw sequence bytes, raw phred+33 quality bytes).
    fn update(&mut self, name: &[u8], seq: &[u8], qual: &[u8]);
```

- [ ] **Step 2: Update `update_batch` to read the name column and pass it**

In `lib.rs` `update_batch`, add the name column lookup and pass it (reconstruct the full FastQC header = `name` + " " + `description` when a description column exists, so tile parsing matches FastQC which splits the whole id):

```rust
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let seq = batch
            .column_by_name("sequence")
            .ok_or_else(|| DataFusionError::Execution("fastqc input missing 'sequence'".into()))?
            .as_string::<i32>();
        let qual = batch
            .column_by_name("quality_scores")
            .ok_or_else(|| {
                DataFusionError::Execution("fastqc input missing 'quality_scores'".into())
            })?
            .as_string::<i32>();
        let name = batch.column_by_name("name").map(|c| c.as_string::<i32>());
        let desc = batch
            .column_by_name("description")
            .map(|c| c.as_string::<i32>());
        let mut hdr: Vec<u8> = Vec::new();
        for i in 0..batch.num_rows() {
            if seq.is_null(i) {
                continue;
            }
            let s = seq.value(i).as_bytes();
            let q = if qual.is_null(i) {
                &[][..]
            } else {
                qual.value(i).as_bytes()
            };
            hdr.clear();
            if let Some(n) = &name {
                if !n.is_null(i) {
                    hdr.extend_from_slice(n.value(i).as_bytes());
                }
            }
            if let Some(d) = &desc {
                if !d.is_null(i) && !d.value(i).is_empty() {
                    if !hdr.is_empty() {
                        hdr.push(b' ');
                    }
                    hdr.extend_from_slice(d.value(i).as_bytes());
                }
            }
            for m in self.modules.iter_mut() {
                m.update(&hdr, s, q);
            }
        }
        Ok(())
    }
```

- [ ] **Step 3: Add the `_name` param to all 10 existing modules**

In each of the 10 module files, change `fn update(&mut self, seq: &[u8], _qual: &[u8])` (or `qual`) to prepend `_name: &[u8]`. Example for `adapter_content.rs`:

```rust
    fn update(&mut self, _name: &[u8], seq: &[u8], _qual: &[u8]) {
```

Apply the analogous one-line change to each module's `update`, keeping its existing `seq`/`qual` parameter names/usage. Also update any `#[cfg(test)]` calls in those files that invoke `m.update(seq, qual)` to `m.update(b"", seq, qual)`.

- [ ] **Step 4: Update `lib.rs` set_tests + physical_exec.rs tests**

In `lib.rs` `set_tests::end_to_end_tidy_batch`, the batch has no `name` column — `update_batch` tolerates that (name lookup is optional). No change needed there. In `physical_exec.rs` tests, `read_batch()` has only sequence+quality — also tolerated. Confirm both still compile.

- [ ] **Step 5: Update the polars-bio scan projection**

In `polars-bio/src/fastqc.rs` `scan`, add name/description to the projected columns:

```rust
        let needed = ["sequence", "quality_scores", "name", "description"];
```

(`filter_map` already drops columns the input schema lacks, so this is safe if a provider omits `description`.)

- [ ] **Step 6: Run the upstream unit tests (regression net)**

Run: `cargo test -p datafusion-bio-function-fastqc`
Expected: PASS — all existing module tests green (no behavior change; only a new unused param).

- [ ] **Step 7: Build the wheel and smoke-test existing Python parity**

Run: `cd /Users/mwiewior/research/git/polars-bio && maturin develop --release && python -m pytest tests/test_fastqc_golden.py -v`
Expected: PASS — the 4 existing golden parity tests still pass (name plumbing didn't perturb any module).

- [ ] **Step 8: Commit**

```bash
git add datafusion/bio-function-fastqc/src -A  # in the datafusion-bio-functions repo
git commit -m "refactor(fastqc): thread read header into QcModule::update (no behavior change)"
# then in polars-bio repo:
git add src/fastqc.rs
git commit -m "refactor(fastqc): project name/description for header-aware modules"
```

---

### Task 2: `per_tile_quality` Rust module

**Files:**
- Create: `datafusion/bio-function-fastqc/src/per_tile_quality.rs`
- Modify: `datafusion/bio-function-fastqc/src/lib.rs` (mod decl line ~14; `use` ~line 85; `ALL_MODULES` add + bump `[&str; 10]`→`[&str; 11]`; `make_module` arm)

**Interfaces:**
- Consumes: `QcModule` trait with `update(name, seq, qual)` (Task 1).
- Produces: module name `"per_tile_quality"`; emits `metric="mean"` rows with `label=<tile>` (stringified), `position=1..=L`, `value=deviation`; plus one `status` row.

- [ ] **Step 1: Write the failing Rust unit test**

Create `per_tile_quality.rs` with only the test first (module body stubbed to `todo!()` so it compiles-fails), then implement in Step 3. Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Header format: split on ':' has >=7 fields -> tile is field index 4.
    fn hdr(tile: u32) -> Vec<u8> {
        format!("SRR.1 D0:7:HG:1:{tile}:1330:1935/1").into_bytes()
    }

    #[test]
    fn deviation_is_tile_mean_minus_unweighted_group_mean() {
        let mut m = PerTileQuality::new();
        // tile 1101: two reads, quality 'I'(40) at both positions.
        m.update(&hdr(1101), b"AC", b"II");
        m.update(&hdr(1101), b"AC", b"II");
        // tile 1102: one read, quality '5'(20) at both positions.
        m.update(&hdr(1102), b"AC", b"55");
        let mut rows = Vec::new();
        m.finalize(&mut rows);
        // group mean per position = (40 + 20)/2 = 30 (unweighted over 2 tiles).
        // tile 1101 deviation = 40 - 30 = +10; tile 1102 = 20 - 30 = -10.
        let dev = |tile: &str, pos: i32| {
            rows.iter()
                .find(|r| {
                    r.label.as_deref() == Some(tile)
                        && r.position == Some(pos)
                        && r.metric == "mean"
                })
                .and_then(|r| r.value)
                .unwrap()
        };
        assert!((dev("1101", 1) - 10.0).abs() < 1e-9);
        assert!((dev("1102", 1) + 10.0).abs() < 1e-9);
        // max abs deviation = 10 -> not > 10 -> WARN (>5), per limits tile warn=5.
        let status = rows
            .iter()
            .find(|r| r.metric == "status")
            .and_then(|r| r.value_str.clone())
            .unwrap();
        assert_eq!(status, "WARN");
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p datafusion-bio-function-fastqc per_tile`
Expected: FAIL (panics in `todo!()` / unresolved `PerTileQuality`).

- [ ] **Step 3: Implement the module**

Replace the stub with the implementation. Phred = `byte - 33`. Tile parse mirrors FastQC `getTile`: split id on `:`; `>=7` fields → index 4, else `>=5` → index 2, else no tile (skip read). State: `BTreeMap<u32, TileAcc>` where `TileAcc { sum: Vec<f64>, count: Vec<u64> }` grown per position.

```rust
use std::any::Any;
use std::collections::BTreeMap;

use crate::{QcModule, TidyRow};

#[derive(Debug, Default, Clone)]
struct TileAcc {
    sum: Vec<f64>,
    count: Vec<u64>,
}

impl TileAcc {
    fn add(&mut self, pos: usize, phred: f64) {
        if self.sum.len() <= pos {
            self.sum.resize(pos + 1, 0.0);
            self.count.resize(pos + 1, 0);
        }
        self.sum[pos] += phred;
        self.count[pos] += 1;
    }
}

/// FastQC "Per tile sequence quality": per (tile, position), the mean quality
/// minus the *unweighted* mean of all tiles' means at that position.
#[derive(Debug, Default)]
pub struct PerTileQuality {
    tiles: BTreeMap<u32, TileAcc>,
    split_position: Option<usize>,
}

impl PerTileQuality {
    pub fn new() -> Self {
        Self::default()
    }

    fn tile_of(&mut self, name: &[u8]) -> Option<u32> {
        let id = std::str::from_utf8(name).ok()?;
        let fields: Vec<&str> = id.split(':').collect();
        let idx = match self.split_position {
            Some(i) => i,
            None => {
                let i = if fields.len() >= 7 {
                    4
                } else if fields.len() >= 5 {
                    2
                } else {
                    return None;
                };
                self.split_position = Some(i);
                i
            },
        };
        fields.get(idx)?.trim().parse::<u32>().ok()
    }
}

impl QcModule for PerTileQuality {
    fn name(&self) -> &'static str {
        "per_tile_quality"
    }

    fn update(&mut self, name: &[u8], _seq: &[u8], qual: &[u8]) {
        let Some(tile) = self.tile_of(name) else {
            return;
        };
        let acc = self.tiles.entry(tile).or_default();
        for (pos, &b) in qual.iter().enumerate() {
            acc.add(pos, (b as f64) - 33.0);
        }
    }

    fn merge(&mut self, other: &dyn QcModule) {
        let o = other
            .as_any()
            .downcast_ref::<PerTileQuality>()
            .expect("merge type mismatch");
        if self.split_position.is_none() {
            self.split_position = o.split_position;
        }
        for (tile, oacc) in &o.tiles {
            let acc = self.tiles.entry(*tile).or_default();
            let n = oacc.sum.len();
            if acc.sum.len() < n {
                acc.sum.resize(n, 0.0);
                acc.count.resize(n, 0);
            }
            for i in 0..n {
                acc.sum[i] += oacc.sum[i];
                acc.count[i] += oacc.count[i];
            }
        }
    }

    fn finalize(&self, out: &mut Vec<TidyRow>) {
        let m = "per_tile_quality";
        if self.tiles.is_empty() {
            out.push(TidyRow::status(m, "PASS"));
            return;
        }
        let max_len = self.tiles.values().map(|a| a.sum.len()).max().unwrap_or(0);
        let n_tiles = self.tiles.len() as f64;
        // Unweighted mean of per-tile means at each position (FastQC divides by
        // the total number of tiles). Fixtures use uniform read length so every
        // tile has data at every position.
        let mut group_mean = vec![0f64; max_len];
        for acc in self.tiles.values() {
            for pos in 0..max_len {
                let tm = if pos < acc.count.len() && acc.count[pos] > 0 {
                    acc.sum[pos] / acc.count[pos] as f64
                } else {
                    0.0
                };
                group_mean[pos] += tm;
            }
        }
        for gm in group_mean.iter_mut() {
            *gm /= n_tiles;
        }
        let mut max_abs_dev = 0f64;
        for (tile, acc) in &self.tiles {
            for pos in 0..acc.sum.len() {
                if acc.count[pos] == 0 {
                    continue;
                }
                let tm = acc.sum[pos] / acc.count[pos] as f64;
                let dev = tm - group_mean[pos];
                max_abs_dev = max_abs_dev.max(dev.abs());
                out.push(TidyRow {
                    module: m,
                    label: Some(tile.to_string()),
                    position: Some((pos + 1) as i32),
                    metric: "mean".to_string(),
                    value: Some(dev),
                    value_str: None,
                });
            }
        }
        let status = if max_abs_dev > 10.0 {
            "FAIL"
        } else if max_abs_dev > 5.0 {
            "WARN"
        } else {
            "PASS"
        };
        out.push(TidyRow::status(m, status));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

- [ ] **Step 4: Register the module in `lib.rs`**

Add `pub mod per_tile_quality;` (keep alphabetical-ish with the others), `use per_tile_quality::PerTileQuality;`, bump `pub const ALL_MODULES: [&str; 10]` to `[&str; 11]` and append `"per_tile_quality"` as the LAST element (append, don't reorder — existing tests assert ordering of the first 10), and add the `make_module` arm:

```rust
        "per_tile_quality" => Box::new(PerTileQuality::new()),
```

- [ ] **Step 5: Run the unit test to verify it passes**

Run: `cargo test -p datafusion-bio-function-fastqc per_tile`
Expected: PASS.

- [ ] **Step 6: Run the full crate test suite**

Run: `cargo test -p datafusion-bio-function-fastqc`
Expected: PASS (ALL_MODULES count/order tests still green with 11 modules).

- [ ] **Step 7: Commit**

```bash
git add datafusion/bio-function-fastqc/src -A   # datafusion-bio-functions repo
git commit -m "feat(fastqc): add per_tile_quality QcModule (11/12)"
```

---

### Task 3: per_tile_quality — fixture, golden, Python surface, parity test

**Files:**
- Create: `tests/data/io/fastq/per_tile_mix.fastq`
- Create: `tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt` (generated)
- Modify: `polars_bio/fastqc_op.py` (`ALL_MODULES`; new `per_tile_quality` property; `fastqc()` docstring)
- Modify: `tests/test_fastqc.py` (`EXPECTED_MODULES`)
- Modify: `benchmarks/fastqc/parity.py` (`_FASTQC_MODULE`, `_emit`)
- Create/append test: `tests/test_fastqc_golden.py` (new `test_per_tile_quality_matches_fastqc_exactly`)

**Interfaces:**
- Consumes: Rust module `per_tile_quality` emitting `metric="mean"`, `label=<tile>`, `position`, `value=deviation`.
- Produces: `pb.fastqc(...).per_tile_quality` LazyFrame with columns `tile, position, deviation`.

- [ ] **Step 1: Create a multi-tile fixture**

Write a small generator script to scratchpad and run it to emit `per_tile_mix.fastq`: 3 tiles (1101, 1102, 1103), 20 uniform-length (60bp) reads each, tile 1103's quality deliberately depressed so a non-trivial (and >5, ≤10 → WARN) deviation appears. Header format must have ≥7 colon fields so FastQC picks index 4:

```python
# scratchpad/gen_per_tile.py
import random
random.seed(0)
BASES = "ACGT"
def rec(i, tile, qch):
    seq = "".join(random.choice(BASES) for _ in range(60))
    qual = qch * 60
    hdr = f"@SIM.{i} D0:7:HG32:1:{tile}:{1000+i}:{2000+i}/1"
    return f"{hdr}\n{seq}\n+\n{qual}\n"
out = []
i = 0
for tile, qch in [(1101, "I"), (1102, "I"), (1103, "5")]:  # I=40, 5=20
    for _ in range(20):
        i += 1
        out.append(rec(i, tile, qch))
open("tests/data/io/fastq/per_tile_mix.fastq", "w").write("".join(out))
print("wrote", i, "reads")
```

Run: `cd /Users/mwiewior/research/git/polars-bio && python scratchpad/gen_per_tile.py`
Expected: `wrote 60 reads`. (If the resulting max abs deviation lands >10, raise tile 1103's qch toward `I`, e.g. `"?"`=30, to keep it in WARN range; the golden — not the guess — is the source of truth for the exact value.)

- [ ] **Step 2: Generate the golden reference**

Run: `cd /Users/mwiewior/research/git/polars-bio && D=$(mktemp -d) && fastqc --extract --nogroup -o "$D" tests/data/io/fastq/per_tile_mix.fastq && cp "$D/per_tile_mix_fastqc/fastqc_data.txt" tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt && grep -c "Per tile" tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt`
Expected: the golden file exists and contains a `>>Per tile sequence quality` section with non-zero deviations for tile 1103.

- [ ] **Step 3: Teach the parity harness to parse the per-tile section**

In `benchmarks/fastqc/parity.py`, add to `_FASTQC_MODULE`:

```python
    "Per tile sequence quality": "per_tile_quality",
```

and add an `_emit` branch (header is `#Tile\tBase\tMean`):

```python
    elif module == "per_tile_quality":
        rows.append(
            dict(
                module=module,
                label=parts[0],                 # tile as string
                position=int(parts[1]),
                metric="mean",
                value=float(parts[2]),
            )
        )
```

- [ ] **Step 4: Write the failing Python parity test**

Append to `tests/test_fastqc_golden.py`:

```python
PER_TILE_GOLDEN = "tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt"
PER_TILE_FASTQ = "tests/data/io/fastq/per_tile_mix.fastq"


def test_per_tile_quality_matches_fastqc_exactly():
    ref = _golden(PER_TILE_GOLDEN).filter(pl.col("module") == "per_tile_quality")
    got = _ours(PER_TILE_FASTQ).filter(pl.col("module") == "per_tile_quality")
    joined = got.join(
        ref,
        on=["module", "label", "position", "metric"],
        how="inner",
        suffix="_ref",
        nulls_equal=True,
    )
    assert joined.height == ref.height > 0
    worst = joined.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
    assert worst <= 1e-6, f"max per_tile deviation diff vs FastQC = {worst}"
```

- [ ] **Step 5: Run it to verify it fails**

Run: `python -m pytest tests/test_fastqc_golden.py::test_per_tile_quality_matches_fastqc_exactly -v`
Expected: FAIL — `per_tile_quality` not in Python `ALL_MODULES` yet (KeyError / empty `got`).

- [ ] **Step 6: Add the Python module surface**

In `polars_bio/fastqc_op.py`: append `"per_tile_quality"` to `ALL_MODULES`; add the property; update the `fastqc()` docstring (mention it and bump the count wording from "ten" to "twelve" once both land — for now "eleven"):

```python
    @property
    def per_tile_quality(self) -> pl.LazyFrame:
        self._require("per_tile_quality")
        return (
            self._module_rows("per_tile_quality")
            .filter(pl.col("metric") == "mean")
            .select(
                pl.col("label").alias("tile"),
                pl.col("position"),
                pl.col("value").alias("deviation"),
            )
            .sort("tile", "position")
        )
```

- [ ] **Step 7: Run the parity test to verify it passes**

Run: `python -m pytest tests/test_fastqc_golden.py::test_per_tile_quality_matches_fastqc_exactly -v`
Expected: PASS (`worst <= 1e-6`).

- [ ] **Step 8: Update `EXPECTED_MODULES` and run the schema tests**

In `tests/test_fastqc.py`, append `"per_tile_quality"` to `EXPECTED_MODULES`.
Run: `python -m pytest tests/test_fastqc.py -v`
Expected: PASS (module count / status-per-module / UDTF row-count tests green with 11).

- [ ] **Step 9: Commit**

```bash
git add tests/data/io/fastq/per_tile_mix.fastq tests/data/io/fastq/golden/per_tile_mix.nogroup.fastqc_data.txt polars_bio/fastqc_op.py tests/test_fastqc.py tests/test_fastqc_golden.py benchmarks/fastqc/parity.py
git commit -m "feat(fastqc): expose per_tile_quality in Python with exact FastQC parity (11/12)"
```

---

### Task 4: `kmer_content` Rust module (faithful FastQC replica)

**Files:**
- Create: `datafusion/bio-function-fastqc/src/kmer_content.rs`
- Modify: `datafusion/bio-function-fastqc/src/lib.rs` (mod/use; `ALL_MODULES` `[&str; 11]`→`[&str; 12]` + append; `make_module` arm)

**Interfaces:**
- Consumes: `QcModule` trait (Task 1).
- Produces: module name `"kmer_content"`; per reported k-mer emits rows `metric="count"` (value=count), `metric="obs_exp_max"`, `metric="max_position"`, `metric="pvalue"` (all with `label=<kmer>`); plus one `status` row.

- [ ] **Step 1: Re-read the authoritative source**

Before coding, read the full `getResults()` in FastQC `KmerContent.java` (source: `https://raw.githubusercontent.com/s-andrews/FastQC/master/uk/ac/babraham/FastQC/Modules/KmerContent.java`) to lock down: per-position (group) `obsExp` and `binomialPValue` arrays; that the reported **PValue** column is the lowest p-value across positions, **Obs/Exp Max** is the max obsExp, **Max Obs/Exp Position** is the argmax; the keep-filter (`p < 0.01 && obsExp > 5` at the qualifying position); descending sort by `maxObsExp`; top-20 cap. The Task-5 golden test at `1e-9`/tolerance is the arbiter — reconcile any ambiguity against it.

- [ ] **Step 2: Write the failing Rust unit test**

Create `kmer_content.rs` with the test first (stub `todo!()`), covering sampling + N-skip + obs/exp:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_every_50th_read_and_skips_n_kmers() {
        let mut m = KmerContent::new();
        // 49 reads skipped, the 50th processed. Use k=7 default.
        let seq = b"AAAAAAACCCCCCC"; // no N
        for _ in 0..49 {
            m.update(b"x", b"GGGGGGG", b"IIIIIII");
        }
        m.update(b"x", seq, &[b'I'; 14]);
        // Only the 50th read contributed: AAAAAAA appears once at pos 0.
        assert_eq!(m.total_kmer_count(), (seq.len() - 7 + 1) as u64);
        assert!(m.kmer_count(b"AAAAAAA") == 1);
        // N-containing kmers are counted in the total but not stored per-kmer.
        let mut m2 = KmerContent::new();
        m2.update(b"x", b"NAAAAAAA", &[b'I'; 8]); // read #1 -> skipped (not 50th)
        for _ in 0..49 {
            m2.update(b"x", b"NAAAAAAA", &[b'I'; 8]);
        }
        // the 50th call is processed: windows "NAAAAAA"(has N, skip store) "AAAAAAA"(store)
        assert_eq!(m2.total_kmer_count(), 2);
        assert_eq!(m2.kmer_count(b"AAAAAAA"), 1);
        assert_eq!(m2.kmer_count(b"NAAAAAA"), 0);
    }
}
```

(Expose `total_kmer_count()`/`kmer_count()` as `#[cfg(test)]` or pub(crate) test helpers.)

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p datafusion-bio-function-fastqc kmer`
Expected: FAIL (unresolved `KmerContent` / `todo!()`).

- [ ] **Step 4: Implement the module**

Implement per the source. K=7 (const `KMER_SIZE: usize = 7`). State:
- `skip_count: u64` (per-read counter for 2% sampling)
- `total_kmer_count: u64`, `total_at_position: Vec<u64>` (include N-kmers)
- `kmers: HashMap<[u8; 7], Kmer>` where `Kmer { count: u64, positions: Vec<u64> }` (exclude N-kmers)

`update`: `skip_count += 1; if skip_count % 50 != 0 { return; }`; truncate `seq` to first 500 bytes; for `i in 0..=seq.len()-KMER_SIZE`: grow `total_at_position` to `i`, `total_kmer_count += 1`, `total_at_position[i] += 1`; if the k-mer window contains `N`/`n`, `continue`; else update the per-kmer entry (`count += 1`, `positions[i] += 1`, growing as needed).

`merge`: add `total_kmer_count`, element-wise add `total_at_position`, and per-kmer `count`/`positions` (grow vectors); `skip_count` also summed (single-partition case: only one partition sampled).

`finalize`: for each stored kmer with `expectedProportion = count / total_kmer_count`, for each position g with data: `predicted = expectedProportion * total_at_position[g]`; `obsExp[g] = positions[g] / predicted`; `pval[g] = if positions[g] as f64 > predicted { (1 - binom_cdf(total_at_position[g] as u64, expectedProportion, positions[g])) * 4f64.powi(KMER_SIZE as i32) } else { 1.0 }`. Track `max_obs_exp` (+argmax position, 1-based) and `min_pval`. Keep kmers where `min_pval < 0.01 && max_obs_exp > 5.0`. Sort kept desc by `max_obs_exp`, take 20. Emit `count`/`obs_exp_max`/`max_position`/`pvalue` rows per kmer (`label = kmer string`). Status from top (first) kmer: `s = -min_pval.log10()`; FAIL if `s > 10`, WARN if `s > 5`, else PASS (if no kept kmers → PASS).

Include an exact binomial CDF helper (no external crate):

```rust
/// P(X <= k) for X ~ Binomial(n, p), computed by summing exact PMF terms.
/// Mirrors Apache-commons BinomialDistribution.cumulativeProbability used by FastQC.
fn binom_cdf(n: u64, p: f64, k: u64) -> f64 {
    if k >= n {
        return 1.0;
    }
    // log-space PMF to avoid overflow for large n.
    let ln_p = p.ln();
    let ln_q = (1.0 - p).ln();
    let mut cum = 0.0f64;
    for i in 0..=k {
        let ln_choose = ln_gamma((n + 1) as f64)
            - ln_gamma((i + 1) as f64)
            - ln_gamma((n - i + 1) as f64);
        cum += (ln_choose + (i as f64) * ln_p + ((n - i) as f64) * ln_q).exp();
    }
    cum.min(1.0)
}

// Lanczos approximation for ln Gamma.
fn ln_gamma(x: f64) -> f64 {
    const G: [f64; 8] = [
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ];
    if x < 0.5 {
        std::f64::consts::PI / ((std::f64::consts::PI * x).sin() * ln_gamma(1.0 - x).exp())
    } else {
        let x = x - 1.0;
        let mut a = 0.99999999999980993;
        let t = x + 7.5;
        for (i, &g) in G.iter().enumerate() {
            a += g / (x + (i as f64) + 1.0);
        }
        0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
    }
}
```

- [ ] **Step 5: Run the unit test to verify it passes**

Run: `cargo test -p datafusion-bio-function-fastqc kmer`
Expected: PASS.

- [ ] **Step 6: Register in `lib.rs`, run full crate tests**

Add `pub mod kmer_content;`, `use kmer_content::KmerContent;`, bump `ALL_MODULES` to `[&str; 12]` appending `"kmer_content"`, add `make_module` arm.
Run: `cargo test -p datafusion-bio-function-fastqc`
Expected: PASS (12 modules).

- [ ] **Step 7: Commit**

```bash
git add datafusion/bio-function-fastqc/src -A   # datafusion-bio-functions repo
git commit -m "feat(fastqc): add kmer_content QcModule replicating FastQC sampling (12/12)"
```

---

### Task 5: kmer_content — fixture, patched-limits golden, Python surface, parity test

**Files:**
- Create: `tests/data/io/fastq/kmer_mix.fastq` (≥1000 reads)
- Create: `tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt` (generated)
- Create: `scratchpad/kmer_limits.txt` (patched limits) — used only to generate golden, not committed unless needed
- Modify: `polars_bio/fastqc_op.py` (`ALL_MODULES`; `kmer_content` property; docstring → "twelve")
- Modify: `tests/test_fastqc.py` (`EXPECTED_MODULES`)
- Modify: `benchmarks/fastqc/parity.py` (`_FASTQC_MODULE`, `_emit`, `TOLERANCES`)
- Append test: `tests/test_fastqc_golden.py`

**Interfaces:**
- Consumes: Rust `kmer_content` rows (`count`/`obs_exp_max`/`max_position`/`pvalue`).
- Produces: `pb.fastqc(...).kmer_content` LazyFrame with `kmer, count, obs_exp_max, max_position, pvalue`.

- [ ] **Step 1: Create a ≥1000-read k-mer-enriched fixture**

```python
# scratchpad/gen_kmer.py
import random
random.seed(1)
BASES = "ACGT"
MOTIF = "GATTACA"[:7] if len("GATTACA") >= 7 else "GATTACAG"  # exactly 7bp motif
MOTIF = "GATTACG"  # 7bp
def rec(i, enrich):
    seq = [random.choice(BASES) for _ in range(60)]
    if enrich:
        seq[10:17] = list(MOTIF)  # constant position -> positional enrichment
    seq = "".join(seq)
    return f"@SIM.{i} D0:7:HG:1:1101:{1000+i}:{2000+i}/1\n{seq}\n+\n{'I'*60}\n"
out = []
for i in range(1, 1201):
    # every 50th read (the FastQC-sampled ones) carries the motif so it is enriched
    out.append(rec(i, enrich=(i % 50 == 0)))
open("tests/data/io/fastq/kmer_mix.fastq", "w").write("".join(out))
print("wrote 1200 reads; sampled(every 50th):", 1200 // 50)
```

Run: `cd /Users/mwiewior/research/git/polars-bio && python scratchpad/gen_kmer.py`
Expected: `wrote 1200 reads; sampled(every 50th): 24`. (Putting the motif on exactly the sampled reads guarantees the k-mer clears `p<0.01 && obs/exp>5`. If FastQC reports no k-mer, increase motif prevalence among sampled reads or motif length.)

- [ ] **Step 2: Generate the kmer-enabled golden**

```bash
cd /Users/mwiewior/research/git/polars-bio
sed 's/^kmer\(\s*\)ignore\(\s*\)1/kmer\1ignore\20/' /opt/homebrew/Cellar/fastqc/0.12.1/libexec/Configuration/limits.txt > scratchpad/kmer_limits.txt
grep -E "^kmer" scratchpad/kmer_limits.txt   # verify: kmer ignore 0
D=$(mktemp -d)
fastqc --extract --nogroup --limits scratchpad/kmer_limits.txt -o "$D" tests/data/io/fastq/kmer_mix.fastq
cp "$D/kmer_mix_fastqc/fastqc_data.txt" tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt
grep -A5 ">>Kmer Content" tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt | head
```
Expected: a `>>Kmer Content` section with `GATTACG` (or the chosen motif) as an enriched k-mer.

- [ ] **Step 3: Teach the parity harness to parse the Kmer section**

In `benchmarks/fastqc/parity.py`, add to `_FASTQC_MODULE`:

```python
    "Kmer Content": "kmer_content",
```

Add `_emit` branch (header `#Sequence\tCount\tPValue\tObs/Exp Max\tMax Obs/Exp Position`):

```python
    elif module == "kmer_content":
        if len(parts) < 5:
            return
        kmer = parts[0]
        for metric, idx in [("count", 1), ("pvalue", 2), ("obs_exp_max", 3), ("max_position", 4)]:
            rows.append(
                dict(module=module, label=kmer, position=None, metric=metric, value=float(parts[idx]))
            )
```

Add tolerances:

```python
    ("kmer_content", "count"): 0.0,
    ("kmer_content", "obs_exp_max"): 1e-6,
    ("kmer_content", "max_position"): 0.0,
    ("kmer_content", "pvalue"): 1e-3,   # FastQC prints float32 p-values
```

- [ ] **Step 4: Write the failing Python parity test**

Append to `tests/test_fastqc_golden.py`:

```python
KMER_GOLDEN = "tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt"
KMER_FASTQ = "tests/data/io/fastq/kmer_mix.fastq"


def test_kmer_content_matches_fastqc_exactly():
    ref = _golden(KMER_GOLDEN).filter(pl.col("module") == "kmer_content")
    got = _ours(KMER_FASTQ).filter(pl.col("module") == "kmer_content")
    # Reported-set membership: our top-20 k-mers must equal FastQC's.
    ref_kmers = set(ref.select("label").to_series().to_list())
    got_kmers = set(got.select("label").to_series().to_list())
    assert got_kmers == ref_kmers and len(ref_kmers) > 0
    joined = got.join(
        ref, on=["module", "label", "metric"], how="inner", suffix="_ref"
    )
    # Exact on count & max_position; tight tolerance on obs/exp; looser on p-value.
    for metric, tol in [("count", 0.0), ("max_position", 0.0), ("obs_exp_max", 1e-6)]:
        sub = joined.filter(pl.col("metric") == metric)
        worst = sub.select((pl.col("value") - pl.col("value_ref")).abs().max()).item()
        assert worst <= tol, f"kmer {metric} diff vs FastQC = {worst}"
```

- [ ] **Step 5: Run it to verify it fails**

Run: `python -m pytest tests/test_fastqc_golden.py::test_kmer_content_matches_fastqc_exactly -v`
Expected: FAIL — `kmer_content` not in Python `ALL_MODULES` yet.

- [ ] **Step 6: Add the Python module surface**

In `polars_bio/fastqc_op.py`: append `"kmer_content"` to `ALL_MODULES`; update the `fastqc()` docstring to list it and say "twelve"; add the property:

```python
    @property
    def kmer_content(self) -> pl.LazyFrame:
        self._require("kmer_content")
        rows = self._module_rows("kmer_content").collect()
        def col(metric, name):
            return rows.filter(pl.col("metric") == metric).select(
                pl.col("label").alias("kmer"), pl.col("value").alias(name)
            )
        return (
            col("count", "count")
            .join(col("obs_exp_max", "obs_exp_max"), on="kmer")
            .join(col("max_position", "max_position"), on="kmer")
            .join(col("pvalue", "pvalue"), on="kmer")
            .sort("obs_exp_max", descending=True)
            .lazy()
        )
```

- [ ] **Step 7: Rebuild the wheel and run the parity test**

Run: `maturin develop --release && python -m pytest tests/test_fastqc_golden.py::test_kmer_content_matches_fastqc_exactly -v`
Expected: PASS. If reported-set membership fails, re-read `getResults()` (Task 4 Step 1) and reconcile the p-value/obsExp position semantics; the golden is the source of truth.

- [ ] **Step 8: Update `EXPECTED_MODULES`, run all fastqc tests**

Append `"kmer_content"` to `EXPECTED_MODULES` in `tests/test_fastqc.py`.
Run: `python -m pytest tests/test_fastqc.py tests/test_fastqc_golden.py -v`
Expected: PASS (12 modules end-to-end).

- [ ] **Step 9: Commit**

```bash
git add tests/data/io/fastq/kmer_mix.fastq tests/data/io/fastq/golden/kmer_mix.nogroup.kmers.fastqc_data.txt polars_bio/fastqc_op.py tests/test_fastqc.py tests/test_fastqc_golden.py benchmarks/fastqc/parity.py
git commit -m "feat(fastqc): expose kmer_content in Python with exact FastQC parity (12/12)"
```

---

### Task 6: Full-suite verification + docs + memory

**Files:**
- Modify: `CHANGELOG` / docs if the repo tracks FastQC module coverage (grep first).
- Modify: memory file `fastqc-feature.md` (record the two new modules + the sampling/single-partition caveat).

- [ ] **Step 1: Run the entire fastqc test suite + the crate tests**

Run: `cargo test -p datafusion-bio-function-fastqc && cd /Users/mwiewior/research/git/polars-bio && python -m pytest tests/test_fastqc*.py -v`
Expected: ALL PASS.

- [ ] **Step 2: Run the live-FastQC parity harness (optional sanity, if quick)**

Run: `python -m pytest tests/test_fastqc_parity.py -v` (skips if fastqc absent; here it will run).
Expected: PASS or SKIP — confirm no regression for the newly added `_FASTQC_MODULE` entries.

- [ ] **Step 3: Update docs / module-coverage references**

Run: `grep -rn "adapter_content\|dup_levels" docs/ README* 2>/dev/null | grep -i fastqc | head`
For each doc listing the module set, add `per_tile_quality` and `kmer_content`. If none exist, skip.

- [ ] **Step 4: Update project memory**

Edit `/Users/mwiewior/.claude/projects/-Users-mwiewior-research-git-polars-bio/memory/fastqc-feature.md`: note per_tile_quality (exact parity; tile parsed from header field index 4/2; unweighted mean of tile means) and kmer_content (replicates FastQC 2% every-50th sampling + 500bp truncation + binomial×4^k p-value; exact parity only for single-partition plain `.fastq`).

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "docs(fastqc): note per_tile_quality + kmer_content (12/12 core modules)"
```

---

## Self-Review

**Spec coverage:** trait change (Task 1) ✓; per_tile algorithm + status + fixture + golden + Python + harness + test (Tasks 2–3) ✓; kmer sampling/truncation/binomial + fixture + patched-limits golden + Python + harness + test (Tasks 4–5) ✓; registries bumped in both `ALL_MODULES` + `EXPECTED_MODULES` ✓; single-partition caveat documented in code + memory ✓.

**Placeholder scan:** algorithm steps carry concrete code; the only research step (Task 4 Step 1: re-read `getResults()`) has an explicit `1e-9`/tolerance golden gate as its verification, not a "TBD".

**Type consistency:** `per_tile_quality` emits `metric="mean"` (property reads `"mean"`); `kmer_content` emits `count`/`obs_exp_max`/`max_position`/`pvalue` (property + harness + test all use those exact strings); `ALL_MODULES` appended (never reordered) so existing order-asserting tests hold; `update(name, seq, qual)` signature identical across trait, `update_batch`, all modules, and tests.
