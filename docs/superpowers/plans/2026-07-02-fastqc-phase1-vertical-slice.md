# FastQC Phase 1 (Vertical Slice + Harness) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a streaming `fastqc(path, modules?)` DataFusion table function computing four FastQC modules (Basic Statistics, Per-Base Sequence Quality, Per-Sequence GC Content, Sequence Duplication Levels) over the existing FASTQ `TableProvider`, exposed as a tidy Arrow stream in SQL and as `pb.fastqc(...)` in Python, plus a parity harness (vs FastQC) and a benchmark harness (vs RastQC).

**Architecture:** New in-repo Rust module tree `src/fastqc/` holds per-module accumulators implementing a `QcModule` trait (`update`/`merge`/`finalize`, mirroring RastQC's `process_sequence`/`merge_from`/`calculate_results`). A `FastqcExec` `ExecutionPlan` accumulates each input partition independently, merges the partition states, and finalizes one **tidy** `RecordBatch` (`module,label,position,metric,value,value_str`). A `FastqcTableProvider` wraps the datafusion-bio-format-fastq `FastqTableProvider` and projects only `sequence,quality_scores`; a `FastqcFunction` UDTF registers it as `fastqc(...)`. A PyO3 `py_register_fastqc_table` + a `FastQCResult` Python wrapper pivot the tidy stream into typed per-module LazyFrames.

**Tech Stack:** Rust (DataFusion 53.0.0, arrow 58.3.0, PyO3/maturin), `datafusion-bio-format-fastq` v1.8.6, Python 3.12, Polars, PyArrow, pytest.

## Global Constraints

- DataFusion pinned to `=53.0.0`; arrow/arrow-schema/arrow-array `58.3.0`. Do not bump.
- Rust accumulator logic lives **in-repo** under `src/fastqc/` for Phase 1 (extraction to an upstream `datafusion-bio-function-fastqc` crate is a later refactor, out of scope here).
- FASTQ input only. Input provider is `datafusion_bio_format_fastq::table_provider::FastqTableProvider`; its schema exposes `name`, `sequence`, `quality_scores` (all Utf8). The QC operator consumes only `sequence` and `quality_scores`.
- Tidy output schema is FIXED regardless of `modules`: `module: Utf8`, `label: Utf8 (nullable)`, `position: Int32 (nullable)`, `metric: Utf8`, `value: Float64 (nullable)`, `value_str: Utf8 (nullable)`.
- Phred offset is 33 (Sanger/Illumina 1.8+); Phase 1 assumes offset 33.
- Module names (exact string keys): `basic_stats`, `per_base_quality`, `per_seq_gc`, `dup_levels`.
- `modules=None` → all modules; a list → that subset. Accessing a non-computed module in Python **raises `KeyError`**.
- Build the Python wheel with `maturin develop --release`; Rust-only checks with `cargo check` / `cargo test`.
- Match FastQC defaults in the harness (`--nogroup` for exact per-position parity; k-mer size irrelevant for Phase 1 modules).

---

## File Structure

**Rust (create):**
- `src/fastqc/mod.rs` — module exports, `QcModule` trait, `TidyRow`, tidy schema, `ModuleSet`, module registry/selection.
- `src/fastqc/basic_stats.rs` — `BasicStats` accumulator.
- `src/fastqc/per_base_quality.rs` — `PerBaseQuality` accumulator.
- `src/fastqc/per_seq_gc.rs` — `PerSeqGc` accumulator.
- `src/fastqc/dup_levels.rs` — `DuplicationLevels` accumulator.
- `src/fastqc/exec.rs` — `FastqcExec` (`ExecutionPlan`).
- `src/fastqc/provider.rs` — `FastqcTableProvider` + `FastqcFunction` (UDTF).

**Rust (modify):**
- `src/lib.rs` — add `mod fastqc;`, add `py_register_fastqc_table` pyfunction to the module.
- `src/context.rs` — register the `fastqc` UDTF.

**Python (create):**
- `polars_bio/fastqc_op.py` — `FastQCOperations` (`fastqc(...)`) + `FastQCResult`.

**Python (modify):**
- `polars_bio/__init__.py` — expose `fastqc`.

**Tests (create):**
- `tests/test_fastqc.py` — Python integration tests.
- `benchmarks/fastqc/parity.py` — three-way parity harness (vs FastQC / RastQC).
- `benchmarks/fastqc/bench.py` — benchmark harness (vs RastQC).
- `tests/test_fastqc_parity.py` — opt-in parity test (skips if `fastqc`/`rastqc` binaries absent).

Rust unit tests live inline (`#[cfg(test)]`) in each accumulator file.

---

### Task 1: `QcModule` trait, `TidyRow`, and `BasicStats` accumulator

**Files:**
- Create: `src/fastqc/mod.rs`, `src/fastqc/basic_stats.rs`
- Modify: `src/lib.rs:1-7` (add `mod fastqc;`)
- Test: inline `#[cfg(test)]` in `src/fastqc/basic_stats.rs`

**Interfaces:**
- Produces:
  - `pub struct TidyRow { pub module: &'static str, pub label: Option<String>, pub position: Option<i32>, pub metric: String, pub value: Option<f64>, pub value_str: Option<String> }`
  - `pub trait QcModule: Send { fn name(&self) -> &'static str; fn update(&mut self, seq: &[u8], qual: &[u8]); fn merge(&mut self, other: &dyn QcModule); fn finalize(&self, out: &mut Vec<TidyRow>); fn as_any(&self) -> &dyn std::any::Any; }`
  - `pub struct BasicStats { … }` implementing `QcModule` with `pub fn new() -> Self`.

- [ ] **Step 1: Add the module tree to the crate**

In `src/lib.rs`, add after `mod context;` (line 1 region):

```rust
mod fastqc;
```

- [ ] **Step 2: Write `src/fastqc/mod.rs` (trait + TidyRow only for now)**

```rust
//! Streaming FastQC modules computed over the FASTQ TableProvider stream.
//! Each module implements `QcModule` (update/merge/finalize), mirroring
//! RastQC's process_sequence/merge_from/calculate_results.

pub mod basic_stats;

use std::any::Any;

/// One row of the uniform tidy output schema.
#[derive(Debug, Clone, PartialEq)]
pub struct TidyRow {
    pub module: &'static str,
    pub label: Option<String>,
    pub position: Option<i32>,
    pub metric: String,
    pub value: Option<f64>,
    pub value_str: Option<String>,
}

impl TidyRow {
    pub fn num(module: &'static str, metric: &str, value: f64) -> Self {
        TidyRow { module, label: None, position: None, metric: metric.to_string(), value: Some(value), value_str: None }
    }
    pub fn status(module: &'static str, status: &str) -> Self {
        TidyRow { module, label: None, position: None, metric: "status".to_string(), value: None, value_str: Some(status.to_string()) }
    }
}

/// A streaming QC module accumulator.
pub trait QcModule: Send {
    /// Stable identifier, e.g. "basic_stats".
    fn name(&self) -> &'static str;
    /// Fold one read (raw sequence bytes and raw phred+33 quality bytes).
    fn update(&mut self, seq: &[u8], qual: &[u8]);
    /// Merge another accumulator of the SAME concrete type into self.
    fn merge(&mut self, other: &dyn QcModule);
    /// Emit tidy rows for this module's result.
    fn finalize(&self, out: &mut Vec<TidyRow>);
    /// For downcasting in `merge`.
    fn as_any(&self) -> &dyn Any;
}
```

- [ ] **Step 3: Write the failing test in `src/fastqc/basic_stats.rs`**

```rust
use std::any::Any;

use super::{QcModule, TidyRow};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_stats_counts_and_gc() {
        let mut m = BasicStats::new();
        // 2 reads: "ACGT" (gc 2/4), "GGGGCA" (gc 5/6)
        m.update(b"ACGT", b"IIII");
        m.update(b"GGGGCA", b"IIIIII");
        let mut rows = Vec::new();
        m.finalize(&mut rows);
        let get = |metric: &str| rows.iter().find(|r| r.metric == metric).and_then(|r| r.value).unwrap();
        assert_eq!(get("n_seq"), 2.0);
        assert_eq!(get("min_len"), 4.0);
        assert_eq!(get("max_len"), 6.0);
        assert_eq!(get("total_bases"), 10.0);
        // GC% = (2 + 5) / 10 * 100 = 70.0
        assert!((get("gc_pct") - 70.0).abs() < 1e-9);
        assert!(rows.iter().any(|r| r.metric == "status" && r.value_str.as_deref() == Some("PASS")));
    }
}
```

- [ ] **Step 4: Run it and confirm it fails to compile (BasicStats missing)**

Run: `cargo test --lib fastqc::basic_stats 2>&1 | head -20`
Expected: FAIL — `cannot find type BasicStats`.

- [ ] **Step 5: Implement `BasicStats` above the test module in `src/fastqc/basic_stats.rs`**

```rust
/// FastQC "Basic Statistics": read count, length range, total bases, GC%.
#[derive(Debug, Default)]
pub struct BasicStats {
    n_seq: u64,
    total_bases: u64,
    gc_bases: u64,
    n_bases: u64,
    min_len: Option<u64>,
    max_len: u64,
}

impl BasicStats {
    pub fn new() -> Self {
        Self::default()
    }
}

impl QcModule for BasicStats {
    fn name(&self) -> &'static str {
        "basic_stats"
    }

    fn update(&mut self, seq: &[u8], _qual: &[u8]) {
        self.n_seq += 1;
        let len = seq.len() as u64;
        self.total_bases += len;
        self.max_len = self.max_len.max(len);
        self.min_len = Some(self.min_len.map_or(len, |m| m.min(len)));
        for &b in seq {
            match b {
                b'G' | b'g' | b'C' | b'c' => self.gc_bases += 1,
                b'N' | b'n' => self.n_bases += 1,
                _ => {},
            }
        }
    }

    fn merge(&mut self, other: &dyn QcModule) {
        let o = other.as_any().downcast_ref::<BasicStats>().expect("merge type mismatch");
        self.n_seq += o.n_seq;
        self.total_bases += o.total_bases;
        self.gc_bases += o.gc_bases;
        self.n_bases += o.n_bases;
        self.max_len = self.max_len.max(o.max_len);
        self.min_len = match (self.min_len, o.min_len) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }

    fn finalize(&self, out: &mut Vec<TidyRow>) {
        let m = "basic_stats";
        out.push(TidyRow::num(m, "n_seq", self.n_seq as f64));
        out.push(TidyRow::num(m, "total_bases", self.total_bases as f64));
        out.push(TidyRow::num(m, "min_len", self.min_len.unwrap_or(0) as f64));
        out.push(TidyRow::num(m, "max_len", self.max_len as f64));
        let gc_pct = if self.total_bases > 0 {
            self.gc_bases as f64 / self.total_bases as f64 * 100.0
        } else {
            0.0
        };
        out.push(TidyRow::num(m, "gc_pct", gc_pct));
        // FastQC Basic Statistics never warns/fails.
        out.push(TidyRow::status(m, "PASS"));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

- [ ] **Step 6: Run the test and confirm it passes**

Run: `cargo test --lib fastqc::basic_stats 2>&1 | tail -20`
Expected: PASS (1 test).

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs src/fastqc/mod.rs src/fastqc/basic_stats.rs
git commit -m "feat(fastqc): QcModule trait + BasicStats accumulator"
```

---

### Task 2: `PerBaseQuality` accumulator

**Files:**
- Create: `src/fastqc/per_base_quality.rs`
- Modify: `src/fastqc/mod.rs` (add `pub mod per_base_quality;`)
- Test: inline in `src/fastqc/per_base_quality.rs`

**Interfaces:**
- Produces: `pub struct PerBaseQuality { … }` impl `QcModule`, `pub fn new() -> Self`. Emits, per 1-based `position`, six rows `metric ∈ {mean, median, q1, q3, p10, p90}`, plus one `status` row.

- [ ] **Step 1: Register the submodule** in `src/fastqc/mod.rs`, after `pub mod basic_stats;`:

```rust
pub mod per_base_quality;
```

- [ ] **Step 2: Write the failing test in `src/fastqc/per_base_quality.rs`**

```rust
use std::any::Any;

use super::{QcModule, TidyRow};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_base_quality_mean_per_position() {
        let mut m = PerBaseQuality::new();
        // qualities '!' = phred 0, 'I' = phred 40, '5' = phred 20
        m.update(b"AA", b"!I"); // pos1 -> 0, pos2 -> 40
        m.update(b"AA", b"I5"); // pos1 -> 40, pos2 -> 20
        let mut rows = Vec::new();
        m.finalize(&mut rows);
        let mean_at = |pos: i32| {
            rows.iter()
                .find(|r| r.position == Some(pos) && r.metric == "mean")
                .and_then(|r| r.value)
                .unwrap()
        };
        assert!((mean_at(1) - 20.0).abs() < 1e-9); // (0+40)/2
        assert!((mean_at(2) - 30.0).abs() < 1e-9); // (40+20)/2
    }
}
```

- [ ] **Step 3: Confirm it fails**

Run: `cargo test --lib fastqc::per_base_quality 2>&1 | head -20`
Expected: FAIL — `cannot find type PerBaseQuality`.

- [ ] **Step 4: Implement `PerBaseQuality`**

```rust
/// Max phred value tracked (0..=93 covers all realistic phred scores).
const QUAL_MAX: usize = 94;

/// FastQC "Per Base Sequence Quality": per-position phred distribution,
/// summarized as mean + median + quartiles + 10th/90th percentiles.
#[derive(Debug, Default)]
pub struct PerBaseQuality {
    /// position -> histogram of phred values (0..QUAL_MAX)
    hist: Vec<[u64; QUAL_MAX]>,
}

impl PerBaseQuality {
    pub fn new() -> Self {
        Self::default()
    }

    fn ensure_len(&mut self, len: usize) {
        if self.hist.len() < len {
            self.hist.resize(len, [0u64; QUAL_MAX]);
        }
    }
}

/// Linear-interpolation-free percentile over an integer histogram, matching
/// FastQC's "nth value" convention (the value at the ceil(p*N)-th observation).
fn percentile(hist: &[u64; QUAL_MAX], total: u64, p: f64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let rank = (p * total as f64).ceil().max(1.0) as u64;
    let mut cum = 0u64;
    for (q, &c) in hist.iter().enumerate() {
        cum += c;
        if cum >= rank {
            return q as f64;
        }
    }
    (QUAL_MAX - 1) as f64
}

impl QcModule for PerBaseQuality {
    fn name(&self) -> &'static str {
        "per_base_quality"
    }

    fn update(&mut self, _seq: &[u8], qual: &[u8]) {
        self.ensure_len(qual.len());
        for (i, &q) in qual.iter().enumerate() {
            let phred = (q.saturating_sub(33)) as usize;
            let phred = phred.min(QUAL_MAX - 1);
            self.hist[i][phred] += 1;
        }
    }

    fn merge(&mut self, other: &dyn QcModule) {
        let o = other.as_any().downcast_ref::<PerBaseQuality>().expect("merge type mismatch");
        self.ensure_len(o.hist.len());
        for (i, oh) in o.hist.iter().enumerate() {
            for q in 0..QUAL_MAX {
                self.hist[i][q] += oh[q];
            }
        }
    }

    fn finalize(&self, out: &mut Vec<TidyRow>) {
        let m = "per_base_quality";
        let mut worst_median = f64::INFINITY;
        let mut worst_q1 = f64::INFINITY;
        for (i, h) in self.hist.iter().enumerate() {
            let total: u64 = h.iter().sum();
            if total == 0 {
                continue;
            }
            let pos = (i + 1) as i32;
            let sum: u64 = h.iter().enumerate().map(|(q, &c)| q as u64 * c).sum();
            let mean = sum as f64 / total as f64;
            let median = percentile(h, total, 0.50);
            let q1 = percentile(h, total, 0.25);
            let q3 = percentile(h, total, 0.75);
            let p10 = percentile(h, total, 0.10);
            let p90 = percentile(h, total, 0.90);
            worst_median = worst_median.min(median);
            worst_q1 = worst_q1.min(q1);
            for (metric, v) in [("mean", mean), ("median", median), ("q1", q1), ("q3", q3), ("p10", p10), ("p90", p90)] {
                out.push(TidyRow { module: m, label: None, position: Some(pos), metric: metric.to_string(), value: Some(v), value_str: None });
            }
        }
        // FastQC status: fail if any lower-quartile<5 or median<20; warn if <10 or <25.
        let status = if worst_q1 < 5.0 || worst_median < 20.0 {
            "FAIL"
        } else if worst_q1 < 10.0 || worst_median < 25.0 {
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

- [ ] **Step 5: Run the test and confirm it passes**

Run: `cargo test --lib fastqc::per_base_quality 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/fastqc/mod.rs src/fastqc/per_base_quality.rs
git commit -m "feat(fastqc): PerBaseQuality accumulator"
```

---

### Task 3: `PerSeqGc` accumulator

**Files:**
- Create: `src/fastqc/per_seq_gc.rs`
- Modify: `src/fastqc/mod.rs` (add `pub mod per_seq_gc;`)
- Test: inline

**Interfaces:**
- Produces: `pub struct PerSeqGc { … }` impl `QcModule`, `pub fn new() -> Self`. Emits one row per GC% bin `0..=100` with `position = gc_bin`, `metric = "count"`, plus a `status` row.

- [ ] **Step 1: Register submodule** in `src/fastqc/mod.rs`: `pub mod per_seq_gc;`

- [ ] **Step 2: Failing test in `src/fastqc/per_seq_gc.rs`**

```rust
use std::any::Any;

use super::{QcModule, TidyRow};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_seq_gc_bins_reads() {
        let mut m = PerSeqGc::new();
        m.update(b"GGCC", b"IIII"); // 100% GC -> bin 100
        m.update(b"ATAT", b"IIII"); // 0% GC   -> bin 0
        m.update(b"ATGC", b"IIII"); // 50% GC  -> bin 50
        let mut rows = Vec::new();
        m.finalize(&mut rows);
        let count_at = |bin: i32| {
            rows.iter()
                .find(|r| r.position == Some(bin) && r.metric == "count")
                .and_then(|r| r.value)
                .unwrap_or(0.0)
        };
        assert_eq!(count_at(0), 1.0);
        assert_eq!(count_at(50), 1.0);
        assert_eq!(count_at(100), 1.0);
    }
}
```

- [ ] **Step 3: Confirm it fails**

Run: `cargo test --lib fastqc::per_seq_gc 2>&1 | head -20`
Expected: FAIL — `cannot find type PerSeqGc`.

- [ ] **Step 4: Implement `PerSeqGc`**

```rust
/// FastQC "Per Sequence GC Content": distribution of per-read GC% over 0..=100.
#[derive(Debug)]
pub struct PerSeqGc {
    /// bins[g] = number of reads whose rounded GC% == g
    bins: [u64; 101],
}

impl PerSeqGc {
    pub fn new() -> Self {
        Self { bins: [0u64; 101] }
    }
}

impl Default for PerSeqGc {
    fn default() -> Self {
        Self::new()
    }
}

impl QcModule for PerSeqGc {
    fn name(&self) -> &'static str {
        "per_seq_gc"
    }

    fn update(&mut self, seq: &[u8], _qual: &[u8]) {
        if seq.is_empty() {
            return;
        }
        let mut gc = 0u64;
        let mut counted = 0u64;
        for &b in seq {
            match b {
                b'G' | b'g' | b'C' | b'c' => {
                    gc += 1;
                    counted += 1;
                },
                b'A' | b'a' | b'T' | b't' | b'U' | b'u' => counted += 1,
                _ => {}, // N and others excluded from the denominator, matching FastQC
            }
        }
        if counted == 0 {
            return;
        }
        let pct = (gc as f64 / counted as f64) * 100.0;
        let bin = pct.round() as usize; // 0..=100
        self.bins[bin.min(100)] += 1;
    }

    fn merge(&mut self, other: &dyn QcModule) {
        let o = other.as_any().downcast_ref::<PerSeqGc>().expect("merge type mismatch");
        for i in 0..101 {
            self.bins[i] += o.bins[i];
        }
    }

    fn finalize(&self, out: &mut Vec<TidyRow>) {
        let m = "per_seq_gc";
        for (g, &c) in self.bins.iter().enumerate() {
            out.push(TidyRow { module: m, label: None, position: Some(g as i32), metric: "count".to_string(), value: Some(c as f64), value_str: None });
        }
        // Phase-1 status: PASS. Exact FastQC theoretical-distribution status
        // (warn>15% / fail>30% deviation) is validated as a follow-up; the
        // parity harness checks the count distribution, which is exact here.
        out.push(TidyRow::status(m, "PASS"));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

- [ ] **Step 5: Run and confirm pass**

Run: `cargo test --lib fastqc::per_seq_gc 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/fastqc/mod.rs src/fastqc/per_seq_gc.rs
git commit -m "feat(fastqc): PerSeqGc accumulator"
```

---

### Task 4: `DuplicationLevels` accumulator (partition-merge stress)

**Files:**
- Create: `src/fastqc/dup_levels.rs`
- Modify: `src/fastqc/mod.rs` (add `pub mod dup_levels;`)
- Test: inline

**Interfaces:**
- Produces: `pub struct DuplicationLevels { … }` impl `QcModule`, `pub fn new() -> Self`. Emits per duplication-level bin (`label ∈ {"1","2",…,"9",">10",">50",">100",">500",">1k",">5k",">10k+"}`) a `metric="pct"` (% of total sequences at that level), plus a `metric="pct_dup"` total and a `status` row.

- [ ] **Step 1: Register submodule** in `src/fastqc/mod.rs`: `pub mod dup_levels;`

- [ ] **Step 2: Failing test in `src/fastqc/dup_levels.rs`**

```rust
use std::any::Any;
use std::collections::HashMap;

use super::{QcModule, TidyRow};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dup_levels_and_merge() {
        // Partition A sees "AAAA" twice; partition B sees "AAAA" once and "CCCC" once.
        let mut a = DuplicationLevels::new();
        a.update(b"AAAA", b"IIII");
        a.update(b"AAAA", b"IIII");
        let mut b = DuplicationLevels::new();
        b.update(b"AAAA", b"IIII");
        b.update(b"CCCC", b"IIII");
        a.merge(&b);
        // After merge: AAAA x3, CCCC x1 -> 2 distinct, 4 observations.
        let mut rows = Vec::new();
        a.finalize(&mut rows);
        // %dup = (total - distinct) / total * 100 = (4 - 2) / 4 * 100 = 50.0
        let pct_dup = rows.iter().find(|r| r.metric == "pct_dup").and_then(|r| r.value).unwrap();
        assert!((pct_dup - 50.0).abs() < 1e-9);
    }
}
```

- [ ] **Step 3: Confirm it fails**

Run: `cargo test --lib fastqc::dup_levels 2>&1 | head -20`
Expected: FAIL — `cannot find type DuplicationLevels`.

- [ ] **Step 4: Implement `DuplicationLevels`**

```rust
/// FastQC caps distinct-sequence tracking to bound memory.
const MAX_TRACKED: usize = 100_000;
/// Only the first N bases are used as the dedup key (FastQC uses 50).
const KEY_PREFIX: usize = 50;

/// FastQC "Sequence Duplication Levels": counts how often each (prefix of a)
/// sequence recurs, then bins recurrence counts into duplication levels.
#[derive(Debug, Default)]
pub struct DuplicationLevels {
    counts: HashMap<Vec<u8>, u64>,
    /// Observations dropped because the tracking table was full (still counted
    /// toward totals via `overflow_obs`).
    overflow_obs: u64,
}

impl DuplicationLevels {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(seq: &[u8]) -> Vec<u8> {
        let n = seq.len().min(KEY_PREFIX);
        seq[..n].to_ascii_uppercase()
    }

    fn add_count(&mut self, key: Vec<u8>, n: u64) {
        if let Some(c) = self.counts.get_mut(&key) {
            *c += n;
        } else if self.counts.len() < MAX_TRACKED {
            self.counts.insert(key, n);
        } else {
            self.overflow_obs += n;
        }
    }

    fn level_bin(count: u64) -> &'static str {
        match count {
            0 => unreachable!(),
            1 => "1",
            2 => "2",
            3 => "3",
            4 => "4",
            5 => "5",
            6 => "6",
            7 => "7",
            8 => "8",
            9 => "9",
            10..=49 => ">10",
            50..=99 => ">50",
            100..=499 => ">100",
            500..=999 => ">500",
            1000..=4999 => ">1k",
            5000..=9999 => ">5k",
            _ => ">10k+",
        }
    }
}

const BINS: [&str; 16] = ["1", "2", "3", "4", "5", "6", "7", "8", "9", ">10", ">50", ">100", ">500", ">1k", ">5k", ">10k+"];

impl QcModule for DuplicationLevels {
    fn name(&self) -> &'static str {
        "dup_levels"
    }

    fn update(&mut self, seq: &[u8], _qual: &[u8]) {
        let key = Self::key(seq);
        self.add_count(key, 1);
    }

    fn merge(&mut self, other: &dyn QcModule) {
        let o = other.as_any().downcast_ref::<DuplicationLevels>().expect("merge type mismatch");
        for (k, &c) in &o.counts {
            self.add_count(k.clone(), c);
        }
        self.overflow_obs += o.overflow_obs;
    }

    fn finalize(&self, out: &mut Vec<TidyRow>) {
        let m = "dup_levels";
        let distinct = self.counts.len() as u64;
        let tracked_obs: u64 = self.counts.values().sum();
        let total_obs = tracked_obs + self.overflow_obs;

        // Duplication-level histogram over tracked sequences.
        let mut level_counts: std::collections::HashMap<&'static str, u64> = std::collections::HashMap::new();
        for &c in self.counts.values() {
            *level_counts.entry(Self::level_bin(c)).or_insert(0) += 1;
        }
        for bin in BINS {
            let n = *level_counts.get(bin).unwrap_or(&0);
            let pct = if distinct > 0 { n as f64 / distinct as f64 * 100.0 } else { 0.0 };
            out.push(TidyRow { module: m, label: Some(bin.to_string()), position: None, metric: "pct".to_string(), value: Some(pct), value_str: None });
        }

        // Overall % duplication over observations.
        let pct_dup = if total_obs > 0 {
            (total_obs - distinct.min(total_obs)) as f64 / total_obs as f64 * 100.0
        } else {
            0.0
        };
        out.push(TidyRow::num(m, "pct_dup", pct_dup));

        // FastQC status: warn if >20% non-unique, fail if >50%.
        let status = if pct_dup > 50.0 { "FAIL" } else if pct_dup > 20.0 { "WARN" } else { "PASS" };
        out.push(TidyRow::status(m, status));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

- [ ] **Step 5: Run and confirm pass**

Run: `cargo test --lib fastqc::dup_levels 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/fastqc/mod.rs src/fastqc/dup_levels.rs
git commit -m "feat(fastqc): DuplicationLevels accumulator with partition merge"
```

---

### Task 5: `ModuleSet` (selection, batch update, merge, tidy `RecordBatch`)

**Files:**
- Modify: `src/fastqc/mod.rs`
- Test: inline in `src/fastqc/mod.rs`

**Interfaces:**
- Consumes: `QcModule`, `TidyRow`, and the four accumulator structs from Tasks 1–4.
- Produces:
  - `pub fn tidy_schema() -> arrow_schema::SchemaRef`
  - `pub struct ModuleSet { modules: Vec<Box<dyn QcModule>> }`
  - `pub fn build(selection: Option<&[String]>) -> datafusion::common::Result<ModuleSet>`
  - `impl ModuleSet { pub fn update_batch(&mut self, batch: &arrow_array::RecordBatch) -> datafusion::common::Result<()>; pub fn merge(&mut self, other: ModuleSet); pub fn finalize(self) -> datafusion::common::Result<arrow_array::RecordBatch>; }`
  - `pub const ALL_MODULES: [&str; 4] = ["basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"];`

- [ ] **Step 1: Add imports + schema + selection to `src/fastqc/mod.rs`**

Append to `src/fastqc/mod.rs`:

```rust
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int32Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};

use basic_stats::BasicStats;
use dup_levels::DuplicationLevels;
use per_base_quality::PerBaseQuality;
use per_seq_gc::PerSeqGc;

pub const ALL_MODULES: [&str; 4] = ["basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"];

/// The fixed tidy output schema (independent of module selection).
pub fn tidy_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("module", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("position", DataType::Int32, true),
        Field::new("metric", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("value_str", DataType::Utf8, true),
    ]))
}

fn make_module(name: &str) -> Result<Box<dyn QcModule>> {
    Ok(match name {
        "basic_stats" => Box::new(BasicStats::new()),
        "per_base_quality" => Box::new(PerBaseQuality::new()),
        "per_seq_gc" => Box::new(PerSeqGc::new()),
        "dup_levels" => Box::new(DuplicationLevels::new()),
        other => {
            return Err(DataFusionError::Plan(format!(
                "unknown fastqc module '{other}'; valid: {}",
                ALL_MODULES.join(", ")
            )))
        },
    })
}

pub struct ModuleSet {
    modules: Vec<Box<dyn QcModule>>,
}

impl ModuleSet {
    /// Build the requested modules (None => all), preserving ALL_MODULES order.
    pub fn build(selection: Option<&[String]>) -> Result<Self> {
        let names: Vec<&str> = match selection {
            None => ALL_MODULES.to_vec(),
            Some(sel) => {
                // Validate + order by ALL_MODULES so output is deterministic.
                for s in sel {
                    if !ALL_MODULES.contains(&s.as_str()) {
                        return Err(DataFusionError::Plan(format!(
                            "unknown fastqc module '{s}'; valid: {}",
                            ALL_MODULES.join(", ")
                        )));
                    }
                }
                ALL_MODULES.iter().copied().filter(|m| sel.iter().any(|s| s == m)).collect()
            },
        };
        let modules = names.iter().map(|n| make_module(n)).collect::<Result<Vec<_>>>()?;
        Ok(Self { modules })
    }

    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        use arrow_array::{cast::AsArray, Array};
        let seq = batch
            .column_by_name("sequence")
            .ok_or_else(|| DataFusionError::Execution("fastqc input missing 'sequence'".into()))?
            .as_string::<i32>();
        let qual = batch
            .column_by_name("quality_scores")
            .ok_or_else(|| DataFusionError::Execution("fastqc input missing 'quality_scores'".into()))?
            .as_string::<i32>();
        for i in 0..batch.num_rows() {
            if seq.is_null(i) {
                continue;
            }
            let s = seq.value(i).as_bytes();
            let q = if qual.is_null(i) { &[][..] } else { qual.value(i).as_bytes() };
            for m in self.modules.iter_mut() {
                m.update(s, q);
            }
        }
        Ok(())
    }

    /// Merge another ModuleSet (same selection/order) into self.
    pub fn merge(&mut self, other: ModuleSet) {
        for (a, b) in self.modules.iter_mut().zip(other.modules.iter()) {
            a.merge(b.as_ref());
        }
    }

    pub fn finalize(self) -> Result<RecordBatch> {
        let mut rows: Vec<TidyRow> = Vec::new();
        for m in &self.modules {
            m.finalize(&mut rows);
        }
        let mut module_b = StringBuilder::new();
        let mut label_b = StringBuilder::new();
        let mut pos_b = Int32Builder::new();
        let mut metric_b = StringBuilder::new();
        let mut value_b = Float64Builder::new();
        let mut vstr_b = StringBuilder::new();
        for r in rows {
            module_b.append_value(r.module);
            match r.label {
                Some(l) => label_b.append_value(l),
                None => label_b.append_null(),
            }
            match r.position {
                Some(p) => pos_b.append_value(p),
                None => pos_b.append_null(),
            }
            metric_b.append_value(r.metric);
            match r.value {
                Some(v) => value_b.append_value(v),
                None => value_b.append_null(),
            }
            match r.value_str {
                Some(s) => vstr_b.append_value(s),
                None => vstr_b.append_null(),
            }
        }
        RecordBatch::try_new(
            tidy_schema(),
            vec![
                Arc::new(module_b.finish()),
                Arc::new(label_b.finish()),
                Arc::new(pos_b.finish()),
                Arc::new(metric_b.finish()),
                Arc::new(value_b.finish()),
                Arc::new(vstr_b.finish()),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}
```

- [ ] **Step 2: Write the failing test** at the bottom of `src/fastqc/mod.rs`

```rust
#[cfg(test)]
mod set_tests {
    use super::*;
    use arrow_array::StringArray;

    fn batch(seqs: &[&str], quals: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("sequence", DataType::Utf8, true),
                Field::new("quality_scores", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(seqs.to_vec())),
                Arc::new(StringArray::from(quals.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn selection_validates_and_orders() {
        assert!(ModuleSet::build(Some(&["bogus".to_string()])).is_err());
        let set = ModuleSet::build(Some(&["per_seq_gc".to_string(), "basic_stats".to_string()])).unwrap();
        assert_eq!(set.modules.len(), 2);
        // Order follows ALL_MODULES: basic_stats before per_seq_gc.
        assert_eq!(set.modules[0].name(), "basic_stats");
        assert_eq!(set.modules[1].name(), "per_seq_gc");
    }

    #[test]
    fn end_to_end_tidy_batch() {
        let mut set = ModuleSet::build(None).unwrap();
        set.update_batch(&batch(&["ACGT", "GGCC"], &["IIII", "IIII"])).unwrap();
        let out = set.finalize().unwrap();
        assert_eq!(out.schema(), tidy_schema());
        assert!(out.num_rows() > 0);
    }
}
```

- [ ] **Step 3: Confirm it fails, then passes**

Run: `cargo test --lib fastqc:: 2>&1 | tail -30`
Expected: after implementing Step 1, all fastqc unit tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/fastqc/mod.rs
git commit -m "feat(fastqc): ModuleSet selection, batch update, merge, tidy RecordBatch"
```

---

### Task 6: `FastqcExec` ExecutionPlan (per-partition accumulate + merge)

**Files:**
- Create: `src/fastqc/exec.rs`
- Modify: `src/fastqc/mod.rs` (add `pub mod exec;`)
- Test: inline in `src/fastqc/exec.rs` (via an in-memory input plan)

**Interfaces:**
- Consumes: `ModuleSet`, `tidy_schema` (Task 5).
- Produces: `pub struct FastqcExec { … }` with `pub fn new(input: Arc<dyn ExecutionPlan>, selection: Option<Vec<String>>) -> Self`, implementing `ExecutionPlan`. Output partitioning is a single partition; `execute(0, …)` drains every input partition, builds one `ModuleSet` per partition, merges them, and yields one tidy `RecordBatch`.

- [ ] **Step 1: Register submodule** in `src/fastqc/mod.rs`: `pub mod exec;`

- [ ] **Step 2: Write `src/fastqc/exec.rs`**

```rust
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::StreamExt;

use super::{tidy_schema, ModuleSet};

/// Physical operator: fold FASTQ (sequence, quality_scores) batches through the
/// selected QC modules and emit a single tidy RecordBatch.
pub struct FastqcExec {
    input: Arc<dyn ExecutionPlan>,
    selection: Option<Vec<String>>,
    cache: PlanProperties,
}

impl FastqcExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, selection: Option<Vec<String>>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(tidy_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self { input, selection, cache }
    }
}

impl fmt::Debug for FastqcExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FastqcExec(selection={:?})", self.selection)
    }
}

impl DisplayAs for FastqcExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FastqcExec: modules={:?}", self.selection)
    }
}

impl ExecutionPlan for FastqcExec {
    fn name(&self) -> &str {
        "FastqcExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FastqcExec::new(children[0].clone(), self.selection.clone())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "FastqcExec has a single output partition");
        let input = self.input.clone();
        let selection = self.selection.clone();
        let n_parts = input.output_partitioning().partition_count();
        let schema = tidy_schema();

        let fut = async move {
            // Accumulate every input partition concurrently, then merge.
            let mut tasks = Vec::with_capacity(n_parts);
            for p in 0..n_parts {
                let input = input.clone();
                let selection = selection.clone();
                let ctx = context.clone();
                tasks.push(async move {
                    let mut set = ModuleSet::build(selection.as_deref())?;
                    let mut stream = input.execute(p, ctx)?;
                    while let Some(batch) = stream.next().await {
                        set.update_batch(&batch?)?;
                    }
                    Ok::<ModuleSet, datafusion::common::DataFusionError>(set)
                });
            }
            let sets = futures::future::try_join_all(tasks).await?;
            let mut merged = ModuleSet::build(selection.as_deref())?;
            for s in sets {
                merged.merge(s);
            }
            let batch = merged.finalize()?;
            Ok::<RecordBatch, datafusion::common::DataFusionError>(batch)
        };

        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryExec;

    fn input_plan(n_parts: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sequence", DataType::Utf8, true),
            Field::new("quality_scores", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["ACGT", "GGCC"])),
                Arc::new(StringArray::from(vec!["IIII", "IIII"])),
            ],
        )
        .unwrap();
        let parts: Vec<Vec<RecordBatch>> = (0..n_parts).map(|_| vec![batch.clone()]).collect();
        Arc::new(MemoryExec::try_new(&parts, schema, None).unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn same_result_1_vs_n_partitions() {
        let ctx = Arc::new(TaskContext::default());
        let run = |n: usize| {
            let ctx = ctx.clone();
            async move {
                let exec = FastqcExec::new(input_plan(n), Some(vec!["basic_stats".to_string()]));
                let mut s = exec.execute(0, ctx).unwrap();
                let b = s.next().await.unwrap().unwrap();
                // n_seq scales with partition count (2 reads per partition).
                let idx = (0..b.num_rows())
                    .find(|&i| {
                        b.column_by_name("metric").unwrap().as_any()
                            .downcast_ref::<StringArray>().unwrap().value(i) == "n_seq"
                    })
                    .unwrap();
                b.column_by_name("value").unwrap().as_any()
                    .downcast_ref::<arrow_array::Float64Array>().unwrap().value(idx)
            }
        };
        assert_eq!(run(1).await, 2.0);
        assert_eq!(run(4).await, 8.0);
    }
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test --lib fastqc::exec 2>&1 | tail -30`
Expected: PASS. (If `MemoryExec` import path differs on DF 53, use `datafusion::physical_plan::memory::MemoryExec`; adjust `PlanProperties`/`Boundedness` imports to whatever `cargo check` reports — the API is stable in 53.0.0.)

- [ ] **Step 4: Commit**

```bash
git add src/fastqc/mod.rs src/fastqc/exec.rs
git commit -m "feat(fastqc): FastqcExec ExecutionPlan with per-partition merge"
```

---

### Task 7: `FastqcTableProvider` + `FastqcFunction` UDTF + SQL registration

**Files:**
- Create: `src/fastqc/provider.rs`
- Modify: `src/fastqc/mod.rs` (add `pub mod provider;`), `src/context.rs` (register UDTF)
- Test: `tests/test_fastqc.py::test_sql_udtf` (deferred to Task 10; here confirm build)

**Interfaces:**
- Consumes: `FastqcExec`, `tidy_schema` (Tasks 5–6); `FastqTableProvider` from `datafusion_bio_format_fastq::table_provider`.
- Produces:
  - `pub struct FastqcTableProvider { input: Arc<dyn TableProvider>, selection: Option<Vec<String>> }` impl `TableProvider`, `pub fn new(input, selection) -> Self`.
  - `pub struct FastqcFunction;` impl `TableFunctionImpl` (`fastqc('path'[, ['mod', …]])`).

- [ ] **Step 1: Register submodule** in `src/fastqc/mod.rs`: `pub mod provider;`

- [ ] **Step 2: Write `src/fastqc/provider.rs`** (mirrors `src/pileup.rs` `DepthTableProvider`/`DepthFunction`)

```rust
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_bio_format_fastq::table_provider::FastqTableProvider;
use log::info;

use super::exec::FastqcExec;
use super::tidy_schema;

pub struct FastqcTableProvider {
    input: Arc<dyn TableProvider>,
    selection: Option<Vec<String>>,
}

impl FastqcTableProvider {
    pub fn new(input: Arc<dyn TableProvider>, selection: Option<Vec<String>>) -> Self {
        Self { input, selection }
    }
}

impl std::fmt::Debug for FastqcTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastqcTableProvider").field("selection", &self.selection).finish()
    }
}

#[async_trait]
impl TableProvider for FastqcTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> arrow_schema::SchemaRef {
        tidy_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Only sequence + quality_scores are needed.
        let input_schema = self.input.schema();
        let needed = ["sequence", "quality_scores"];
        let projection: Vec<usize> = needed.iter().filter_map(|n| input_schema.index_of(n).ok()).collect();
        let input_plan = self.input.scan(state, Some(&projection), &[], None).await?;
        Ok(Arc::new(FastqcExec::new(input_plan, self.selection.clone())))
    }
}

/// `SELECT * FROM fastqc('reads.fastq'[, ['per_base_quality', ...]])`.
#[derive(Debug, Default)]
pub struct FastqcFunction;

impl TableFunctionImpl for FastqcFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let path = match args.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s.clone(),
            _ => return Err(DataFusionError::Plan("fastqc() requires a string literal path as first argument".into())),
        };
        // Optional second arg: a list of module names.
        let selection: Option<Vec<String>> = match args.get(1) {
            None => None,
            Some(Expr::Literal(ScalarValue::List(arr), _)) => {
                let mut out = Vec::new();
                let values = arr.values();
                let strs = values
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| DataFusionError::Plan("fastqc() modules list must be strings".into()))?;
                for i in 0..strs.len() {
                    if !strs.is_null(i) {
                        out.push(strs.value(i).to_string());
                    }
                }
                Some(out)
            },
            Some(_) => return Err(DataFusionError::Plan("fastqc() second argument must be a list of module names".into())),
        };

        info!("fastqc() UDTF: path={path}, modules={selection:?}");

        // Validate selection early (surfaces bad module names at plan time).
        super::ModuleSet::build(selection.as_deref())?;

        let provider: Arc<dyn TableProvider> = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                FastqTableProvider::new(path.clone(), None)
                    .map(|p| Arc::new(p) as Arc<dyn TableProvider>)
                    .map_err(|e| DataFusionError::Execution(format!("Failed to create FASTQ provider: {e}")))
            })
        })?;

        Ok(Arc::new(FastqcTableProvider::new(provider, selection)))
    }
}
```

- [ ] **Step 3: Register the UDTF in `src/context.rs`**

After the depth registration (`ctx.register_udtf("depth", …)`, ~line 124), add:

```rust
    // Register fastqc UDTF for SQL: SELECT * FROM fastqc('file.fastq')
    ctx.register_udtf("fastqc", std::sync::Arc::new(crate::fastqc::provider::FastqcFunction));
```

- [ ] **Step 4: Confirm the crate builds**

Run: `cargo check 2>&1 | tail -30`
Expected: no errors. (If `ScalarValue::List` field arity differs on DF 53, match the compiler's suggested pattern — the list is an `Arc<dyn Array>`/`ListArray` variant.)

- [ ] **Step 5: Commit**

```bash
git add src/fastqc/mod.rs src/fastqc/provider.rs src/context.rs
git commit -m "feat(fastqc): FastqcTableProvider + fastqc UDTF registration"
```

---

### Task 8: PyO3 binding `py_register_fastqc_table`

**Files:**
- Modify: `src/lib.rs`
- Test: exercised via Python in Task 10.

**Interfaces:**
- Consumes: the `fastqc` UDTF (Task 7); the existing `PyBioSessionContext` (`ctx`), `register_frame`-style helpers.
- Produces: pyfunction `py_register_fastqc_table(py_ctx: &PyBioSessionContext, path: String, modules: Option<Vec<String>>) -> PyResult<String>` returning a registered table name whose scan yields the tidy schema.

- [ ] **Step 1: Read the existing `py_register_pileup_table`** to copy its shape.

Run: `grep -n "py_register_pileup_table" src/lib.rs`
Then read ~40 lines around it. Replicate: build a unique table name, construct the provider via `FastqcFunction`-equivalent logic (call the UDTF path directly), and register it on `py_ctx.ctx`.

- [ ] **Step 2: Add the pyfunction to `src/lib.rs`** (place next to `py_register_pileup_table`)

```rust
#[pyfunction]
fn py_register_fastqc_table(
    py_ctx: &context::PyBioSessionContext,
    path: String,
    modules: Option<Vec<String>>,
) -> PyResult<String> {
    use pyo3::exceptions::PyValueError;

    let table_name = format!("fastqc_{}", uuid::Uuid::new_v4().simple());
    let ctx = &py_ctx.ctx;

    // Validate module selection eagerly for a clean Python error.
    fastqc::ModuleSet::build(modules.as_deref())
        .map_err(|e| PyValueError::new_err(format!("{e}")))?;

    let provider: std::sync::Arc<dyn datafusion::datasource::TableProvider> =
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                let fq = datafusion_bio_format_fastq::table_provider::FastqTableProvider::new(path.clone(), None)
                    .map_err(|e| format!("Failed to create FASTQ provider: {e}"))?;
                Ok::<_, String>(std::sync::Arc::new(fastqc::provider::FastqcTableProvider::new(
                    std::sync::Arc::new(fq),
                    modules,
                )) as std::sync::Arc<dyn datafusion::datasource::TableProvider>)
            })
        })
        .map_err(|e| PyValueError::new_err(e))?;

    ctx.register_table(&table_name, provider)
        .map_err(|e| PyValueError::new_err(format!("Failed to register fastqc table: {e}")))?;
    Ok(table_name)
}
```

- [ ] **Step 3: Add it to the `#[pymodule]` registration**

Find the `m.add_function(wrap_pyfunction!(py_register_pileup_table, m)?)?;` line and add below it:

```rust
    m.add_function(wrap_pyfunction!(py_register_fastqc_table, m)?)?;
```

- [ ] **Step 4: Build the wheel**

Run: `maturin develop --release 2>&1 | tail -20`
Expected: build succeeds; `import polars_bio` works.

- [ ] **Step 5: Smoke-check the binding from Python**

Run:
```bash
python -c "from polars_bio.polars_bio import py_register_fastqc_table; print('ok')"
```
Expected: `ok`.

- [ ] **Step 6: Commit**

```bash
git add src/lib.rs
git commit -m "feat(fastqc): py_register_fastqc_table PyO3 binding"
```

---

### Task 9: Python `FastQCOperations` + `FastQCResult` wrapper

**Files:**
- Create: `polars_bio/fastqc_op.py`
- Modify: `polars_bio/__init__.py`
- Test: Task 10.

**Interfaces:**
- Consumes: `py_register_fastqc_table`, `py_get_table_schema`, `py_read_table` (existing), `register_io_source` streaming pattern (copy from `polars_bio/pileup_op.py`).
- Produces:
  - `class FastQCResult` with `.tidy: pl.LazyFrame`, `.computed: set[str]`, properties `.basic_stats`, `.per_base_quality`, `.per_seq_gc`, `.dup_levels` (each `pl.LazyFrame`), `.summary() -> pl.LazyFrame`.
  - `class FastQCOperations` with `@staticmethod fastqc(path, modules=None, group=True) -> FastQCResult`.

- [ ] **Step 1: Write `polars_bio/fastqc_op.py`**

```python
from typing import Iterator, List, Optional, Union

import polars as pl
import pyarrow as pa
from polars.io.plugins import register_io_source

from .context import ctx
from .logging import logger

ALL_MODULES = ["basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"]


def _tidy_lazyframe(path: str, modules: Optional[List[str]]) -> pl.LazyFrame:
    """Single-pass fastqc run exposed as the raw tidy LazyFrame."""
    from polars_bio.polars_bio import (
        py_get_table_schema,
        py_register_fastqc_table,
    )

    table_name = py_register_fastqc_table(ctx, path, modules)
    schema = py_get_table_schema(ctx, table_name)
    empty = pa.table({f.name: pa.array([], type=f.type) for f in schema})
    polars_schema = dict(pl.from_arrow(empty).schema)

    def _source(
        with_columns: Union[pl.Expr, None],
        predicate: Union[pl.Expr, None],
        n_rows: Union[int, None],
        _batch_size: Union[int, None],
    ) -> Iterator[pl.DataFrame]:
        from polars_bio.polars_bio import py_read_table

        from .context import ctx as _ctx

        query_df = py_read_table(_ctx, table_name)
        stream = query_df.execute_stream()
        for batch in stream:
            out = pl.DataFrame(batch.to_pyarrow())
            if predicate is not None:
                out = out.filter(predicate)
            if with_columns is not None:
                out = out.select(with_columns)
            yield out
        try:
            _ctx.deregister_table(table_name)
        except Exception:
            pass

    return register_io_source(_source, schema=polars_schema)


class FastQCResult:
    """Result of a single streaming `fastqc` pass.

    Each per-module property is an Arrow-backed LazyFrame pivoted from the tidy
    stream. Accessing a module that was not computed raises KeyError.
    """

    def __init__(self, tidy: pl.LazyFrame, computed: List[str]):
        self.tidy = tidy
        self.computed = set(computed)

    def _require(self, module: str) -> None:
        if module not in self.computed:
            raise KeyError(
                f"module '{module}' was not computed "
                f"(requested: {sorted(self.computed)}); "
                f"call fastqc(..., modules=[..., '{module}'])"
            )

    def _module_rows(self, module: str) -> pl.LazyFrame:
        return self.tidy.filter(pl.col("module") == module)

    @property
    def basic_stats(self) -> pl.LazyFrame:
        self._require("basic_stats")
        return (
            self._module_rows("basic_stats")
            .filter(pl.col("metric") != "status")
            .select("metric", "value")
        )

    @property
    def per_base_quality(self) -> pl.LazyFrame:
        self._require("per_base_quality")
        return (
            self._module_rows("per_base_quality")
            .filter(pl.col("position").is_not_null())
            .collect()
            .pivot(values="value", index="position", on="metric")
            .lazy()
            .sort("position")
        )

    @property
    def per_seq_gc(self) -> pl.LazyFrame:
        self._require("per_seq_gc")
        return (
            self._module_rows("per_seq_gc")
            .filter(pl.col("metric") == "count")
            .select(pl.col("position").alias("gc_pct"), pl.col("value").alias("count"))
            .sort("gc_pct")
        )

    @property
    def dup_levels(self) -> pl.LazyFrame:
        self._require("dup_levels")
        return (
            self._module_rows("dup_levels")
            .filter(pl.col("metric") == "pct")
            .select(pl.col("label").alias("dup_level"), pl.col("value").alias("pct"))
        )

    def summary(self) -> pl.LazyFrame:
        return (
            self.tidy.filter(pl.col("metric") == "status")
            .select(pl.col("module"), pl.col("value_str").alias("status"))
        )


class FastQCOperations:
    @staticmethod
    def fastqc(
        path: str,
        modules: Optional[List[str]] = None,
        group: bool = True,
    ) -> FastQCResult:
        """Compute FastQC modules over a FASTQ file in one streaming pass.

        Args:
            path: Path to a FASTQ file (plain, .gz, or .bgz).
            modules: Module names to compute; None computes all
                (basic_stats, per_base_quality, per_seq_gc, dup_levels).
                Accessing a non-computed module on the result raises KeyError.
            group: Reserved for FastQC-style position binning of long reads
                (group=False == FastQC --nogroup). No-op for Phase 1 modules.

        Returns:
            FastQCResult with `.tidy`, per-module LazyFrames, and `.summary()`.
        """
        if modules is not None:
            unknown = [m for m in modules if m not in ALL_MODULES]
            if unknown:
                raise ValueError(f"unknown fastqc modules {unknown}; valid: {ALL_MODULES}")
        computed = list(modules) if modules is not None else list(ALL_MODULES)
        tidy = _tidy_lazyframe(path, modules)
        if not group:
            logger.debug("group=False has no effect for Phase 1 modules")
        return FastQCResult(tidy, computed)
```

- [ ] **Step 2: Expose it in `polars_bio/__init__.py`**

After the pileup line (`from .pileup_op import PileupOperations as pileup_operations`), add:

```python
from .fastqc_op import FastQCOperations as fastqc_operations
```

And after `depth = pileup_operations.depth`, add:

```python
fastqc = fastqc_operations.fastqc
```

- [ ] **Step 3: Smoke test the import**

Run: `python -c "import polars_bio as pb; print(pb.fastqc)"`
Expected: prints a function reference.

- [ ] **Step 4: Commit**

```bash
git add polars_bio/fastqc_op.py polars_bio/__init__.py
git commit -m "feat(fastqc): Python FastQCOperations + FastQCResult wrapper"
```

---

### Task 10: Python integration tests

**Files:**
- Create: `tests/test_fastqc.py`
- Uses fixture: `tests/data/io/fastq/example.fastq`

**Interfaces:**
- Consumes: `pb.fastqc`, `pb.sql`, `pb.set_option` (for partition count).

- [ ] **Step 1: Write `tests/test_fastqc.py`**

```python
import polars as pl
import pytest

import polars_bio as pb

FASTQ = "tests/data/io/fastq/example.fastq"


def test_tidy_schema_and_all_modules():
    qc = pb.fastqc(FASTQ)
    tidy = qc.tidy.collect()
    assert tidy.columns == ["module", "label", "position", "metric", "value", "value_str"]
    assert set(tidy["module"].unique()) == {
        "basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"
    }


def test_basic_stats_values():
    qc = pb.fastqc(FASTQ, modules=["basic_stats"])
    bs = qc.basic_stats.collect()
    metrics = dict(zip(bs["metric"], bs["value"]))
    assert metrics["n_seq"] > 0
    assert metrics["max_len"] >= metrics["min_len"]
    assert 0.0 <= metrics["gc_pct"] <= 100.0


def test_per_base_quality_shape():
    qc = pb.fastqc(FASTQ, modules=["per_base_quality"])
    pbq = qc.per_base_quality.collect()
    assert set(["position", "mean", "median", "q1", "q3", "p10", "p90"]).issubset(pbq.columns)
    assert pbq["position"].min() == 1


def test_non_computed_module_raises():
    qc = pb.fastqc(FASTQ, modules=["basic_stats"])
    with pytest.raises(KeyError):
        _ = qc.per_base_quality


def test_unknown_module_raises():
    with pytest.raises(ValueError):
        pb.fastqc(FASTQ, modules=["not_a_module"])


def test_summary_has_status_per_module():
    qc = pb.fastqc(FASTQ)
    summary = qc.summary().collect()
    assert set(summary["module"]) == {"basic_stats", "per_base_quality", "per_seq_gc", "dup_levels"}
    assert summary["status"].is_in(["PASS", "WARN", "FAIL"]).all()


def test_sql_udtf():
    df = pb.sql(f"SELECT * FROM fastqc('{FASTQ}') WHERE metric = 'status'").collect()
    assert "module" in df.columns
    assert df.height == 4


@pytest.mark.parametrize("n_parts", [1, 4])
def test_partition_merge_invariant(n_parts):
    pb.set_option("datafusion.execution.target_partitions", str(n_parts))
    qc = pb.fastqc(FASTQ, modules=["basic_stats", "dup_levels"])
    bs = dict(zip(*qc.basic_stats.collect().to_dict(as_series=False).values()))
    # n_seq must be identical regardless of partition count.
    assert bs["n_seq"] == pytest.approx(_expected_n_seq())


def _expected_n_seq() -> float:
    pb.set_option("datafusion.execution.target_partitions", "1")
    bs = pb.fastqc(FASTQ, modules=["basic_stats"]).basic_stats.collect()
    return float(dict(zip(bs["metric"], bs["value"]))["n_seq"])
```

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/test_fastqc.py -v 2>&1 | tail -30`
Expected: all PASS. (If `pivot` on an empty group errors for a tiny fixture, confirm the fixture has ≥1 read with quality — `example.fastq` does.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_fastqc.py
git commit -m "test(fastqc): Python integration tests (schema, modules, SQL, partition merge)"
```

---

### Task 11: Three-way parity harness (vs FastQC / RastQC)

**Files:**
- Create: `benchmarks/fastqc/parity.py`, `tests/test_fastqc_parity.py`

**Interfaces:**
- Consumes: `pb.fastqc`; external binaries `fastqc` and `rastqc` (optional; test skips if absent).
- Produces:
  - `parse_fastqc_data(path) -> pl.DataFrame` (tidy schema) — parses `fastqc_data.txt`.
  - `parity_report(pb_tidy, ref_tidy, tolerances) -> pl.DataFrame` — per `(module)` rows compared / exact / within-tol / mismatch.

- [ ] **Step 1: Write `benchmarks/fastqc/parity.py`**

```python
"""Parse FastQC/RastQC outputs into polars-bio's tidy schema and compare.

FastQC (s-andrews) is the correctness oracle. Run references with --nogroup
for exact per-position parity.
"""
import subprocess
import tempfile
from pathlib import Path

import polars as pl

# Map FastQC fastqc_data.txt module headers -> polars-bio module ids.
_FASTQC_MODULE = {
    "Basic Statistics": "basic_stats",
    "Per base sequence quality": "per_base_quality",
    "Per sequence GC content": "per_seq_gc",
    "Sequence Duplication Levels": "dup_levels",
}

# Per-metric absolute tolerance (0 == exact match required).
TOLERANCES = {
    ("basic_stats", "n_seq"): 0.0,
    ("basic_stats", "total_bases"): 0.0,
    ("basic_stats", "gc_pct"): 0.5,
    ("per_base_quality", "mean"): 0.1,
    ("per_base_quality", "median"): 0.0,
    ("per_seq_gc", "count"): 0.0,
    ("dup_levels", "pct_dup"): 0.5,
}
DEFAULT_TOL = 0.1


def run_fastqc(fastq: str, outdir: str) -> str:
    subprocess.run(
        ["fastqc", "--extract", "--nogroup", "-o", outdir, fastq],
        check=True, capture_output=True,
    )
    stem = Path(fastq).name
    for suffix in (".fastq.gz", ".fastq", ".fq.gz", ".fq"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return str(Path(outdir) / f"{stem}_fastqc" / "fastqc_data.txt")


def parse_fastqc_data(path: str) -> pl.DataFrame:
    """Parse fastqc_data.txt >>Module ... >>END_MODULE sections to tidy rows."""
    rows = []
    module = None
    header = None
    for line in Path(path).read_text().splitlines():
        if line.startswith(">>END_MODULE"):
            module, header = None, None
            continue
        if line.startswith(">>"):
            name = line[2:].rsplit("\t", 1)[0].strip()
            module = _FASTQC_MODULE.get(name)
            header = None
            continue
        if module is None:
            continue
        if line.startswith("#"):
            header = line[1:].split("\t")
            continue
        parts = line.split("\t")
        _emit(rows, module, header, parts)
    return pl.DataFrame(rows, schema={
        "module": pl.Utf8, "label": pl.Utf8, "position": pl.Int32,
        "metric": pl.Utf8, "value": pl.Float64,
    })


def _emit(rows, module, header, parts):
    # Module-specific row shaping into (module, label, position, metric, value).
    if module == "basic_stats":
        key = {"Total Sequences": "n_seq", "%GC": "gc_pct"}.get(parts[0])
        if key:
            rows.append(dict(module=module, label=None, position=None, metric=key, value=float(parts[1])))
    elif module == "per_base_quality":
        # header: Base, Mean, Median, Lower Quartile, Upper Quartile, 10th Percentile, 90th Percentile
        pos = int(parts[0].split("-")[0])
        for metric, idx in [("mean", 1), ("median", 2), ("q1", 3), ("q3", 4), ("p10", 5), ("p90", 6)]:
            rows.append(dict(module=module, label=None, position=pos, metric=metric, value=float(parts[idx])))
    elif module == "per_seq_gc":
        rows.append(dict(module=module, label=None, position=int(float(parts[0])), metric="count", value=float(parts[1])))
    elif module == "dup_levels":
        if parts[0].startswith("#Total Deduplicated Percentage") or len(parts) < 3:
            return
        # columns: Duplication Level, Percentage of deduplicated, Percentage of total
        rows.append(dict(module=module, label=parts[0], position=None, metric="pct", value=float(parts[2])))


def pb_tidy(fastq: str, modules) -> pl.DataFrame:
    import polars_bio as pb
    return (
        pb.fastqc(fastq, modules=modules).tidy.collect()
        .select("module", "label", "position", "metric", "value")
    )


def parity_report(pb_df: pl.DataFrame, ref_df: pl.DataFrame) -> pl.DataFrame:
    keys = ["module", "label", "position", "metric"]
    joined = pb_df.join(ref_df, on=keys, how="inner", suffix="_ref")
    def verdict(row):
        tol = TOLERANCES.get((row["module"], row["metric"]), DEFAULT_TOL)
        diff = abs((row["value"] or 0.0) - (row["value_ref"] or 0.0))
        return "exact" if diff == 0 else ("within_tol" if diff <= tol else "mismatch")
    joined = joined.with_columns(
        pl.struct(["module", "metric", "value", "value_ref"])
        .map_elements(verdict, return_dtype=pl.Utf8).alias("verdict")
    )
    return joined.group_by("module", "verdict").len().sort("module", "verdict")


if __name__ == "__main__":
    import sys
    fastq = sys.argv[1]
    with tempfile.TemporaryDirectory() as d:
        data = run_fastqc(fastq, d)
        ref = parse_fastqc_data(data)
        got = pb_tidy(fastq, None)
        print(parity_report(got, ref))
```

- [ ] **Step 2: Write the opt-in pytest `tests/test_fastqc_parity.py`**

```python
import shutil
import tempfile

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("fastqc") is None, reason="FastQC binary not installed"
)

FASTQ = "tests/data/io/fastq/example.fastq"


def test_parity_against_fastqc():
    from benchmarks.fastqc.parity import parity_report, parse_fastqc_data, pb_tidy, run_fastqc

    with tempfile.TemporaryDirectory() as d:
        ref = parse_fastqc_data(run_fastqc(FASTQ, d))
        got = pb_tidy(FASTQ, None)
        report = parity_report(got, ref)
        mism = report.filter(report["verdict"] == "mismatch")
        assert mism.height == 0, f"parity mismatches:\n{mism}"
```

- [ ] **Step 3: Run (skips cleanly if FastQC absent)**

Run: `python -m pytest tests/test_fastqc_parity.py -v 2>&1 | tail -20`
Expected: PASS or SKIPPED (if `fastqc` not on PATH).

- [ ] **Step 4: Commit**

```bash
git add benchmarks/fastqc/parity.py tests/test_fastqc_parity.py
git commit -m "test(fastqc): three-way parity harness vs FastQC (opt-in)"
```

---

### Task 12: Benchmark harness (vs RastQC)

**Files:**
- Create: `benchmarks/fastqc/bench.py`

**Interfaces:**
- Consumes: `pb.fastqc`; external `rastqc` binary (optional); `pb.scan_fastq` for the scan-only baseline.

- [ ] **Step 1: Write `benchmarks/fastqc/bench.py`**

```python
"""Benchmark polars-bio fastqc against RastQC.

Reports per-run wall time, throughput, and a scan-only baseline so the QC
math is isolated from FASTQ decode. Run 1-thread and all-core.
"""
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import polars_bio as pb


def _timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def scan_only(fastq: str) -> float:
    # Force full materialization of the FASTQ scan with no QC.
    return _timed(lambda: pb.scan_fastq(fastq).select(pl_len()).collect())


def pl_len():
    import polars as pl
    return pl.len()


def pb_fastqc(fastq: str, threads: int) -> float:
    pb.set_option("datafusion.execution.target_partitions", str(threads))
    return _timed(lambda: pb.fastqc(fastq).tidy.collect())


def rastqc(fastq: str, threads: int) -> float:
    if shutil.which("rastqc") is None:
        return float("nan")
    with tempfile.TemporaryDirectory() as d:
        return _timed(lambda: subprocess.run(
            ["rastqc", "-t", str(threads), "--nozip", "-o", d, fastq],
            check=True, capture_output=True,
        ))


def main(fastq: str):
    n_reads = pb.scan_fastq(fastq).select(pl_len()).collect().item()
    size_mb = Path(fastq).stat().st_size / 1e6
    print(f"file={fastq} reads={n_reads} size={size_mb:.1f}MB")
    for threads in (1, 0):  # 0 == all cores (DataFusion default)
        t = threads or "all"
        base = scan_only(fastq)
        pbt = pb_fastqc(fastq, threads or _all_cores())
        rqt = rastqc(fastq, threads or _all_cores())
        print(f"[threads={t}] scan_only={base:.3f}s  pb.fastqc={pbt:.3f}s "
              f"(qc_only={pbt-base:.3f}s, {n_reads/pbt:,.0f} reads/s)  rastqc={rqt:.3f}s")


def _all_cores() -> int:
    import os
    return os.cpu_count() or 1


if __name__ == "__main__":
    import sys
    main(sys.argv[1])
```

- [ ] **Step 2: Smoke-run against the fixture**

Run: `python benchmarks/fastqc/bench.py tests/data/io/fastq/example.fastq 2>&1 | tail -10`
Expected: prints timing lines; `rastqc=nan` if the binary is absent (acceptable — it still exercises the polars-bio path and scan baseline).

- [ ] **Step 3: Commit**

```bash
git add benchmarks/fastqc/bench.py
git commit -m "bench(fastqc): benchmark harness vs RastQC with scan-only baseline"
```

---

## Self-Review

**Spec coverage (Phase 1 slice of the design):**
- Streaming UDTF over existing FASTQ provider → Tasks 6–8. ✅
- Tidy Arrow schema (exact columns/types) → Task 5 `tidy_schema`. ✅
- `modules` gates *computation* (unselected never allocated) → Task 5 `ModuleSet::build` + Task 6 per-partition build. ✅
- Four vertical-slice modules (scalar/positional/histogram/hash) → Tasks 1–4. ✅
- Python `pb.fastqc` + typed per-module frames + raise-on-non-computed + `summary()` + `.tidy` → Task 9; tests Task 10. ✅
- Partition-merge correctness (1 vs N) → Task 6 Rust test + Task 10 Python test. ✅
- Parity vs FastQC (ground truth, `--nogroup`) → Task 11. ✅
- Benchmark vs RastQC (1-thread/all-core, scan-only baseline) → Task 12. ✅
- SQL surface `SELECT * FROM fastqc(...)` incl. subset + status filter → Task 7 + Task 10 `test_sql_udtf`. ✅
- `group` toggle placeholder → Task 9 (no-op for Phase 1 modules, documented). ✅ (Full binning lands with positional Phase 3 modules.)

**Deferred by design (not Phase 1):** exact FastQC theoretical-distribution status for `per_seq_gc` (Task 3 emits counts exactly + PASS status; noted); BAM/CRAM input; MultiQC JSON export; upstream crate extraction.

**Placeholder scan:** No TBD/TODO; every code step contains full code. The one intentional simplification (`per_seq_gc` status = PASS) is documented inline and excluded from parity assertions via `TOLERANCES` (no `status` tolerance key → status rows aren't numeric-compared).

**Type consistency:** `QcModule`/`TidyRow` signatures (Task 1) are used unchanged in Tasks 2–6; `ModuleSet::build(Option<&[String]>)` is consumed identically in `FastqcExec` (Task 6), `FastqcFunction` (Task 7), and `py_register_fastqc_table` (Task 8); module id strings (`basic_stats`, `per_base_quality`, `per_seq_gc`, `dup_levels`) match across Rust `ALL_MODULES`, Python `ALL_MODULES`, tests, and the parity map.

**Known API-drift risk (flagged, not blocking):** exact DataFusion 53 paths for `PlanProperties`/`Boundedness`/`EmissionType`/`MemoryExec` and the `ScalarValue::List` pattern may need a one-line adjustment against `cargo check` output — noted inline in Tasks 6–7. The pileup code (`src/pileup.rs`) is the in-repo reference for the stable subset of these APIs.
