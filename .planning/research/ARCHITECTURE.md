# Architecture Research

**Domain:** RNA-seq feature counting pipeline (BAM + GTF -> gene counts)
**Researched:** 2026-03-20
**Confidence:** HIGH (polars-bio source verified directly; featureCounts algorithm from official docs and paper)

## Standard Architecture

### How featureCounts Works Internally

featureCounts (Subread package) operates in four logical stages:

```
Stage 1: GTF Parsing
  Read GTF → extract "exon" features → group by gene_id → build meta-feature index
  (hash table: chrom -> sorted exon intervals -> gene_id)

Stage 2: BAM Read Filter
  For each read:
    - Skip: secondary alignments (flag 0x100), supplementary (flag 0x800)
    - Skip: MAPQ below threshold (default: 0, i.e. no MAPQ filter)
    - Skip: unmapped reads (flag 0x4)
    - Retain: primary alignments only by default

Stage 3: Overlap Detection
  For each passing read:
    - Find all exon intervals overlapping the read (using hierarchical bin search)
    - Collect the set of gene_ids for overlapping exons
    - Rule: if exactly ONE gene_id -> assign read to that gene
    - Rule: if ZERO gene_ids     -> classify as "__no_feature"
    - Rule: if >1 gene_id        -> classify as "__ambiguous" (discard by default)
    - Within same gene: read spanning multiple exons still counts ONCE (de-duplicate by gene_id)

Stage 4: Summarization
  GROUP BY gene_id → SUM(assigned_reads) → output count matrix
```

Key semantics:
- Feature type filtered to "exon" by default (`-t exon`)
- Meta-feature key is `gene_id` by default (`-g gene_id`)
- Overlap condition: at least 1 base of the read overlaps the exon
- Unstranded mode (`-s 0`): no strand check, both strands count
- Ambiguity default: discard multi-gene reads (not `-O` mode)

### How HTSeq-count Works (Union Mode)

HTSeq-count is the correctness reference. In union mode:

```
For each read position i, compute S(i) = {features overlapping position i}
Union S = union of all S(i) across all read positions
If |union S| == 1 → count for that feature
If |union S| == 0 → __no_feature
If |union S| > 1  → __ambiguous (not counted)
```

Union mode matches featureCounts default behavior for single-end unstranded counting.

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      User / Notebook Layer                           │
│  count_features(bam_path, gtf_path) -> LazyFrame[gene_id, count]   │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────┐
│                    Feature Counting Orchestrator                      │
│  (new Python function in polars_bio/ — composes existing primitives) │
├──────────────────┬────────────────────────┬─────────────────────────┤
│  GTF Loader      │   BAM Loader           │  Ambiguity Filter       │
│  scan_gtf()      │   scan_bam()           │  (Polars groupby/join)  │
│  filter: exon    │   filter: primary      │                         │
│  select: gene_id │   select: chrom/pos    │                         │
└──────────────────┴───────────┬────────────┴─────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                    Interval Join Layer                                │
│  pb.overlap(reads_lf, exons_lf, ...)                                 │
│  → pairs (read_name, gene_id) with coordinate match                  │
│  Output: LazyFrame[read_name, gene_id]                               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                  Ambiguity Resolution Layer                           │
│  groupby(read_name) → count(distinct gene_id)                        │
│  keep only read_name with exactly 1 gene_id                          │
│  Output: LazyFrame[read_name, gene_id]                               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                    Aggregation Layer                                  │
│  groupby(gene_id) → count(read_name)                                 │
│  Output: LazyFrame[gene_id, count]                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Implemented By |
|-----------|---------------|----------------|
| GTF Loader | Read annotation file, filter to exon rows, extract gene_id as column | `pb.scan_gtf(attr_fields=["gene_id"]).filter(pl.col("feature") == "exon")` |
| BAM Loader | Read alignments, filter unmapped/secondary/supplementary | `pb.scan_bam().filter(~pl.col("flags").cast(pl.Int32).and_(0x104) > 0)` |
| Interval Join | Find (read, exon) pairs where genomic coordinates overlap | `pb.overlap(reads_lf, exons_lf)` — existing primitive |
| Ambiguity Resolution | Per-read: if read hits >1 gene_id, discard | Polars `.group_by("read_name").agg(pl.col("gene_id").n_unique())` → filter |
| Aggregation | Per-gene: count unambiguous reads | Polars `.group_by("gene_id").agg(pl.len())` |

## Data Flow

### Full Pipeline Flow (BAM + GTF -> gene counts)

```
scan_gtf("gencode.gtf", attr_fields=["gene_id"])
    │ filter: feature == "exon"
    │ select: chrom, start, end, gene_id
    ▼
exons_lf: LazyFrame[chrom, start, end, gene_id]   (1-based, ~1M rows Gencode)

scan_bam("sample.bam")
    │ filter: (flags & 0x904) == 0   (not unmapped, secondary, supplementary)
    │ select: name, chrom, pos, cigar_end_pos
    ▼
reads_lf: LazyFrame[name, chrom, start=pos, end=cigar_end_pos]   (1-based)

pb.overlap(reads_lf, exons_lf,
           cols1=["chrom","start","end"],
           cols2=["chrom","start","end"])
    │ Rust CoITrees spatial join
    │ Returns pairs: (read_name, gene_id) for each overlapping exon
    ▼
overlaps_lf: LazyFrame[name, gene_id, ...]   (may have same name multiple times
                                               if read overlaps multiple exons/genes)

.group_by("name").agg(pl.col("gene_id").n_unique().alias("n_genes"),
                       pl.col("gene_id").first())
    │ Polars streaming groupby
    ▼
per_read_lf: LazyFrame[name, n_genes, gene_id]

.filter(pl.col("n_genes") == 1)   # keep unambiguous reads only
    ▼
unambiguous_lf: LazyFrame[name, gene_id]

.group_by("gene_id").agg(pl.len().alias("count"))
    │ Polars streaming groupby
    ▼
counts_df: DataFrame[gene_id, count]   <- FINAL OUTPUT
```

### Coordinate System Handling

This is the most important implementation detail. BAM and GTF are both 1-based in polars-bio's default output mode, which is correct for featureCounts matching:

```
GTF (Gencode): 1-based, closed intervals  [start, end]
BAM (htslib):  0-based internally; polars-bio scan_bam() outputs 1-based by default
               (pos column = 1-based POS from SAM)

With default use_zero_based=None (-> False) for both:
  - scan_gtf() -> coordinate_system_zero_based=False (1-based)
  - scan_bam() -> coordinate_system_zero_based=False (1-based)
  - pb.overlap() reads metadata, uses FilterOp.Weak (1-based semantics)

Result: coordinates are compatible, no off-by-one error.
```

Warning: if `use_zero_based=True` is set globally or on either call, both must match. The safest approach is to use polars-bio defaults (leave `use_zero_based` as None on all I/O calls) so metadata propagation works automatically.

### BAM Flag Filtering (featureCounts Default Equivalence)

featureCounts default excludes secondary (`0x100`) and unmapped (`0x4`) reads. Supplementary (`0x800`) reads are typically excluded too. The polars-bio equivalent:

```python
# Bitwise: exclude unmapped (4), secondary (256), supplementary (2048)
EXCLUDE_FLAGS = 0x4 | 0x100 | 0x800  # = 2308

reads_lf = pb.scan_bam(bam_path).filter(
    (pl.col("flags").cast(pl.Int32) & EXCLUDE_FLAGS) == 0
)
```

The `flags` column from `scan_bam()` is a standard SAM FLAG integer field.

### read_name Uniqueness Requirement

BAM reads need a unique key for ambiguity resolution. The `name` column from `scan_bam()` is the query name (QNAME). For single-end data this is sufficient. For paired-end data (out of scope for v1 benchmark), read pairs share the same QNAME — differentiation would require `name` + mate flag bits.

### The Dual-Groupby Pattern (Critical for correctness)

The double-groupby is what makes this match featureCounts semantics:

```
Groupby 1 (by read): de-duplicates across exons of same gene,
                     detects multi-gene ambiguity

Groupby 2 (by gene): final counting
```

Alternative using `count_overlaps` only works if there is no ambiguity resolution needed, which there is. `count_overlaps(exons_lf, reads_lf)` counts how many reads hit each exon interval — it does not: (a) de-duplicate reads spanning multiple exons of the same gene, nor (b) handle the ambiguous multi-gene case. Therefore `overlap()` + two Polars `group_by()` calls is the correct composition.

## Recommended Project Structure

```
polars_bio/
├── feature_count.py          # New: FeatureCountOperations class
│                             # count_features() public function
├── __init__.py               # Add: count_features alias
└── (no Rust changes needed)

notebooks/                    # (or docs/notebooks/)
├── feature_counting_benchmark.ipynb
│   ├── Cell 1: Data download & setup
│   ├── Cell 2: featureCounts reference run
│   ├── Cell 3: HTSeq-count reference run
│   ├── Cell 4: polars-bio count_features() run
│   ├── Cell 5: Correctness validation (count comparison)
│   └── Cell 6: Performance comparison (wall time, RSS)

tests/
└── test_feature_count.py     # Unit tests with small BAM+GTF fixture
```

## Architectural Patterns

### Pattern 1: Overlap-then-Groupby (Recommended)

**What:** Use `pb.overlap()` to get the full join of (read, exon) pairs, then apply two Polars `group_by()` calls to resolve ambiguity and aggregate counts.

**When to use:** Whenever featureCounts-compatible semantics are needed — unambiguous read assignment to exactly one gene.

**Trade-offs:** One full-scan `overlap()` join + two streaming groupbys. All three stages are lazy-compatible and streaming-compatible in Polars. Peak memory is determined by the overlap output, which can be large for 100M-read datasets (each read may appear in multiple rows).

**Example:**
```python
import polars_bio as pb
import polars as pl

EXCLUDE_FLAGS = 0x4 | 0x100 | 0x800

def count_features(bam_path: str, gtf_path: str) -> pl.LazyFrame:
    exons = (
        pb.scan_gtf(gtf_path, attr_fields=["gene_id"])
        .filter(pl.col("feature") == "exon")
        .select(["seqname", "start", "end", "gene_id"])
        .rename({"seqname": "chrom"})
    )

    reads = (
        pb.scan_bam(bam_path)
        .filter((pl.col("flags").cast(pl.Int32) & EXCLUDE_FLAGS) == 0)
        .select(["name", "chrom", "pos", "end"])  # end = pos + alignment length
    )

    overlaps = pb.overlap(
        reads,
        exons,
        cols1=["chrom", "pos", "end"],
        cols2=["chrom", "start", "end"],
        suffixes=("_read", "_exon"),
    ).select(["name_read", "gene_id"])

    per_read = (
        overlaps
        .group_by("name_read")
        .agg([
            pl.col("gene_id").n_unique().alias("n_genes"),
            pl.col("gene_id").first().alias("gene_id"),
        ])
        .filter(pl.col("n_genes") == 1)
    )

    return (
        per_read
        .group_by("gene_id")
        .agg(pl.len().alias("count"))
        .sort("gene_id")
    )
```

### Pattern 2: count_overlaps-only (Simpler, Less Correct)

**What:** Use `pb.count_overlaps(exons, reads)` to get a per-exon read count, then groupby gene_id to sum.

**When to use:** Quick approximation or when correctness vs featureCounts is not required.

**Why not recommended:** Does not handle the two correctness requirements: (a) a read spanning two exons of the same gene counts twice, (b) reads overlapping multiple genes are not discarded. This produces inflated, incorrect counts.

### Pattern 3: SQL-based (Alternative)

**What:** Register both tables, use a SQL JOIN + GROUP BY query via `pb.sql()`.

**When to use:** If DataFusion SQL optimization can push down predicates more aggressively than the Python API allows, or for prototyping.

**Trade-offs:** SQL path goes through DataFusion's optimizer; may be faster for simple cases but loses the Polars streaming benefits for the groupby stages. The ambiguity detection (n_genes == 1) is harder to express cleanly in SQL.

## Anti-Patterns

### Anti-Pattern 1: Using count_overlaps() Directly for Gene Counts

**What people do:** Call `pb.count_overlaps(exons_lf, reads_lf)` and sum results by gene_id.

**Why it's wrong:** count_overlaps counts exon-level hits, not gene-level reads. A 100bp read spanning 2 exons of the same gene would contribute 2 to the sum. Reads overlapping 2 different genes would be counted for both genes rather than discarded.

**Do this instead:** Use `pb.overlap()` + double `group_by()` as shown in Pattern 1.

### Anti-Pattern 2: Coordinate System Mismatch

**What people do:** Set `use_zero_based=True` on `scan_bam()` but leave GTF at default (1-based), or vice versa.

**Why it's wrong:** BAM and GTF will be in different coordinate spaces. The `pb.overlap()` call will raise `CoordinateSystemMismatchError` if metadata is properly set, or silently produce wrong intervals if metadata is missing.

**Do this instead:** Use polars-bio defaults (don't set `use_zero_based` explicitly on either) so both BAM and GTF emit 1-based coordinates with correct metadata, and `pb.overlap()` picks up `FilterOp.Weak` automatically.

### Anti-Pattern 3: Ignoring BAM Flag Filtering

**What people do:** Call `pb.scan_bam()` without filtering the `flags` column.

**Why it's wrong:** Secondary alignments and supplementary alignments would be counted as separate reads, inflating counts for multimappers and chimeric reads. featureCounts excludes these by default.

**Do this instead:** Filter on the `flags` column before the overlap join, keeping only primary, mapped reads: `(flags & 0x904) == 0`.

### Anti-Pattern 4: Losing read_name after Overlap

**What people do:** Drop the read `name` column after the overlap join to save memory.

**Why it's wrong:** The ambiguity resolution requires grouping by read name to detect reads hitting multiple genes. Without `name`, there is no way to implement the discard rule.

**Do this instead:** Keep `name` through the overlap result, drop it only after the final `group_by("gene_id")`.

## Integration Points

### polars-bio Primitives Used

| Primitive | What It Does in Pipeline | Notes |
|-----------|--------------------------|-------|
| `pb.scan_gtf(attr_fields=["gene_id"])` | Lazy GTF read with gene_id attribute extraction | GtfLazyFrameWrapper handles re-registration when gene_id is requested |
| `pb.scan_bam()` | Lazy BAM read with flag/coordinate columns | `name`, `chrom`, `pos`, `flags` needed; cigar-derived end position needed |
| `pb.overlap()` | Spatial join returning (read, exon) pairs | Uses CoITrees; returns LazyFrame |
| Polars `.filter()` | Flag filtering, ambiguity filtering | Pushed down into DataFusion for BAM; client-side for post-overlap |
| Polars `.group_by().agg()` | Ambiguity resolution + count aggregation | Streaming-compatible |

### BAM Schema Awareness

`scan_bam()` standard output columns relevant to feature counting:

| Column | Type | Role |
|--------|------|------|
| `name` | String | Read QNAME — key for ambiguity groupby |
| `chrom` | String | Chromosome — overlap join key |
| `pos` | Int32 | Alignment start (1-based, POS in SAM) |
| `flags` | Int32 | SAM FLAG — used for read filter |
| `cigar` | String | CIGAR string — needed to compute alignment end position |

The `end` position is not directly in the BAM schema — it must be computed from `pos` + alignment length derived from CIGAR. This is a gap: polars-bio does not expose a `cigar_end` column directly. The approach is either: (a) parse CIGAR in Polars using `pl.col("cigar").str.extract_all(...)`, or (b) use a BAM tag that encodes end position if available, or (c) filter reads_lf to use `pos` + approximate read length as `end` (sufficient for most cases when read length is uniform).

This CIGAR-end computation is the only non-trivial preprocessing step outside existing primitives.

### GTF Schema Awareness

`scan_gtf(attr_fields=["gene_id"])` output columns relevant to feature counting:

| Column | Type | Role |
|--------|------|------|
| `seqname` | String | Chromosome — rename to `chrom` for overlap |
| `start` | Int32 | Feature start (1-based) |
| `end` | Int32 | Feature end (1-based, closed) |
| `feature` | String | Feature type — filter to `"exon"` |
| `gene_id` | String | Gene identifier (extracted from attributes) |

## Build Order Implications

The pipeline composes existing primitives, so build order is driven by testing needs:

```
Phase 1: CIGAR-end computation
  → polars_bio/feature_count.py with _compute_read_end() helper
  → unit tests with tiny BAM fixture

Phase 2: GTF exon extraction
  → scan_gtf with attr_fields=["gene_id"] + feature=="exon" filter
  → verify gene_id column extraction works at scale (Gencode has ~1.5M exon rows)

Phase 3: Overlap join
  → pb.overlap(reads, exons) — existing primitive, test with small dataset
  → verify coordinate system metadata flows correctly (both 1-based)

Phase 4: Ambiguity resolution + aggregation
  → double group_by pattern
  → correctness validation: compare with featureCounts on 10k-read subset

Phase 5: Scale testing
  → human-scale BAM (50-100M reads) + Gencode GTF
  → memory profiling (peak RSS), wall time benchmarking

Phase 6: Benchmark notebook
  → featureCounts vs HTSeq-count vs polars-bio comparison
```

Phases 1-4 can be completed on small test data and must be correct before Phase 5.
Phase 4 correctness validation gates everything downstream.

## Scaling Considerations

| Scale | Bottleneck | Approach |
|-------|-----------|----------|
| < 1M reads | None | Simple collect() is fine |
| 10M reads | overlap output memory (each read → multiple rows) | Use `output_type="polars.LazyFrame"` to keep overlap lazy; streaming groupby |
| 50-100M reads | overlap join intermediate result | CoITrees O(log n + k) is fast; but 100M reads * avg 2 exon hits = 200M rows in overlap. Use `low_memory=True` on `pb.overlap()` |
| > 100M reads | Polars groupby memory for ambiguity step | Partition by chromosome before overlap; concat counts at end |

For 50-100M human RNA-seq reads, the expected intermediate size (reads * avg exon hits per read) is 150-300M rows. This is the dominant memory cost. The `low_memory=True` flag in `pb.overlap()` caps batch sizes at the cost of some throughput — this should be the first lever to pull.

## Sources

- featureCounts algorithm: [Liao et al. 2014, Bioinformatics](https://academic.oup.com/bioinformatics/article/30/7/923/232889)
- featureCounts documentation: [subread.sourceforge.net/featureCounts.html](https://subread.sourceforge.net/featureCounts.html)
- HTSeq-count union mode: [htseq.readthedocs.io/en/master/htseqcount.html](https://htseq.readthedocs.io/en/master/htseqcount.html)
- polars-bio overlap API: verified from source `/Users/mwiewior/research/git/polars-bio/polars_bio/range_op.py`
- polars-bio GTF scan API: verified from source `/Users/mwiewior/research/git/polars-bio/polars_bio/io.py`
- polars-bio coordinate system docs: verified from `/Users/mwiewior/research/git/polars-bio/docs/features.md`
- featureCounts flag filtering: [RDocumentation Rsubread::featureCounts](https://www.rdocumentation.org/packages/Rsubread/versions/1.22.2/topics/featureCounts)

---
*Architecture research for: RNA-seq feature counting (polars-bio)*
*Researched: 2026-03-20*
