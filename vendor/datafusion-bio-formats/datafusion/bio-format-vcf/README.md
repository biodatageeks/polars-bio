# datafusion-bio-format-vcf

VCF (Variant Call Format) file format support for Apache DataFusion, enabling SQL queries on genetic variation data.

## Overview

This crate provides a DataFusion `TableProvider` implementation for reading and writing VCF files, the standard format for storing genetic variants discovered through DNA sequencing. It supports both single-sample and multi-sample VCF files with an optimized columnar genotypes layout for analytical queries.

## Features

- Read and write VCF files directly as DataFusion tables
- Support for compressed files (GZIP, BGZF)
- Parallel reading of BGZF-compressed files via TBI/CSI indexes
- Cloud storage support (GCS, S3, Azure Blob Storage)
- Preserves case sensitivity for INFO and FORMAT fields
- Projection pushdown for efficient querying
- Columnar multi-sample genotypes with analytical UDFs
- Dual-view support: columnar (`vcf_table`) + per-sample (`vcf_table_long`)
- Sample selection: query a subset of samples from large cohort VCFs

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-bio-format-vcf = { git = "https://github.com/biodatageeks/datafusion-bio-formats" }
datafusion = "52.1.0"
```

## Schema

VCF files are read into tables with core variant columns plus dynamically generated INFO and FORMAT columns.

### Core Columns

| Column | Type | Description |
|--------|------|-------------|
| `chrom` | Utf8 | Chromosome name |
| `start` | UInt32 | Start position (0-based or 1-based depending on config) |
| `end` | UInt32 | End position |
| `id` | Utf8 | Variant identifier |
| `ref` | Utf8 | Reference allele |
| `alt` | Utf8 | Alternate alleles |
| `qual` | Float64 | Quality score |
| `filter` | Utf8 | Filter status |

### INFO Columns

Dynamically created based on VCF header definitions. Names preserve original case from the header (e.g., `DP`, `AF`, `AN`).

### FORMAT/Genotype Columns

The schema depends on the number of samples in the VCF file:

#### Single-Sample (1 sample)

FORMAT fields are top-level scalar columns — no `genotypes` wrapper:

```
chrom: Utf8, start: UInt32, ..., GT: Utf8, DP: Int32, GQ: Int32
```

```sql
SELECT chrom, start, "GT", "DP" FROM vcf_table WHERE "GT" = '0/1'
```

#### Multi-Sample (2+ samples)

FORMAT fields are grouped under a `genotypes` column using a **columnar Struct-of-Lists** layout. Each FORMAT field becomes a `List<T>` with one element per sample, in sample order:

```
genotypes: Struct<
  GT: List<Utf8>,      -- ["0/1", "1/1", "./.", ...]
  DP: List<Int32>,     -- [20, 30, 10, ...]
  GQ: List<Int32>,     -- [99, 95, 50, ...]
>
```

Sample names are stored in the `genotypes` field metadata under the key `bio.vcf.genotypes.sample_names` as a JSON array.

This columnar layout enables efficient analytical queries — computing `AVG(DP)` across all samples only touches the `DP` list without parsing other FORMAT fields.

## Table Registration

### Basic Registration

```rust
use datafusion::prelude::*;
use datafusion_bio_format_vcf::table_provider::VcfTableProvider;
use std::sync::Arc;

let ctx = SessionContext::new();

let table = VcfTableProvider::new(
    "data/variants.vcf.gz".to_string(),
    Some(vec!["AF".to_string(), "DP".to_string()]),  // INFO fields (None = all)
    Some(vec!["GT".to_string(), "DP".to_string()]),  // FORMAT fields (None = all)
    None,   // object_storage_options
    true,   // coordinate_system_zero_based
)?;
ctx.register_table("vcf_table", Arc::new(table))?;
```

### With Sample Selection

Select a subset of samples from a large cohort VCF. Only the selected samples appear in the genotypes lists, in the order specified:

```rust
let table = VcfTableProvider::new_with_samples(
    "data/cohort.vcf.gz".to_string(),
    Some(vec!["AF".to_string()]),                     // INFO fields
    Some(vec!["GT".to_string(), "DP".to_string()]),   // FORMAT fields
    Some(vec!["NA12878".to_string(), "NA12891".to_string()]),  // samples to include
    None,   // object_storage_options
    true,   // coordinate_system_zero_based
)?;
```

### Register UDFs

Register analytical UDFs for multi-sample queries:

```rust
use datafusion_bio_format_vcf::register_vcf_udfs;

register_vcf_udfs(&ctx);
```

## Analytical Queries (Multi-Sample)

The columnar genotypes layout combined with list UDFs enables bcftools-style analytical pipelines in SQL.

### Average FORMAT Values Across Samples

```sql
-- Filter variants where average genotype quality is high
SELECT chrom, start, qual, list_avg(genotypes."GQ") AS avg_gq
FROM vcf_table
WHERE list_avg(genotypes."GQ") >= 15
  AND list_avg(genotypes."DP") BETWEEN 15 AND 150
```

### Per-Sample GT Masking (bcftools --set-GTs)

Set genotypes to missing where per-sample quality thresholds aren't met:

```sql
SELECT chrom, start, "end", "ref", alt, qual, filter, id,
    named_struct(
        'GT', vcf_set_gts(
            genotypes."GT",
            list_and(
                list_gte(genotypes."GQ", 10),
                list_and(
                    list_gte(genotypes."DP", 10),
                    list_lte(genotypes."DP", 200)
                )
            )
        ),
        'GQ', genotypes."GQ",
        'DP', genotypes."DP"
    ) AS genotypes
FROM vcf_table
WHERE qual >= 20
```

The optional third argument controls the replacement value (default `"./."`):
```sql
-- Replace failing GTs with "." instead of "./."
vcf_set_gts(genotypes."GT", mask, '.')
```

This replicates the bcftools pipeline:
```bash
bcftools filter --exclude 'FORMAT/GQ<10 | FORMAT/DP<10 | FORMAT/DP>200' --set-GTs '.'
```

### Full bcftools Pipeline in SQL

```bash
# Original bcftools pipeline:
bcftools view --samples-file samples.txt input.vcf.gz \
  | bcftools filter --exclude 'QUAL<20 || AVG(FORMAT/GQ)<15 || AVG(FORMAT/DP)<15 || AVG(FORMAT/DP)>150' \
  | bcftools filter --exclude 'FORMAT/GQ<10 | FORMAT/DP<10 | FORMAT/DP>200' --set-GTs '.'
```

```sql
-- Equivalent SQL:
SELECT chrom, start, "end", "ref", alt, qual, filter, id,
    named_struct(
        'GT', vcf_set_gts(
            genotypes."GT",
            list_and(
                list_gte(genotypes."GQ", 10),
                list_and(list_gte(genotypes."DP", 10), list_lte(genotypes."DP", 200))
            )
        ),
        'GQ', genotypes."GQ",
        'DP', genotypes."DP"
    ) AS genotypes
FROM vcf_table
WHERE qual >= 20
  AND list_avg(genotypes."GQ") >= 15
  AND list_avg(genotypes."DP") BETWEEN 15 AND 150
```

## UDF Reference

| Function | Signature | Description |
|----------|-----------|-------------|
| `list_avg` | `List<Int32\|Float32> -> Float64` | Average of non-null list elements. Returns NULL if all elements are null. |
| `list_gte` | `(List<Int32\|Float32>, scalar) -> List<Boolean>` | Element-wise `>=` comparison. NULL elements produce NULL in the result. |
| `list_lte` | `(List<Int32\|Float32>, scalar) -> List<Boolean>` | Element-wise `<=` comparison. NULL elements produce NULL in the result. |
| `list_and` | `(List<Boolean>, List<Boolean>) -> List<Boolean>` | Element-wise AND. NULL in either input produces NULL. |
| `vcf_set_gts` | `(List<Utf8>, List<Boolean> [, Utf8]) -> List<Utf8>` | Replace GT where mask is false/NULL. Optional 3rd arg sets the replacement value (default `"./."`). |
| `vcf_an` | `List<Utf8> -> Int32` | Allele Number — count of called (non-missing) alleles across samples. |
| `vcf_ac` | `(List<Utf8> [, Utf8]) -> List<Int32>` | Allele Count — per-ALT-allele call count from GT strings. Optional 2nd arg is the `alt` column (pipe-separated) to ensure output length matches the number of ALT alleles. |
| `vcf_af` | `(List<Utf8> [, Utf8]) -> List<Float64>` | Allele Frequency — per-ALT-allele frequency (AC/AN). Optional 2nd arg is the `alt` column (pipe-separated) to ensure output length matches the number of ALT alleles. When AN=0, produces NULLs. |

## Write Semantics

When writing VCF output via `INSERT OVERWRITE`, be aware of the following:

- **INFO fields are passed through as-is.** If you subset samples or filter rows, INFO fields like `AC`, `AF`, and `AN` will be stale because they describe the original cohort. Use `vcf_an`, `vcf_ac`, and `vcf_af` to recompute them after subsetting.
- **FORMAT layout is global.** The output FORMAT header is the union of all FORMAT fields present in the Arrow schema, not per-row. Every sample column in every row shares the same set of FORMAT keys.

### Subset + Recompute Workflow

After subsetting samples or filtering rows, recompute allele statistics before writing:

```sql
INSERT OVERWRITE output_vcf
SELECT
    chrom, start, "end", "ref", alt, qual, filter, id,
    -- Recompute INFO allele stats from the current GT column
    -- Pass the alt column to ensure correct cardinality at multi-allelic sites
    vcf_an(genotypes."GT") AS "AN",
    vcf_ac(genotypes."GT", alt) AS "AC",
    vcf_af(genotypes."GT", alt) AS "AF",
    genotypes
FROM vcf_table
WHERE chrom = '22'
```

## Dual View: Per-Sample Queries

For per-sample lookups (e.g., "what is NA12878's genotype at each variant?"), the long view unnests the columnar genotypes into one row per variant x sample:

### Register the Long View

```rust
use datafusion_bio_format_vcf::auto_register_vcf_long_view;

// After registering vcf_table, auto-register vcf_table_long
auto_register_vcf_long_view(&ctx, "vcf_table").await?;
```

Or manually specify sample names and FORMAT fields:

```rust
use datafusion_bio_format_vcf::register_vcf_long_view;

register_vcf_long_view(
    &ctx,
    "vcf_table",
    &["NA12878".to_string(), "NA12891".to_string()],
    &["GT".to_string(), "DP".to_string(), "GQ".to_string()],
).await?;
```

### Query the Long View

```sql
-- Get a specific sample's genotype across all variants
SELECT chrom, start, "GT", "GQ", "DP"
FROM vcf_table_long
WHERE sample_id = 'NA12878'

-- Count heterozygous calls per sample
SELECT sample_id, COUNT(*) AS het_count
FROM vcf_table_long
WHERE "GT" LIKE '%0/1%' OR "GT" LIKE '%0|1%'
GROUP BY sample_id

-- Compare two samples
SELECT a.chrom, a.start, a."GT" AS gt_878, b."GT" AS gt_891
FROM vcf_table_long a
JOIN vcf_table_long b ON a.chrom = b.chrom AND a.start = b.start
WHERE a.sample_id = 'NA12878' AND b.sample_id = 'NA12891'
  AND a."GT" != b."GT"
```

### When to Use Each View

| Use Case | View | Why |
|----------|------|-----|
| Cross-sample aggregates (AVG, COUNT) | `vcf_table` + UDFs | Operates on lists directly, no row explosion |
| Per-sample GT masking | `vcf_table` + `vcf_set_gts` | Keeps columnar layout for write-back |
| Lookup one sample's genotypes | `vcf_table_long` | Natural row-per-sample for filtering |
| Per-sample statistics (GROUP BY sample) | `vcf_table_long` | One row per sample enables standard SQL aggregation |

## Supported File Types

- Uncompressed VCF (`.vcf`)
- GZIP-compressed VCF (`.vcf.gz`)
- BGZF-compressed VCF (`.vcf.bgz`, `.vcf.bgzf`)
- Cloud storage URLs (`gs://`, `s3://`, `https://`)

## Important Notes

- **Case Sensitivity**: INFO and FORMAT field names are case-sensitive per VCF specification. Use quoted identifiers in SQL (e.g., `"GT"`, `"DP"`).
- **Single-sample mode**: When a VCF has exactly one sample, FORMAT fields are top-level columns — no `genotypes` struct.
- **Git Dependency**: This crate uses a forked version of noodles for enhanced VCF support.

## License

Licensed under Apache-2.0
