---
draft: false
date:
  created: 2026-03-07
categories:
  - releases

---

# polars-bio 0.26.0: GTF Support and Smart Tag Type Inference

polars-bio 0.26.0 brings first-class [GTF](https://en.wikipedia.org/wiki/Gene_transfer_format) format support, automatic SAM tag type inference for custom/nanopore tags, and critical bug fixes for multi-partition writes and VCF contig metadata.

<!-- more -->

## GTF Format Support

GTF (Gene Transfer Format) is now fully supported with `read_gtf()`, `scan_gtf()`, and `register_gtf()` — the same trio available for every other format in polars-bio. GTF support includes predicate pushdown, projection pushdown, attribute flattening, coordinate system handling, compressed files, and object storage (S3, GCS, Azure).

### Basic read

```python
import polars_bio as pb

df = pb.read_gtf("annotations.gtf")
df.head()
```

### Attribute flattening

GTF files store metadata as key-value pairs in the attributes column. Use `attr_fields` to extract specific attributes into their own columns:

```python
df = pb.read_gtf("annotations.gtf", attr_fields=["gene_id", "gene_name"])
df.select(["chrom", "start", "end", "type", "gene_id", "gene_name"]).head()
```

### Lazy scan with filtering

```python
import polars as pl

exons = (
    pb.scan_gtf("annotations.gtf")
    .filter(pl.col("type") == "exon")
    .collect()
)
```

### SQL queries

```python
pb.register_gtf("annotations.gtf", "genes")
result = pb.sql("SELECT chrom, start, end, gene_id FROM genes WHERE type = 'exon'")
```

## Custom SAM Tag Type Inference

Previously, custom/unknown SAM tags (those not in the ~40 well-known tags) defaulted to `Utf8` (string), which meant nanopore-specific integer and float tags like `pt`, `de`, and `ch` would come back as strings. polars-bio 0.26.0 now **samples the file** to infer the correct Arrow type automatically.

### Auto-inference (default)

```python
# Tags are automatically inferred as Int32, Float32, etc.
df = pb.read_bam("nanopore.bam", tag_fields=["pt", "de", "ch"])
print(df.dtypes)  # pt: Int32, de: Float32, ch: Int32
```

### Explicit type hints

Override or supplement inference with `tag_type_hints`:

```python
df = pb.read_bam(
    "nanopore.bam",
    tag_fields=["pt"],
    tag_type_hints=["pt:i"],  # force Int32
)
```

### Disable inference + use hints only

```python
df = pb.read_bam(
    "nanopore.bam",
    tag_fields=["de"],
    infer_tag_types=False,
    tag_type_hints=["de:f"],  # Float32
)
```

The `infer_tag_types`, `infer_tag_sample_size`, and `tag_type_hints` parameters are available on `read_bam()`, `scan_bam()`, `read_sam()`, `scan_sam()`, `read_cram()`, `scan_cram()`, `describe_bam()`, `describe_sam()`, `describe_cram()`, and `depth()`.

## Critical Bug Fixes

### Multi-partition write data loss (#338)

When DataFusion's `target_partitions` was greater than 1 (the default on multi-core machines), single-file writes (`write_vcf`, `write_bam`, `write_cram`, `write_fastq`) would silently drop data from all partitions except the first. This release adds a coalesce step before writing, ensuring all data is written correctly.

### VCF contig metadata preservation (#340)

`##contig` header lines were previously lost during VCF write/sink operations. They are now correctly preserved in the output.

## Install

```bash
pip install polars-bio==0.26.0
```

- [Documentation](https://biodatageeks.org/polars-bio/)
- [GitHub](https://github.com/biodatageeks/polars-bio)
- [PyPI](https://pypi.org/project/polars-bio/)
- [Changelog](https://github.com/biodatageeks/polars-bio/releases/tag/v0.26.0)
