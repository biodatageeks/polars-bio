# Writing files

polars-bio writes DataFrames back to bioinformatics formats, with both **eager** (`write_*`) and **streaming** (`sink_*`) methods, optional coordinate-sorted output, header preservation, and compression auto-detected from the file extension.

**On this page:** [Output format support](#output-format-support) · [Sorted output](#sorted-output-with-sort_on_write) · [Header preservation](#header-preservation) · [BAM/SAM tags on write](#adding-bamsam-tags-on-write) · [Compression](#compression)

## File output

polars-bio supports writing DataFrames back to bioinformatic file formats. Two methods are available for each supported format:

- `write_*` - Eager write that collects the DataFrame and writes it to disk, returns row count
- `sink_*` - Streaming write for LazyFrames that processes data in batches without full materialization

### Output format support

| Format | write_* | sink_* | Compression | Notes |
|--------|---------|--------|-------------|-------|
| [VCF](../api/writing.md#polars_bio.data_input.write_vcf) | :white_check_mark: | :white_check_mark: | `.vcf.gz`, `.vcf.bgz` | Auto-detected from extension |
| [BAM](../api/writing.md#polars_bio.data_input.write_bam) | :white_check_mark: | :white_check_mark: | BGZF (built-in) | Binary alignment format |
| [SAM](../api/writing.md#polars_bio.data_input.write_sam) | :white_check_mark: | :white_check_mark: | None | Plain text alignment format |
| [CRAM](../api/writing.md#polars_bio.data_input.write_cram) | :white_check_mark: | :white_check_mark: | Built-in | Requires reference FASTA |
| [FASTA](../api/writing.md#polars_bio.data_input.write_fasta) | :white_check_mark: | :white_check_mark: | `.fasta.gz`, `.fasta.bgz` | Auto-detected from extension |
| [FASTQ](../api/writing.md#polars_bio.data_input.write_fastq) | :white_check_mark: | :white_check_mark: | `.fastq.gz`, `.fastq.bgz` | Auto-detected from extension |

### Basic usage

```python
import polars_bio as pb

# Read, transform, and write back
df = pb.read_bam("input.bam", tag_fields=["NM", "AS"])
filtered = df.filter(pl.col("mapping_quality") > 20)
pb.write_bam(filtered, "output.bam")

# Streaming write with LazyFrame
lf = pb.scan_vcf("variants.vcf")
pb.sink_vcf(lf.filter(pl.col("qual") > 30), "filtered.vcf.bgz")
```

### Sorted output with `sort_on_write`

BAM, SAM, and CRAM write functions support the `sort_on_write` parameter to produce coordinate-sorted output:

```python
import polars_bio as pb

# Write coordinate-sorted BAM
df = pb.read_bam("unsorted.bam")
pb.write_bam(df, "sorted.bam", sort_on_write=True)

# Streaming sorted write
lf = pb.scan_sam("input.sam")
pb.sink_bam(lf, "sorted.bam", sort_on_write=True)
```

When `sort_on_write=True`:

- Records are sorted by `(chrom ASC, start ASC)` during write
- Output header contains `@HD ... SO:coordinate`

When `sort_on_write=False` (default):

- Records are written in input order
- Output header contains `@HD ... SO:unsorted`

### CRAM output

CRAM format requires a reference FASTA file for writing:

```python
import polars_bio as pb

# CRAM write requires reference_path
df = pb.read_cram("input.cram", reference_path="reference.fa")
pb.write_cram(df, "output.cram", reference_path="reference.fa")

# Streaming CRAM write
lf = pb.scan_cram("input.cram", reference_path="reference.fa")
pb.sink_cram(lf, "output.cram", reference_path="reference.fa", sort_on_write=True)
```

!!! warning
    The `reference_path` parameter is **required** for `write_cram()` and `sink_cram()`. Attempting to write CRAM without a reference will raise an error.

### Output compression

Output compression is auto-detected from the file extension for VCF, FASTA, and FASTQ formats:

| Extension | Compression |
|-----------|-------------|
| `.vcf` / `.fastq` | None (plain text) |
| `.vcf.gz` / `.fastq.gz` | GZIP |
| `.vcf.bgz` / `.fastq.bgz` | BGZF (block gzip) |

```python
import polars_bio as pb

# VCF
df = pb.read_vcf("variants.vcf")
pb.write_vcf(df, "output.vcf")        # plain text
pb.write_vcf(df, "output.vcf.gz")     # GZIP
pb.write_vcf(df, "output.vcf.bgz")    # BGZF (recommended for indexing)

# FASTA
df = pb.read_fasta("sequences.fasta")
pb.write_fasta(df, "output.fasta")       # plain text
pb.write_fasta(df, "output.fasta.gz")    # GZIP
pb.write_fasta(df, "output.fasta.bgz")   # BGZF

# FASTQ
df = pb.read_fastq("reads.fastq")
pb.write_fastq(df, "output.fastq")       # plain text
pb.write_fastq(df, "output.fastq.gz")    # GZIP
pb.write_fastq(df, "output.fastq.bgz")   # BGZF (recommended for parallel reads with GZI index)

# Streaming write
lf = pb.scan_fasta("large_sequences.fasta.gz")
pb.sink_fasta(lf.limit(1000), "sample.fasta")

lf = pb.scan_fastq("large_reads.fastq.gz")
pb.sink_fastq(lf.limit(1000), "sample.fastq")
```

### Header preservation

When reading and writing alignment files (BAM/SAM/CRAM), polars-bio preserves header metadata including:

- `@SQ` (sequence dictionary)
- `@RG` (read groups)
- `@PG` (program records)

This enables lossless roundtrip workflows:

```python
import polars_bio as pb

# Read with full header preservation
df = pb.read_bam("input.bam")

# Filter records
filtered = df.filter(pl.col("mapping_quality") > 20)

# Write back - header metadata is preserved
pb.write_bam(filtered, "filtered.bam")
```

### Adding BAM/SAM tags on write

You can create new BAM/SAM tag columns with standard Polars expressions and write them back out.
For most numeric, string, and array tags, the SAM type is inferred from the column dtype:

```python
import polars as pl
import polars_bio as pb

df = pb.read_bam("input.bam").head(2)

df = df.with_columns(
    pl.Series("XI", [7, 8], dtype=pl.Int32),             # integer tag
    pl.Series("XF", [0.25, 0.50], dtype=pl.Float32),     # float tag
    pl.Series("XZ", ["alpha", "beta"], dtype=pl.Utf8),   # string tag (Z)
    pl.Series("ML", [[1, 2, 3], [2, 3, 4]], dtype=pl.List(pl.UInt8)),   # B:C
    pl.Series("FZ", [[1000, 2000], [1001, 2001]], dtype=pl.List(pl.UInt16)),  # B:S
)

pb.write_bam(df, "with_tags.bam")

# BAM optional tags are only parsed when requested on read.
roundtrip = pb.read_bam(
    "with_tags.bam",
    tag_fields=["XI", "XF", "XZ", "ML", "FZ"],
)
```

If you call `read_bam("with_tags.bam")` or `scan_bam("with_tags.bam")` without
`tag_fields`, you will only see the 12 core BAM columns. The tags are still present in
the file; they are simply not parsed by default.

For ambiguous string tags such as `A` (single ASCII character) and `H` (hex), pass
`tag_type_overrides` explicitly:

```python
import polars as pl
import polars_bio as pb

df = pb.read_bam("input.bam").head(2).with_columns(
    pl.Series("XA", ["A", "B"], dtype=pl.Utf8),          # should be SAM type A
    pl.Series("XH", ["0A0B", "C0FFEE"], dtype=pl.Utf8),  # should be SAM type H
)

pb.write_bam(
    df,
    "with_ambiguous_tags.bam",
    tag_type_overrides={"XA": "A", "XH": "H"},
)
```

The same parameter is available on `write_sam()`, `sink_bam()`, `sink_sam()`, and the
Polars namespace methods (`df.pb.write_bam(...)`, `lf.pb.sink_sam(...)`).

If a tag already existed in the source BAM/SAM and was read with its exact type, that type
is preserved automatically through ordinary Polars transforms:

```python
import polars as pl
import polars_bio as pb

df = pb.read_bam("input.bam", tag_fields=["tp", "ML"])

edited = df.with_columns(
    pl.col("tp"),
    pl.col("ML"),
)

# Existing exact tag types are preserved; no override needed here.
pb.write_bam(edited, "roundtrip.bam")
```

### Polars extension methods

Write functions are also available as Polars namespace extensions:

```python
import polars_bio as pb

# DataFrame extensions
df = pb.read_bam("input.bam")
df.pb.write_bam("output.bam", sort_on_write=True)
df.pb.write_sam("output.sam")
df.pb.write_cram("output.cram", reference_path="ref.fa")
df.pb.write_vcf("output.vcf.bgz")
df.pb.write_fasta("output.fasta.gz")
df.pb.write_fastq("output.fastq.gz")

# LazyFrame extensions
lf = pb.scan_bam("input.bam")
lf.pb.sink_bam("output.bam", sort_on_write=True)
lf.pb.sink_sam("output.sam")
lf.pb.sink_cram("output.cram", reference_path="ref.fa")
lf.pb.sink_vcf("output.vcf.bgz")
lf.pb.sink_fasta("output.fasta.bgz")
lf.pb.sink_fastq("output.fastq.bgz")
```



## Compression
*polars-bio* supports **GZIP** (default file extension `*.gz`) and **Block GZIP** (BGZIP, default file extension `*.bgz`) when reading files from local and cloud storages.
For BGZIP-compressed FASTQ files, parallel decoding of compressed blocks is **automatic** — see [Automatic parallel partitioning](reading.md#parallel-reads-partitioning) and [Index file generation](reading.md#generating-index-files) for details. Please take a look at the following [GitHub discussion](https://github.com/biodatageeks/polars-bio/issues/132).


