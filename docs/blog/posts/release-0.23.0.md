---
draft: false
date:
  created: 2026-02-14
categories:
  - releases
  - performance

---

# polars-bio 0.23.0: Faster Parsing and Python 3.14 Support

polars-bio 0.23.0 is here with significant parsing performance improvements across [VCF](https://en.wikipedia.org/wiki/Variant_Call_Format), [BAM](https://en.wikipedia.org/wiki/Binary_Alignment_Map), and [FASTQ](https://en.wikipedia.org/wiki/FASTQ_format) formats, plus first-day support for Python 3.14. This release bumps the underlying datafusion-bio-formats engine to 0.5.0, delivering up to **3.6x faster** VCF parsing with no API changes required.

<!-- more -->

## Performance Improvements

The headline of this release is [PR #300](https://github.com/biodatageeks/polars-bio/pull/300), which bumps datafusion-bio-formats to 0.5.0. The changes span two pull requests and touch every major format:

**VCF**: The previous implementation allocated a new `Vec<String>` per record and then copied it into an Arrow array — O(n * k) allocations where *n* is the number of records and *k* is the number of INFO/tag fields. The new code writes directly into Arrow `StringBuilder` / `PrimitiveBuilder` arrays, reducing this to O(k) builder appends per record with no intermediate heap allocations. A reusable `String` buffer for multi-value INFO field joining eliminates repeated allocation on every record, and residual filter evaluation was moved before accumulation so non-matching rows are discarded before any Arrow builder work happens.

**BAM**: Tag parsing followed the same pattern — auxiliary tags were collected into temporary `Vec`s before being copied into Arrow arrays. This has been replaced with direct `StringBuilder` / `ListBuilder` appends, cutting per-record allocations from O(t) (where *t* is the number of tags) to zero. Additionally, `libdeflate` was enabled for all BGZF dependencies, yielding ~30-50% faster gzip decompression across both BAM and bgzipped VCF reads.

**FASTQ**: Replaced `Vec<String>` accumulation with `StringBuilder` for direct Arrow construction, and fixed a backpressure spin-loop that was cloning data unnecessarily.

### Version Comparison (1 thread)

The chart below compares polars-bio 0.22.0 and 0.23.0 on the same datasets used in our [companion benchmark post](genomic-formats-benchmark-2026-02.md), running single-threaded on Apple Silicon:

![polars-bio version comparison](figures/release-0.23.0/version_comparison.png)

**Wall-time speedups:**

| Format | 0.22.0 | 0.23.0 | Speedup |
|--------|--------|--------|---------|
| VCF with INFO | 249.2s | 69.1s | **3.6x** |
| BAM with tags | 39.4s | 20.8s | **1.9x** |
| VCF without INFO | 44.4s | 33.6s | **1.3x** |
| BAM without tags | 18.6s | 15.0s | **1.2x** |
| FASTQ | 4.7s | 4.8s | ~1.0x |

FASTQ was already highly optimized and remains essentially unchanged. The largest gains are in VCF with INFO parsing, where the combination of allocation elimination and early filtering produces a dramatic 3.6x speedup.

Peak memory increased modestly (~150-180 MB to ~200-225 MB) as a tradeoff for the speed gains — the new code uses slightly larger intermediate buffers to enable zero-copy construction.

For a full multi-library comparison (pysam, oxbow, biobear, polars-bio) across all formats and thread counts, see the [Benchmarking Genomic Format Readers in Python](genomic-formats-benchmark-2026-02.md) post.

## Python 3.14 Support

[PR #301](https://github.com/biodatageeks/polars-bio/pull/301) adds Python 3.14 support, closing [#299](https://github.com/biodatageeks/polars-bio/issues/299). The Python upper bound was raised from `<3.14` to `<3.15`, and pyarrow's cap was bumped from `<22` to `<23` (pyarrow 22.0.0 is the first version shipping cp314 wheels). No runtime code changes were needed — the native extension uses the stable `abi3` ABI (cp39+), and all dependencies were already compatible. Python 3.14 has been added to the CI test matrix.

## Breaking Change

The `thread_num` parameter has been removed from `read_bed()`, `scan_bed()`, and `register_bed()`. Threading is now handled automatically by the upstream engine. If you were passing `thread_num`, simply remove the argument — everything else works the same.

## Install

```bash
pip install polars-bio==0.23.0
```

- [Documentation](https://biodatageeks.org/polars-bio/)
- [GitHub](https://github.com/biodatageeks/polars-bio)
- [PyPI](https://pypi.org/project/polars-bio/)
- [Changelog](https://github.com/biodatageeks/polars-bio/releases/tag/v0.23.0)
