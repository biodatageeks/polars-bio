# Genomic operations

polars-bio implements the common genomic **interval operations** with a DataFrame API (both [Polars](https://pola.rs/) and [Pandas](https://pandas.pydata.org/)) and a native parallel engine. The comparison tables below map each operation to its equivalent in other libraries and link to the polars-bio [API reference](../api/index.md).

**On this page:** [Interval operations](#genomic-ranges-operations) · [Pileup / depth](#pileup-operations)

## Genomic ranges operations

| operation | Bioframe | polars-bio | PyRanges0 | PyRanges1 | Pybedtools | GenomicRanges |
|-----------|----------|------------|-----------|-----------|------------|---------------|
| overlap | [overlap](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.overlap) | [overlap](../api/operations.md#polars_bio.range_operations.overlap) | [join](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.join)<sup>1</sup> | [join_overlaps](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html#the-most-versatile-join-overlaps) | [intersect](https://bedtools.readthedocs.io/en/latest/content/tools/intersect.html)<sup>2</sup> | [find_overlaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.find_overlaps)<sup>3</sup> |
| nearest | [closest](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.closest) | [nearest](../api/operations.md#polars_bio.range_operations.nearest) | [nearest](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.nearest) | [nearest_ranges](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html#find-the-closest-interval-nearest-ranges) | [closest](https://daler.github.io/pybedtools/autodocs/pybedtools.bedtool.BedTool.closest.html)<sup>4</sup> | [nearest](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.nearest)<sup>5</sup> |
| count_overlaps | [count_overlaps](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.count_overlaps) | [count_overlaps](../api/operations.md#polars_bio.range_operations.count_overlaps) | [count_overlaps](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.count_overlaps) | [count_overlaps](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html) | [intersect](https://bedtools.readthedocs.io/en/latest/content/tools/intersect.html)<sup>6</sup> | [count_overlaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.count_overlaps) |
| cluster | [cluster](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.cluster) | [cluster](../api/operations.md#polars_bio.range_operations.cluster) | [cluster](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.cluster) | [cluster_overlaps](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html#grouping-overlapping-intervals-cluster-overlaps) | [cluster](https://bedtools.readthedocs.io/en/latest/content/tools/cluster.html) | |
| merge | [merge](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.merge) | [merge](../api/operations.md#polars_bio.range_operations.merge) | [merge](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.merge) | [merge_overlaps](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html) | [merge](https://bedtools.readthedocs.io/en/latest/content/tools/merge.html) | [reduce](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.reduce)<sup>7</sup> |
| complement | [complement](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.complement) | [complement](../api/operations.md#polars_bio.range_operations.complement) | | [complement_ranges](https://pyranges1.readthedocs.io/en/latest/how_to_genomic_ops.html#interval-complement) | [complement](https://bedtools.readthedocs.io/en/latest/content/tools/complement.html) | [gaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.gaps)<sup>8</sup> |
| subtract | [subtract](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.subtract) | [subtract](../api/operations.md#polars_bio.range_operations.subtract) | [subtract](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.subtract) | [subtract_overlaps](https://pyranges1.readthedocs.io/en/latest/how_to_overlap.html#interval-manipulation-operations-intersect-overlaps-subtract-overlaps) | [subtract](https://bedtools.readthedocs.io/en/latest/content/tools/subtract.html) | [subtract](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.subtract) |
| coverage | [coverage](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.coverage) | [coverage](../api/operations.md#polars_bio.range_operations.coverage) | [coverage](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.coverage) | | [coverage](https://bedtools.readthedocs.io/en/latest/content/tools/coverage.html) | [coverage](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.coverage) |
| expand | [expand](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.expand) | [expand](../api/operations.md#range-operations) | [extend](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.extend) | [extend_ranges](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html) | [slop](https://bedtools.readthedocs.io/en/latest/content/tools/slop.html) | [resize](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.resize) |
| sort | [sort_bedframe](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.sort_bedframe) | [sort](../api/operations.md#range-operations) | [sort](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.sort) | [sort_ranges](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html) | [sort](https://bedtools.readthedocs.io/en/latest/content/tools/sort.html) | [sort](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.sort) |
| read_table | [read_table](https://bioframe.readthedocs.io/en/latest/api-fileops.html#bioframe.io.fileops.read_table) | [read_table](../api/reading.md#polars_bio.data_input.read_table) | [read_bed](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/readers/index.html#pyranges.readers.read_bed) | [read_bed](https://pyranges1.readthedocs.io/en/latest/pyranges_module.html#pyranges.read_bed) | [BedTool](https://daler.github.io/pybedtools/topical-create-a-bedtool.html#creating-a-bedtool) | [read_bed](https://biocpy.github.io/GenomicRanges/tutorial.html#from-bioinformatic-file-formats) |

!!! note
    1. There is an [overlap](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.overlap) method in PyRanges, but its output is only limited to indices of intervals from the other Dataframe that overlap.
    In Bioframe's [benchmark](https://bioframe.readthedocs.io/en/latest/guide-performance.html#vs-pyranges-and-optionally-pybedtools) also **join** method instead of overlap was used.
    2. **wa** and **wb** options used to obtain a comparable output.
    3. Output contains only a list with the same length as query, containing hits to overlapping indices. Data transformation is required to obtain the same output as in other libraries.
      Since the performance was far worse than in more efficient libraries anyway, additional data transformation was not included in the benchmark.
    4. **s=first** was used to obtain a comparable output.
    5. **select="arbitrary"** was used to obtain a comparable output.
    6. **-c** flag used with `intersect` to count overlaps per feature.
    7. GenomicRanges exposes merge as [reduce()](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.reduce).
    8. GenomicRanges exposes complement as [gaps()](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.gaps).


!!! Limitations
    For now *polars-bio* uses `int32` positions encoding for interval operations ([issue](https://github.com/dcjones/coitrees/issues/18)) meaning that it does not support operation on chromosomes longer than **2Gb**. `int64` support is planned for future releases ([issue](https://github.com/biodatageeks/polars-bio/issues/169)).


## Pileup operations

Per-base read depth computation from alignment files using CIGAR operations. Produces mosdepth-compatible coverage blocks.

| Feature | mosdepth | samtools depth | polars-bio |
|---------|----------|----------------|------------|
| [depth](../api/operations.md#polars_bio.pileup_operations.depth) | :white_check_mark: | :white_check_mark: | :white_check_mark: |

```python
import polars_bio as pb

# Compute per-base depth from a BAM file
df = pb.depth("alignments.bam").collect()

# With MAPQ filter (equivalent to samtools depth -q 20)
df = pb.depth("alignments.bam", min_mapping_quality=20).collect()

# Via SQL
df = pb.sql("SELECT * FROM depth('alignments.bam')").collect()
```

## FastQC quality control

Streaming FastQC quality-control modules over FASTQ files (plain, `.gz`, or BGZF) in a **single out-of-core pass**. All 12 core modules are implemented and **bit-exact against FastQC 0.12.1** (`--nogroup`), computed in parallel and merged, so results are identical regardless of the number of partitions.

| Module | Module | Module |
|--------|--------|--------|
| `basic_stats` | `per_base_quality` | `per_seq_quality` |
| `per_base_content` | `per_seq_gc` | `per_base_n` |
| `seq_length` | `overrepresented` | `adapter_content` |
| `dup_levels` | `per_tile_quality` | `kmer_content` |

```python
import polars_bio as pb

# One streaming pass computes every module; access each as a LazyFrame.
qc = pb.fastqc("reads_R1.fastq.gz")
qc.per_base_quality.collect()
qc.per_tile_quality.collect()
qc.summary().collect()          # PASS/WARN/FAIL status per module

# Compute only selected modules
qc = pb.fastqc("reads_R1.fastq.gz", modules=["basic_stats", "adapter_content"])

# Via SQL (tidy long-form output)
df = pb.sql("SELECT * FROM fastqc('reads_R1.fastq.gz')").collect()
```

!!! note
    `per_tile_quality` and `kmer_content` reproduce FastQC's own subsampling
    (per-tile: 10% after the first 10k reads; kmer: 2% of reads, file-order
    dependent). Exact k-mer parity therefore requires a single-partition scan;
    the other ten modules are partition-invariant and exact on all reads.
    `dup_levels`/`overrepresented` use FastQC's 100k-unique observation
    cutoff, matching FastQC's estimate on high-diversity libraries.

