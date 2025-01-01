## Genomic ranges operations

| Features     | Bioframe           | polars-bio         | PyRanges           | Pybedtools          | PyGenomics         | GenomicRanges      |
|--------------|--------------------|--------------------|--------------------|---------------------|--------------------|--------------------|
| overlap      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark:  | :white_check_mark: | :white_check_mark: |
| nearest      | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark:  |                    | :white_check_mark: |
| cluster      | :white_check_mark: |                    | :white_check_mark: | :white_check_mark:  |                    |                    |
| merge        | :white_check_mark: |                    | :white_check_mark: | :white_check_mark:  |                    | :white_check_mark: |
| complement   | :white_check_mark: | :construction:     |                    | :white_check_mark:  | :white_check_mark: |                    |
| coverage     | :white_check_mark: |                    | :white_check_mark: | :white_check_mark:  |                    | :white_check_mark: |
| expand       | :white_check_mark: | :construction:     | :white_check_mark: | :white_check_mark:  |                    | :white_check_mark: |
| sort         | :white_check_mark: | :construction:     | :white_check_mark: | :white_check_mark:  |                    | :white_check_mark: |

## API comparison between libraries
There is no standard API for genomic ranges operations in Python.
This table compares the API of the libraries. The table is not exhaustive and only shows the most common operations used in benchmarking.

|operation| Bioframe                                                                                       | polars-bio                                                             | PyRanges0                                                                                                        | PyRanges1                                                                                                     | Pybedtools                                                                                                                                    | GenomicRanges                                                                                                                                      |
|---------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
|overlap  | [overlap](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.overlap) | [overlap](https://biodatageeks.org/polars-bio/api/#polars_bio.overlap) | [join](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.join)<sup>1</sup> | [join_ranges](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html#pyranges.PyRanges.join_ranges) | [intersect](https://bedtools.readthedocs.io/en/latest/content/tools/intersect.html?highlight=intersect#usage-and-option-summary)<sup>2</sup>  | [find_overlaps](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.find_overlaps)<sup>3</sup> |
|nearest  | [closest](https://bioframe.readthedocs.io/en/latest/api-intervalops.html#bioframe.ops.closest) | [nearest](https://biodatageeks.org/polars-bio/api/#polars_bio.nearest) | [nearest](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.nearest)       | [nearest](https://pyranges1.readthedocs.io/en/latest/pyranges_objects.html#pyranges.PyRanges.nearest)         | [closest](https://daler.github.io/pybedtools/autodocs/pybedtools.bedtool.BedTool.closest.html#pybedtools.bedtool.BedTool.closest)<sup>4</sup> | [nearest](https://biocpy.github.io/GenomicRanges/api/genomicranges.html#genomicranges.GenomicRanges.GenomicRanges.nearest)<sup>5</sup>             |

!!! note
    1. There is an [overlap](https://pyranges.readthedocs.io/en/latest/autoapi/pyranges/index.html#pyranges.PyRanges.overlap) method in PyRanges, but its output is only limited to indices of intervals from the other Dataframe that overlap.
    In Bioframe's [benchmark](https://bioframe.readthedocs.io/en/latest/guide-performance.html#vs-pyranges-and-optionally-pybedtools) also **join** method instead of overlap was used.
    2. **wa** and **wb** options used to obtain a comparable output.
    3. Output contains only a list with the same length as query, containing hits to overlapping indices. Data transformation is required to obtain the same output as in other libraries.
    Since the performance was far worse than in more efficient libraries anyway, additional data transformation was not included in the benchmark.
    4. **s=first** was used to obtain a comparable output.
    5. **select="arbitrary"** was used to obtain a comparable output.

## Input/Output
| I/O              | Bioframe           | polars-bio             | PyRanges           | Pybedtools | PyGenomics | GenomicRanges          |
|------------------|--------------------|------------------------|--------------------|------------|------------|------------------------|
| Pandas DataFrame | :white_check_mark: | :white_check_mark:     | :white_check_mark: |            |            | :white_check_mark:     |
| Polars DataFrame |                    | :white_check_mark:     |                    |            |            | :white_check_mark:     |
| Polars LazyFrame |                    | :white_check_mark:     |                    |            |            |                        |
| Native readers   |                    | :white_check_mark:     |                    |            |            |                        |
