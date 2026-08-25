# cooler-io Specification (delta)

## ADDED Requirements

### Requirement: Cooler scan and read APIs

The system SHALL provide `scan_cool` and `read_cool` APIs that read the pixels table of a cooler data collection from `.cool` and `.mcool` files into a Polars LazyFrame or DataFrame, without depending on the Python `cooler`/`h5py` stack at runtime.

#### Scenario: Scan a single-resolution .cool file

- **WHEN** `scan_cool("contacts.cool")` is collected
- **THEN** it returns one row per pixel with columns `chrom1`, `start1`, `end1`, `chrom2`, `start2`, `end2`, `count`, matching the reference `cooler` implementation's joined pixels output

#### Scenario: Scan a resolution of an .mcool file

- **WHEN** `scan_cool("contacts.mcool", resolution=10000)` is collected
- **THEN** it returns the pixels of the `/resolutions/10000` data collection

#### Scenario: Cooler URI syntax

- **WHEN** `scan_cool("contacts.mcool::/resolutions/10000")` is collected
- **THEN** the data collection addressed by the URI suffix is scanned

#### Scenario: Ambiguous .mcool resolution

- **WHEN** `scan_cool` is called on an `.mcool` file containing multiple resolutions without a `resolution` argument or URI suffix
- **THEN** an error is raised listing the available resolutions

#### Scenario: Missing resolution

- **WHEN** `scan_cool` is called with a `resolution` not present in the file
- **THEN** an error is raised listing the available resolutions

#### Scenario: Raw COO output

- **WHEN** `scan_cool(path, join_bins=False)` is collected
- **THEN** it returns `bin1_id`, `bin2_id`, and `count` without bin-coordinate joining or coordinate conversion

#### Scenario: Balancing weights exposed

- **WHEN** `scan_cool(path, include_weights=True)` is collected on a balanced cooler with a `bins/weight` column
- **THEN** the output additionally contains `weight1` and `weight2` as Float64 aligned with each pixel's bins

#### Scenario: Float count dtype

- **WHEN** the stored `pixels/count` dataset has a floating-point dtype
- **THEN** the `count` column is emitted as Float64 instead of Int32

#### Scenario: Remote path rejected

- **WHEN** `scan_cool` is called with an `s3://`, `gs://`, or `http(s)://` path
- **THEN** a clear not-supported error is raised

### Requirement: Cooler coordinate-system handling

Cooler bins are natively 0-based half-open. The system SHALL apply the standard polars-bio coordinate-system convention to `start1`/`end1` and `start2`/`end2`, honoring `use_zero_based` and the global default, and SHALL attach the standard coordinate-system metadata to returned frames.

#### Scenario: One-based default output

- **WHEN** `scan_cool` runs with the global default coordinate system (1-based closed)
- **THEN** `start1` and `start2` are emitted as stored start + 1 and ends remain closed

#### Scenario: Zero-based output

- **WHEN** `scan_cool(path, use_zero_based=True)` is collected
- **THEN** coordinates are emitted exactly as stored (0-based half-open) and metadata records the zero-based system

### Requirement: Cooler metadata description

The system SHALL provide a `describe_cool` API returning the data collections of a `.cool` or `.mcool` file — including resolution (bin size), bin count, non-zero pixel count, chromosome count, and genome assembly when present — without scanning pixel data.

#### Scenario: Describe an .mcool file

- **WHEN** `describe_cool("contacts.mcool")` is called
- **THEN** one row per stored resolution is returned with its bin size, nbins, nnz, and assembly attributes, consistent with `cooler.fileops.list_coolers` and `Cooler.info`

### Requirement: Cooler SQL registration

The system SHALL provide a `register_cool` API that registers a cooler data collection as a DataFusion table queryable via `pb.sql`.

#### Scenario: Register and query

- **WHEN** `register_cool("contacts.mcool", "hic", resolution=10000)` is followed by `pb.sql("SELECT chrom1, count FROM hic LIMIT 5")`
- **THEN** the query executes against the cooler table provider and returns pixel rows

### Requirement: Cooler projection pushdown

The cooler table provider SHALL honor projection pushdown so that only HDF5 datasets required by the projected columns are read, and SHALL report its projection in the physical plan display.

#### Scenario: Projected scan avoids unused datasets

- **WHEN** `scan_cool(path).select(["chrom1", "start1", "count"])` is collected with projection pushdown enabled
- **THEN** second-axis bin lookups are skipped and the plan display shows the pushed projection

#### Scenario: Count star without pixel decode

- **WHEN** `scan_cool(path).select(pl.len())` is collected
- **THEN** the row count is served from the cooler index/nnz metadata without materializing pixel value arrays

### Requirement: Cooler genomic predicate pushdown

The cooler table provider SHALL prune pixel row ranges for supported first-axis predicates (`chrom1` equality/membership and `start1`/`end1` range conjunctions) using the cooler `chrom_offset` and `bin1_offset` indexes, reporting pushed filters as inexact so client-side re-filtering preserves exact semantics.

#### Scenario: First-axis region filter pruned

- **WHEN** `scan_cool(path).filter((pl.col("chrom1") == "chr1") & (pl.col("start1") < 1_000_000))` is collected with predicate pushdown enabled
- **THEN** only pixel rows within the pruned bin1 range are read and the result equals the same query with pushdown disabled

#### Scenario: Unsupported predicate stays client-side

- **WHEN** a filter references only `chrom2` or `count`
- **THEN** the scan remains correct with the predicate applied client-side or as residual filtering

### Requirement: Cooler parallel scan partitions

The cooler table provider SHALL support splitting a scan into DataFusion partitions along bin1 boundaries derived from `bin1_offset` when parallelism is requested, producing the same row set as a single-partition scan.

#### Scenario: Partitioned scan equivalence

- **WHEN** the same `scan_cool` query runs with `target_partitions` of 1 and of 4
- **THEN** both produce identical row sets

### Requirement: Cooler correctness and performance validation against the reference implementation

The system SHALL validate cooler support against the Python `cooler` reference implementation: test fixtures SHALL be generated with `cooler` tooling, scan outputs SHALL be asserted equal to equivalent `cooler` queries after coordinate normalization, and a benchmark comparing scan performance against the `cooler` chunked-pandas approach SHALL be added to the `biodatageeks/bioformats-benchmark` suite. `cooler` SHALL remain a development/test dependency only.

#### Scenario: Row-for-row parity with cooler

- **WHEN** the parity test suite compares `scan_cool` output (full, projected, region-filtered, raw COO, and weighted modes) with the corresponding `cooler` API results on the generated fixtures
- **THEN** all compared frames are equal row-for-row after normalizing coordinate system and dtypes

#### Scenario: Benchmark against the chunked-pandas workaround

- **WHEN** the `bioformats-benchmark` cool benchmarks run a full streaming scan and a region query on a realistic public `.mcool` with both polars-bio and the `cooler` pixels/fetch API
- **THEN** wall time, peak memory, and polars-bio partition scalability are reported for both implementations following the repository's standard result format
