# VCF Zarr Benchmark Notebook Design

## Context

polars-bio has added VCF Zarr read support through `scan_vcf_zarr` and `read_vcf_zarr`, backed by the `datafusion-bio-formats` VCF Zarr provider. The next step is an executable benchmark notebook that compares large-scale VCF Zarr reads and analytical queries across polars-bio, sgkit, and zarr-python.

The notebook is a benchmark artifact, not a new runtime API. It should be self-contained, easy to rerun locally, and explicit about the differences between the three tool layers:

- polars-bio exposes a logical VCF table through DataFusion and Polars.
- sgkit works through the PyData/xarray/Dask ecosystem and may need a compatibility path for VCF Zarr stores depending on installed versions.
- zarr-python reads raw Zarr arrays directly and does not perform VCF logical schema reconstruction.

The notebook will use 1000 Genomes VCF inputs converted to VCF Zarr with bio2zarr/vcf2zarr. Expensive download and conversion steps will be present but gated by environment variables.

## Goals

- Create a complete notebook benchmark harness under `notebooks/`.
- Support end-to-end data preparation: dataset selection, optional download, optional VCF-to-VCF-Zarr conversion, and benchmark execution.
- Compare polars-bio, sgkit, and zarr-python on both I/O-oriented and analytical query scenarios.
- Benchmark thread/partition scalability, with polars-bio `datafusion.execution.target_partitions` as the primary scaling axis.
- Produce in-notebook pandas/Polars result tables and plots.
- Keep all expensive operations opt-in through clear environment gates.

## Non-Goals

- No CSV, JSON, or artifact export in the first version.
- No helper Python module in the first version.
- No CI workflow integration.
- No automatic dependency installation from the notebook.
- No remote VCF Zarr reading benchmark; inputs are downloaded or provided locally.
- No claim that sgkit, zarr-python, and polars-bio perform identical physical work.

## Notebook Location

Create:

`notebooks/vcf_zarr_benchmark.ipynb`

This matches the existing pattern used by `notebooks/feature_counting_benchmark.ipynb`: a notebook-focused benchmark with environment-configured data paths and executable setup cells.

## Configuration

The notebook will read these environment variables, with documented defaults:

```bash
VCZ_BENCH_PROFILE=30x_chr22
VCZ_BENCH_DATA_DIR=/path/to/data
VCZ_BENCH_ENABLE_DOWNLOAD=0
VCZ_BENCH_ENABLE_CONVERT=0
VCZ_BENCH_FORCE_REBUILD=0
VCZ_BENCH_TARGET_PARTITIONS=1,2,4,8,16
VCZ_BENCH_REPETITIONS=3
VCZ_BENCH_WARMUPS=1
VCZ_BENCH_SAMPLE_COUNT=16
VCZ_BENCH_SAMPLES=
```

Additional implementation-level configuration may include dataset-specific region defaults and a custom path override:

```bash
VCZ_BENCH_CUSTOM_VCF=/path/to/input.vcf.gz
VCZ_BENCH_CUSTOM_VCZ=/path/to/input.vcz
VCZ_BENCH_REGION=22:20000000-25000000
```

The notebook will show the selected configuration and planned disk/network costs before download or conversion cells run.

## Dataset Profiles

The notebook will define dataset profiles as metadata dictionaries. Each profile describes input VCF URLs, index URLs, expected local paths, output VCZ path, default genomic regions, and expected scale.

Profiles:

- `phase3_chr22`: 1000 Genomes Phase 3 chr22 genotype VCF. This is the small fallback profile.
- `30x_chr22`: 1000 Genomes high-coverage chr22 phased SNV/INDEL/SV panel. This is the recommended default.
- `phase3_wgs_sites`: Phase 3 WGS sites-only VCF. This is useful for variant-only large scans without genotype-heavy sample work.
- `phase3_wgs_autosomes`: Phase 3 chr1-22 genotype VCFs. This is a large multi-input profile.
- `30x_wgs_autosomes`: 30x high-coverage chr1-22 VCFs. This is the stress profile.
- `legacy_wes_exome`: older exome-like 1000 Genomes VCF profile for WES-style sparse/exome workload exploration.
- `custom`: user-provided VCF or VCZ paths.

The default profile should be `30x_chr22` when disk and network are available, with `phase3_chr22` documented as the fastest practical fallback.

## Gated Data Preparation

The notebook will include cells for:

1. Creating the benchmark data directory.
2. Downloading VCF and index files.
3. Running bio2zarr conversion:
   - `vcf2zarr explode`
   - `vcf2zarr encode`
4. Inspecting the resulting `.vcz` store.

Download behavior:

- If `VCZ_BENCH_ENABLE_DOWNLOAD != "1"` and required VCF inputs are missing, the notebook will stop before benchmark execution with a clear message naming `VCZ_BENCH_ENABLE_DOWNLOAD=1`.
- Existing VCF files are reused unless `VCZ_BENCH_FORCE_REBUILD=1`.

Conversion behavior:

- If `VCZ_BENCH_ENABLE_CONVERT != "1"` and the VCZ output is missing, the notebook will stop before benchmark execution with a clear message naming `VCZ_BENCH_ENABLE_CONVERT=1`.
- Existing VCZ stores are reused unless `VCZ_BENCH_FORCE_REBUILD=1`.

The notebook will not silently start large downloads or rebuilds.

## Benchmark Harness

The notebook will define in-notebook helper functions for:

- dependency/version reporting,
- benchmark configuration parsing,
- timing with warmups and repetitions,
- result validation,
- exception capture,
- conversion of raw result rows into pandas and Polars tables,
- plotting.

Each benchmark result row will include:

- tool,
- scenario,
- profile,
- partition or thread setting,
- repetition index,
- status: `ok`, `skipped`, or `failed`,
- elapsed time,
- success flag,
- failure message,
- validation summary,
- optional notes.

Warmup runs are not included in the summarized benchmark table.

## Tool Adapters

### polars-bio

The polars-bio adapter will use:

- `pb.set_option("datafusion.execution.target_partitions", str(partitions))`
- `pb.scan_vcf_zarr(path, ...)`
- lazy `.select(...)`, `.filter(...)`, `.collect()`

The notebook will run polars-bio scenarios across every configured `VCZ_BENCH_TARGET_PARTITIONS` value.

### sgkit

The sgkit adapter will use the installed sgkit/xarray/Dask stack where it can operate on the selected VCF Zarr store. Current sgkit documentation directs VCF users toward bio2zarr and vcztools, and notes that sgkit documentation may not always be in sync with bio2zarr. Therefore, the notebook must treat sgkit direct VCZ operation as a compatibility-sensitive adapter:

- Attempt to open the VCZ store through xarray/Zarr metadata.
- Use sgkit functions only for scenarios where the opened dataset exposes variables in a sgkit-compatible shape and naming convention.
- Record unsupported sgkit scenarios as skipped result rows instead of failing the whole notebook.
- Document when sgkit is benchmarking a dataset-level xarray/Dask operation rather than the same logical VCF table abstraction as polars-bio.

### zarr-python

The zarr-python adapter will open the same VCZ store and read raw arrays directly:

- core arrays such as `variant_contig`, `variant_position`, `variant_allele`, and `variant_quality`,
- optional INFO arrays such as `variant_AF` or `variant_DP` when available,
- optional FORMAT arrays such as `call_GT` when available.

zarr-python scenarios will be the closest raw-array equivalent of the logical query, not a full VCF row reconstruction unless that reconstruction is explicitly implemented in the notebook helper.

## Benchmark Scenarios

### Open And Metadata

- Open the store or dataset.
- Inspect variant count, sample count, dimensions, available arrays, and chunk shapes.

### Full Table Scan

- Core variant columns: chrom/start/end/ref/alt.
- Core plus one available INFO field, preferring `AF` then `DP`.
- Optional genotype-heavy scan for a selected FORMAT field and bounded sample subset.

### Projection Pruning

- Read only `chrom,start`.
- Read `chrom,start,qual`.
- Read one projected INFO field.

### Range Filtering

- Run a bounded region filter using the profile default region or `VCZ_BENCH_REGION`.
- For multi-chromosome profiles, include a second region on another chromosome when available.

### Variant Lookup

- Exact position lookup using positions sampled from the dataset during setup.
- Small list lookup using several sampled variant positions.

### Sample-Focused Query

- Select samples from comma-separated `VCZ_BENCH_SAMPLES` when provided; otherwise select the first `VCZ_BENCH_SAMPLE_COUNT` sample IDs.
- Extract `GT` or the closest available FORMAT field for a genomic window.
- Validate dimensions and representative values when possible.

### Simple Analytics

- Count variants by chromosome.
- Count/filter variants by quality or allele frequency/depth when the field exists.
- For sample subset scenarios, compute a lightweight genotype availability/count metric when the necessary call arrays are available.

## Fairness And Limitations

The notebook must clearly document that this is a practical benchmark, not proof that all tools execute the same physical plan.

Fairness rules:

- All tools use the same local VCZ store where technically possible.
- Each scenario validates a result shape, count, or representative value.
- Warmup and measured repetitions are separate.
- Failures and skipped scenarios are retained in the raw results table.
- polars-bio scalability is measured by `target_partitions`.
- sgkit and zarr-python are plotted as fixed baselines unless a reliable per-run thread control is identified during implementation.

Known limitations:

- Direct zarr-python reads skip DataFusion planning and logical VCF reconstruction.
- sgkit compatibility depends on installed versions and on how the VCZ arrays map into xarray/sgkit-compatible variables.
- Different libraries may materialize different intermediate structures.
- Operating system file cache can affect repeated local-read timings; the notebook should report warmups and repetitions but will not attempt cache eviction.
- Results are local-machine benchmarks and should not be compared across machines without environment metadata.

## Outputs

The notebook will produce in-memory outputs only:

- raw results table,
- summarized results table grouped by tool, scenario, and partition/thread setting,
- scenario runtime comparison plot,
- polars-bio thread/partition scalability plot with speedup,
- environment table with Python version, package versions, CPU count, selected profile, input paths, store size, variant count, sample count, and chunk shape.

Plotting should use matplotlib/seaborn if available. If plotting dependencies are missing, the notebook should still produce result tables and print a clear message.

## Error Handling

The notebook should fail early for missing required local inputs when gates are disabled. Runtime benchmark errors should be recorded per scenario and should not abort the entire notebook unless the selected VCZ store itself cannot be opened by any tool.

Common errors should include direct remediation text:

- missing VCF input: enable `VCZ_BENCH_ENABLE_DOWNLOAD=1` or provide `VCZ_BENCH_CUSTOM_VCF`,
- missing VCZ output: enable `VCZ_BENCH_ENABLE_CONVERT=1` or provide `VCZ_BENCH_CUSTOM_VCZ`,
- missing optional dependency: install the named package and rerun,
- unsupported sgkit scenario: inspect the skipped result row and notes.

## Testing And Verification

Notebook verification should include:

- smoke-run the configuration and metadata cells against the repo-local small VCF Zarr fixture,
- run benchmark helper functions on at least one tiny scenario,
- verify gated download/convert cells do not run when gates are off,
- execute a full benchmark path on an existing local VCZ store when available,
- ensure the notebook JSON has no execution-only absolute paths baked into source cells except documented env-var defaults.

Before completion, run formatting/check commands appropriate for notebook changes and verify `git diff --check`.

## Implementation Plan Handoff

After this design is approved and committed, create a detailed implementation plan using the Superpowers writing-plans workflow. The first implementation version should remain notebook-only and should avoid adding helper modules unless the notebook becomes unmaintainably large during implementation.
