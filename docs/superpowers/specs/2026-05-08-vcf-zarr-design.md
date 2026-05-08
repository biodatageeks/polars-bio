# VCF Zarr Read/Scan Support Design

## Context

polars-bio currently reads VCF through `datafusion-bio-formats` table providers and exposes data through explicit Python APIs such as `scan_vcf` and `read_vcf`. The existing scan path registers a DataFusion table provider, obtains an Arrow schema without materializing data, and uses a Polars `register_io_source` callback to push selected columns and filters back into DataFusion before streaming record batches.

VCF Zarr stores VCF data as chunked Zarr arrays. The current VCF Zarr specification is version 0.4 and is based on Zarr v2. The initial implementation will target stores produced by `bio2zarr vcf2zarr encode` and read them using the Rust `zarrs` ecosystem.

## Goals

- Add explicit `scan_vcf_zarr` and `read_vcf_zarr` APIs.
- Keep the same logical schema as existing `scan_vcf` unless a query explicitly requests a VCF Zarr specific feature later.
- Preserve DataFusion-driven projection and predicate pushdown.
- Use VCF Zarr `region_index` when available for genomic region pruning.
- Fall back to lightweight position-array chunk filtering when `region_index` is absent.
- Support local filesystem stores first.
- Support VCF Zarr spec 0.4 only in the first version.

## Non-Goals

- Writing or converting VCF Zarr stores.
- Auto-routing `.vcz` or `.zarr` paths through `scan_vcf`.
- Cloud/object storage support in the first version.
- Arbitrary Zarr v3 or non-VCF-Zarr layouts.
- Raw VCF Zarr variable-name schema as the primary API.

## Repository Workflow

The provider implementation belongs in the local checkout:

`/Users/mwiewior/CLionProjects/datafusion-bio-formats`

Development will use feature branch `add-vcf-zarr-support` in both repositories:

- `datafusion-bio-formats`: implement `VcfZarrTableProvider` and tests.
- `polars-bio`: integrate explicit Python APIs, PyO3 options, `InputFormat::VcfZarr`, docs, and Python tests.

During development, `polars-bio` will temporarily reference the local `datafusion-bio-formats` checkout with a path dependency. Before final review, that local reference should be replaced with a git revision or other publishable dependency reference.

## Architecture

The VCF Zarr reader will be a native DataFusion table provider implemented in `datafusion-bio-formats`, using `zarrs::filesystem::FilesystemStore` for local Zarr access.

The provider responsibilities are:

- Open and validate the VCF Zarr root group.
- Require `vcf_zarr_version = "0.4"` in the root attributes.
- Discover mandatory and optional arrays.
- Build an Arrow schema that matches polars-bio's existing logical VCF schema.
- Implement `TableProvider::scan` with projection, predicate, limit, and partition handling.
- Stream Arrow `RecordBatch` output without materializing the full store.

polars-bio responsibilities are:

- Add explicit `scan_vcf_zarr` and `read_vcf_zarr` APIs.
- Add a VCF Zarr read-options class mirroring the relevant VCF options.
- Register the new provider from `src/scan.rs`.
- Reuse the existing `_lazy_scan` pushdown path.
- Preserve coordinate-system and source metadata behavior.

## Logical Schema Mapping

The primary API exposes the same logical schema as existing VCF scans:

- `chrom`: lookup from `variant_contig` through `contig_id`
- `start`: from `variant_position`, adjusted by `use_zero_based`
- `end`: from `variant_position` plus `variant_length` when available, with coordinate-system conversion applied
- `id`: from `variant_id`
- `ref` and `alt`: from `variant_allele`
- `qual`: from `variant_quality`
- `filter`: from `variant_filter` and `filter_id`
- INFO fields: `variant_<ID>` arrays exposed as `<ID>`
- FORMAT fields: `call_<ID>` arrays exposed consistently with existing VCF behavior
- multisample FORMAT output: nested `genotypes`

If `variant_length` is absent, the provider will derive simple allele length where possible and otherwise use point-variant semantics. Unsupported or ambiguous stores should fail early only when required data cannot be represented correctly.

## API Semantics

The initial APIs mirror the existing VCF read options where applicable:

- `info_fields`
- `format_fields`
- `samples`
- `projection_pushdown`
- `predicate_pushdown`
- `use_zero_based`

`info_fields`, `format_fields`, and `samples` act as early array-selection hints. Polars `.select()` and `.filter()` still drive DataFusion projection and predicate pushdown inside the scan path.

The API is explicit only:

- `scan_vcf_zarr(path, ...)`
- `read_vcf_zarr(path, ...)`

Existing `scan_vcf` and `read_vcf` will not auto-detect `.vcz` or `.zarr` paths in this change.

## Projection Pruning

The provider will maintain a dependency graph from logical columns to raw arrays. It opens and reads only arrays required by:

- requested logical schema columns,
- pushed predicates,
- required dictionary arrays such as `contig_id` and `filter_id`,
- selected INFO and FORMAT fields,
- selected sample indexes.

Examples:

- `select(["chrom", "start"])` reads `variant_contig`, `contig_id`, and `variant_position`.
- Selecting an INFO field reads only the matching `variant_<ID>` array plus required core arrays.
- Selecting FORMAT fields reads only the requested `call_<ID>` arrays and selected sample positions.

## Predicate And Chunk Pruning

Genomic predicates on `chrom`, `start`, and `end` are used for region/chunk pruning.

When `region_index` exists:

- Use it to select candidate variant chunks.
- Use `variant_length` for maximum-end aware overlap pruning when present.
- Read heavier INFO/FORMAT arrays only for selected candidate chunks.

When `region_index` is absent:

- Scan lightweight `variant_contig` and `variant_position` chunks.
- Identify candidate variant chunks.
- Read projected heavier arrays only for those chunks.

Record-level predicates that can be translated by DataFusion are applied inside or immediately after batch construction. Unsupported predicates fall back to the existing client-side filtering path in `polars-bio`, preserving correctness.

## Sample Pruning

The provider resolves requested sample names through `sample_id` with exact, case-sensitive matching. Output order follows the requested order. Missing requested samples are skipped with warning semantics, matching existing VCF behavior.

Where the Zarr chunk layout permits direct sample slicing, the provider reads only selected sample positions. If a chunk layout requires reading a wider sample chunk, the provider slices before emitting Arrow batches.

## Parallelism

DataFusion partitions are based on selected variant chunks or groups of selected chunks, bounded by session `target_partitions`. Full scans without predicates partition over variant chunks. Region-pruned scans partition over candidate chunks from `region_index` or fallback position-array pruning.

## Error Handling

The provider fails early with clear errors for:

- missing `vcf_zarr_version`,
- unsupported `vcf_zarr_version`,
- unsupported Zarr layout or codecs not readable by configured `zarrs`,
- missing mandatory arrays,
- malformed array shapes or dimension metadata,
- unsupported dtype mappings.

Errors should name the failing path or array where possible.

## Testing

Tests should exist in both repositories.

In `datafusion-bio-formats`:

- VCF Zarr metadata validation tests.
- Schema mapping tests for core, INFO, FORMAT, sample, and filter fields.
- Projection pruning tests with physical plan checks or array-read instrumentation.
- Region-index pruning tests.
- Fallback pruning tests without `region_index`.
- Sample subset tests.
- Fixtures generated from current `bio2zarr vcf2zarr`.

In `polars-bio`:

- `scan_vcf_zarr` and `read_vcf_zarr` API tests.
- Output parity tests against existing `scan_vcf` on equivalent source data.
- Polars `.select()` and `.filter()` pushdown tests.
- Coordinate-system metadata tests.
- VCF header/sample/source metadata tests.
- Documentation examples.

Verification before completion:

- `cargo test` for affected `datafusion-bio-formats` crate(s).
- `cargo check` or `cargo test` in `polars-bio`.
- Targeted VCF Zarr pytest suite.
- Existing VCF regression tests.
- `openspec validate add-vcf-zarr-support --strict`.

