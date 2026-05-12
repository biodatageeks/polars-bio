## Context

polars-bio integrates bioinformatics file readers through DataFusion table providers. The current VCF reader already supports logical VCF schema output, INFO/FORMAT field selection, sample selection, coordinate-system metadata, projection pushdown, and predicate pushdown through the Polars lazy scan wrapper.

VCF Zarr stores VCF data as Zarr arrays. The latest VCF Zarr specification checked for this design is version 0.4, which targets Zarr v2. The initial implementation will target VCF Zarr 0.4 stores produced by `bio2zarr vcf2zarr encode` and readable by `zarrs`.

## Goals

- Read and scan local VCF Zarr stores.
- Preserve polars-bio's existing logical VCF schema.
- Keep query pruning in the DataFusion provider layer.
- Support projected INFO and FORMAT fields while preserving supported VCF Zarr primitive types.
- Support sample subset selection.
- Support genomic predicate pruning through `region_index` when available.
- Provide a fallback pruning path when `region_index` is absent.
- Default VCF Zarr `GT` output to raw typed genotype calls, with string encoding available as an explicit compatibility option.

## Non-Goals

- VCF Zarr writing.
- VCF to VCF Zarr conversion.
- Cloud/object storage support.
- Auto-detection through existing `scan_vcf` / `read_vcf`.
- Raw VCF Zarr array-name output as the primary API.
- Arbitrary Zarr v3 support.

## Decisions

### Provider Location

Implement `VcfZarrTableProvider` in `/Users/mwiewior/CLionProjects/datafusion-bio-formats` on branch `add-vcf-zarr-support`.

Rationale: format providers already live in that repository. Implementing there keeps DataFusion provider logic reusable and avoids a polars-bio only fork of format behavior.

### polars-bio Integration

Use branch `add-vcf-zarr-support` in polars-bio and temporarily point Cargo dependencies at the local `datafusion-bio-formats` checkout during development.

Rationale: this allows fast cross-repo development while keeping the final integration path clear. Before review, the local path dependency should be replaced with a git revision.

### API Shape

Add explicit APIs:

- `scan_vcf_zarr(path, ...)`
- `read_vcf_zarr(path, ...)`

Do not auto-detect `.vcz` or `.zarr` in `scan_vcf`.

Rationale: `.zarr` is generic, and explicit APIs avoid surprising routing behavior.

### Logical Schema

Expose the same logical schema as existing VCF scans:

- core VCF columns as `chrom`, `start`, `end`, `id`, `ref`, `alt`, `qual`, `filter`
- INFO fields as their field IDs, preserving supported VCF Zarr primitive dtypes
- FORMAT fields using the current single-sample and multisample conventions, preserving supported non-GT primitive dtypes

Rationale: existing polars-bio filters, projections, metadata handling, and range operations should work without users learning raw VCF Zarr array names.

### Typed INFO and FORMAT Mapping

VCF Zarr INFO and non-GT FORMAT arrays should map directly to Arrow types instead of first converting values into VCF text:

- one-dimensional numeric and boolean INFO arrays become scalar Arrow numeric or boolean columns,
- INFO arrays with an additional value dimension become `List<T>` columns,
- non-GT FORMAT arrays keep the selected-sample dimension in `genotypes.<ID>`,
- non-GT FORMAT arrays with an additional value dimension become nested list values under `genotypes.<ID>`,
- string-typed source arrays remain strings.

The implementation should preserve the source Arrow-compatible primitive width where practical. If a source dtype cannot be represented directly by Arrow or Polars, it may be promoted to the smallest compatible Arrow type and documented in code/tests.

Rationale: current stringification adds CPU work, allocations, and downstream casts. Preserving types lets DataFusion and Polars evaluate numeric filters and aggregations on native values.

### Genotype Encoding

VCF Zarr `GT` has two mutually exclusive encodings:

- raw typed encoding, selected by default,
- VCF-style string encoding, selected by an explicit `genotype_encoding_raw=False` option.

The API should not expose raw and string `GT` representations in the same scan. In raw mode, `genotypes.GT` preserves raw allele calls with selected-sample and ploidy dimensions. If the store contains phasing or genotype mask arrays, the reader may expose those as typed genotype metadata fields needed to interpret raw calls. In string mode, `genotypes.GT` keeps the current VCF-style string representation and suppresses raw `GT` fields.

Rationale: raw `GT` avoids the most expensive VCZ conversion path and is the better default for analytical queries. String mode remains available for callers that need the familiar VCF representation.

### Pruning Strategy

Projection pruning will happen before Zarr array reads using a logical-column to raw-array dependency graph.

Genomic predicate pruning will:

- use `region_index` when present,
- fall back to scanning `variant_contig` and `variant_position` chunks when absent,
- apply residual record-level filtering to preserve correctness.

Rationale: this delivers the requested pruning optimizations while accepting simpler VCF Zarr stores that omit the experimental region index.

## Risks / Trade-offs

- `zarrs` supports a compatible subset of Zarr v2, so some VCF Zarr stores may use codecs or metadata layouts that need explicit error handling.
- Mapping raw arrays into the logical VCF schema adds small compute costs for dictionary lookup and allele/filter reconstruction, but this is outweighed by API compatibility and can still be pruned at the array level.
- Sample pruning depends on chunk layout. Some layouts may require reading wider sample chunks before slicing.
- Fallback pruning without `region_index` still needs to read position arrays, but it avoids reading heavy INFO/FORMAT arrays until candidate chunks are known.

## Migration Plan

This is an additive feature. Existing VCF APIs and behavior remain unchanged.

During development, polars-bio will use a local path dependency. Before final review, replace it with a git revision or another publishable dependency reference.

## Open Questions

- None for the initial implementation scope.
