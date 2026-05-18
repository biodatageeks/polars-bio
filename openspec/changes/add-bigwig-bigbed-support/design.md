## Context

polars-bio integrates most genomic file formats through DataFusion `TableProvider`s. The Python scan APIs register a file-backed table, fetch its schema without materializing rows, then return a Polars `LazyFrame` through `register_io_source`. Lazy selections and filters are translated back into DataFusion select/filter operations so provider-level projection and predicate pushdown can be used.

`bigtools` is a Rust library for BigWig and BigBed files. Its documented read entrypoints are `BigWigRead::open` and `BigBedRead::open` over `Read + Seek`, plus `open_file` helpers. The main data APIs return interval iterators for a given `chrom`, `start`, and `end`, which maps well to genomic predicate pruning. It also exposes chromosome metadata, summaries, zoom records, BigBed autoSQL, and a reopen pattern that can support independent readers per DataFusion partition.

## Goals

- Read and lazily scan BigWig files.
- Read and lazily scan BigBed files.
- Keep the public API consistent with existing `scan_*`, `read_*`, and `register_*` methods.
- Preserve polars-bio coordinate-system metadata and `use_zero_based` behavior.
- Push lazy projections into DataFusion and provider execution.
- Push genomic predicates into BigWig/BigBed interval queries where possible.
- Use DataFusion `target_partitions` for parallel indexed scans.
- Preserve correctness when filters cannot be pushed down exactly.

## Non-Goals

- BigWig or BigBed writing.
- BigWig base-level dense value arrays, binned summaries, or zoom-record APIs as the initial tabular scan output.
- Full S3/GCS/OpenDAL seekable access in the first version.
- Pushdown for arbitrary BigBed text expressions beyond parsed scalar autoSQL fields.
- Auto-detecting `.bb`, `.bigBed`, `.bw`, or `.bigWig` through existing `scan_bed` or other APIs.

## Decisions

### Provider Location

Use this polars-bio OpenSpec change as the authoritative tracking artifact for both repositories. Do not create a separate mirrored OpenSpec proposal in `biodatageeks/datafusion-bio-formats`; keep design decisions, task status, and scope decisions in this change.

Implement provider logic in `biodatageeks/datafusion-bio-formats` rather than directly in `polars_bio`, because existing genomic file readers are DataFusion `TableProvider`s supplied by that companion provider workspace.

Recommended structure:

- `datafusion-bio-format-bigwig` with `BigWigTableProvider` and `BigWigExec`
- `datafusion-bio-format-bigbed` with `BigBedTableProvider` and `BigBedExec`
- shared helper code in `datafusion-bio-format-core` only if both providers need it

If upstream prefers one crate, `datafusion-bio-format-bbi` can contain both providers, but the public provider types should still be separate.

### API Shape

Add explicit methods:

- `scan_bigwig(path, ..., predicate_pushdown=True, projection_pushdown=True, use_zero_based=None)`
- `read_bigwig(...)`
- `scan_bigbed(path, ..., predicate_pushdown=True, projection_pushdown=True, use_zero_based=None, schema="auto")`
- `read_bigbed(...)`
- `register_bigwig(path, name=None, ...)`
- `register_bigbed(path, name=None, ..., schema="auto")`

Do not route these through `scan_bed` or `scan_table`. BigBed is related to BED, but it is indexed binary BBI data with different schema discovery and query behavior.

### Schemas

BigWig output schema:

- `chrom: Utf8`
- `start: UInt32`
- `end: UInt32`
- `value: Float32`

BigBed output schema:

- Always include `chrom: Utf8`, `start: UInt32`, `end: UInt32`.
- When autoSQL is present and can be parsed, expose supported scalar fields after the first three BED fields.
- Supported initial autoSQL types: string-like fields as `Utf8`, signed integers as `Int64`, unsigned integers as `UInt64`, floating values as `Float64`.
- When autoSQL is absent or unsupported, expose `rest: Utf8` containing the raw trailing BigBed fields.

Coordinate conversion follows the existing BED convention. BigWig and BigBed are natively 0-based half-open; when `use_zero_based=False`, emit `start + 1` and keep `end` as the closed 1-based end.

### Predicate Pushdown

Provider-level genomic pushdown is feasible and should be included:

- `chrom = "..."`
- `chrom IN (...)`
- conjunctions involving `start` and `end` range comparisons supported by the existing genomic filter helper

The provider should mark these filters as `Inexact`, query the BBI interval index through bigtools, and rely on DataFusion or provider residual filtering to preserve exact semantics.

For BigWig, `value` predicates can be applied as provider-side record filtering after interval retrieval, but they do not reduce index I/O. For BigBed, predicates over parsed autoSQL scalar columns can be applied after parsing requested rows. Unsupported predicates must remain client-side/DataFusion residual filters.

### Projection Pushdown

Projection pushdown is feasible and should be included:

- BigWig should avoid building the `value` array when not projected.
- BigBed should avoid parsing non-projected autoSQL fields unless a residual predicate needs them.
- Empty projection for `count(*)` should work without materializing unused value/rest arrays.

All providers should implement readable `DisplayAs` output such as `BigWigExec: projection=[chrom, start]` and `BigBedExec: projection=[chrom, start, name]` so existing plan-inspection tests can be extended.

### Parallel Execution

Parallel scans are feasible with limits:

- Build DataFusion partitions from pushed genomic regions or from all chromosomes on full scans.
- If a single large chromosome dominates, split it into sub-regions using chromosome length and `target_partitions`.
- Each partition opens or reopens an independent bigtools reader and processes its assigned regions sequentially.
- To prevent duplicates at partition boundaries, assign ownership by original record start position for full-scan sub-regions and keep residual filtering for pushed queries.

This is useful for indexed local files. Without a seekable source or when region splitting is disabled, the provider should fall back to one partition.

### Storage Scope

Initial support should be local filesystem first. HTTP(S) can be enabled only if the bigtools `remote` feature is compiled and covered by tests. S3/GCS support should return a clear not-supported error until there is a tested seekable reader backed by polars-bio's object storage layer.

## Risks / Trade-offs

- BigBed autoSQL can describe richer field types than the initial Arrow mapping. Unsupported complex fields need clear fallback or errors.
- Region chunking can duplicate boundary-overlapping records unless ownership filtering is explicit.
- Bigtools exposes interval-oriented reads, so full scans must be represented as chromosome-wide interval reads.
- BigWig zooms and dense base arrays are valuable but should be separate APIs to avoid overloading the initial tabular scan design.
- bigtools default features include writing and CLI support; provider crates should use minimal features to avoid unnecessary wheel dependencies.
- Until the libdeflater compatibility fix is merged upstream, the provider should pin the `biodatageeks/bigtools` fork revision used for the upstream PR.

## Migration Plan

This is additive. Existing APIs and file readers are unchanged.

Development should happen behind this OpenSpec change. During provider development, polars-bio may temporarily point Cargo dependencies at a local `datafusion-bio-formats` checkout and switch to a git revision before review.

## Open Questions

- BigBed autoSQL parsing should fall back to the `rest` schema when the autoSQL schema is absent or contains unsupported field types.
- HTTP(S), S3, and GCS support are deferred from the first implementation unless local-file support is complete and `bigtools` HTTP(S) reading is enabled with dedicated tests.
