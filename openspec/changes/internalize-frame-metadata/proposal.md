# Change: Internalize frame metadata with bounded patching

## Why

polars-bio needs metadata for coordinate correctness and genomic format roundtrips.
The current `polars-config-meta` dependency uses a global object-ID registry and
patches hundreds of methods. The [audit](audit.md) reproduced stale metadata
inheritance, a defensive gap remaining in upstream PR #67, and namespace collisions.

The executable application uses only `config_meta.set()` and `get_metadata()`.
We will own that small surface and the documented Parquet compatibility writer,
while preserving ordinary Polars frame types and method chains. This is the
selected bounded-patching approach; subclasses and custom transform callbacks
are not part of the migration.

## What Changes

- Store metadata directly on DataFrame/LazyFrame instances in one private module,
  provisionally `polars_bio/_frame_metadata.py`. Remove the object-ID registry,
  weakref cleanup scans, and return-annotation-based method discovery.
- Register `config_meta` on DataFrame and LazyFrame. Retain `set(**values)`
  returning `None`, `get_metadata()`, and the legacy `write_parquet()` method.
  Preserve the coordinate flag, source format/path, and complete serialized source
  header, including VCF layout and BAM/SAM tag types.
- Wrap exactly 57 class/method pairs on Polars 1: 33 preserve metadata through common frame
  operations, 10 carry it through ordinary grouping, and 14 invalidate it after
  generic multi-input operations. Grouped `agg`, `len`, `head`, `tail`, and `having`
  retain normal syntax and types, with per-group metadata snapshots.
  The [design](design.md) contains the complete manifest.
  Forward compatibility tests for PR #478 cover Polars 2.0/2.1 with 56 entries,
  omitting only `LazyFrame.with_context`, which those versions removed.
- Preserve original result types, signatures, exceptions, and lazy execution.
  Recommend `pb.scan_csv()` / `pb.scan_parquet()` for genomic interval data,
  with explicit `zero_based=` and optional output conversion via `use_zero_based=`.
  These entry points validate/normalize interval columns and attach metadata.
  Frames created directly through `pl.DataFrame()` or `pl.scan_parquet()` work
  after explicit tagging as an interoperability path, without adoption or a
  custom transform callback.
- Make installation idempotent, preflight conflicts, and avoid partially installed
  patches after initialization failure. Leave Series, custom `map_groups`,
  rolling/dynamic grouping entry points, module functions, and native batch-stream
  methods outside the patch manifest.
- Use native Polars Parquet metadata APIs for compatible writer options and scalar
  schemas, retaining the legacy representation and a small PyArrow fallback for
  other options or schemas, including nested and categorical data.
- Remove the external runtime dependency after integration checks; update maintained
  packaging, attribution, documentation, and examples.
- **BREAKING:** Generic joins/stacks/updates clear result metadata, including
  successful in-place changes, even when input tags match. Domain-specific
  polars-bio range operations retain their explicit validation/output tagging.
- **BREAKING:** `get_metadata()` returns a shallow snapshot. Top-level updates
  must use `set()`; nested user values remain shallowly shared.
- **BREAKING:** The wider dependency API is excluded: Series metadata, arbitrary
  namespace forwarding, generic merge/update/clear APIs, `ConfigMetaOpts`,
  diagnostics, and standalone upstream Parquet reader imports. Methods outside
  the manifest have no propagation guarantee.
- **BREAKING:** Initializing the original plugin alongside this provider is
  unsupported. Detect conflicts at initialization or subsequent owned metadata use
  with an actionable error. Merely having its distribution installed is harmless.

## Impact

- Specs: add `frame-metadata` and packaging requirements. Coordinate rules also
  relate to the unarchived `switch-default-to-zero-based-coordinates` and
  `add-coordinate-safe-tabular-readers` changes.
- Runtime integration: `polars_bio/__init__.py`, `_metadata.py`, `_tabular_io.py`,
  and metadata access/copy sites in `io.py`. No new frame hierarchy, reader
  adoption layer, or Rust changes are expected.
- Preserve existing Cooler/GFF/GTF adapters and the native
  `collect_batches(lazy=True)._inner` interface consumed by writers/range operations.
- Remove the dependency from `pyproject.toml`, maintained `uv.lock`, and
  `bioconda-recipe/meta.yaml`. PyArrow remains an existing dependency.
- Include required upstream MIT attribution for adapted code/tests in wheel and
  sdist. Leave unrelated generated files and historical artifacts alone.
- Keep the application dependency within Polars `>=1.40.0,<2` in this PR, while
  testing the provider and its combination with PR #478 on Polars 2.0/2.1.
  The application dependency change belongs to #478. Arrow coordinate extension
  types remain separate work.

## Delivery

The bounded implementation, integration, dependency removal, documentation, and
artifact checks are complete. See the [implementation checklist](tasks.md),
[acceptance plan](validation.md), and [verification results](verification.md).
The provider passes 205 tests on each of eight Python/Polars combinations, and
the full `make test` target passes. The initial local run discovered an untracked
PGEN test file absent from HEAD and clean CI checkouts. Its 54 failures reproduced
with unchanged production code; after the user requested deletion of that local
file, the complete test target passed without an additional exclusion.

Wheel and sdist contents are verified, and the wheel passes workflow checks in
a fresh environment without the old distribution. The Bioconda dependency and
license changes are staged for the migration release: its source version and
checksum must be updated together when that release exists. No release has
been published.

The accepted tradeoff is process-wide wrapping of 57 Polars methods (56 on Polars 2). Untagged
frames still pass through those wrappers, so compatibility and overhead checks
are release gates. Owning a small manifest limits that cost; it does not eliminate
the effects of patching.
