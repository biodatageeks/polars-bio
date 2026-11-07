## ADDED Requirements

### Requirement: Frame-owned metadata

The library SHALL store metadata on Polars DataFrame and LazyFrame Python instances
without a process-global frame-ID registry. It SHALL preserve
`coordinate_system_zero_based`, `source_format`, `source_path`, and complete
serialized `source_header` content.

#### Scenario: Independent frame lifetimes
- **WHEN** tagged frames are released and unrelated frames are created, including concurrent creation of independent frames
- **THEN** unrelated frames SHALL start without another frame's metadata
- **AND** the provider SHALL not retain dead frames or scan a global registry for cleanup

#### Scenario: Complete and independent copies
- **WHEN** metadata is copied through a supported operation
- **THEN** VCF header/sample/layout content and alignment tag-type declarations SHALL survive
- **AND** the destination SHALL own an independent top-level dictionary

### Requirement: Bounded metadata namespace

The library SHALL provide `config_meta.set(**values)`, `get_metadata()`, and the
legacy `write_parquet()` method on DataFrame and LazyFrame. Series registration,
generic namespace forwarding, configuration/diagnostic APIs, and standalone
upstream package imports SHALL remain outside this compatibility contract.

#### Scenario: Read and update metadata
- **WHEN** a caller sets metadata
- **THEN** the setter SHALL update entries and return `None`
- **WHEN** a caller reads metadata
- **THEN** the getter SHALL return a shallow snapshot whose top-level mutation does not modify the frame

#### Scenario: Preserve legacy Parquet representation
- **WHEN** a caller uses `config_meta.write_parquet()`
- **THEN** the writer SHALL preserve JSON under `polars_plugin_meta` in both Arrow schema metadata and Parquet file metadata, retaining unrelated schema metadata where available
- **AND** it SHALL preserve the explicit coordinate declaration contract of polars-bio Parquet readers

#### Scenario: Native writing and legacy options
- **WHEN** writer options and the schema have verified native Polars equivalents that preserve both metadata locations
- **THEN** the writer SHALL use native writing with both metadata locations supplied explicitly
- **AND** it SHALL not collect LazyFrame rows solely to attach metadata
- **WHEN** legacy PyArrow options cannot be mapped faithfully or native writing does not preserve schema metadata, including nested and categorical schemas
- **THEN** it SHALL retain their semantics through a bounded fallback

### Requirement: Explicit propagation scope

The provider SHALL preserve metadata through `filter`, `select`, `with_columns`,
`drop`, `rename`, `sort`, `slice`, `head`, `tail`, `limit`, `clone`, `cast`,
`unique`, `reverse`, and `explode` on DataFrame/LazyFrame; DataFrame `lazy` and
frame-returning `__getitem__`; and LazyFrame `collect`. These 33 class/method pairs
SHALL retain ordinary syntax, original result types, public introspection
signatures, exceptions, and lazy behavior without collection solely for propagation.
The provider SHALL NOT require a metadata subclass, adoption step, or custom
transform callback.

#### Scenario: Ordinary lazy chain
- **WHEN** a tagged native LazyFrame is filtered, projected, and collected, including with the streaming engine
- **THEN** its result SHALL retain the complete metadata and the original Polars result type
- **AND** the caller SHALL use ordinary `.filter(...).select(...).collect()` syntax

#### Scenario: Externally created frame
- **WHEN** a caller tags a native frame created directly through Polars
- **THEN** the same supported transformations SHALL propagate metadata without adoption or changing the frame's class

#### Scenario: Indexing return kinds
- **WHEN** DataFrame indexing returns a DataFrame
- **THEN** the result SHALL retain metadata
- **WHEN** indexing returns a Series or scalar
- **THEN** the provider SHALL leave that result's kind unchanged and SHALL NOT attach frame metadata

#### Scenario: Untagged fast path
- **WHEN** a supported preserving method is called on an untagged frame
- **THEN** the provider SHALL delegate normally without fabricating metadata or creating registry state

### Requirement: Ordinary grouped metadata propagation

The provider SHALL carry metadata through ordinary eager and lazy
`group_by(...).agg(...)`, `len`, `head`, `tail`, and `having` chains without changing
native frame/group types, signatures, error behavior, or lazy execution. It SHALL
add exactly ten wrappers: DataFrame/LazyFrame `group_by`, and GroupBy/LazyGroupBy
`agg`, `head`, `tail`, and `having`. The supported `len` path SHALL preserve
metadata through its delegation to `agg` without a separate wrapper.

#### Scenario: Aggregate a tagged interval frame
- **WHEN** a tagged frame is grouped and aggregated using ordinary syntax, including named keys/aggregations and `maintain_order`
- **THEN** the native result SHALL retain an independent top-level copy of the coordinate convention and complete source metadata
- **AND** a lazy grouped query SHALL remain lazy without collecting rows or resolving schema solely for metadata propagation

#### Scenario: Snapshot at grouping time
- **WHEN** a caller creates a grouped query and then retags the source frame
- **THEN** the grouped query SHALL retain the snapshot taken at `group_by()` time
- **AND** independent grouped results SHALL NOT share a mutable top-level metadata dictionary

#### Scenario: Grouped summaries and row selection
- **WHEN** a marked grouping object produces a result through `len`, `head`, or `tail`
- **THEN** the result SHALL preserve the group snapshot, original arguments, and native result type
- **AND** count-only summaries SHALL remain subject to existing interval-column checks if used in a genomic operation

#### Scenario: Having preserves native identity behavior
- **WHEN** `having` returns a new grouping object
- **THEN** the new object SHALL receive an independent top-level snapshot
- **WHEN** `having` returns its receiver
- **THEN** metadata propagation SHALL preserve that identity and the existing snapshot

#### Scenario: Coordinate-changing aggregation
- **WHEN** an aggregation changes coordinate meaning through arbitrary expressions
- **THEN** metadata copying SHALL NOT claim to infer the new convention or rewrite source headers
- **AND** documentation SHALL require the caller to retag coordinate conversions correctly, as for `with_columns`

#### Scenario: Unmarked and excluded grouping paths
- **WHEN** a shared patched GroupBy method is invoked on a grouping object without the private snapshot
- **THEN** the provider SHALL delegate without fabricating metadata
- **AND** custom `map_groups`, rolling/dynamic grouping, group iteration, and arbitrary container traversal SHALL remain outside the propagation guarantee

### Requirement: Recommended interval reader entry points

The migration documentation SHALL recommend `pb.scan_csv()` and
`pb.scan_parquet()`, and their eager equivalents, for genomic interval data.
It SHALL preserve their explicit source-coordinate declaration and optional
output conversion contracts, while retaining explicitly tagged external Polars
frames as an interoperability path.

#### Scenario: Recommended reader workflow
- **WHEN** documentation demonstrates loading interval CSV or Parquet data
- **THEN** it SHALL use a polars-bio reader with explicit `zero_based=` and ordinary subsequent Polars method chaining
- **AND** it SHALL distinguish the source declaration from optional output conversion through `use_zero_based=`

#### Scenario: Manually tagged external frame
- **WHEN** documentation describes tagging a frame created through Polars
- **THEN** it SHALL explain that tagging does not perform the interval validation, column normalization, or coordinate conversion provided by polars-bio readers
- **AND** it SHALL NOT require adoption into a custom frame class

### Requirement: Explicit multi-input invalidation

The provider SHALL invalidate result metadata after successful `join`, `join_asof`,
`join_where`, `update`, and `merge_sorted` calls on DataFrame/LazyFrame; DataFrame
`vstack`, `hstack`, and `extend`; and LazyFrame `with_context` on Polars 1.
Polars 2 removed `with_context`; it SHALL NOT be installed or required there.
These 14 class/method pairs on Polars 1 (13 on Polars 2) SHALL preserve existing
domain-specific coordinate validation
and output tagging in polars-bio operations without generic source-header merging.

#### Scenario: Generic combination
- **WHEN** a listed operation successfully combines frames with matching, conflicting, or missing metadata
- **THEN** its result SHALL be untagged, even if internal eager/lazy calls temporarily copied metadata
- **AND** non-mutated inputs SHALL retain their metadata

#### Scenario: In-place combination
- **WHEN** a listed operation successfully modifies the receiver in place
- **THEN** the receiver's metadata SHALL be invalidated without changing the original return convention

#### Scenario: Operation failure
- **WHEN** the underlying operation raises an error
- **THEN** the wrapper SHALL preserve the exception and SHALL NOT perform additional metadata mutation

#### Scenario: Existing concatenation validation
- **WHEN** the existing two-input `pl.concat` regression is exercised
- **THEN** missing-coordinate validation SHALL continue to apply
- **AND** the provider SHALL NOT patch the module-level function or infer merged metadata

### Requirement: Bounded patch installation

The provider SHALL wrap exactly the 57 class/method pairs on Polars 1, or 56 on
Polars 2, listed by the frame propagation, grouped propagation, and invalidation
requirements and SHALL NOT
discover additional methods from annotations. Installation SHALL preflight all
required methods and namespace ownership, preserve originals, serialize
initialization, and avoid duplicate wrapping.

#### Scenario: Removed Polars 2 method
- **WHEN** the provider initializes on Polars 2.0 or 2.1
- **THEN** the manifest SHALL omit only the removed `LazyFrame.with_context` entry
- **AND** every other method SHALL remain mandatory during preflight
- **AND** the provider SHALL NOT introduce a replacement `with_context` method

#### Scenario: Inspect patch scope and signatures
- **WHEN** initialization completes
- **THEN** only the listed existing class methods SHALL have been replaced
- **AND** wrappers SHALL expose original introspection signatures and `__wrapped__` references
- **AND** Series, unlisted GroupBy methods, rolling/dynamic grouping entry points, other unlisted methods, and module functions SHALL remain unpatched by the provider

#### Scenario: Repeated initialization
- **WHEN** initialization is requested again, including concurrently
- **THEN** already-owned namespaces and wrappers SHALL be reused without stacking wrappers

#### Scenario: Installation cannot complete
- **WHEN** required methods or namespace slots fail preflight
- **THEN** initialization SHALL fail with an actionable error before changing classes
- **WHEN** failure occurs during installation
- **THEN** the installer SHALL restore its still-owned mutations without overwriting intervening foreign edits

### Requirement: Preserve native streaming and adapters

The provider SHALL preserve existing source-specific adapters and leave
`collect_batches()` and its native Arrow stream interface unpatched. Existing
writers and range operations SHALL obtain source metadata before passing streams
to Rust.

#### Scenario: Stream into native operations
- **WHEN** an operation accesses `collect_batches(lazy=True)` for public Arrow C stream export, or its existing `_inner` export on Polars 1
- **THEN** the object SHALL remain native and Arrow-stream exportable
- **AND** metadata support SHALL NOT replace it with a Python generator or eagerly materialize rows

#### Scenario: Source-specific transformations
- **WHEN** Cooler or GFF/GTF frames use their existing count, projection, or deferred-predicate paths
- **THEN** those behaviors and required complete source metadata SHALL remain correct without a new metadata subclass hierarchy

### Requirement: Detect incompatible metadata providers

The library SHALL detect a foreign `config_meta` namespace before installing its
provider and later replacement before metadata use through its helpers, cached
accessors, or supported tagged frame/group operations. This boundary detection
SHALL NOT require import hooks or an upstream compatibility package.

#### Scenario: Original plugin initializes first
- **WHEN** the original plugin has initialized before polars-bio
- **THEN** polars-bio SHALL raise an actionable conflict error before replacing the namespace or trusting its registry

#### Scenario: Original plugin initializes later
- **WHEN** another plugin replaces the namespace after initialization
- **THEN** the next owned metadata use SHALL raise an actionable error before consuming ambiguous state, including for old frames with cached accessors and previously created grouped queries

#### Scenario: Distribution installed but unused
- **WHEN** the original distribution is installed without initializing a conflicting provider
- **THEN** polars-bio SHALL operate normally without importing it
