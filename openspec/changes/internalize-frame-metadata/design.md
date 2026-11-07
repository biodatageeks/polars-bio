# Design: minimal metadata with bounded patching

## Goals and boundaries

Keep ordinary Polars DataFrame/LazyFrame objects and normal method chains,
including frames created outside polars-bio. Preserve coordinate semantics and
source headers without maintaining the full upstream plugin. The selected approach
patches an explicit set of base-class methods; it accepts their process-wide
effects in exchange for preserving native types and avoiding an adoption step.

No new metadata subclass, composition wrapper, custom transform callback,
return-annotation discovery, or Arrow coordinate-type redesign is included.
Ordinary grouping participates through instance-owned metadata snapshots on the
existing GroupBy/LazyGroupBy objects. Existing source-specific adapters remain
intact.

## 1. Instance storage

Use a private attribute such as `_polars_bio_metadata` on the Python frame.
Leave native `_df`/`_ldf` handles and physical data unchanged. Provide private
get/set/copy/clear helpers; route internal consumers through these helpers.

Copy the top-level dictionary when transferring metadata. Built-in values are a
boolean and strings: coordinate flag, source format, source path, and the full JSON
source header. Preserve all header content, including VCF samples/layout and
alignment tag-type declarations. Extra user values use shallow-copy semantics;
nested mutable payloads are not independently cloned.

`get_metadata()` returns a shallow snapshot; `set(**values)` updates entries and
returns `None`. An untagged fast path reads the private attribute without creating
a namespace accessor, metadata dictionary, or registry entry. An identity result
keeps its object identity and valid state.

There is no frame-ID map, weakref cleanup scan, or process-global store of frame
metadata. A bounded map of original method functions is installation state, not a
frame registry. Accessors retain their owner so expressions such as
`temporary_frame.config_meta.get_metadata()` remain valid after attribute lookup.
Polars caches the accessor on its frame, and ordinary cyclic GC collects that
cycle; lifetime tests verify collection. A weak accessor owner was tested and
rejected because it breaks temporary-frame calls. Accessors look up the current
dictionary on each use so invalidation reaches cached accessors. Concurrent
independent frames must be safe; concurrent mutation of the same frame is not a
new supported operation.

## 2. Small compatibility namespace and Parquet writer

Register `config_meta` only on DataFrame and LazyFrame:

| API | Contract |
| --- | --- |
| `.set(**values)` | Update entries; return `None` |
| `.get_metadata()` | Return a shallow dictionary snapshot |
| `.write_parquet(destination, **kwargs)` | Preserve the legacy compatibility writer |

Retain JSON under `polars_plugin_meta` in both Arrow schema metadata and Parquet
file metadata. Prefer native `write_parquet` / `sink_parquet` for the common path.
Supply both `metadata=` and `arrow_schema=`: using only one loses compatibility
with one of the existing metadata read paths. For a LazyFrame, obtain the schema
with `collect_schema().to_arrow()`; do not collect rows solely to attach metadata.

Preserve relevant PyArrow writer defaults, including Snappy compression. Map only
an explicitly tested small set of options with equivalent native semantics.
Route unmappable or PyArrow-specific options to a bounded PyArrow fallback, which
may materialize a LazyFrame. Also route nested, categorical/dictionary, and other
unverified Arrow schemas through that fallback: native Polars can rebuild such
schemas and silently drop Arrow-schema metadata despite `arrow_schema=`. Native
writing is restricted to verified scalar schemas (primitive, string/binary,
decimal, and null types). Reuse an eager Arrow table when selecting the fallback.
Preserve unrelated schema metadata where available.
Test paths and seekable buffers, both metadata read paths, and option/error
compatibility. Do not build a comprehensive translator or add a public backend
switch. Existing `pb.read_parquet`/`scan_parquet` retain their explicit coordinate
declaration behavior.

Native metadata APIs are present in the working 1.40.1; their unstable status
makes minimum/latest supported-version checks necessary. No Polars 2 upgrade is
required. See the native-support follow-up in [the audit](audit.md).

Exclude Series registration, generic namespace forwarding, `merge`, `update`,
`clear_metadata`, diagnostics, and `ConfigMetaOpts`. Do not install a
`polars_config_meta` compatibility package or alias in `sys.modules`.
Standalone upstream Parquet-reader imports are excluded. Document these boundaries
and snapshot semantics in release notes.

## 3. Exact propagation manifest

Wrap the following existing methods with `functools.wraps`, preserving
introspection signatures, documentation, `__wrapped__`, arguments, return kinds,
identity behavior, and original errors. Never select methods from annotations.
Call the original and copy metadata only when its result is a DataFrame/LazyFrame;
do not reconstruct the result or change its class.

| Class | Metadata-preserving methods | Count |
| --- | --- | --- |
| DataFrame and LazyFrame | `filter`, `select`, `with_columns`, `drop`, `rename`, `sort`, `slice`, `head`, `tail`, `limit`, `clone`, `cast`, `unique`, `reverse`, `explode` | 30 |
| DataFrame | `lazy`, frame-returning `__getitem__` | 2 |
| LazyFrame | `collect`, including `engine="streaming"` | 1 |

There are **33 preserving class/method pairs**. DataFrame indexing that returns a
Series or scalar is untouched by metadata copying. Untagged calls delegate
normally without allocating metadata. Propagation must not resolve a schema,
serialize headers again, or collect data solely for copying.

Recommend polars-bio's readers for genomic interval CSV and Parquet inputs:

```python
import polars as pl
import polars_bio as pb

lf = pb.scan_parquet("intervals.parquet", zero_based=True)
# CSV equivalent: pb.scan_csv("intervals.csv", zero_based=True)
result = lf.filter(pl.col("start") > 100).select("chrom", "start", "end").collect()
assert type(result) is pl.DataFrame
assert result.config_meta.get_metadata()["coordinate_system_zero_based"] is True
```

`zero_based` declares the source convention; `use_zero_based` optionally converts
to the requested output convention. Existing readers validate coordinate column
types, support column-name mapping and point intervals, and record the resulting
coordinate flag plus source format/path. They return ordinary LazyFrames for
normal Polars chaining. Recommend `pb.read_csv()` / `pb.read_parquet()` for eager
loading on the same terms. Do not infer a coordinate convention from a CSV or
Parquet filename or silently prioritize embedded Parquet metadata.

Keep direct Polars readers and manually tagged frames as an interoperability path.
For example, `pl.scan_parquet(...)` followed by
`lf.config_meta.set(coordinate_system_zero_based=True)` remains supported; the
caller is responsible for the convention and interval schema. Manual tagging
alone does not perform the validation, normalization, or conversion supplied by
the polars-bio readers. Use ordinary Polars readers for unrelated, non-interval
tables. No adoption call is needed. Existing specialized polars-bio adapters also
keep their current types.

No direct wrappers are installed on Series, `map_groups`, rolling/dynamic grouping
entry points, `collect_batches`,
`collect_async`, `pl.collect_all`, `pl.concat`, or arbitrary containers and
generators. Unlisted methods may incidentally preserve metadata through listed
methods; that is not a guarantee. Do not recursively wrap returned containers.

A copied source header is original provenance, not a recomputed schema description.
Expressions that modify coordinate meaning cannot be analyzed by this provider;
callers must explicitly set the correct convention after coordinate conversion.

### Ordinary GroupBy propagation

Add exactly ten grouping wrappers to the 33 frame-preserving wrappers:

| Class | Method | Purpose | Count |
| --- | --- | --- | --- |
| DataFrame and LazyFrame | `group_by` | Snapshot source metadata onto the native grouping object | 2 |
| GroupBy and LazyGroupBy | `agg`, `head`, `tail` | Copy the group snapshot onto the native frame result | 6 |
| GroupBy and LazyGroupBy | `having` | Preserve the snapshot through grouped filtering | 2 |

Support ordinary `.group_by(...).agg(...)`, `.len(name=...)`, `.head(n)`,
`.tail(n)`, and `.having(...).agg(...)` on eager and lazy frames. `len` delegates
to `agg` on the audited versions and needs no direct wrapper; test that public
behavior at every supported version. Other convenience methods may also delegate
to `agg`, but are not separately guaranteed without coverage. Keep grouping keys,
named keys/aggregations, `maintain_order`, original errors, signatures, and result
types. Do not collect rows or resolve schemas to carry metadata.

```python
span = lf.group_by("chrom").agg(
    pl.col("start").min(),
    pl.col("end").max(),
)
# Ordinary LazyFrame; retains the source coordinate convention and provenance.
```

Use a private group attribute such as `_polars_bio_group_metadata`. Snapshot at
`group_by()` time, so retagging the source afterward does not silently retag an
existing grouped query. Copy the top-level mapping onto each new group/frame
result. Preserve identity when `having` returns its receiver: on the audited
versions eager `having` returns a new GroupBy, whereas lazy `having` returns the
same LazyGroupBy. Support both without changing native behavior. No group registry,
extra source-frame reference, public GroupBy namespace, or wrapper class is needed.

An untagged source uses an explicit empty-snapshot marker on its ordinary group.
This prevents eager aggregation from inheriting a tag added to the source after
group creation through Polars' internal lazy calls. No metadata keys are fabricated.
Only grouping objects carrying the private snapshot participate. Shared GroupBy
methods delegate normally for unmarked objects, including objects created through
excluded grouping entry points. Conflict guards cover marked grouping objects
as well as frames; do not let delayed `.agg()` bypass namespace ownership checks.
Explicit wrappers around eager endpoints ensure the original snapshot wins over
any intermediate metadata copied by internal lazy calls.

Aggregation carries source provenance and the declared coordinate convention;
it does not certify that arbitrary expressions produce valid intervals. For
example, `.len()` produces a summary with no start/end columns. Existing interval
column checks still apply if that summary is passed into a genomic operation.
Coordinate arithmetic requires correct retagging just as in `with_columns`;
there is no expression analyzer or automatic source-header rewrite.

Custom `map_groups` callbacks, rolling/dynamic grouping, iteration over groups,
and returned container traversal remain outside the propagation guarantee.
The [grouping probe](audit/groupby_probe.py) tests the ten wrappers and a collect
bridge in isolation; it is evidence of feasibility, not the production provider.

## 4. Generic multi-input invalidation

Wrap this separate, explicit list and clear metadata on successful results:

| Class | Invalidating methods | Count |
| --- | --- | --- |
| DataFrame and LazyFrame | `join`, `join_asof`, `join_where`, `update`, `merge_sorted` | 10 |
| DataFrame | `vstack`, `hstack`, `extend` | 3 |
| LazyFrame (Polars 1 only) | `with_context` | 1 |

These **14 class/method pairs** bring the manifest to **57**: 33 frame-preserving,
10 grouping, and 14 invalidating. The original frame manifest was checked for
callable availability on local Polars 1.40.1; the grouping extension has its own
versioned feasibility evidence. The production implementation must test the
complete manifest at the supported-version endpoints.

Forward compatibility with PR #478 additionally tests Polars 2.0 and 2.1. Those
versions removed `LazyFrame.with_context`, so the manifest explicitly omits that
single entry by major version: 56 entries, including 13 invalidating operations.
Every remaining entry is still required. Do not replace this rule with generic
method-availability filtering, which could silently reduce supported behavior.

Invalidate even if tags match, one input is untagged, or internal eager/lazy calls
temporarily copied metadata. Clearing includes source headers as well as coordinate
flags. Successful in-place variants invalidate the mutated receiver; preserve
non-mutated inputs. On failure, propagate the original exception and do not add
metadata mutations of our own. Do not promise rollback of mutations made by Polars.

Do not infer generic header merges or coordinate agreement from these APIs.
Users may normalize inputs and retag the result. Preserve explicit validation and
output tagging in `pb.overlap`, `nearest`, `coverage`, and other domain operations.

Leave `pl.concat` unpatched. Retain the existing two-input concat regression
requiring missing-coordinate validation; if a supported version breaks it, resolve
that before release without quietly extending the manifest. Do not claim every
external reconstruction or single-input concat discards metadata.

## 5. Installation, ownership, and conflicts

Initialize from `polars_bio/__init__.py` before modules that attach metadata.
Use Polars' registration API for the compatibility namespace.

1. Preflight all 57 required method attributes (56 on Polars 2) and both namespace slots. Capture
   originals and ownership markers. Missing methods or foreign namespaces produce
   actionable errors before installation changes any classes; do not silently skip.
2. Serialize initialization and make repeated initialization idempotent. A repeat
   checks ownership and returns without wrapping wrappers or replacing accessors.
   This is not a public runtime configuration/unpatching framework.
3. Publish wrappers and namespace descriptors as one controlled initialization.
   If registration fails, restore only mutations still owned by this installer,
   including registration bookkeeping. Do not overwrite intervening foreign edits.
4. Preserve original decorated Polars implementations; the fact a method is
   already a decorator wrapper does not itself indicate a foreign metadata provider.
   Keep bounded installation state without retaining frame instances.

Compare ownership on subsequent initialization and metadata access. The original
distribution may remain installed but must not initialize alongside this provider.
If its namespace exists first, fail before overriding it and tell users to remove
the direct plugin import and restart the interpreter.

If it initializes later and replaces the namespace, reject the next owned
metadata read/write or supported tagged operation before trusting ambiguous
metadata, including operations on already-created grouping objects. Check
descriptor ownership without instantiating accessors. Include cached accessors
and internal helpers in this guard. Preserve the ordinary
untagged fast path; it need not inspect every global method on every call.

Detection occurs at owned boundaries. It is not an import hook and cannot prevent
an external package from replacing methods or bypassing this provider altogether.
No import interception, silent delegation to the old registry, or promise of
transparent interoperability with arbitrary patching is included.

Selected base methods are globally wrapped; unselected existing methods must retain
their identities. Test the exact set changed, repeat installation, rollback, and
namespace conflicts in fresh subprocesses.

## 6. Application and streaming integration

Replace the upstream import/initialization and route direct metadata accesses in
`_metadata.py`, `_tabular_io.py`, and `io.py` through owned helpers. Preserve
Pandas attrs and DataFusion schema handling. There is no reader-boundary adoption
layer and no changes to `from_polars(name, df)`.

Preserve Cooler count projection and GFF/GTF attribute projection/deferred
predicates. Keep their existing class hierarchy and explicit metadata copies until
tests prove a copy redundant. Verify full source headers, not just coordinates.

The current integration consumes `collect_batches(lazy=True)._inner`; PR #478
uses the batch object's public `__arrow_c_stream__` interface on all versions.
Leave the collection method and native stream object unpatched. Read
metadata before passing the stream to Rust; individual batches need no namespace
wrapper. Test repeated collection and existing streaming writer paths.

## 7. Verification, packaging, and rollout

Follow [the acceptance matrix](validation.md) and [ordered tasks](tasks.md).
Test the manifest and namespace in isolation, then application workflows on the
minimum/latest supported Polars versions and applicable Python 3.11–3.14 jobs.
Measure tagged and untagged overhead against unpatched Polars and the current
plugin; record methodology and avoid conflating I/O with wrapper cost.

Remove the dependency only after behavior checks pass. Update `pyproject.toml`,
maintained `uv.lock`, and `bioconda-recipe/meta.yaml`; build wheel/sdist, inspect
dependencies/licenses, and smoke-test without the old distribution. Include the
upstream MIT notice for adapted source or tests. Correct broken setter examples
and the collection test with no assertion.

Use a single reviewed migration release. If validation fails, keep the current
dependency path until fixed; do not ship a partial installer or run two providers
as a fallback. Reverting the migration requires reverting dependency/integration
changes together and restarting affected interpreters. Hot unpatching is excluded.

## Specification interactions

Only `packaging` is canonical under `openspec/specs/` today. Coordinate behavior
still lives in the unarchived `switch-default-to-zero-based-coordinates` and
`add-coordinate-safe-tabular-readers` changes. Add `frame-metadata` rather than
MODIFY a missing canonical requirement. Reconcile dependency-specific wording
during integration without changing newer coordinate-resolution rules.

The pending Arrow-extension-types proposal retains table metadata as a fallback.
This port neither implements nor replaces it.

## Alternatives and remaining risks

| Alternative | Reason for not selecting it |
| --- | --- |
| Vendor the full plugin | Retains unnecessary API, registry problems, and broad discovery |
| Keep the dependency with PR #67 | Retains release coupling, broad patching, and the audited defensive gap |
| Attach a dictionary only | Ordinary transformations lose required metadata |
| Explicit custom transform helper | Does not meet the requested ordinary method-chain API |
| Owned subclasses | Avoids global patches, but changes exact types and requires adoption for external frames; existing adapters add integration work |
| Composition wrapper / Arrow extension types | Requires a broader interoperability or coordinate-type redesign |

The subclass experiment remains in [the audit](audit.md) as evidence for the
alternative, not an implementation dependency.

Per-instance Python state and wrapped Polars behavior still require version
compatibility checks. Process-wide overhead and conflicts remain real costs.
Pickle, multiprocessing transfer, Arrow/IPC roundtrips, arbitrary third-party
patchers, unlisted methods, and expression-aware provenance are outside the
propagation contract. Same-frame concurrent mutation gains no new guarantee.
