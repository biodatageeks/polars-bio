# Metadata dependency audit

Date: 2026-11-07. Repository revision: `17fabb9e9807481d49d892f42d1193bd67e75467`.
This report separates reproduced defects, integration hazards, and documented or
intentional limitations. It does not claim that all arbitrary Polars operations
can preserve biologically meaningful metadata.

## Evidence and reproducibility

- Upstream master inspected: [`5739e3818d8e8625883419c0f5091a6a8b526f86`](https://github.com/lmmx/polars-config-meta/tree/5739e3818d8e8625883419c0f5091a6a8b526f86).
  The installed 0.3.4 implementation matches the inspected upstream `__init__.py`
  byte-for-byte. Its SHA-256 is in each observation report.
- Fix inspected: [PR #67](https://github.com/lmmx/polars-config-meta/pull/67),
  head `b72b0c7ced8bce826b70bc78d4a1bc20d8ec282c`, still open when inspected.
- Python 3.12.2, macOS arm64. Dependency probes ran on Polars **1.40.0** (the
  repository minimum), **1.40.1** (the working environment), and **1.44.2** (latest
  stable below the repository's `<2` ceiling at audit time).
- Ten isolated probes per configuration; three released configurations plus
  PR #67 on Polars 1.40.1: **40 subprocess probes completed**.
- The upstream test suite passes unchanged in the working environment:
  **23 passed**, with PyArrow 24.0.0. Passing these tests does not cover the defects
  below. The full polars-bio suite was not run because runtime code is unchanged.
- The proposal passes `openspec validate internalize-frame-metadata --strict`;
  the reproduction harness passes Ruff lint and format checks. Evidence JSON and
  relative document links were checked. No runtime implementation was changed.
- Artifacts: [harness](audit/reproduce.py), [working baseline](audit/baseline.json),
  [minimum Polars](audit/polars-minimum.json),
  [latest supported Polars](audit/polars-latest-supported.json), and
  [PR #67 observations](audit/pr67.json).

Run baseline probes from the repository root:

```sh
.venv/bin/python openspec/changes/internalize-frame-metadata/audit/reproduce.py
```

To inspect PR #67, check out the pinned revision separately and set `PYTHONPATH`
to its `src` directory before running the same command. The distribution version
can still say 0.3.4; the report hashes the actual imported source. The harness does
not fetch code or mutate the application. Each probe runs in a fresh interpreter
to contain changes to Polars classes.

## Actual application dependency surface

An AST scan of executable Python found **7 calls to `set()` and 21 references to
`get_metadata()`** in `polars_bio/`, and 54/11 respectively in `tests/`. Comments
and docstrings are excluded. There are no application calls to the upstream
configuration class, merge/update/clear operations, diagnostics, generic
namespace forwarding, or standalone Parquet helpers. No Series metadata usage
was found in application code, tests, or documentation notebooks.

The actual state consists of:

| Key | Representation | Why needed |
| --- | --- | --- |
| `coordinate_system_zero_based` | Boolean | Coordinate validation and interval semantics |
| `source_format` | String | Source identification and writer behavior |
| `source_path` | String | Provenance |
| `source_header` | JSON string | VCF headers/sample names/layout, BAM/SAM tag types, other format headers |

`polars_bio/_metadata.py:81` already owns domain behavior and stores headers as
JSON strings (`:575`). `_tabular_io.py:123` copies complete metadata after
collection. `io.py:5516` and `:5539` copy it for the Cooler LazyFrame subclass.
The public `pb.get_metadata()` decodes the header and returns a normalized result;
it must remain compatible.

One exception to the small executable surface is the unarchived coordinate
specification's explicit promise of `config_meta.write_parquet()`. Retain a small
compatibility writer unless that promise is deliberately removed in a separate
change. Absence of repository call sites alone does not establish absence of users.

## Findings

### F1 — High: stale entries can change a fresh frame's coordinate system

Upstream `__init__.py:145–168` registers objects by ID and scans a global weakref
dictionary during cleanup. A concurrent registration or nested cleanup can
invalidate that iteration. The upstream [issue #66](https://github.com/lmmx/polars-config-meta/issues/66)
contains the concurrent reproduction. This audit reproduced the consequence
deterministically by planting a dead weakref and metadata under a fresh frame's
ID: accessing the namespace returns the dead frame's coordinate flag and path.
It occurs on all three tested Polars releases. The audit did not rerun the
probabilistic threaded cleanup stress test.

Port consequence: remove the ID registry entirely if possible. If retained,
registration, reads, copies, and cleanup all need identity checks, plus concurrency
tests. Copying the existing registry unchanged is unacceptable.

### F2 — High under the stale-entry precondition: PR #67 misses the copy path

`_copy_metadata_to_result` (`__init__.py:64–78`) reads the source dictionary using
only `id(source)`. With a planted stale source entry, `fresh.clone()` copies that
metadata before anyone accesses `fresh.config_meta`. On the PR's exact head:

- Direct namespace access resets the fresh source to `{}`, as intended.
- The clone already contains the dead frame's coordinate system and source path.

This is a remaining defensive hole, not evidence that the fixed cleanup mechanism
creates new stale entries. The precondition was deliberately injected. Any design
that retains the registry should validate the source weakref before copying; the
proposed instance-owned dictionary eliminates this state altogether.

### F3 — High migration risk: naive vendoring creates two incompatible stores

Loading a second copy under another module name produces namespace override
warnings. Existing frames retain cached accessors from the first implementation,
while new accessors use the second store. First-copy method wrappers still write
to the first store. In the probe, both an old frame and a new frame lose visible
metadata after `clone()`. This occurs across the tested Polars versions.

This is an integration hazard when vendoring, not a defect in a single isolated
installation. A port needs an explicit coexistence policy and tests of both import
orders; simply changing the import to a copied package is insufficient.

### F4 — High domain risk: multi-input operations inherit only the receiver

`join` and `vstack` with a zero-based left input and one-based right input retain
the left coordinate flag and source header without conflict handling. This is
what the generic source-to-result copier does; it has no biological merge rules.
Propagation of a value does not prove that value describes the combined data.

For the minimal port, generic multi-input operations should invalidate metadata,
requiring deliberate retagging. Preserve polars-bio range operations' existing
domain-specific validation and output tagging. Do not invent a provenance merge
policy in this port.

### F5 — Medium: propagation is incomplete, and some gaps are intentional

The following observations agree across the three tested Polars releases:

| Operation | Current metadata behavior |
| --- | --- |
| `filter`, `select`, `with_columns`, `head`, `clone`, `lazy().collect()` | Preserved |
| `iter_slices()` | Preserved indirectly through internal slicing |
| `df[[...]]` returning a DataFrame | Lost |
| `group_by().agg()` | Lost |
| `partition_by()` children | Lost |
| `collect_batches()` children | Lost |
| `pl.concat([left, right])` | Lost |

Discovery skips dunder methods and only copies direct frame/Series return values,
not returned containers, iterators, or GroupBy objects. The README acknowledges
module-function limitations. Furthermore,
`tests/test_coordinate_system_resolution.py:208` explicitly requires concatenation
to lose its tag and receive an actionable missing-coordinate error downstream.
Do not automatically turn every gap into new port scope.

### F6 — Medium: broad patching damages public method introspection

The first namespace access patches **327 methods** on Polars 1.40.x and **332** on
1.44.2, including Series methods. `discovery.py:123–148` wraps them without
`functools.wraps`. `inspect.signature(pl.DataFrame.filter)` becomes
`(self, *args, **kwargs)` and there is no `__wrapped__` attribute. Documentation,
annotations, and method identity are also replaced. This affects the entire
process, including frames unrelated to polars-bio.

Port consequence: use an explicit method list, preserve wrapped signatures and
documentation, make installation idempotent, and leave untagged frames alone.

### F7 — Lower priority: writable metadata and nested aliases

`get_metadata()` returns the live internal dictionary. Derived frames get a
shallow copy, so mutating a nested list/dictionary through one frame affects its
parent. Both behaviors were reproduced. These are API/design limitations rather
than a proven violation of upstream's documented contract.

polars-bio's four built-in values are immutable scalars/JSON strings, so a deep-copy
framework is unnecessary. Use independent top-level dictionaries and return shallow
snapshots. Document that arbitrary mutable user values are not deeply isolated.

### F8 — Medium documentation/test defects in polars-bio

- `docs/features/reading.md:1028` and `docs/developers.md:387–388` assign the result
  of `config_meta.set()` back to a frame. The method returns `None`; those examples
  destroy their frame references. Correct the examples, keep the setter contract.
- `tests/test_source_metadata.py:49` collects a tagged LazyFrame but has **no
  assertion**, and its comments describe obsolete metadata-loss behavior.
- Several eager-reader comments still say collection loses metadata, although
  supported upstream versions preserve it. They must not become evidence for
  dropping full-header propagation; writers rely on the headers.
- Upstream's merge test sets `df1` twice where its comment says `df2`, so it does
  not test the stated conflict case. Diagnostic tests partly print results rather
  than assert failures. The passing upstream suite is only a starting point.

## Smallest defensible port

Keep instance-owned frame metadata, the two used namespace operations, bounded
single-input propagation, explicit internal copy/clear helpers, and a legacy
Parquet writer. Preserve Python-level DataFrame/LazyFrame types and native batch
streams. Exclude Series metadata, dynamic discovery, generic method forwarding,
runtime enable/disable switches, arbitrary merging, and standalone reader APIs.

A private dictionary can be attached to both DataFrame and LazyFrame on all three
tested Polars versions without changing the underlying native object. A weakref
confirms that this dictionary alone does not keep its owner alive. This is a
feasibility check, not a portable guarantee from Polars or a completed replacement:
the implementation must test Python 3.11–3.14 and retain checks for future Polars
updates. See [design](design.md) and [tasks](tasks.md).

## Follow-up: native Polars metadata support

An additional inspection and isolated runtime checks of Polars 2.1.0 established:

- There is still no general frame-level `attrs` facility with automatic
  propagation. The upstream [feature request #5117](https://github.com/pola-rs/polars/issues/5117)
  remains open. Ordinary Arrow table metadata was absent after `from_arrow` and
  custom file metadata was lost through `scan_parquet → filter → sink_parquet`
  without explicitly supplying it to the sink.
- Native [Parquet writing](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html)
  and [sinks](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html)
  accept file metadata via `metadata=` and schema/field metadata via
  `arrow_schema=`. These APIs are also present in the working Polars 1.40.1.
- In 2.1.0, using only `metadata=` made the custom value visible to
  `pl.read_parquet_metadata`, but not `pyarrow.parquet.read_schema(...).metadata`.
  Using only `arrow_schema=` did the reverse. A subsequent check on 1.40.1 using
  both preserved the same JSON under `polars_plugin_meta` in both views. The
  implementation should verify this combination across the supported matrix.
- Native [extension types](https://docs.pola.rs/api/python/stable/reference/api/polars.datatypes.Extension.html)
  carry column-type metadata. A generic integer-backed coordinate extension
  preserved its metadata through head, projection, row filtering, and lazy
  collection in 2.1.0. However, comparing that column to integer zero and adding
  integer one both raised `SchemaError`. This does not preclude custom integration;
  it means extension types are not a transparent replacement for current integer
  coordinate columns. They also do not supply frame-level source-header storage.

Decision: use native Parquet metadata facilities within the small compatibility
writer where options can be mapped faithfully, preserving both metadata locations
and using PyArrow only for incompatible legacy options. Keep owned in-memory
frame propagation. No Polars 2 upgrade or coordinate-type redesign is part of
this port. These follow-up checks are separate from the 40 archived probes above.

## Follow-up: ordinary LazyFrame syntax without global method replacement

The requested user experience excludes a custom transform callback. A second
feasibility probe uses owned `pl.LazyFrame`/`pl.DataFrame` subclasses with a few
explicit overrides. [Probe source](audit/subclass_probe.py) and observations for
[1.40.0](audit/subclass-minimum.json), [1.40.1](audit/subclass-current.json), and
[1.44.2](audit/subclass-latest-supported.json) are preserved separately.

On all three versions:

- Ordinary `filter().with_columns().select().collect(engine="streaming")` chains
  preserved both the coordinate flag and full source-header string.
- Eager selection/filtering also preserved metadata. Results passed `isinstance`
  checks for their Polars base classes; exact type checks were false.
- Adoption reused the existing native lazy plan without collecting or copying
  its data. Base Polars method objects stayed unchanged and signatures were
  preserved. The native `CollectBatches._inner` stream interface remained usable.
- `pl.concat`, `pl.collect_all`, and `group_by().agg()` returned ordinary frames
  without metadata. A deliberately unoverridden `sort` returned a subclass
  instance without metadata, demonstrating that inheritance alone is insufficient.

Final decision: the user selected bounded patching to retain ordinary native frame
types and propagation for externally created frames without adoption. The subclass
spike remains evidence for the alternative; it is not part of the implementation.
It does not establish universal subclass interoperability, existing GFF/GTF/Cooler
adapter compatibility, or full polars-bio correctness. The selected manifest and
release gates are in [the design](design.md) and [acceptance plan](validation.md).
There is no supported way to promise exact native frame types, arbitrary unchanged
method chains, and automatic Python metadata propagation without intercepting
operations or upstream support.

## Follow-up: ordinary grouped aggregation in the bounded port

The user expanded the selected migration to include ordinary GroupBy chains.
The earlier exclusion was a scope choice, not a technical impossibility. Keep
the original plugin/subclass observations above as historical evidence.

A new [isolated probe](audit/groupby_probe.py) installs ten explicit grouping
wrappers plus a collect bridge. It does not import polars-bio or the old plugin,
and does not implement production namespaces, installation, or conflict handling.
Reports are archived for [1.40.0](audit/groupby-minimum.json),
[1.40.1](audit/groupby-current.json), and
[1.44.2](audit/groupby-latest-supported.json), all on Python 3.12.2.

Across these three versions, **60 operation/tagging cases passed**: five grouped
paths, eager/lazy frames, and tagged/untagged inputs. The probe also checked
snapshot isolation, `having` identity, original error messages/types, signatures,
and absence of row collection/schema resolution during lazy group construction.

- `group_by` needs to snapshot source metadata onto the native grouping object.
- GroupBy/LazyGroupBy `agg`, `head`, and `tail` copy the snapshot to native results.
  `len(name=...)` delegates to `agg` and works without a direct wrapper.
- GroupBy/LazyGroupBy `having` needs explicit handling: eager grouping returns a
  new object; lazy grouping returns the receiver on these versions.
- Retagging the source after grouping does not alter the grouped snapshot.
  Results retain independent top-level dictionaries and all four built-in keys.
- Lazy results preserve metadata through the probe's collect bridge, including
  streaming collection. Results match unpatched Polars for the probed operations.

Decision: extend the production plan from 47 to **57 class/method pairs** with
these ten wrappers. Carry provenance and the declared coordinate convention
through aggregation, without asserting that arbitrary aggregates are valid
intervals or rewriting source headers. Custom `map_groups`, rolling/dynamic
grouping, group iteration, and container traversal remain outside the guarantee.

This evidence is limited to the isolated grouping mechanism. Named keys,
empty inputs, namespace conflicts, source adapters, complete application chains,
and interactions with the other wrappers remain implementation acceptance gates
in [validation.md](validation.md). At the time of this probe the production port
was still pending.

## Implementation follow-up

The owned provider and its integration are now implemented. Production tests
cover the acceptance cases above; see [verification.md](verification.md) for the
version matrix, application checks, package checks, and measured overhead.

Two details needed explicit regression coverage beyond the initial probes:
an empty grouped snapshot must prevent later source tagging from leaking into
aggregation, and a namespace accessor must retain a temporary source frame until
its method finishes. The implementation handles both, with garbage-collection
tests for cached accessors.

Native Polars Parquet writing can retain file-level key/value metadata while
dropping the encoded Arrow-schema metadata for nested and categorical schemas.
The compatibility writer therefore uses native writing only for the verified
scalar schema/option subset and retains the PyArrow fallback for other cases.
This is tested independently through PyArrow reads at both supported Polars
endpoints. Lazy fallback writes may materialize the frame, as documented.
