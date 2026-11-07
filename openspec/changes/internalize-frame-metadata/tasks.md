## 1. Audit and boundary definition

- [x] 1.1 Inspect upstream implementation, tests, license, and open registry fix.
- [x] 1.2 Inventory executable application/test usage and documented commitments.
- [x] 1.3 Reproduce problems in isolated subprocesses on minimum/current/latest supported Polars; preserve reports.
- [x] 1.4 Check PR #67 source-copy behavior separately from its cleanup fix.
- [x] 1.5 Identify native batch-stream and Cooler/GFF/GTF adapter integration constraints.
- [x] 1.6 Write proposal, design, compatibility exclusions, and delta specifications.
- [x] 1.7 Validate OpenSpec strictly, check audit-script lint/format, and verify evidence files and document links.
- [x] 1.8 Inspect native Polars metadata support and distinguish file metadata, Arrow-schema metadata, extension types, and frame propagation.
- [x] 1.9 Probe subclasses as an alternative; record normal-chain, exact-type, native-stream, and external-function boundaries.
- [x] 1.10 Select bounded patching with ordinary native frames; define the initial frame manifest and acceptance gates.
- [x] 1.11 Extend the plan with ten ordinary-grouping wrappers (57 total); archive minimum/current/latest-supported grouping probes and validate the updated specifications.

## 2. Implement the minimal provider

- [x] 2.1 Add `polars_bio/_frame_metadata.py` with per-instance state, private get/set/copy/clear helpers, snapshot reads, full source-header preservation, and no frame registry.
- [x] 2.2 Register DataFrame/LazyFrame `config_meta` with `set()` returning `None` and `get_metadata()`; omit the wider upstream API.
- [x] 2.3 Install exactly the 33 frame-preserving, 10 grouping, and 14 invalidating wrappers from `design.md` (13 invalidating on Polars 2, which removed `with_context`), using `functools.wraps`, unchanged result types, untagged fast paths, per-group snapshots, and successful in-place invalidation. Preserve `having` identity and verify `len` delegation without directly wrapping it.
- [x] 2.4 Implement preflight, serialized/idempotent initialization, ownership markers, and rollback of owned mutations on installation failure; test namespace conflicts before and after installation, including cached accessors.
- [x] 2.5 Implement the legacy Parquet writer with both native metadata locations, a small verified option map, retained defaults, and a bounded PyArrow fallback.
- [x] 2.6 Record upstream origin and retain the MIT notice for any adapted implementation/test material.

## 3. Integrate existing application behavior

- [x] 3.1 Replace the external initialization import in `polars_bio/__init__.py`; route metadata consumers in `_metadata.py`, `_tabular_io.py`, and `io.py` through canonical helpers. Retain existing reader types and domain-specific output tagging.
- [x] 3.2 Preserve Cooler count projection and GFF/GTF projection/deferred predicates; retain explicit metadata copies until demonstrated redundant, plus existing Pandas/DataFusion handling.
- [x] 3.3 Preserve native `collect_batches(lazy=True)._inner` use in the current integration and the public Arrow stream interface used by PR #478, without iterator proxies or collection solely for metadata.
- [x] 3.4 Fix setter examples and obsolete collection comments; replace the no-assertion source-metadata collection test with a complete-header regression.

## 4. Verify the acceptance plan

Follow [validation.md](validation.md); new provider behavior must be tested independently of the legacy plugin.

- [x] 4.1 Test every manifest entry on native tagged/untagged frames and grouping objects, including externally created frames, indexing return kinds, metadata isolation, signatures, original errors, exact types, and lazy execution. Cover grouped `agg`/`len`/`head`/`tail`/`having`, named keys/aggregations, ordering, snapshot isolation after source retagging, and summary outputs without interval columns.
- [x] 4.2 Test repeat/concurrent initialization, installation rollback, exact changed-method scope, frame collection, no cross-frame inheritance, and concurrent creation/collection of independent frames.
- [x] 4.3 Test invalidation with matching/conflicting/missing input metadata, eager/lazy internal calls, and successful in-place variants; retain existing two-input concat rejection.
- [x] 4.4 Test both original-plugin import orders, old/new frames, already-created grouped queries, cached accessors, and the installed-but-unused distribution case in subprocesses.
- [x] 4.5 Verify native/fallback Parquet paths, buffers, defaults/options/errors, file/schema metadata, unrelated schema metadata where available, and unchanged coordinate declarations in polars-bio readers.
- [x] 4.6 Run coordinate metadata/resolution/semantics, tabular/source/comprehensive metadata, Cooler/GFF/GTF, VCF header/layout, and typed alignment roundtrip regressions.
- [x] 4.7 Verify native stream identity and export, repeated/concurrent collection, and streaming writers using required test-process isolation.
- [x] 4.8 Run supported-version endpoints and applicable Python jobs; measure tagged/untagged wrapper overhead with a recorded, bounded benchmark.
- [x] 4.9 Address the first Codex/Claude review, test the provider on Polars 2.0/2.1 across Python 3.11–3.14, and run the full application target with PR #478 at both Polars 2 endpoints. Record the version-specific manifest and integration evidence.

## 5. Package and document the migration

- [x] 5.1 Remove the runtime dependency from `pyproject.toml`, maintained `uv.lock`, and `bioconda-recipe/meta.yaml` after integration gates pass; update maintained recipe docs.
- [x] 5.2 Build wheel/sdist, verify dependency metadata and attribution, and smoke-test reader/transform/range-operation/writer workflows without the original distribution installed.
- [x] 5.3 Recommend `pb.scan_csv()` / `pb.scan_parquet()` (and eager equivalents) for interval data with explicit `zero_based=` and optional `use_zero_based=`; document normal chaining, the method manifest, manual external-frame tagging as interoperability, generic invalidation/retagging, snapshot semantics, API exclusions, and conflict remediation.
- [x] 5.4 Reconcile current coordinate-spec/project references without changing newer coordinate rules or rewriting unrelated historical proposals.
- [x] 5.5 Run required project checks and strict OpenSpec validation; record actual results and remaining limits before release.

Implementation and verification tasks are complete. Actual results and remaining
limits are recorded in [verification.md](verification.md). The full `make test`
target passes after the user-requested removal of the unrelated local untracked
PGEN test file. Its initial failures are retained as historical evidence only.
No release was published; the staged Bioconda source version/checksum update
belongs to that release.
