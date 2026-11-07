# Acceptance plan: bounded frame metadata

This is the implementation acceptance plan. Actual results and remaining limits
are recorded in [verification.md](verification.md). Historical audit probes
describe the old plugin, the subclass alternative, and isolated grouping
feasibility; the production provider is verified separately by its contract tests,
application regressions, version matrix, and installed-artifact checks.

## Provider contract

Add focused tests in `tests/test_frame_metadata.py`, using subprocesses for
installation and import-order cases so patches cannot leak between cases. Test
observable behavior, including failures that the upstream suite did not cover.

| Area | Required evidence |
| --- | --- |
| Storage | Independent top-level dictionaries; no metadata on unrelated frames; full source-header string retained; discarded frames collect without registry cleanup |
| Public namespace | `set()` returns `None`; snapshot mutation cannot alter the frame; extra nested values have documented shallow semantics; cached accessors see invalidation |
| Preserving manifest | Each of 33 entries covered on tagged/untagged native frames; correct arguments/return kinds; exact native types where the original returns native types |
| Grouping manifest | Each of 10 additional entries covered on marked/unmarked groups; ordinary `agg`, `len`, `head`, `tail`, and `having` chains; independent snapshots and preserved result/identity behavior |
| External frames | Direct `pl.DataFrame()` and `pl.scan_parquet()` tagging works through normal chains without adoption |
| Laziness | Lazy operations and metadata copies do not collect rows or resolve schema for propagation; `collect(engine="streaming")` retains metadata |
| Invalidating manifest | Each of 14 entries covered with matching/conflicting/missing tags; no left/right metadata leaks; in-place receiver invalidation; unchanged non-mutated inputs and original errors |
| Installation | Exact changed-method set; signatures and `__wrapped__`; repeat/concurrent initialization; preflight and injected mid-install failure; rollback without overwriting foreign edits |
| Conflicts | Original plugin first/later; old/new frames and existing grouped queries; cached accessors; actionable errors; installed-but-unused distribution succeeds |
| Native boundaries | `collect_batches` method identity and public Arrow export intact, plus existing `_inner` export on Polars 1; Series, `map_groups`, rolling/dynamic grouping entry points, module functions, and unlisted methods not patched |
| Parquet | Native/fallback paths, paths/buffers, default compression and representative options/errors; identical JSON in file/schema metadata; unrelated schema metadata where available; nested/categorical schema fallback and temporal/list/struct value roundtrips |
| Lifetime/concurrency | Weakref-based observations in tests confirm frames are collectible; concurrent independent-frame creation/collection has no cross-frame state; no guarantee for concurrent mutation of one frame |

The exact 57-entry manifest (56 on Polars 2, omitting only `with_context`) belongs in one runtime constant and an independent
expected test set derived from the design. Do not derive test expectations from
the constant under test. Give each entry a valid operation and metadata assertion;
checking that methods exist is insufficient. Exercise invalidating methods on
otherwise untagged receivers too, because tagged secondary inputs must not leak
metadata through internal Polars calls.

For snapshot tests, change a top-level key and confirm the source is unchanged.
Do not accidentally promise recursive isolation. For indexing, cover DataFrame,
Series, and scalar results. Compare errors and result kinds against unpatched
Polars in separate subprocesses when necessary.

For ordinary grouping, snapshot metadata at `group_by()` and retag the source
before aggregation to prove the grouped query retains the original snapshot.
Cover separate branches, repeated execution, named grouping keys/aggregations,
empty inputs, `maintain_order`, `len(name=...)`, eager/lazy `having` identity, and
lazy collection with the streaming engine. Check all four built-in metadata keys.
Verify both no early row collection and no schema resolution solely for metadata.
Test grouped span aggregation (minimum start/maximum end), count-only summaries,
and arithmetic that changes coordinate meaning: copying provenance does not infer
conversion or exempt summaries from existing interval-column checks.

Unmarked grouping objects must pass through shared patched methods without
fabricated metadata. Excluded `map_groups` and rolling/dynamic grouping have no
propagation guarantee; check method identities and absence of any added callback
proxy. Verify ownership conflicts when the group was created before a foreign
provider initialized. Do not infer these guarantees from the limited audit probe.

## Application regressions

| Workflow | Existing test locations / required additions |
| --- | --- |
| Coordinate semantics and missing tags | `test_coordinate_system_metadata.py`, `test_coordinate_system_resolution.py`, `test_coordinate_system_semantics.py`; retain the two-input concat regression |
| Tabular readers and explicit declarations | `test_tabular_readers.py`; preserve source declaration, optional output conversion, column normalization, and tagging; ensure Parquet file metadata does not silently change declaration precedence |
| Complete metadata | `test_source_metadata.py`, `test_comprehensive_metadata.py`; replace the empty collect test with complete-header assertions |
| Grouped interval workflow | Add reader → grouped min(start)/max(end) → ordinary filter/collect → range-operation coverage; verify full metadata and normal types, and ensure count-only summaries still fail existing interval-column checks |
| Source adapters | `test_io_cool.py`, `test_io_cool_parity.py`, `test_io_gff.py`, `test_io_gtf.py`, `test_filter_select_attributes_bug_fix.py`, `test_projection_pushdown.py`, `test_pushdown_equivalence.py` |
| VCF roundtrips | `test_vcf_write.py`, `test_vcf_write_header_lines.py`, `test_vcf_write_record_layout.py`, `test_vcf_write_info_prefix.py`; check samples, raw headers, and record layout |
| Typed alignments | `test_io_bam.py` and typed-tag roundtrip coverage; assert both values and exact BAM/SAM tag types after transformations |
| Streaming and repeated collection | `test_concurrent_collect.py`, `test_streaming.py`, `test_fasta_write.py`, `test_fastq_write.py`, and VCF/alignment writer cases |

All paths above are relative to `tests/`. Add missing focused regressions to the
relevant existing suite rather than duplicating whole workflows. Source-specific
adapters must keep their current optimizations and predicate behavior.

Suggested first checks after the implementation is integrated:

```sh
uv run pytest tests/test_frame_metadata.py
uv run pytest tests/test_coordinate_system_metadata.py tests/test_coordinate_system_resolution.py tests/test_coordinate_system_semantics.py tests/test_tabular_readers.py tests/test_source_metadata.py tests/test_comprehensive_metadata.py
```

Run the remaining affected suites from the matrix, then the repository-required
release checks. Respect the process isolation encoded in `Makefile`: the broad
suite excludes `test_overlap_algorithms.py` and `test_streaming.py`, which run
separately, and warnings tests also receive a separate run. The planned release
gate is `make test` in a correctly built environment. Do not collapse these runs
into one pytest process.

## Versions and overhead

- Verify Polars 1.40.0 and the newest stable release satisfying `<2` at execution
  time. Audit snapshots used 1.40.0, 1.40.1, and 1.44.2; they are dated evidence,
  not a floating claim about the latest version.
- Exercise applicable Python 3.11–3.14 jobs using supported wheel combinations.
  Record resolver/wheel incompatibilities separately from provider failures.
- Also run standalone provider checks on Polars 2.0/2.1 and application tests in
  an isolated worktree combining this migration with PR #478. The application
  dependency bound stays unchanged in this migration PR.
- Run the complete manifest and Parquet checks at both Polars endpoints. A missing
  method or metadata-writing API blocks the selected version claim; resolve the
  compatibility issue explicitly instead of silently reducing behavior.
- In fresh processes on the same environment, compare unpatched Polars, the
  current plugin, and the new provider. Measure repeated lazy-plan construction
  and small eager operations with and without tags; warm up and report medians
  across repetitions. Exclude file I/O and initialization from per-call timings.
- Record absolute overhead as well as ratios. Investigate repeatable regressions
  against the current provider before release. Do not introduce a flaky timed
  assertion into ordinary unit tests or launch a broad genomic benchmark project.

## Packaging and release

After behavior gates pass, remove the old dependency from maintained manifests
and the lockfile. Build wheel and sdist using the project build configuration;
inspect runtime requirements, provider inclusion, and upstream attribution in
both artifacts.

Install the wheel in a fresh environment without `polars-config-meta`. Confirm it
does not import the old package, then smoke-test a tagged reader → ordinary
transform → range operation → writer flow and an externally created native frame.
Test the old-distribution-installed-but-unused case separately from conflicting
imports. Documentation must lead with `pb.scan_csv()` / `pb.scan_parquet()` and
their eager equivalents for interval data; manual tagging of external frames is
the interoperability path. Document source declaration versus output conversion,
removals, snapshot reads, generic invalidation, retagging, and interpreter restart
instructions for conflicts.

Record actual versions, commands, results, and limitations before release. Run:

```sh
openspec validate internalize-frame-metadata --strict
```

Implementation completion requires the runtime and artifact evidence in the
verification report, not just the historical audit or specification validation.
The report also records historical failures in a local untracked PGEN test file
absent from HEAD and clean CI checkouts. After its removal at the user's request,
the full `make test` target passed; this is no longer an outstanding exception.
