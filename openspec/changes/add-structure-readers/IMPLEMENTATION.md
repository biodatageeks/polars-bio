# Implementation evidence — 2026-09-10

The four scan/read pairs and two SQL registration APIs are implemented for PDB,
mmCIF, mixed text collections, and local Foldcomp files/databases. Parsing,
conformer selection, peptide connectivity, grouping, and all six angles execute
in the native format providers from datafusion-bio-formats PR #251. Python adds
option conversion, source metadata, exports and documentation; bio-functions
remains unchanged.

## Integration decisions

- `src/structure.rs` retains an immutable native provider through a DataFusion
  DataFrame. The existing `_lazy_scan` DataFrame path already supports fresh
  execution, projection and residual predicates. No shared scan rewrite, catalog
  lease or new InputFormat enum variant is necessary.
- Public functions live in `polars_bio/structure.py`, exported from the package
  and through IOOperations/SQL compatibility classes. Schema metadata uses the
  existing metadata helper; genomic coordinate settings do not affect structures.
- Native geometry precedes filtering/projection, so selecting a middle residue or
  only phi does not lose its neighbors. Foldcomp selectors are resolved natively
  before payload decoding. Tests corrupt an unselected payload to verify this
  boundary; exact K-decode counters are checked in the formats tests.
- All format dependencies share one immutable formats Git revision. No temporary
  paths or functions version bump are included.
- Fixtures and goldens are hash-checked copies of the formats manifest. Only the
  native repository regenerates oracles with pinned Gemmi/Biopython/Foldcomp.
- Native third-party notices are included explicitly in wheel and sdist packaging.

## Local validation

macOS arm64, Python 3.12, DataFusion 53 / Arrow 58:

- 260 tests passed across structure/Foldcomp integration, lazy streaming,
  pushdown helpers and BED regressions.
- 182 additional projection, predicate translation, source metadata and coordinate
  metadata regressions passed.
- 19 Rust binding unit tests passed. Clippy with warnings denied, Rust formatting,
  and Ruff for the new Python modules/scripts/tests passed.
- The extension was built and imported with pinned Git dependencies. Offline
  tests cover eager/lazy/SQL, all six angles, schema/metadata, filters/limits/counts,
  repeated/concurrent collection, HTTP/gzip, changed files, model/conformer options,
  same-basename sources and Foldcomp selector/corruption behavior.

## Remaining release gates

The wheel CI matrix exercises the committed offline fixtures on Linux, Windows,
and runnable macOS wheels. Cross-platform completion is determined by those jobs,
not inferred from the local macOS run. Intel macOS cross-built wheels cannot be
imported on an arm64 runner by this smoke step.

Development measurements are committed under `benchmark/structure`; they include
full Python materialization and 1/2/4/8 workers, but do not establish release-speed,
cold-cache, large-database, first-batch or production memory guarantees. Native
parse/Arrow/first-batch/selection counters are in the companion repository.

Publishing a formats release and polars-bio release, merging the PRs, and closing
issue #455 after release remain maintainer actions. Remote Foldcomp, BinaryCIF,
arbitrary-DataFrame transforms and writers remain outside this feature.
