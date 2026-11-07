# Implementation verification

Verified on 2026-11-07. The approved bounded migration is implemented, with
ordinary Polars frame types and ordinary grouping syntax. No Rust source was
changed. No release was published.

## Delivered behavior

- `polars_bio/_frame_metadata.py` owns instance metadata, the three-method
  compatibility namespace, and the explicit 57-entry patch manifest: 33
  preserving frame operations, 10 grouping boundaries, and 14 invalidating
  combination methods. There is no object-ID registry or method discovery.
- Grouped `agg`, `len`, `head`, `tail`, and `having` retain snapshots from grouping
  time. Empty snapshots also prevent later source tagging from leaking into a
  result. Results keep normal native types and lazy construction behavior.
- Installation preflights conflicts, is serialized/idempotent, and rolls back
  owned changes on failure. Existing accessors and tagged queries detect later
  namespace replacement. Installing the old distribution without importing it
  remains supported.
- Application metadata access uses canonical helpers. Existing source adapters,
  complete serialized headers, coordinate validation, and native batch-stream
  interfaces remain intact. The empty source-collection test now asserts actual
  preservation and dictionary independence.
- The Parquet compatibility writer retains both JSON metadata locations. Native
  writing handles the verified scalar schema/common-option subset; nested,
  categorical, and other unverified cases use the PyArrow fallback. A strong
  accessor owner supports temporary frames; GC tests cover the resulting cache
  cycle. Both details are reflected in the updated design.
- Maintained runtime dependency declarations no longer include the external
  plugin. Wheel/sdist include its MIT notice and attribution. Documentation
  recommends polars-bio's CSV/Parquet readers and explains compatibility changes.

## Tests

The final provider has **205 passing tests in every matrix job**. These are real
isolated environments, including both original-plugin import-order tests:

| Python | Polars 1.40.0 | Polars 1.44.2 | PyArrow |
| --- | --- | --- | --- |
| 3.11.13 | 205 passed | 205 passed | 24.0.0 |
| 3.12.8 | 205 passed | 205 passed | 24.0.0 |
| 3.13.5 | 205 passed | 205 passed | 24.0.0 |
| 3.14.7 | 205 passed | 205 passed | 24.0.0 |

The [machine-readable matrix](audit/provider-matrix.json) records versions and
counts. `.github/workflows/frame_metadata.yml` repeats the provider checks on
Python 3.11–3.14 against minimum and newest resolved Polars `<2`, without requiring
a Rust build. This local matrix does not claim every intermediate Polars release
has been individually tested.

Application verification used Python 3.12.2, Polars 1.44.2, PyArrow 24.0.0, and a
rebuilt native extension. Required process isolation from `Makefile` was retained:

| Suite | Result |
| --- | --- |
| Main suite, excluding the two isolated suites as configured by the Makefile | 2,547 passed, 1 skipped, 25 warnings |
| Overlap algorithms, separate process | 15 passed |
| Warnings, separate process | 11 passed |
| Streaming, separate process | 37 passed, 1 warning |

The main run includes the provider, coordinate semantics/resolution, tabular and
source metadata, grouped interval workflows, Cooler/GFF/GTF adapters, VCF and
typed alignment roundtrips, and concurrent collection. The warnings run is an
intentional repetition required by the Makefile, not additional unique coverage.
Results are in [integration-results.json](audit/integration-results.json).

The initial local `make test` reached the test suite but reported **54 failures**,
all in the pre-existing untracked `tests/test_pgen_query_paths.py`. That file is
absent from HEAD and therefore absent from a clean CI checkout; pytest discovered
it because it exists on disk in this workspace. These are local extra-test
failures, not failures of master's committed test suite.

For comparison, an isolated export of production code from HEAD
`17fabb9e9807481d49d892f42d1193bd67e75467` was augmented with that same untracked
test file. Using the original provider and the same Polars version reproduced
exactly the same 54 failing node IDs. This comparison was intentionally not a
clean-HEAD test inventory: it established that the metadata migration did not
cause those extra tests to fail. They expect additional APIs such as `rows=`,
`companion_cache=`, `iter_pgen_matrix`, and `PgenScanSource` that the production
code does not provide. The two passing cases in that file also passed on this
comparison run.
See [existing-pgen-failures.json](audit/existing-pgen-failures.json). That initial
run preceded six additional Parquet regression cases; the final main-suite count
above includes them. No PGEN implementation was changed.

At the user's request, the untracked `tests/test_pgen_query_paths.py` was then
deleted from disk. A subsequent full **`make test` passed with exit code 0**,
including all four required test processes. The table above reflects this final
run. The historical local extra-test issue is resolved; it is not an outstanding
test exception. The complete log is
`/tmp/polars-bio-metadata-clean-make-test.log`.

Reproduce the final application runs with the built environment:

```sh
make test
```

Ruff lint and the repository's commit hooks (including Black and isort) pass for
changed Python files and audit harnesses.
`uv lock --check` and strict OpenSpec validation pass. The workflow YAML and
documentation navigation were checked; updated metadata documentation renders
with its code and tables.

## Bounded overhead measurement

The [benchmark harness](audit/benchmark_provider.py) measures separate sequential
processes on Python 3.12.2 / Polars 1.44.2. Each case has 100 warmup iterations and
seven repetitions of 2,000 calls. Tagged cases use a 10 KB source header. The old
provider is activated before measuring even untagged cases, because its wrappers
are installed on first namespace use. I/O and initialization are excluded.

Median microseconds per operation (a lazy operation here is a three-method chain):

| Case | Native, unpatched | Original 0.3.4 | Owned provider |
| --- | ---: | ---: | ---: |
| Untagged lazy chain | 10.992 | 12.492 | 12.315 |
| Untagged eager clone | 0.188 | 0.424 | 0.384 |
| Tagged lazy chain | 11.000 | 15.035 | 14.003 |
| Tagged eager clone | 0.188 | 1.172 | 0.801 |

These measurements show no regression against the active original provider in
these cases. They also show overhead versus unpatched Polars, especially for very
small eager operations. They are local microbenchmarks, not genomic throughput
claims or CI timing assertions. Raw samples: [native](audit/benchmark-native.json),
[original](audit/benchmark-legacy.json), [owned](audit/benchmark-owned.json).

## Artifacts and fresh installation

Built the wheel with `maturin build --locked` and the sdist with `maturin sdist`.
The source build used a clean HEAD export overlaid with the migration files to
exclude unrelated untracked workspace material. Provider, tests, metadata docs,
and project configuration in the sdist matched the workspace byte for byte at
artifact inspection. Commit hooks subsequently normalized import ordering and
test formatting; the provider implementation and package behavior are unchanged.
Both distributions contain the provider and required attribution, and neither
declares the original package as a runtime dependency.

Artifact paths, sizes, SHA-256 hashes, and inspection results are recorded in
[artifacts.json](audit/artifacts.json); artifacts are under
`/tmp/polars-bio-metadata-dist/`. These are local verification builds.

A fresh environment installed the wheel without the original distribution.
The [installed-wheel harness](audit/installed_wheel_smoke.py), run with Python
isolated mode, verified exact installed provider source, attribution, CSV →
grouping → range operation → Parquet, externally created frame tagging, nested
schema metadata fallback, and a VCF sample/header roundtrip. It also verified that
the old package was neither installed nor imported. See
[clean-install.json](audit/clean-install.json).

Bioconda's dependency/license updates are staged for the migration release. Its
existing source version/checksum must be updated together when the release
archive exists; this work does not invent a release checksum or publish a recipe.

## Remaining limits

The 57 wrappers still affect the process globally. Compatibility depends on the
listed Polars APIs and should continue to be covered at supported endpoints.
Unlisted methods, module functions, custom group callbacks, rolling/dynamic
grouping, and serialization do not gain a propagation guarantee.

Metadata is provenance and a declared coordinate convention, not proof that an
arbitrary expression or aggregate produces valid intervals. Generic combining
methods clear it; summaries without interval columns still fail existing range
validation. Top-level dictionaries are independent, while nested user values are
shallowly shared. Concurrent mutation of a single frame is not promised.

The compatibility Parquet fallback can collect a LazyFrame. Plain Polars writers
do not persist the namespace automatically, and polars-bio readers continue to
require explicit coordinate declarations. Namespace conflicts require removing
the competing import and restarting the interpreter.
