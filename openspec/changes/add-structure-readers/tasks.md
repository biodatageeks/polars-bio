# polars-bio implementation tasks

This checklist owns Python/SQL integration for issue #455. Native parsing, residue assembly, geometry, the codec, and canonical fixture generation are owned by [datafusion-bio-formats tasks](../../../../datafusion-bio-formats/openspec/changes/add-structure-readers/tasks.md). Implementation evidence and deviations are recorded in [IMPLEMENTATION.md](IMPLEMENTATION.md). Open boxes identify incomplete cross-platform or release-scale gates, not missing reader APIs.

Dependency order: PB-0 accompanies the native BF-0 spike; PB-1 consumes BF-2 (PDB/mmCIF with residue output); PB-2 consumes BF-3 (Foldcomp); PB-3 consumes the BF-4 performance/release result. Planning and option-binding preparation can start before native providers land; product parity gates require the corresponding native implementation.

## PB-0. Binding contract and wheel feasibility

- [x] PB-0.1 Freeze the public API/options/schema contract from design.md with formats: atom/residue levels, all/first/numbered models, conformer policy, physical units, empty selections, and outgoing omega. Ownership across the two repositories is already agreed.
- [x] PB-0.2 Prepare temporary local dependency overrides in an isolated integration checkout for the new format crates; keep all formats dependencies and DataFusion/Arrow versions compatible. Avoid replacing unrelated shared scan/PGEN work or updating functions dependencies.
- [ ] PB-0.3 Wire minimal native spike providers into a test wheel and exercise import, schema discovery, one atom batch, one residue batch, and one FCZ decode on supported Linux/macOS/Windows architectures. Feed build/precision failures back to BF-0 before freezing the backend.
- [x] PB-0.4 Define the Python exception mapping, immutable native option conversion, fixed-schema access, and retained-provider source interface in a new src/structure.rs binding module. Validation rules remain implemented once in formats.
- [x] PB-0.5 Define how the small integration fixture subset and source manifest are copied from the formats corpus; pin hashes/version and prohibit independent golden regeneration here.

Gate: compatible provider interfaces and successful representative wheel smoke tests. No parser or geometry is implemented in Python or in a polars-bio provider wrapper.

## PB-1. PDB/mmCIF atom and residue APIs

Prerequisite: BF-2 passes native atom/residue gates; consume its compatible formats commit or release.

- [x] PB-1.1 Add datafusion-bio-format-structure to Cargo.toml/Cargo.lock, enable its text-formats feature, and add StructureReadOptions in src/structure.rs using the existing retained DataFusion source route (no InputFormat change required). One native format option distinguishes pdb/mmcif/auto; do not duplicate algorithms by Python entry-point name.
- [x] PB-1.2 Implement the retained provider factory in src/structure.rs: retain normalized source manifests and native options/provider references, expose Arrow schema without collection-wide decoding, create a fresh execution for each collect, and register Python bindings in src/lib.rs. Register complete native providers through src/scan.rs where the existing registration API requires it.
- [x] PB-1.3 Add scan_pdb, scan_mmcif, scan_structures and matching read_* methods in polars_bio/io.py; accept PathLike, explicit lists, and local globs via the common native source contract. Default to atom rows; level="residue" asks the native provider for its residue schema. Export APIs in polars_bio/__init__.py.
- [x] PB-1.4 Add register_structure in polars_bio/sql.py using the same options/provider constructors. Retain full source/options identity when a named SQL table and an independent LazyFrame read the same files.
- [x] PB-1.5 Reuse the existing DataFusion DataFrame hook in _lazy_scan, including its Arrow stream path and residual-filter/projection behavior. Prevent catalog collisions across same-path scans, concurrent collects, models, output levels, and altloc choices. Retain safe behavior for existing PGEN/Cooler sources.
- [x] PB-1.6 Add structural source metadata in metadata_extractors.py/_metadata.py and _format_to_string as needed. Skip genomic coordinate metadata for structure sources without changing existing format defaults. Carry units/schema version/policies through both eager and lazy results.
- [x] PB-1.7 Register only correctly typed predicate support: author IDs are strings, label IDs may be signed/null, coordinates/angles are Float64. Preserve unsupported expressions as residual filters and apply limits only after remaining client predicates. Do not move residue filters into atom input.
- [x] PB-1.8 Add tests/test_io_structure.py and tests/data/structure/ (consolidating residue and query-path cases) with the pinned formats subset. Compare complete keys, counts, dtypes, null masks and values for eager/lazy/SQL, partial projections, phi-only queries, middle-residue filters, limits, empty projections, and rootless counts.
- [x] PB-1.9 Test model/altloc options, same-basename sources, repeated/concurrent collect, input changes between executions, source cleanup, gzip, contextual errors, and global genomic-coordinate-setting independence.
- [x] PB-1.10 Document reading/API/SQL examples in docs/features/reading.md, docs/api/reading.md and docs/api/sql.md. Explain identifier joins, physical units, missing residues/atoms, altloc/connectivity policies, explicit sorting, and per-entry memory behavior.

Gate: PDB/mmCIF collections and all six residue quantities pass the frozen native corpus through Python and SQL. Existing shared I/O paths affected by the source/metadata changes continue to pass. This is the first releasable milestone.

## PB-2. Foldcomp selectors and public integration

Prerequisite: BF-3 provides a validated local FCZ/database provider and PB-1's structural source bridge is available.

- [x] PB-2.1 Add datafusion-bio-format-foldcomp and native FoldcompReadOptions bindings, referencing the same structure crate version. Extend the retained structural source factory with a Foldcomp variant; reuse the Arrow/metadata path.
- [x] PB-2.2 Add scan_foldcomp, read_foldcomp, and register_foldcomp. Preserve None versus empty lists at the binding boundary; support mutually exclusive ids and entry_keys selectors; fail early for invalid combinations without changing the native selection semantics.
- [x] PB-2.3 Pass level/model/conformer options supported by the native provider and preserve reconstructed identities, lookup name, title, entry key, source provenance, and available codec metadata. Both atom and residue output come directly from formats.
- [x] PB-2.4 Add tests/test_io_foldcomp.py and extend query-path tests: standalone FCZ, full scan, one/two IDs, ids=[], duplicates, missing/ambiguous names, numeric keys without lookup, title/name mismatch, invalid sidecars, eager/lazy/SQL parity, and repeated/concurrent collection.
- [x] PB-2.5 Verify Python-level subset selection reaches native scheduling before decoding; use a test metrics hook or controlled source fixture to assert K distinct selected payload decodes and zero for an empty selector. Metadata scan cost is measured separately.
- [x] PB-2.6 Document reconstructed-coordinate and coordinate-derived-angle semantics, local storage scope, supported database layouts, empty/missing ID behavior, and why codec B-factor values are not automatically pLDDT. Include a small database-subset example with actual returned schema.

Gate: the requested local Foldcomp subset workflow produces canonical atom/residue tables with correct selectors and descriptor values. This completes functional scope for #455 before release hardening.

## PB-3. Regression checks, performance, and release

- [x] PB-3.1 Run the focused structure/Foldcomp suites, shared predicate/projection/count/metadata/SQL/source-lifetime regressions, Rust binding tests, and repository-required checks. Use the repository's isolation rules when running the broader test suite before release.
- [ ] PB-3.2 Benchmark full Python-visible parse-to-DataFrame and descriptor-to-DataFrame work against the pinned oracles. Pair with BF-4 native metrics: wall time, RSS, first batch, bytes read, payload decodes, and 1/2/4/8-worker behavior under cold/warm cache.
- [ ] PB-3.3 Validate representative atom, residue, and Foldcomp reads in built wheels for supported Python/OS/architectures and verify native libraries/notices in wheel/sdist packaging. CI runs committed fixtures offline; oracle regeneration remains a dedicated formats job.
- [x] PB-3.4 Consume the compatible formats release/tag/commit for both new crates and the rest of the formats dependency family; update Cargo.lock without a functions version bump. Native crates and polars-bio need compatible DataFusion/Arrow types throughout the dependency graph.
- [x] PB-3.5 Add changelog and release documentation with API/schema guarantees, measured limits, examples, and corpus provenance. Issue closure is deferred until PDB/mmCIF collections, residue descriptors, and local Foldcomp subsets are released; this PR prepares the documentation.

Verification commands:

```sh
cargo test --lib
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
uv run maturin develop --release
uv run pytest tests/test_io_structure.py tests/test_io_foldcomp.py
uv run pytest tests/test_projection_pushdown.py tests/test_predicate_translator_units.py tests/test_source_metadata.py tests/test_io_cool.py
openspec validate add-structure-readers --strict
```

Gate: released formats dependency, validated wheels, offline fixture parity, and measured collection/subset behavior. Remote Foldcomp, BinaryCIF, arbitrary-DataFrame residue transforms, side-chain descriptors, and writers remain separate feature proposals.
