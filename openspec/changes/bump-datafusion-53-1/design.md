## Context
polars-bio links directly to Rust DataFusion and datafusion-python while also consuming DataFusion-based bio-formats and bio-functions crates by pinned git revisions. Rust DataFusion 53.1.0 is available, but datafusion-python and the PyPI datafusion package are currently published at 53.0.0.

## Goals / Non-Goals
- Goals: move Rust query execution to DataFusion 53.1.0; keep Python package metadata on the DataFusion 53 line; preserve SQL, scan, write, pileup, range operations, streaming, and projection/predicate pushdown behavior.
- Non-Goals: change public Python APIs, change coordinate metadata semantics, add new file formats, or wait for an unpublished datafusion-python 53.1.0 release.

## Decisions
- Use `datafusion = "53.1.0"` in Rust.
- Use `datafusion-python = "53.0.0"` in Rust with default features disabled so its `mimalloc` global allocator is not linked into the polars-bio extension.
- Use `pyo3 = "0.28.3"` and `pyo3-log = "0.13.3"` to keep one `links = "python"` provider in Cargo resolution.
- Use the published crates.io `datafusion-python` 53 crate directly rather than a local vendored patch.
- Use `datafusion>=53.0.0,<54` in Python metadata.
- Use `pyarrow>=22.0.0,<25` for all supported Python versions so PyArrow stays compatible with DataFusion Python 53 and Python 3.14 wheels.
- Use DataFusion 53-compatible upstream commits for bio-formats and bio-functions.

## Risks
- Rust and Python DataFusion patch versions differ by one patch release. Cargo resolution shows this is compatible, but tests must cover Python SQL and DataFusion context flows.
- Arrow 58 Rust crates and PyArrow 22+ have different release cadences. FFI paths must be tested through Polars/PyArrow conversion.
- Downstream pinned git revisions must be updated atomically to avoid duplicate DataFusion versions in the dependency tree.
- The published datafusion-python crate controls its own optional transitive dependencies, so Cargo resolution must be checked for duplicate native link providers and duplicate DataFusion/Arrow major versions.
