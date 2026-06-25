# Change: Bump DataFusion dependencies to the 53 line

## Why
polars-bio needs to consume DataFusion 53-compatible bio-formats and bio-functions crates while continuing to expose the Python API through the latest published DataFusion Python bindings.

## What Changes
- Update Rust DataFusion from 50.3.0 to 53.1.0.
- Update Rust datafusion-python from 50.1.0 to 53.0.0 because 53.1.0 is not published.
- Update direct PyO3 dependencies to the PyO3 line required by datafusion-python 53.
- Use the published datafusion-python 53 crate directly, without vendoring it into polars-bio.
- Update Python runtime dependency from `datafusion>=50.0.0,<51` to `datafusion>=53.0.0,<54`.
- Raise the Python `pyarrow` lower bound to `>=23.0.1` so dependency resolution avoids vulnerable `pyarrow<23.0.1` releases.
- Update Rust Arrow crate pins to the Arrow 58.3.0 line resolved by DataFusion 53.1.0.
- Update pinned git revisions for datafusion-bio-formats and datafusion-bio-functions to DataFusion 53-compatible commits.

## Impact
- Affected specs: packaging
- Affected issues: #398
- Affected code: `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, Rust wrapper code, Python tests.
