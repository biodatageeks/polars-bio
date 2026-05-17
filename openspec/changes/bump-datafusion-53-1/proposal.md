# Change: Bump DataFusion dependencies to the 53 line

## Why
polars-bio needs to consume DataFusion 53-compatible bio-formats and bio-functions crates while continuing to expose the Python API through the latest published DataFusion Python bindings.

## What Changes
- Update Rust DataFusion from 50.3.0 to 53.1.0.
- Update Rust datafusion-python from 50.1.0 to 53.0.0 because 53.1.0 is not published.
- Update direct PyO3 dependencies to the PyO3 line required by datafusion-python 53.
- Patch datafusion-python 53 crates locally so their DataFusion dependency uses the same no-compression/no-avro feature set as the rest of the bio stack.
- Update Python runtime dependency from `datafusion>=50.0.0,<51` to `datafusion>=53.0.0,<54`.
- Update Rust Arrow crate pins to the Arrow 58.3.0 line resolved by DataFusion 53.1.0.
- Update pinned git revisions for datafusion-bio-formats and datafusion-bio-functions to DataFusion 53-compatible commits.

## Impact
- Affected specs: packaging
- Affected code: `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, Rust wrapper code, Python tests.
