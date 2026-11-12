# Change: Bump DataFusion dependencies to the 55 line

## Why
DataFusion 53's external sorter can starve under `FairSpillPool` when several partitions sort at once (apache/datafusion#20642, fixed in 54.0.0). datafusion-bio-formats and datafusion-bio-functions move to DataFusion 55.2.0 for that fix. polars-bio links both crates and must move with them to keep one DataFusion in its build.

## What Changes
- Update Rust DataFusion from 53.0.0 to 55.2.0.
- Update Rust datafusion-python from 53.0.0 to 55.0.0. 55.2.0 is not published; 55.0.0 requires `datafusion ^55.1.0`, so it resolves against 55.2.0.
- Update PyO3 from 0.28.3 to 0.29.3, the line datafusion-python 55 and arrow-pyarrow 59 require. Update `pyo3-log` to 0.13.5, the first release that accepts PyO3 0.29.
- Declare the abi3 floor explicitly (`abi3-py311`). With bare `abi3`, PyO3 0.29 takes the floor from the build interpreter. Build Linux wheels against CPython 3.11 only, so the release keeps one abi3 wheel per platform.
- Update Rust Arrow crate pins to 59.3.2, the line DataFusion 55.2.0 resolves.
- Update the Rust toolchain from 1.91.0 to 1.94.0, DataFusion 55.2.0's MSRV.
- Update the Python runtime dependency from `datafusion>=53.0.0,<54` to `datafusion>=55.0.0,<56`, here and in the in-repo bioconda recipe.
- Pin datafusion-bio-formats and datafusion-bio-functions to their DataFusion 55 commits (biodatageeks/datafusion-bio-formats#248, biodatageeks/datafusion-bio-functions#332) until they are tagged.

## Impact
- Affected specs: packaging
- Affected code: `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `.github/workflows/publish_to_pypi.yml`, `pyproject.toml`, `uv.lock`, `bioconda-recipe/meta.yaml`, Rust wrapper code (`src/fastqc.rs`, `src/pileup.rs`, `src/write.rs`).
- Users must install `datafusion` 55.x alongside polars-bio.
