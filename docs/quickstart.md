[polars-bio](https://pypi.org/project/polars-bio/) is available on PyPI and can be installed with pip:
```shell
pip install polars-bio
```
There are binary versions for Linux (x86_64), MacOS (x86_64 and arm64) and Windows (x86_64).
In case of other platforms (or errors indicating incompatibilites between Python's ABI), it is fairly easy to build polars-bio from source with [maturin](https://github.com/PyO3/maturin):
```shell
RUSTFLAGS="-Ctarget-cpu=native" maturin build --release -m Cargo.toml
```
and you should see the following output:
```shell
Compiling polars_bio v0.5.2 (/Users/mwiewior/research/git/polars-bio)
Finished `release` profile [optimized] target(s) in 1m 25s
📦 Built wheel for abi3 Python ≥ 3.8 to /Users/mwiewior/research/git/polars-bio/target/wheels/polars_bio-0.5.2-cp38-abi3-macosx_11_0_arm64.whl
```
and finally install the package with pip:
```bash
pip install /Users/mwiewior/research/git/polars-bio/target/wheels/polars_bio-0.2.11-cp38-abi3-macosx_11_0_arm64.whl
```

!!! tip
    Required dependencies:

    * Python>=3.9
    * cmake,
    * Rust compiler
    * Cargo
    are required to build the package from source. [rustup](https://rustup.rs/) is the recommended way to install Rust.