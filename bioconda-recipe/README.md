# Conda packaging for polars-bio

Source of truth for the conda recipes. The copies that actually get built live in
the upstream repositories; this directory is where they are edited and reviewed
before being copied there.

## Contents

| Path | Target repository | Purpose |
| --- | --- | --- |
| `meta.yaml`, `build.sh`, `test_overlap.py` | `bioconda/bioconda-recipes` → `recipes/polars-bio/` | The polars-bio package |
| `conda-forge-recipes/polars-config-meta/meta.yaml` | `conda-forge/staged-recipes` → `recipes/polars-config-meta/` | A dependency that is not yet packaged for conda |

## Ordering constraint

`polars-config-meta` must land on conda-forge **before** the bioconda recipe can
build, because it is a hard runtime dependency of polars-bio (imported
unconditionally from `polars_bio/__init__.py`). Everything else polars-bio needs
is already on conda-forge:

| Dependency | Required | conda-forge |
| --- | --- | --- |
| `polars` | `>=1.37.1` | yes |
| `pyarrow` | `>=23.0.1,<25` | yes |
| `datafusion` | `>=53.0.0,<54` | yes (53.0.0) |
| `tqdm` | `>=4.67.0,<5` | yes |
| `polars-config-meta` | `>=0.3.0,<1` | **no — submit first** |

Note that `polars-config-meta` declares `dependencies = []` in its
`pyproject.toml` and lists polars only as an optional extra, but imports polars
at module scope. The conda recipe therefore declares polars as a hard run
dependency; without it the package installs but fails on import.

## Build notes

Two things in the source tree need handling in `build.sh`, which is why the
recipe uses a build script rather than an inline `script:` entry:

- `rust-toolchain.toml` pins an exact toolchain for local development. The conda
  build supplies its own `rustc` and has no `rustup` to satisfy the pin with, so
  the file is removed.
- Upstream CI builds PyPI wheels with `-Ctarget-cpu=skylake` / `apple-m1` and
  `-Dwarnings`. Both are wrong for a redistributable package, so `RUSTFLAGS` is
  cleared.

The crate graph is large (662 crates) and six dependencies resolve from git
tags, so the build environment needs `git` and network access. `Cargo.lock` is
shipped in the sdist, so those revisions are pinned. `cargo-bundle-licenses`
records the licences of that crate graph into `THIRDPARTY.yml`, which is listed
in `license_file` alongside `LICENSE`.

One local-only gotcha: running `conda-build` without conda-forge's pinning lets
it choose a macOS deployment target from the host SDK, which can exceed the
running OS version. maturin then tags the wheel with that version and the test
phase fails `pip check` with "not supported on this platform". Pass
`--variants "{MACOSX_DEPLOYMENT_TARGET: ['11.0']}"` to reproduce what CI does.
This does not affect bioconda, which pins the target well below the runner.

## Local verification

```bash
conda create -n cbuild -c conda-forge conda-build
conda activate cbuild

# polars-config-meta (noarch)
conda-build conda-forge-recipes/polars-config-meta \
  -c conda-forge --override-channels --variants "{python_min: ['3.10']}"

# polars-bio, picking the dependency up from the local channel
conda-build . -c local -c conda-forge --override-channels --python 3.12
```

For a build that matches bioconda CI more closely:

```bash
conda create -n bioconda -c conda-forge -c bioconda bioconda-utils
conda activate bioconda
bioconda-utils lint  --packages polars-bio
bioconda-utils build --docker --mulled-test --packages polars-bio
```

## Platforms

Bioconda builds `linux-64` and `osx-64` by default; `osx-arm64` and
`linux-aarch64` require an explicit `extra: additional-platforms:` entry. The
recipe currently targets the defaults only — cross-compiling a Rust tree this
large is worth adding once the package is green, not as part of the initial
submission. Bioconda does not build Windows packages at all; Windows users
continue to install from PyPI.

## Release automation

`.github/workflows/update_bioconda_recipe.yml` opens a version-bump pull request
against `bioconda/bioconda-recipes` when a release is published. Bioconda's own
autobump bot usually does this unprompted once the package exists; the workflow
makes the bump deterministic rather than waiting on the bot. It requires a
`BIOCONDA_PAT` secret with `public_repo` scope.

## Maintainers

- @mwiewior

## References

- [Bioconda contributor guidelines](https://bioconda.github.io/contributor/guidelines.html)
- [Building locally](https://bioconda.github.io/contributor/building-locally.html)
- [conda-forge Rust knowledge base](https://conda-forge.org/docs/maintainer/knowledge_base.html#rust)
