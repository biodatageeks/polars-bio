# Conda packaging for polars-bio

Source of truth for the conda recipes. The copies that actually get built live in
the upstream repositories; this directory is where they are edited and reviewed
before being copied there.

## Contents

| Path | Target repository | Purpose |
| --- | --- | --- |
| `meta.yaml`, `build.sh`, `test_overlap.py` | `bioconda/bioconda-recipes` → `recipes/polars-bio/` | The polars-bio package |
| `conda-forge-recipes/polars-config-meta/recipe.yaml` | `conda-forge/staged-recipes` → `recipes/polars-config-meta/` | A dependency that is not yet packaged for conda |

The two recipes deliberately use different formats. conda-forge deprecated the
v0 `meta.yaml` format for new recipes, so `polars-config-meta` is a v1
`recipe.yaml` built with `rattler-build`. Bioconda has not migrated, so the
polars-bio recipe stays on v0.

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
conda create -n cbuild -c conda-forge conda-build rattler-build
conda activate cbuild

# polars-config-meta (noarch, v1 recipe -> rattler-build).
# python_min comes from conda-forge's global pinning on CI, so a local build
# has to supply it or the recipe fails to render.
printf 'python_min:\n  - "3.10"\n' > /tmp/variant-pymin.yaml
rattler-build build \
  --recipe conda-forge-recipes/polars-config-meta/recipe.yaml \
  --variant-config /tmp/variant-pymin.yaml \
  --output-dir ./output

# polars-bio, picking the dependency up from rattler-build's output channel
conda-build . -c ./output -c conda-forge --override-channels --python 3.12
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
recipe opts in to `osx-arm64` only. Both ARM targets are built natively on
CircleCI (`osx-arm64` on an Apple Silicon runner, `linux-aarch64` on ARM Linux) —
there is no cross-compilation involved. `osx-arm64` is safe because polars-bio's
own CI already builds macOS aarch64 wheels from the same crate graph;
`linux-aarch64` is left out because nothing upstream exercises ARM Linux.
Bioconda does not build Windows packages at all; Windows users continue to
install from PyPI.

## Release automation

Nothing to do, and no secret required. Bioconda's autobump bot watches the
recipe: its source URL matches the bot's PyPI hoster, so a new release on PyPI
is picked up automatically, and Mergify auto-approves and merges the bot's
bump PR once the tests pass (its rule requires `commits[*].author=BiocondaBot`
and `label=autobump`, which the bot's own PRs satisfy).

An earlier `.github/workflows/update_bioconda_recipe.yml` did the same job
deterministically on release-published, but it duplicated the bot and needed a
`BIOCONDA_PAT` secret, so it was removed.

**One thing the automation cannot do:** autobump does not track changed
dependencies. If a release changes the pins in `pyproject.toml`, the `run:`
section here has to be updated by hand — the bot only rewrites version, sha256
and build number.

## Maintainers

- @mwiewior

## References

- [Bioconda contributor guidelines](https://bioconda.github.io/contributor/guidelines.html)
- [Building locally](https://bioconda.github.io/contributor/building-locally.html)
- [conda-forge Rust knowledge base](https://conda-forge.org/docs/maintainer/knowledge_base.html#rust)
