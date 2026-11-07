# Conda packaging for polars-bio

Source of truth for the conda recipes. The copies that actually get built live in
the upstream repositories; this directory is where they are edited and reviewed
before being copied there.

## Contents

| Path | Target repository | Purpose |
| --- | --- | --- |
| `meta.yaml`, `build.sh`, `test_overlap.py` | `bioconda/bioconda-recipes` → `recipes/polars-bio/` | The polars-bio package |
| `conda-forge-recipes/polars-config-meta/recipe.yaml` | `conda-forge/staged-recipes` → `recipes/polars-config-meta/` | Historical standalone plugin recipe; no longer a polars-bio dependency |

The two recipes deliberately use different formats. conda-forge deprecated the
v0 `meta.yaml` format for new recipes, so `polars-config-meta` is a v1
`recipe.yaml` built with `rattler-build`. Bioconda has not migrated, so the
polars-bio recipe stays on v0.

## Runtime dependencies

All runtime dependencies are available on conda-forge. Frame metadata is now
provided by polars-bio itself; the external `polars-config-meta` distribution is
not required. Its standalone recipe is retained as historical packaging work.

| Dependency | Required | conda-forge |
| --- | --- | --- |
| `polars` | `>=1.40.0,<2.2` | yes |
| `pyarrow` | `>=23.0.1,<25` | yes |
| `datafusion` | `>=53.0.0,<54` | yes (53.0.0) |
| `tqdm` | `>=4.67.0,<5` | yes |

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
conda-build . -c conda-forge --override-channels --python 3.12
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
recipe opts in to both ARM targets. polars-bio's own CI builds macOS arm64
and Linux arm64 wheels and runs the Python suite on a native Linux arm64 runner.
Bioconda does not build Windows packages at all; Windows users continue to
install from PyPI.

## Release automation

For releases without recipe changes, Bioconda's autobump bot watches the
recipe: its source URL matches the bot's PyPI hoster, so a new release on PyPI
is picked up automatically, and Mergify auto-approves and merges the bot's
bump PR once the tests pass (its rule requires `commits[*].author=BiocondaBot`
and `label=autobump`, which the bot's own PRs satisfy).

An earlier `.github/workflows/update_bioconda_recipe.yml` did the same job
deterministically on release-published, but it duplicated the bot and needed a
`BIOCONDA_PAT` secret, so it was removed.

**One thing the automation cannot do:** autobump does not track changed
dependencies. If a release changes the pins in `pyproject.toml`, the `run:`
section here and in the upstream Bioconda recipe has to be updated by hand —
the bot only rewrites version, sha256 and build number.

### 0.37.0 handoff

The runtime requirements and license entry in `meta.yaml` are already staged for
0.37.0. Once its sdist is published, update the
[upstream Bioconda recipe](https://github.com/bioconda/bioconda-recipes/blob/master/recipes/polars-bio/meta.yaml)
in the same PR as the source bump:

1. Set `version` to `0.37.0`, use the published sdist's SHA256, and reset the
   build number to `0`. Update the local source version and checksum together too.
2. Remove `polars-config-meta` from `requirements.run`. Older released sdists
   still require it, so apply the removal together with the new source artifact.
3. Set the Polars runtime requirement to `>=1.40.0,<2.2` and remove the obsolete
   comment that Polars 2 is unsupported.
4. Add `polars_bio/licenses/polars-config-meta/LICENSE` to `about.license_file`
   to retain attribution for the adapted metadata provider.
5. Run the recipe's `pip check` and overlap smoke test in a fresh build/test
   environment without `polars-config-meta`.

No `build.sh` change or removal of the standalone conda-forge package is needed.
The local version and SHA256 remain paired with a published sdist until the
0.37.0 artifact is available; this staged recipe must not be built against that
older source.

## Maintainers

- @mwiewior

## References

- [Bioconda contributor guidelines](https://bioconda.github.io/contributor/guidelines.html)
- [Building locally](https://bioconda.github.io/contributor/building-locally.html)
- [conda-forge Rust knowledge base](https://conda-forge.org/docs/maintainer/knowledge_base.html#rust)
