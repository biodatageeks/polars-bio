# Polars 2 compatibility and review follow-up

The user requested testing this migration together with
[PR #478](https://github.com/biodatageeks/polars-bio/pull/478), followed by an
iterative Codex/Claude review. This supplements the [initial verification](verification.md).
Application dependency bounds remain unchanged in this PR; #478 supplies the
Polars 2 IO/error/streaming changes and widens the bound to `<2.2`.

## Compatibility finding and fix

The original provider failed during initialization on Polars 2.1.0 because
`LazyFrame.with_context` was removed in Polars 2. This prevented importing
polars-bio before any application test could run.

The manifest now explicitly includes `with_context` only on Polars 1. Every
other entry remains mandatory. There are 57 wrappers on Polars 1 and 56 on
Polars 2; the provider does not discover methods or silently skip missing APIs.
The independent manifest expectation and rollback preflight test cover this
distinction. Stream tests check the public Arrow C stream interface on all
versions and retain the existing `_inner` check on Polars 1.

## Verification

The standalone provider passes **200 tests in each of eight new environments**:
Python 3.11.13, 3.12.8, 3.13.5, and 3.14.7, each with Polars 2.0.0 and 2.1.0,
using PyArrow 24.0.0. Both actual upstream-plugin import orders are included.
The five fewer cases versus Polars 1 are the five input-tag combinations for the
removed `with_context` method. See [version evidence](audit/polars2-provider-matrix.json).

The CI provider matrix now also covers Polars 2.0 and 2.1 on Python 3.11–3.14.
These standalone jobs do not imply the entire application supports Polars 2
without the changes in #478.

Application tests used a separate worktree combining migration commit
`2b09a41b8b881b0cd5238d9188814a4bd388fe23` plus these review/compatibility fixes
with #478 head `b325911cca43395a579c582f91a6ad617f4b05c5`. Merge conflicts in
the changelog, IO imports, and lockfile were resolved by preserving both features:
owned metadata, #478's IO error wrapper, Polars 2.1 lock, and no legacy dependency.
Native sources and Cargo manifests were verified identical, so the existing
native extension was reused. A fresh Python 3.12.2 environment installed all
extras; `PYTHONPATH=.` selected the combined worktree source.

| Changes / version | Main suite | Isolated overlap / warnings / streaming |
| --- | --- | --- |
| This migration + review fixes, Polars 1.44.2 | 2,550 passed, 1 skipped | 15 / 11 / 37 passed |
| Combined with #478, Polars 2.0.0 | 2,601 passed, 6 skipped | 15 / 11 / 37 passed |
| Combined with #478, Polars 2.1.0 | 2,601 passed, 6 skipped | 15 / 11 / 37 passed |

All three complete Makefile test targets exited 0. The fresh environment also
enabled optional MSA coverage; test counts are environment-dependent. The clean
combined environment omits the original plugin, so its two import-order cases
are covered by the standalone matrix instead. Commands, exact summaries, source
hashes, and the combined tree ID are in [integration evidence](audit/polars2-integration.json).
The initial worktree run lacked `PYTHONPATH` and failed package discovery; this
was corrected before the successful application runs above.

## First review responses

Codex reported no major findings on the initial commit. Claude's six comments
were addressed as follows:

1. Removed the redundant copy after native tabular `collect()`. Reader/header
   regressions verify propagation. Source-specific adapter copies remain.
2. Documented `GroupBy.len()` delegation to wrapped `agg` beside the manifest.
   The independent behavior tests cover `len()` across all matrix versions.
3. Merged duplicate native/wrapper branches in coordinate getters and setters.
4. Added `tests/conftest.py` to the workflow's push path filter.
5. Documented rollback's dependency on the namespace descriptor's private `_ns`
   attribute; injected rollback failures continue to be tested across versions.
6. Removed the exception catch that turned broken metadata accessors into empty
   metadata. Three regressions verify `KeyError`, `AttributeError`, and `TypeError`
   remain visible instead of being mistaken for an unknown coordinate convention.

Ruff lint, repository commit hooks, and strict OpenSpec validation pass. The
initial wheel/sdist evidence describes the pre-review build; the current source
behavior is covered by the updated provider and combined application runs above.
