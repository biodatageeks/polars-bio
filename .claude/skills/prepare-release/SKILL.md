---
name: prepare-release
description: >-
  Cut a new polars-bio release — bump the version, finalize the changelog, run
  release-readiness checks, open the release PR, and draft a LinkedIn
  announcement. Use this whenever the user asks to "prepare a release", "cut
  X.Y.Z", "bump the version", "do the 0.32.0 release", "release readiness", or
  anything about shipping a new polars-bio version. polars-bio keeps its version
  in THREE places that must stay in lockstep (Cargo.toml, pyproject.toml,
  polars_bio/__init__.py) plus the regenerated Cargo.lock — this skill exists so
  none of them is forgotten and the changelog is cut correctly from
  [Unreleased]. Reach for it before manually editing any version string.
---

# Preparing a polars-bio release

## Why this exists

polars-bio's version lives in **three** sources of truth that must always
match, and a release is easy to get half-right (bump two of three files, forget
to cut the changelog, skip the compile check). This skill encodes the exact,
verified sequence so a release is complete and consistent every time.

## The version lives in 3 places (+ lockfile)

| File | Line shape |
|------|------------|
| `Cargo.toml` | `version = "X.Y.Z"` under `[package]` |
| `pyproject.toml` | `version = "X.Y.Z"` under `[project]` |
| `polars_bio/__init__.py` | `__version__ = "X.Y.Z"` |
| `Cargo.lock` | auto-updates for the `polars_bio` package on the next `cargo` run |

## Workflow

Track these as todos so none is skipped.

### 1. Establish the version and what's shipping

- Current version: `git describe --tags --abbrev=0` and `grep version Cargo.toml`.
- Commits since last tag: `git log --oneline <last-tag>..HEAD`.
- Decide the new version with the user if not given (semver: breaking → minor
  while pre-1.0, features → minor, fixes → patch). polars-bio has been bumping
  the **minor** for feature releases (0.30 → 0.31 → 0.32).
- Cross-check `git log` against the `[Unreleased]` section of `CHANGELOG.md` —
  **the most recent merged PRs are often missing**. Add anything absent (check
  the latest commit/PR especially, e.g. a last-minute fix).

### 2. Create the release branch

```bash
git checkout -b release/X.Y.Z
```

Never bump version on `master` directly — it goes through a PR.

### 3. Bump the version in all 3 files

Edit each of `Cargo.toml`, `pyproject.toml`, `polars_bio/__init__.py`. Match on
the surrounding line (e.g. `name = "polars_bio"` above the Cargo version) so the
edit is unambiguous.

### 4. Cut the changelog

`CHANGELOG.md` follows Keep a Changelog. Convert the open `[Unreleased]` heading
into a dated release heading, leaving a fresh empty `[Unreleased]` above it:

```markdown
## [Unreleased]

## [X.Y.Z] - YYYY-MM-DD

### Added
...
```

Use today's date (it's in the session context — do not invent one). Keep the
`### Added / Changed / Fixed` subsections that were already accumulated.

### 5. Release-readiness checks (do not skip)

- **Version consistency** — confirm zero stragglers and all three at the new version:
  ```bash
  grep -rn "<OLD>" Cargo.toml pyproject.toml polars_bio/__init__.py   # expect: no matches
  grep -n "<NEW>" Cargo.toml pyproject.toml polars_bio/__init__.py CHANGELOG.md
  ```
- **Compile** — `cargo check` (run in background; the workspace is large). Must exit 0.
- **Lockfile** — confirm `Cargo.lock` now shows the new version for `polars_bio`
  (`cargo check` regenerates it). Stage it.
- Report the readiness results plainly (pass/fail with evidence), per
  verification-before-completion.

### 6. Commit, push, open the PR

```bash
git add CHANGELOG.md Cargo.toml Cargo.lock pyproject.toml polars_bio/__init__.py
git commit   # message: "chore: release X.Y.Z"  (pre-commit hooks run black/isort)
git push -u origin release/X.Y.Z
gh pr create --title "chore: release X.Y.Z" --body "..."
```

End the commit message with the standard `Co-Authored-By:` trailer and the PR
body with the standard Claude Code generation line.

The PR body should list the highlights since the last release (Added / Changed /
Fixed, with PR numbers) and the release-readiness results.

### 7. Draft the LinkedIn announcement

See `references/linkedin-template.md`. Emoji-led, scannable, one emoji per
highlight, ends with `pip install polars-bio==X.Y.Z`, a GitHub star CTA, and
genomics/Python/Rust hashtags. Show it to the user — they post it manually.

## What this skill does NOT do

- It does not tag or publish to PyPI/crates.io — that's the maintainer's CI on
  merge. Stop at the PR + announcement draft unless the user asks for more.
- It does not edit upstream `datafusion-bio-formats` versions.
