# Change: Relax dependency version constraints

GitHub Issue: https://github.com/biodatageeks/polars-bio/issues/261

## Why

The current dependency specifications use overly restrictive version specifiers (`~=`) that prevent compatibility with newer minor versions. For example, `typing-extensions~=4.14.0` blocks users from installing `typing-extensions>=4.15`, even though minor version increments are typically backward-compatible per semantic versioning. This causes unnecessary installation conflicts when users have other packages requiring newer versions of shared dependencies.

## What Changes

- Replace `~=` (compatible release) specifiers with `>=X.Y,<(X+1)` ranges for:
  - `pyarrow~=21.0.0` → `pyarrow>=21.0.0,<22`
  - `datafusion~=50.0.0` → `datafusion>=50.0.0,<51`
  - `tqdm~=4.67.1` → `tqdm>=4.67.0,<5`
  - `typing-extensions~=4.14.0` → `typing-extensions>=4.14.0,<5`
- Keep `polars>=1.30.0` as-is (already permissive)
- Relax `mkdocs-glightbox (>=0.5.1,<0.6.0)` to `mkdocs-glightbox>=0.5.1,<1`
- Update both `[project]` and `[tool.poetry.dependencies]` sections in `pyproject.toml`

## Impact

- Affected specs: packaging (new capability spec)
- Affected code: `pyproject.toml`
- **User benefit**: Better compatibility with other packages in user environments
- **Risk**: Minimal - minor version updates follow semver and should be backward-compatible
- **Testing**: CI will validate compatibility with resolved versions
