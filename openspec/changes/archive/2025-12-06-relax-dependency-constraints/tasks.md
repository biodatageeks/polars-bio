# Implementation Tasks

## 1. Update pyproject.toml

- [x] 1.1 Update `[project].dependencies` section with relaxed version constraints
- [x] 1.2 Update `[tool.poetry.dependencies]` section to match
- [x] 1.3 Ensure version specifier syntax is correct for both PEP 508 and Poetry formats

## 2. Validation

- [x] 2.1 Run `poetry lock` to regenerate lock file
- [x] 2.2 Run `poetry install` to verify dependencies resolve correctly
- [x] 2.3 Run test suite to confirm no regressions
- [x] 2.4 Build a package with maturin and test installation in a clean virtual environment to ensure no conflicts
- [x] 2.5 Run all tests commands as in Makefile to ensure full coverage

## 3. Documentation

- [x] 3.1 Update `openspec/project.md` to reflect new dependency version policy
