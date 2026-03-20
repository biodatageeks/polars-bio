# Coding Conventions

**Analysis Date:** 2026-03-20

## Naming Patterns

**Files:**
- Python modules use `snake_case` (e.g., `predicate_translator.py`, `pileup_op.py`)
- Rust source files use `snake_case` (e.g., `context.rs`, `operation.rs`)
- Test files follow pytest convention: `test_*.py` prefix (e.g., `test_io_bam.py`, `test_predicate_pushdown.py`)
- Private/internal modules prefixed with underscore (e.g., `_metadata.py`, `_expected.py`)

**Functions:**
- Python functions use `snake_case` (e.g., `read_fasta()`, `scan_bam()`, `translate_predicate()`)
- Private functions prefixed with single underscore: `_lazy_scan()`, `_overlap_source()`, `_validate_tag_type_hints()`
- Rust functions use `snake_case` (e.g., `do_range_operation()`, `register_table()`)
- PyO3 wrapper functions prefixed with `py_`: `py_read_sql()`, `py_pileup_depth()`, `py_describe_bam()`
- Static methods on classes use descriptive names: `IOOperations.read_bam()`, `IOOperations.scan_vcf()`

**Variables:**
- Python uses `snake_case` for all variables (e.g., `DATA_DIR`, `projected_columns`)
- Constants in UPPERCASE: `DEFAULT_BATCH_SIZE`, `LEFT_TABLE`, `DEFAULT_COLUMN_NAMES`
- Rust uses `snake_case` for variables and static strings
- Module-level context variables use underscore prefix in Python: `_ACTIVE_STRING_COLS`, `_local`
- Dictionary keys for format types use uppercase strings: `"Bam"`, `"Sam"`, `"Cram"`, `"Vcf"`, `"Gff"` (matching PyO3 InputFormat enum discriminant names)

**Types & Classes:**
- Python classes use `PascalCase` (e.g., `IOOperations`, `PredicateTranslationError`, `CoordinateSystemMismatchError`)
- Rust structs use `PascalCase` (e.g., `PyBioSessionContext`, `FilterOp`)
- Exception classes end with `Error`: `PredicateTranslationError`, `CoordinateSystemMismatchError`, `MissingCoordinateSystemError`

## Code Style

**Formatting:**
- Python: Black formatter (version 24.10.0) enforced via pre-commit
  - Line length: Black default (88 characters)
  - String quotes: Black default (single quotes preferred, double quotes when containing single quotes)
- Import sorting: isort with Black profile (via pre-commit hook)
- Rust: `rustfmt` configured (see `rustfmt.toml`)

**Linting:**
- Python: ruff available but currently disabled in pre-commit (commented out)
- Rust: cargo-check via pre-commit hooks
- Pre-commit hooks also check: mixed line endings, trailing whitespace, case conflicts, Python AST validity

**Pre-commit configuration:**
- File: `.pre-commit-config.yaml`
- Active hooks: pre-commit-hooks, isort, black, rust formatting/check
- Disabled: ruff linting (commented out, likely due to conflicts with Black)

## Import Organization

**Order (Python):**
1. Standard library imports (`os`, `logging`, `contextlib`, `threading`)
2. Third-party imports (`polars`, `datafusion`, `pysam`, `pytest`, `pandas`)
3. Local/relative imports (from `polars_bio.*`)
4. Conditional/try-except imports for optional dependencies

**Path Aliases:**
- No import aliases defined in `pyproject.toml`
- Standard imports used directly: `import polars as pl`, `from datafusion import DataFrame`
- Relative imports for submodules: `from . import polars_ext`, `from ._metadata import ...`

**Module Structure:**
- Main public API exposed in `__init__.py` using explicit imports
- Functions aliased at package level: `IOOperations as data_input`, `IntervalOperations as range_operations`
- Private implementation modules import specific items (not wildcard imports)
- Clear separation: core logic in `io.py`, `pileup_op.py`, `range_op.py`; utilities in `utils.py`, `predicate_translator.py`

## Error Handling

**Custom Exceptions:**
- File: `polars_bio/exceptions.py`
- Two main domain exceptions:
  - `CoordinateSystemMismatchError`: Raised when two DataFrames have different coordinate systems (0-based vs 1-based)
  - `MissingCoordinateSystemError`: Raised when DataFrame lacks required coordinate system metadata
- Exceptions have detailed docstrings with usage examples
- PyO3 layer converts Rust errors to Python exceptions: `PyValueError::new_err()`, `PyRuntimeError::new_err()`

**Error Handling Patterns:**
- Try-except blocks with fallback behavior: `_lazy_scan()` falls back to client-side filtering if predicate pushdown fails
- Exception chaining: `raise PredicateTranslationError(...) from e`
- Validation before processing: `_validate_tag_type_hints()` validates input format before passing to Rust
- Safe error wrapping: `.map_err(|e| PyValueError::new_err(e.to_string()))` in Rust FFI code

**Assertions in Code:**
- Used in tests for expected behavior (e.g., `assert len(self.df) == 2333`)
- Condition checks with error messages: `.map_err(|e| ...)` patterns in Rust
- Test-time soft assertions: pytest.assume() context manager in conftest (collects all failures, fails once at end)

## Logging

**Framework:** Standard Python `logging` module

**Setup:**
- Module: `polars_bio/logging.py`
- Root logger initialized with WARNING level default
- Per-module logger: `logger = logging.getLogger("polars_bio")`
- Levels: DEBUG, INFO, WARN (not increased, can only decrease)

**Logging Patterns:**
- Import at module level: `import logging` and `logger = logging.getLogger(__name__)`
- Log at appropriate levels: info/debug for operations, warn for compatibility issues
- Rust side uses `log` crate: `use log::{debug, error, info}`
- User-facing API: `set_loglevel(level: str)` accepts "debug", "info", "warn", "warning"
- Logging must be set as first step after import; level can only be decreased, not increased

## Comments

**When to Comment:**
- Docstrings required on all public functions and classes (module-level, class-level, function-level)
- Inline comments for non-obvious algorithm logic (e.g., predicate translation, range operations)
- Comments explaining "why" not "what" for complex branching or workarounds
- Comments noting version compatibility issues or known gotchas

**JSDoc/Docstring Format:**
- Python: Google-style or numpy-style docstrings (observed in codebase)
- Format elements: summary line, blank line, detailed description, Parameters/Args section, Returns section, Raises section
- Example from `read_fasta()`: includes Parameters with descriptions, and !!! Example block with code
- Markdown formatting in docstrings: code blocks with ```python, ```shell, bold/italic for emphasis
- Special mkdocs directives: !!! Example, !!! note blocks for documentation generation

**File-level Docstrings:**
- Modules with docstrings explain purpose and usage (e.g., predicate_translator.py has 10-line module docstring)
- Rare but used for complex modules like context/query translators

## Function Design

**Size:**
- Functions generally 15-60 lines
- Large functions (100+ lines) typical for complex operations like `_overlap_source()` (implements full pipeline)
- Preference for composition: helper functions called from larger functions

**Parameters:**
- Type hints used throughout: `def _validate_tag_type_hints(tag_type_hints: list[str]) -> None`
- Union types for multiple valid inputs: `Union[pl.DataFrame, pl.LazyFrame]`
- Optional parameters: `Optional[Set[str]] = None`
- Keyword-only args for complex functions (seen in PyO3 decorators)
- Sentinel/default values common: `compression_type: str = "auto"`, `chunk_size: int = 8`

**Return Values:**
- Explicit return type hints: `-> pl.DataFrame:`, `-> Iterator[pl.DataFrame]:`
- Single return type preferred (no multiple return types unless using Union)
- DataFrames/LazyFrames returned directly from I/O functions
- Generator/Iterator pattern for streaming results: `Iterator[pl.DataFrame]` in `_overlap_source()`

## Module Design

**Exports:**
- Explicit `__all__` list in `__init__.py` enumerates public API (100+ items)
- Private items not exported (prefixed with `_`)
- Public API aliases defined at package level (e.g., `data_input = IOOperations`)

**Barrel Files:**
- Not used; imports are explicit
- Main package init imports from submodules and re-exports

**Module Boundaries:**
- Clear separation of concerns: I/O (`io.py`), range operations (`range_op.py`), pileup (`pileup_op.py`)
- Shared utilities in dedicated modules: `utils.py`, `predicate_translator.py`, `sql.py`
- Configuration in `constants.py` and `context.py`
- Metadata handling centralized in `_metadata.py`

## Python Version & Type Checking

**Target Python Version:** 3.10+ (from `pyproject.toml`)

**Type Checking:**
- mypy enabled with minimal overrides (ignores `polars.utils.udfs`)
- Type hints used consistently throughout codebase
- PyO3 provides type stubs for Rust bindings

## Rust-Python Integration

**PyO3 Conventions:**
- Functions exposed to Python decorated with `#[pyfunction]`
- Signature decorators for flexible argument handling: `#[pyo3(signature = (...))]`
- Error conversion: Rust `Result<T, E>` → Python exceptions via `.map_err()`
- GIL management: `py.allow_threads()` for CPU-bound work, GIL held during FFI operations
- Arrow FFI used for zero-copy data exchange: `PyArrowType<ArrowArrayStreamReader>`

**Binary Compilation:**
- Maturin build backend in `pyproject.toml`
- Rust module name: `polars_bio` (matches crate name)
- Compiled as C dynamic library (`cdylib`)
