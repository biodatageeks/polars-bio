# Coding Conventions

**Analysis Date:** 2026-03-20

## Naming Patterns

**Files:**
- Python modules: lowercase with underscores (e.g., `predicate_translator.py`, `range_op_helpers.py`)
- Test files: `test_*.py` prefix (e.g., `test_io_bam.py`, `test_predicate_pushdown.py`)
- Rust files: lowercase snake_case (e.g., `pileup.rs`, `operation.rs`)

**Functions:**
- Lowercase with underscores: `_validate_tag_type_hints()`, `_cleanse_fields()`, `_quote_sql_identifier()`
- Private helpers prefixed with single underscore: `_lazy_scan()`, `_overlap_source()`, `_extract_column_names_from_expr()`
- Public methods use camelCase in class methods when wrapping APIs: `read_bam()`, `scan_vcf()`, `register_gff()`

**Classes:**
- PascalCase: `IOOperations`, `IntervalOperations`, `PileupOperations`, `PyBioSessionContext`
- Exception classes end with `Error`: `CoordinateSystemMismatchError`, `MissingCoordinateSystemError`, `PredicateTranslationError`

**Variables:**
- Lowercase with underscores: `projection_pushdown`, `predicate_pushdown`, `tag_type_hints`, `zero_based`
- Module-level constants: `UPPERCASE_WITH_UNDERSCORES` (e.g., `LEFT_TABLE`, `DEFAULT_COLUMN_NAMES`, `_VALID_SAM_TYPE_CODES`)
- Private module state: `_ACTIVE_STRING_COLS`, `_ACTIVE_UINT32_COLS`, `_ACTIVE_FLOAT32_COLS`
- Dictionary keys for format-based lookups use string keys (not enums): `_FORMAT_COLUMN_TYPES = {"Bam": ..., "Vcf": ..., "Gff": ...}` (PyO3 InputFormat is not hashable)

**Types:**
- TypeVar and generic types follow standard conventions: `Union[list[str], None]`, `Optional[bool]`
- Type hints use `from __future__ import annotations` for forward references

## Code Style

**Formatting:**
- Black formatter configured with implicit settings (line length 88)
- isort for import sorting with `--profile black`
- Applied via pre-commit hooks

**Linting:**
- Ruff configuration present but currently commented out in `.pre-commit-config.yaml`
- Black and isort are active linting/formatting tools
- Manual compliance with pre-commit hooks on `check-ast`, `mixed-line-ending`, `trailing-whitespace`, `check-case-conflict`

**Formatting Command:**
```bash
# Pre-commit enforces these:
black .                           # Format code
isort . --profile black           # Sort imports
cargo fmt                         # Rust formatting
cargo check                       # Rust compilation check
```

## Import Organization

**Order:**
1. Standard library imports (`import logging`, `import json`, `from pathlib import Path`)
2. Third-party imports (`import polars as pl`, `import pytest`, `from datafusion import DataFrame`)
3. Local/relative imports (`from . import polars_ext`, `from ._metadata import ...`)

**Examples:**
```python
# polars_bio/io.py
import logging
import weakref as _weakref
from typing import Dict, Iterator, Optional, Union

import polars as pl

logger = logging.getLogger(__name__)
from datafusion import DataFrame
from polars.io.plugins import register_io_source
from tqdm.auto import tqdm

from polars_bio.polars_bio import (
    BamReadOptions,
    BamWriteOptions,
    ...
)

from ._metadata import get_vcf_metadata, set_coordinate_system, set_vcf_metadata
from .context import _resolve_zero_based, ctx
from .predicate_translator import (
    BAM_INT32_COLUMNS,
    BAM_STRING_COLUMNS,
    ...
)
```

**Path Aliases:**
- Relative imports preferred for internal modules
- Absolute imports from `polars_bio.polars_bio` (the PyO3-compiled Rust module)
- No path aliases configured in pyproject.toml

## Error Handling

**Patterns:**
- Custom exceptions inherit from `Exception` and are defined in `polars_bio/exceptions.py`
- Exceptions have detailed docstrings with examples: `CoordinateSystemMismatchError`, `MissingCoordinateSystemError`
- Try-except blocks catch broad exceptions during fallback operations, with silent fallback to client-side filtering:
  ```python
  try:
      query_df = query_df.filter(datafusion_predicate)
      datafusion_predicate_applied = True
  except Exception as e:
      datafusion_predicate_applied = False
      # Fallback to Python-level filtering
  ```
- PyO3 functions raise `PyValueError` and `PyRuntimeError` for Rust-Python boundary errors
- Validation functions raise `ValueError` with descriptive messages: `_validate_tag_type_hints()` checks format and valid type codes

**Validation Pattern:**
```python
def _validate_tag_type_hints(tag_type_hints: list[str]) -> None:
    """Validate tag_type_hints format before passing to Rust."""
    for hint in tag_type_hints:
        parts = hint.split(":")
        if len(parts) != 2 or len(parts[0]) != 2 or not parts[1]:
            raise ValueError(f"Invalid tag_type_hint '{hint}': expected 'TAG:TYPE' format...")
        type_code = parts[1]
        if type_code not in _VALID_SAM_TYPE_CODES:
            raise ValueError(f"Invalid type code '{type_code}'...")
```

## Logging

**Framework:** Python's built-in `logging` module

**Setup Pattern:**
```python
# polars_bio/logging.py
import logging

logging.basicConfig()
root_logger = logging.getLogger()
root_logger.setLevel(logging.WARN)
logger = logging.getLogger("polars_bio")
logger.setLevel(logging.WARN)

def set_loglevel(level: str):
    """Set log level for logger and root logger."""
    level = level.lower()
    if level == "debug":
        logger.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
        ...
```

**Usage Pattern:**
```python
# In modules
from polars_bio.logging import logger
logger = logging.getLogger(__name__)

# In Rust modules
use log::{debug, error, info};
```

**When to Log:**
- Info level: Major operations like file registration, table creation
- Debug level: Detailed operation flow, intermediate states
- Warn level: Default; deprecated patterns, fallback behavior

**Loglevel Control:**
```python
import polars_bio as pb
pb.set_loglevel("info")  # Must be set as first step after import
```

## Comments

**When to Comment:**
- Algorithm explanation: When predicate translation or optimization logic is non-obvious
- Bug workarounds: Mark with inline comments (though project uses `# FIXME` in pre-commit config)
- Complex parsing logic: Explain parsing approach (e.g., PyO3 string manipulation in `_strip_polars_wrapping()`)

**Module-Level Docstrings:**
Comprehensive module docstrings explaining purpose and design:
```python
"""
Polars to DataFusion predicate translator for bio-format table providers.

This module converts Polars expressions to DataFusion expressions for predicate pushdown optimization.
Uses the DataFusion Python DataFrame API instead of SQL string construction for better type safety.

Supports BAM/SAM/CRAM, VCF, and GFF formats. Each format defines its own known column types;
unknown columns (BAM tags, VCF INFO/FORMAT fields, GFF attribute fields) are handled permissively,
allowing all operators and letting DataFusion type-check at execution time.
"""
```

**JSDoc/TSDoc:**
- Python docstrings use Google-style format with sections: Description, Parameters, Returns, Raises, Examples
- Rust documentation uses `///` for public items with markdown formatting
- Example in docstrings are enclosed in markdown code blocks with language specification

**Docstring Example:**
```python
def scan_fasta(
    path: str,
    chunk_size: int = 8,
    concurrent_fetches: int = 1,
    ...
) -> pl.LazyFrame:
    """
    Lazily read a FASTA file into a LazyFrame.

    Parameters:
        path: The path to the FASTA file.
        chunk_size: The size in MB of a chunk when reading from an object store. Default is 8 MB.
        concurrent_fetches: [GCS] The number of concurrent fetches. Default is 1.
        ...

    !!! Example
        ```shell
        wget https://www.ebi.ac.uk/ena/browser/api/fasta/BK006935.2?download=true -O /tmp/test.fasta
        ```

        ```python
        import polars_bio as pb
        pb.scan_fasta("/tmp/test.fasta").limit(1).collect()
        ```
    """
```

## Function Design

**Size:** Functions are typically 20-50 lines; longer functions broken into smaller helpers:
- `_overlap_source()` in `polars_bio/utils.py` is 200+ lines but handles complete predicate+projection+fallback pipeline
- Most utility functions are 10-30 lines

**Parameters:**
- Type hints on all parameters and return types
- Optional parameters use `Union[Type, None]` or `Optional[Type]` with defaults
- Common pattern: `projection_pushdown: bool = True`, `predicate_pushdown: bool = False`
- Static methods on operation classes accept multiple input types: `Union[str, pl.DataFrame, pl.LazyFrame, "pd.DataFrame"]`

**Return Values:**
- Explicit return type hints on all functions
- Functions returning DataFrames may return LazyFrames or DataFrames depending on input
- Fallible operations return tuples of (result, applied_flag) for debugging: `datafusion_projection_applied = True/False`

**Context Management:**
- Temporary module state protected with try/finally blocks:
  ```python
  global _ACTIVE_STRING_COLS, _ACTIVE_UINT32_COLS, _ACTIVE_FLOAT32_COLS
  _ACTIVE_STRING_COLS = string_cols
  try:
      return _translate_polars_expr(predicate)
  finally:
      _ACTIVE_STRING_COLS = None
  ```

## Module Design

**Exports:**
- All public APIs defined in `polars_bio/__init__.py` with explicit `__all__` list
- Class methods statically exposed as module-level functions: `read_bam = data_input.read_bam`
- Aliases for backward compatibility: `data_input`, `data_processing`, `pileup_operations`, `range_operations`

**Barrel Files:**
- Not used; imports are explicit

**Organization:**
- `polars_bio/io.py`: All I/O operations (read_*, scan_*, write_*, sink_*, register_*)
- `polars_bio/range_op.py`: Genomic interval operations (overlap, nearest, merge, etc.)
- `polars_bio/pileup_op.py`: Depth/pileup operations
- `polars_bio/sql.py`: SQL query execution and table registration
- `polars_bio/predicate_translator.py`: Polars-to-DataFusion expression translation
- `polars_bio/_metadata.py`: Coordinate system metadata management
- `polars_bio/exceptions.py`: Custom exception definitions
- `polars_bio/logging.py`: Logging configuration

## Rust Conventions

**Code Style:**
- Rust formatting enforced via `cargo fmt` in pre-commit hooks
- Edition 2021

**Error Handling:**
- Functions return `PyResult<T>` for PyO3 interop
- Internal errors wrapped in `PyValueError::new_err()` or `PyRuntimeError::new_err()`
- GIL handling: Explicit `py.allow_threads()` for CPU-bound work

**Naming:**
- Module names: lowercase snake_case (`mod context`, `mod operation`, `mod pileup`)
- Function names: lowercase snake_case (`register_frame_from_batches()`)
- Constants: UPPERCASE (`LEFT_TABLE`, `DEFAULT_COLUMN_NAMES`)

---

*Convention analysis: 2026-03-20*
