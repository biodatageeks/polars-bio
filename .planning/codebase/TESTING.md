# Testing Patterns

**Analysis Date:** 2026-03-20

## Test Framework

**Runner:**
- pytest 8.3.3+
- Config file: `pyproject.toml` (tool.poetry section references pytest)

**Assertion Library:**
- pytest built-in assertions with custom `pytest.assume()` fixture for soft assertions

**Run Commands:**
```bash
python -m pytest tests/test_io_*.py -v           # Run IO tests
python -m pytest tests/test_predicate_pushdown.py -v  # Run specific test file
python -m pytest -k "test_overlap" -v            # Run tests matching pattern
python -m pytest tests/ -v --tb=short            # All tests with short traceback
```

**Coverage:**
```bash
python -m pytest --cov=polars_bio --cov-report=html tests/
```

## Test File Organization

**Location:**
- Test files co-located in `tests/` directory at project root
- Test files organized by functionality: `test_io_bam.py`, `test_io_fastq.py`, `test_io_indexed.py`, `test_predicate_pushdown.py`, etc.
- Expected test data in `tests/_expected.py` (constants and fixtures)

**Naming:**
- Test files: `test_*.py` prefix
- Test classes: `Test*` prefix (e.g., `TestIOBAM`, `TestFastq`, `TestParallelFastq`)
- Test methods: `test_*` prefix (e.g., `test_count`, `test_fields`, `test_register`)

**File Structure:**
```
tests/
├── _expected.py              # Expected data and fixtures
├── conftest.py               # pytest configuration and shared fixtures
├── test_io_bam.py           # BAM I/O tests
├── test_io_fastq.py         # FASTQ I/O tests
├── test_io_indexed.py       # Indexed format tests
├── test_predicate_pushdown.py # Predicate translation tests
├── test_polars.py           # Polars-specific operation tests
├── test_pileup.py           # Pileup/depth operation tests
├── test_coordinate_system_metadata.py  # Metadata tests
└── ...                      # Other test modules
```

## Test Structure

**Suite Organization:**
Tests organize around functionality using class-based test grouping:

```python
class TestIOBAM:
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")

    def test_count(self):
        assert len(self.df) == 2333

    def test_fields(self):
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123

    def test_register(self):
        pb.register_bam(f"{DATA_DIR}/io/bam/test.bam", "test_bam")
        count = pb.sql("select count(*) as cnt from test_bam").collect()
        assert count["cnt"][0] == 2333
```

**Patterns:**

1. **Class-level setup:** Data loaded once per test class for efficiency
   ```python
   class TestFastq:
       def test_count(self):
           assert (
               pb.scan_fastq(f"{DATA_DIR}/io/fastq/example.fastq.bgz")
               .count()
               .collect()["name"][0]
               == 200
           )
   ```

2. **Lazy vs eager testing:** Tests check both `.collect()` (eager) and `.limit().collect()` (lazy) paths
   ```python
   class TestParallelFastq:
       def test_read_parallel_fastq_with_limit(self):
           lf = pb.scan_fastq(f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz").limit(10)
           print(lf.explain())
           df = lf.collect()
           assert len(df) == 10
   ```

3. **Predicate testing:** SQL generation and end-to-end validation separated
   ```python
   class TestPredicatePushdownSQLGeneration:
       def test_simple_string_equality(self):
           predicate = pl.col("chrom") == "chr22"
           sql_where = _build_sql_where_from_predicate_safe(predicate)
           assert sql_where == "\"chrom\" = 'chr22'"

   class TestPredicatePushdownEndToEnd:
       def test_pushdown_with_registered_table(self):
           pb.register_gff(f"{DATA_DIR}/io/gff/annotation.gff3", "gff_table")
           result = pb.sql("select * from gff_table where chrom='chrY'").collect()
           # Verify results match predicate
   ```

## Mocking

**Framework:** pytest's built-in monkeypatch fixture (no external mocking library used)

**Patterns:**

1. **Fixture-based data setup** (preferred):
   ```python
   @pytest.fixture(scope="module")
   def sample_vcf_data():
       # Set up test data once per module
       df = pb.read_vcf(f"{DATA_DIR}/io/vcf/test.vcf")
       return df
   ```

2. **Temporary file handling** using temporary directories:
   ```python
   import tempfile
   from pathlib import Path

   with tempfile.TemporaryDirectory() as tmpdir:
       output_path = Path(tmpdir) / "output.vcf"
       pb.write_vcf(df, str(output_path))
       # Verify written file
   ```

**What to Mock:**
- External file system operations (when testing I/O without actual files)
- Environment variables (via monkeypatch)
- Session context options

**What NOT to Mock:**
- Core DataFusion operations (test with real data)
- Polars operations (test against real Polars)
- Actual file I/O for I/O tests (use real test data)

## Fixtures and Factories

**Test Data:**
Stored in `tests/_expected.py`:

```python
# _expected.py
from pathlib import Path
import polars as pl

DATA_DIR = Path(__file__).parent / "data"

# Polars DataFrames with metadata
PL_DF1 = pl.DataFrame({
    "contig": ["chr1", "chr2"],
    "pos_start": [1000, 2000],
    "pos_end": [2000, 3000],
})
PL_DF1.config_meta.set(coordinate_system_zero_based=False)

# Expected results
PL_DF_OVERLAP = pl.DataFrame({...})
PL_DF_NEAREST = pl.DataFrame({...})
```

**Location:**
- `tests/_expected.py`: Contains test data and expected results
- `tests/data/`: Actual test files (BAM, VCF, FASTQ, etc.)
- Test fixtures accessed via `_expected.DATA_DIR` constant

**Fixture Pattern:**
```python
@pytest.fixture(scope="module")
def annotation_file():
    """Load annotation once per test module."""
    return pb.read_gff(f"{DATA_DIR}/io/gff/annotation.gff3")

@pytest.fixture(scope="class")
def tmp_output_dir():
    """Provide temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
```

## Coverage

**Requirements:** No explicit coverage threshold enforced in CI

**View Coverage:**
```bash
python -m pytest --cov=polars_bio --cov-report=html tests/
# View coverage report in htmlcov/index.html
```

## Test Types

**Unit Tests:**
- Scope: Individual functions and methods
- Examples: `test_simple_string_equality()` (predicate translation), `test_validate_tag_type_hints()` (validation)
- Approach: Test with multiple input cases using parameterization
- Location: `tests/test_predicate_pushdown.py`, `tests/test_custom_tag_inference.py`

**Integration Tests:**
- Scope: I/O + operations + metadata pipeline
- Examples: `TestIOBAM.test_register()` (read → register → SQL query), `TestParallelFastq.test_read_parallel_fastq()`
- Approach: Real file I/O, real DataFusion execution
- Location: Most test files (`test_io_*.py`, `test_polars.py`)

**End-to-End Tests:**
- Scope: Full user workflows
- Examples: `TestCoordinateSystemMetadata.*` (coordinate system tracking through operations), `TestPredicatePushdownEndToEnd.*`
- Approach: Real data, real SQL queries, coordinate system validation
- Location: `tests/test_coordinate_system_metadata.py`, `tests/test_predicate_pushdown.py`

**E2E/Regression Tests:**
- Framework: pytest
- Examples: `test_optimization_bug_fix.py` (tests for previously fixed bugs), `test_filter_select_attributes_bug_fix.py`
- Location: Named `test_*_bug_fix.py` or `test_optimization_*.py`

## Common Patterns

**Parametrized Testing:**
```python
@pytest.mark.parametrize("partitions", [1, 2, 3, 4])
def test_read_parallel_fastq(self, partitions):
    pb.set_option("datafusion.execution.target_partitions", str(partitions))
    df = pb.read_fastq(f"{DATA_DIR}/io/fastq/sample_parallel.fastq.bgz")
    assert len(df) == 2000
```

**Soft Assertions:**
Custom `pytest.assume()` fixture allows multiple assertions to run before test failure:

```python
# conftest.py provides pytest.assume()
@contextlib.contextmanager
def assume():
    try:
        yield
    except AssertionError as e:
        failures = getattr(_local, "failures", None)
        if failures is not None:
            failures.append(str(e))
        else:
            raise

# Usage:
def test_multiple_assertions():
    with pytest.assume():
        assert condition1
    with pytest.assume():
        assert condition2
    # Test fails at teardown with summary of both failures
```

**Async Testing:**
Not used; all operations are synchronous or use Polars' lazy evaluation.

**Error Testing:**
Tests verify exceptions are raised correctly:

```python
def test_missing_coordinate_system_raises():
    """Test that missing metadata raises appropriate error."""
    df = pb.read_vcf(f"{DATA_DIR}/io/vcf/test.vcf")
    # Clear metadata
    if hasattr(df, "config_meta"):
        df.config_meta.set(coordinate_system_zero_based=None)

    with pytest.raises(MissingCoordinateSystemError):
        pb.overlap(df, df)

def test_coordinate_mismatch_raises():
    """Test that coordinate system mismatch raises."""
    df1 = pb.read_vcf(f"{DATA_DIR}/io/vcf/test1.vcf")  # 0-based
    df2 = pb.read_vcf(f"{DATA_DIR}/io/vcf/test2.vcf")  # 1-based

    with pytest.raises(CoordinateSystemMismatchError):
        pb.overlap(df1, df2)
```

**Test Isolation:**
- Class-level data setup shared across methods (efficient)
- Fixtures with `scope="module"` for expensive setup
- Fixtures with `scope="function"` for isolated tests
- Autouse fixture `_assume_collector()` resets soft assertion state per test

**Test Data Versioning:**
- Real genomic test files in `tests/data/` subdirectories (io/bam, io/vcf, io/fastq, etc.)
- Test data is committed to repo and not generated dynamically
- Expected results hardcoded in `_expected.py` for reproducibility

## Rust Tests

**Framework:** cargo test (inline tests using `#[cfg(test)]`)

**Location:**
- Rust tests defined inline with `#[cfg(test)]` modules
- Example: `src/write.rs` has a test module
- Run: `cargo test`

**Pattern:**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_name() {
        // Test implementation
    }
}
```

## Running Tests

**All tests:**
```bash
python -m pytest tests/ -v
```

**Specific test file:**
```bash
python -m pytest tests/test_io_bam.py -v
```

**Specific test class:**
```bash
python -m pytest tests/test_io_bam.py::TestIOBAM -v
```

**Specific test method:**
```bash
python -m pytest tests/test_io_bam.py::TestIOBAM::test_count -v
```

**Tests matching pattern:**
```bash
python -m pytest -k "test_overlap" -v
```

**With output:**
```bash
python -m pytest tests/ -v -s  # Show print statements
```

**With debugging:**
```bash
python -m pytest tests/ -v --pdb  # Drop into debugger on failure
```

---

*Testing analysis: 2026-03-20*
