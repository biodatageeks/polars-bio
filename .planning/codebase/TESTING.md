# Testing Patterns

**Analysis Date:** 2026-03-20

## Test Framework

**Runner:**
- pytest 8.3.3+ (from `pyproject.toml`)
- Config: `pyproject.toml` only (no pytest.ini or setup.cfg)
- Auto-discovery: standard pytest pattern (tests in `tests/` directory, test_*.py files)

**Assertion Library:**
- pytest built-in assertions
- Polars testing utilities: `pl.testing` module (exposed in conftest for compatibility)
- Custom soft-assertion: `pytest.assume()` context manager (implemented in conftest.py)

**Coverage:**
- Tool: pytest-cov (^6.0.0)
- No explicit coverage target enforced in config

**Run Commands:**
```bash
python -m pytest tests/test_io_*.py -v              # Run IO tests with verbose output
python -m pytest tests/ -v                          # Run all tests
python -m pytest tests/test_io_bam.py::TestIOBAM::test_count -v  # Single test
pytest --cov=polars_bio tests/                      # Coverage report (requires pytest-cov)
```

## Test File Organization

**Location:**
- Tests co-located in separate `tests/` directory (not alongside source)
- Organized by feature area: `test_io_bam.py`, `test_io_vcf.py`, `test_predicate_pushdown.py`, `test_overlap_algorithms.py`
- Path: `/Users/mwiewior/research/git/polars-bio/tests/`

**Naming:**
- Prefix pattern: `test_*.py`
- Descriptive names describing what's tested: `test_io_bam.py` (I/O operations on BAM), `test_predicate_pushdown.py` (predicate translation)
- Test methods use `test_` prefix: `test_count()`, `test_fields()`, `test_overlap_count()`

**Data/Fixtures Directory:**
- Test data: `tests/data/` directory
- Shared test constants and fixtures: `tests/_expected.py` (pandas DataFrames, paths, expected results)
- Configuration: `tests/conftest.py` (pytest fixtures, soft-assertion framework)

## Test Structure

**Suite Organization:**
```python
class TestIOBAM:
    """Group related tests in classes for organization."""
    df = pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")  # Class-level setup

    def test_count(self):
        """Test individual assertions."""
        assert len(self.df) == 2333

    def test_fields(self):
        """Test multiple assertions in one test."""
        assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
        assert self.df["flags"][3] == 1123
        assert self.df["cigar"][4] == "101M"
```

**Patterns:**
- Setup at class level (shared across methods): `df = pb.read_bam(...)`
- Per-test setup rarely needed (data is read once, reused)
- Teardown: rare, used for cleanup (see tmp_path fixture usage in `test_bam_scan_indexed_no_coor_only_records`)
- Grouped tests by class for related functionality: `TestIOBAM`, `TestIOSAM`, `TestIOVCF`

## Test Structure Examples

**Basic Assertion Pattern:**
```python
def test_count(self):
    """Test record count."""
    assert len(self.df) == 2333
```

**Multiple Field Assertions:**
```python
def test_fields(self):
    """Test field values at specific indices."""
    assert self.df["name"][2] == "20FUKAAXX100202:1:22:19822:80281"
    assert self.df["flags"][3] == 1123
    assert self.df["cigar"][4] == "101M"
```

**DataFrame Equality:**
```python
def test_overlap_schema_rows(self):
    """Test result matches expected DataFrame."""
    result = self.result_frame.sort(by=self.result_frame.columns)
    assert self.expected.equals(result)
```

**Parametrized Tests (set comprehension):**
```python
def test_simple_numeric_comparison(self):
    """Test multiple predicates via iteration."""
    test_cases = [
        (pl.col("start") > 100000, '"start" > 100000'),
        (pl.col("start") < 500000, '"start" < 500000'),
        (pl.col("start") >= 100000, '"start" >= 100000'),
    ]
    for predicate, expected_sql in test_cases:
        sql_where = _build_sql_where_from_predicate_safe(predicate)
        assert sql_where == expected_sql
```

**Exception Testing:**
```python
def test_missing_coordinate_system_error(self):
    """Test exception is raised in strict mode."""
    pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)
    try:
        with pytest.raises(MissingCoordinateSystemError):
            pb.overlap(df1, df2, ...)
    finally:
        pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)
```

**Temporary File Testing:**
```python
def test_bam_scan_indexed_no_coor_only_records(self, tmp_path):
    """Test with temporary BAM files."""
    header = {"HD": {"VN": "1.6", "SO": "coordinate"}, ...}
    unsorted_path = str(tmp_path / "no_coor.unsorted.bam")
    sorted_path = str(tmp_path / "no_coor.sorted.bam")

    with pysam.AlignmentFile(unsorted_path, "wb", header=header) as out:
        # Write records
        out.write(record)

    pysam.sort("-o", sorted_path, unsorted_path)
    pysam.index(sorted_path)
    df = pb.scan_bam(sorted_path).collect()
    assert len(df) == 2
```

## Mocking

**Framework:** No explicit mocking library detected; pysam used for creating test BAM files in-memory

**Patterns:**
- Real files used preferentially: `pb.read_bam(f"{DATA_DIR}/io/bam/test.bam")`
- File creation via libraries: pysam for BAM/SAM records
- Temporary files: pytest `tmp_path` fixture for ephemeral test data

**What to Mock:**
- External file I/O: use real test data files in `tests/data/`
- Temporary file operations: use pytest `tmp_path` fixture
- External API calls: not applicable (local bioinformatics library)

**What NOT to Mock:**
- DataFrame operations (Polars/datafusion is stable)
- File format reading (test against real files)
- Rust bindings (compiled into wheel, tested via Python API)

## Fixtures and Factories

**Test Data:**
- Path setup: `DATA_DIR = TEST_DIR / "data"` in `_expected.py`
- Pandas DataFrames created manually with expected results:

```python
PD_DF_OVERLAP = pd.DataFrame({
    "contig_1": ["chr1", "chr1", ...],
    "pos_start_1": [150, 150, ...],
    "pos_end_1": [250, 250, ...],
    ...
}).astype({
    "pos_start_1": "int64",
    "pos_end_1": "int64",
})
```

- Polars DataFrames created from Pandas: `PL_DF_OVERLAP = pl.from_pandas(PD_DF_OVERLAP)`
- CSV test data: `DF_OVER_PATH1 = f"{DATA_DIR}/overlap/reads.csv"` loaded via pandas
- Parquet test data: `BIO_DF_PATH1 = f"{DATA_DIR}/exons/*.parquet"`

**Location:**
- Fixtures: `tests/_expected.py` (not pytest fixtures, but test data constants)
- Conftest: `tests/conftest.py` (pytest.assume() shim, polars.testing exposure)

**Pytest Fixtures Used:**
- `tmp_path`: pytest built-in for temporary directories (used in BAM creation tests)
- `autouse=True` fixture for assumption collection (in conftest)

## Coverage

**Requirements:** None enforced (target TBD)

**View Coverage:**
```bash
pytest --cov=polars_bio --cov-report=html tests/
# View: htmlcov/index.html
```

## Test Types

**Unit Tests:**
- Scope: Individual functions and classes
- Approach: Test method, field access, error conditions
- Examples: `test_count()` validates record count, `test_fields()` validates field values
- File: `test_io_bam.py` has 20+ unit tests for BAM I/O
- Assertion style: direct equality checks on small data

**Integration Tests:**
- Scope: Multi-component workflows (I/O → predicate pushdown → result)
- Approach: Read file, apply filters, verify output matches expected
- Examples: `test_predicate_pushdown.py` tests SQL generation and end-to-end filtering
- File: `test_projection_pushdown.py`, `test_predicate_pushdown.py`
- Assertion style: DataFrame equality with `.equals()` or sorted comparison

**End-to-End Tests:**
- Scope: Full user workflows
- Approach: Range operations (overlap, nearest, merge) with real data
- Examples: `test_overlap.py`, `test_nearest.py` with Pandas/Polars DataFrames
- File: `test_polars.py`, `test_pandas.py` (inferred from imports in test files)
- Assertion style: result count, schema validation, sorted DataFrame comparison

**Format-Specific Tests:**
- BAM: `test_io_bam.py` (core fields, tags, SQL queries, indexed scans)
- VCF: `test_vcf_format_columns.py` (format columns)
- GFF/GTF: `test_predicate_pushdown.py` (coordinate system, attribute fields)
- FASTQ: `test_io_fastq.py` (assumed from file listing)

**E2E Tests:**
- No dedicated E2E framework (pytest sufficient)
- User-facing workflows tested via integration tests with real file data

## Common Patterns

**Async Testing:**
- No async tests found (I/O is synchronous)
- Tokio runtime used in Rust, but exposed synchronously to Python

**Error Testing:**
```python
def test_missing_coordinate_system_error(self):
    """Test exception is raised."""
    with pytest.raises(MissingCoordinateSystemError):
        pb.overlap(df1, df2, ...)
```

**Parametrization Pattern (Manual):**
```python
# Instead of @pytest.mark.parametrize, use list iteration
test_cases = [
    (input1, expected1),
    (input2, expected2),
]
for input_val, expected in test_cases:
    assert transform(input_val) == expected
```

**Soft Assertions (Multiple failures per test):**
```python
# In conftest: pytest.assume() context manager
with pytest.assume():
    assert condition1
with pytest.assume():
    assert condition2
# Test fails at end with all failures summarized
```

**State Reset Patterns:**
- Context managers for option changes:
```python
pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, True)
try:
    # test code
finally:
    pb.set_option(POLARS_BIO_COORDINATE_SYSTEM_CHECK, False)
```

**DataFrame Assertion:**
```python
# Column presence
assert "name" in df.columns
assert len(df.columns) == 15

# Row count
assert len(df) == 2333

# Value equality
assert df["name"][2] == "expected_value"

# Null counts
assert df["CB"].null_count() == 0

# Set equality
assert set(df["CB"].to_list()) == {"CELL1", "CELL2"}

# DataFrame equality
result = result_frame.sort(by=result_frame.columns)
assert expected.equals(result)
```

## Test Organization Summary

**By module:**
- `test_io_*.py`: Files for each format (BAM, VCF, GFF, FASTQ, etc.)
- `test_predicate_pushdown.py`: SQL predicate translation
- `test_projection_pushdown.py`: Column projection optimization
- `test_overlap_algorithms.py`: Range operations
- `test_warnings.py`: Error conditions and coordinate system metadata
- `test_polars.py`: Polars DataFrame integration
- `test_pandas.py`: Pandas DataFrame integration (inferred)

**By concern:**
- Core functionality: `test_io_*.py` (read/write/register)
- Optimization: `test_predicate_pushdown.py`, `test_projection_pushdown.py`
- Integration: `test_polars.py`, `test_pandas.py`, `test_user_scenario.py`
- Regression/edge cases: `test_*_bug_fix.py` files (e.g., `test_optimization_bug_fix.py`)
