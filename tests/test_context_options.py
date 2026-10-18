import re
from pathlib import Path

import polars as pl
import pytest

import polars_bio as pb
from polars_bio.context import Context


def test_set_option_accepts_numeric_values():
    key = "datafusion.execution.target_partitions"
    original = pb.get_option(key)
    try:
        pb.set_option(key, 2)
        assert pb.get_option(key) == "2"
    finally:
        if original is not None:
            pb.set_option(key, original)


# --- datafusion.runtime.* options (issue #484) ---------------------------

MEMORY_LIMIT = "datafusion.runtime.memory_limit"
TEMP_DIRECTORY = "datafusion.runtime.temp_directory"
GROUP_BY_QUERY = (
    "SELECT value % 1000000 AS k, count(*) AS c "
    "FROM generate_series(1, 2000000) GROUP BY k"
)


@pytest.fixture
def reset_runtime():
    """Restore runtime options so the singleton context does not leak them."""
    yield
    pb.sql(f"RESET {MEMORY_LIMIT}").collect()
    pb.sql(f"RESET {TEMP_DIRECTORY}").collect()


def _df_setting(name):
    return pb.sql(
        f"SELECT value FROM information_schema.df_settings WHERE name = '{name}'"
    ).collect()["value"][0]


def test_runtime_memory_limit_reaches_runtime_env(reset_runtime):
    # Previously panicked in the datafusion-python SessionConfig mirror.
    pb.set_option(MEMORY_LIMIT, "64M")
    assert _df_setting(MEMORY_LIMIT) == "64M"
    assert pb.get_option(MEMORY_LIMIT) == "64M"
    assert Context().ctx.runtime_memory_limit() == 64 * 1024 * 1024


def test_runtime_memory_limit_get_option_tracks_sql_reset(reset_runtime):
    pb.set_option(MEMORY_LIMIT, "64M")
    pb.sql(f"RESET {MEMORY_LIMIT}").collect()
    assert pb.get_option(MEMORY_LIMIT) == "unlimited"
    assert Context().ctx.runtime_memory_limit() is None


def test_runtime_memory_limit_is_enforced(reset_runtime):
    pb.set_option(MEMORY_LIMIT, "1K")
    with pytest.raises(Exception, match="Resources exhausted"):
        pb.sql(GROUP_BY_QUERY).collect()


def test_runtime_memory_limit_spills(reset_runtime):
    pb.set_option(MEMORY_LIMIT, "2M")
    plan = pb.sql(f"EXPLAIN ANALYZE {GROUP_BY_QUERY}").collect()
    metrics = plan[plan.columns[-1]][0]
    spill_counts = [int(n) for n in re.findall(r"spill_count=(\d+)", metrics)]
    assert spill_counts, f"no spill_count metric in EXPLAIN ANALYZE output:\n{metrics}"
    assert max(spill_counts) > 0
    assert pb.sql(GROUP_BY_QUERY).collect().height == 1_000_000


@pytest.mark.parametrize(
    "key, value",
    [
        (MEMORY_LIMIT, "64X"),
        ("datafusion.runtime.no_such_option", "1"),
        ("datafusion.execution.no_such_option", "1"),
    ],
)
def test_invalid_datafusion_option_raises_value_error(key, value, reset_runtime):
    with pytest.raises(ValueError, match="Failed to set option"):
        pb.set_option(key, value)
    assert pb.get_option(MEMORY_LIMIT) == "unlimited"


def test_extension_options_stay_lenient():
    key = "bio.issue_484_unregistered_option"
    pb.set_option(key, "x")
    assert pb.get_option(key) == "x"


def test_runtime_temp_directory(tmp_path, reset_runtime):
    pb.set_option(TEMP_DIRECTORY, str(tmp_path))
    # DataFusion creates its own `datafusion-XXXX` directory inside the path.
    assert pb.get_option(TEMP_DIRECTORY).startswith(str(tmp_path))


def _py_ctx_spill_dirs(root):
    """datafusion-python working dirs nested in the main context's ones."""
    return list(Path(root).glob("datafusion-*/datafusion-*"))


@pytest.mark.parametrize("via_sql", [False, True])
def test_runtime_temp_directory_applies_to_interval_ctx(
    tmp_path, via_sql, reset_runtime
):
    from polars_bio.interval_op_helpers import get_py_ctx

    old_dir, new_dir = tmp_path / "old", tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()
    pb.set_option(TEMP_DIRECTORY, str(old_dir))
    if via_sql:
        pb.sql(f"SET {TEMP_DIRECTORY} = '{new_dir}'").collect()
    else:
        pb.set_option(TEMP_DIRECTORY, str(new_dir))
    py_ctx = get_py_ctx()  # holds its disk manager's working dir open
    assert _py_ctx_spill_dirs(new_dir)
    assert not _py_ctx_spill_dirs(old_dir)
    del py_ctx


def _intervals(n):
    return pl.DataFrame(
        {
            "chrom": ["chr1"] * n,
            "start": list(range(0, 10 * n, 10)),
            "end": list(range(5, 10 * n + 5, 10)),
        }
    )


def test_runtime_memory_limit_applies_to_count_overlaps(reset_runtime):
    # naive_query=False runs on a separate datafusion-python SessionContext.
    df1, df2 = _intervals(200_000), _intervals(200_000)
    pb.set_option(MEMORY_LIMIT, "1K")
    with pytest.raises(Exception, match="Resources exhausted"):
        pb.count_overlaps(df1, df2, naive_query=False, output_type="polars.DataFrame")
    pb.sql(f"RESET {MEMORY_LIMIT}").collect()
    result = pb.count_overlaps(
        df1, df2, naive_query=False, output_type="polars.DataFrame"
    )
    assert result.height == 200_000
