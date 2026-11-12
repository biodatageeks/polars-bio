"""The `datafusion.bio.progress_bar` session option (issue #504)."""

import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import polars as pl
import pytest
from tqdm.auto import tqdm

import polars_bio as pb

REPO_ROOT = Path(__file__).resolve().parents[1]
BAM_PATH = "tests/data/io/bam/test.bam"
BED_PATH = "tests/data/io/bed/test.bed"

# tqdm prints the rate with the unit, e.g. `10rows [00:00, 5120.63rows/s]`.
BAR_MARKER = "rows/s"


@pytest.fixture(autouse=True)
def restore_progress_bar():
    """Restore the option so the singleton context does not leak it."""
    original = pb.get_option(pb.POLARS_BIO_PROGRESS_BAR)
    yield
    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, original)


def _reader():
    return pb.scan_bed(BED_PATH)


def _sql():
    return pb.sql("SELECT * FROM generate_series(1, 10)")


def _range_operation():
    df = pl.DataFrame({"chrom": ["chr1", "chr1"], "start": [1, 20], "end": [10, 30]})
    df.config_meta.set(coordinate_system_zero_based=False)
    return pb.overlap(df, df, output_type="polars.LazyFrame")


def _depth():
    return pb.depth(BAM_PATH)


# One source per place where polars-bio streams batches into Polars.
LAZY_SOURCES = [
    pytest.param(_reader, id="reader"),
    pytest.param(_sql, id="sql"),
    pytest.param(_range_operation, id="range-operation"),
    pytest.param(_depth, id="depth"),
]


def test_option_key_and_default():
    assert pb.POLARS_BIO_PROGRESS_BAR == "datafusion.bio.progress_bar"
    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "true"


@pytest.mark.parametrize("source", LAZY_SOURCES)
def test_progress_bar_is_shown_by_default(source, capsys):
    result = source().collect()

    assert result.height > 0
    assert BAR_MARKER in capsys.readouterr().err


@pytest.mark.parametrize("source", LAZY_SOURCES)
def test_progress_bar_can_be_disabled(source, capsys):
    expected = source().collect()
    capsys.readouterr()

    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)
    result = source().collect()

    assert BAR_MARKER not in capsys.readouterr().err
    assert result.equals(expected)


def test_option_is_read_when_the_frame_is_collected(capsys):
    lazy_frame = _sql()

    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)
    lazy_frame.collect()
    assert BAR_MARKER not in capsys.readouterr().err

    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, True)
    lazy_frame.collect()
    assert BAR_MARKER in capsys.readouterr().err


@pytest.mark.parametrize(
    ("value", "stored"),
    [
        (False, "false"),
        (True, "true"),
        ("false", "false"),
        ("FALSE", "false"),
        ("True", "true"),
    ],
)
def test_option_accepts_booleans_and_their_names(value, stored):
    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, value)

    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == stored


@pytest.mark.parametrize("value", ["flase", "off", "", 0, 1, None])
def test_option_rejects_other_values(value):
    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)

    with pytest.raises(ValueError, match="datafusion.bio.progress_bar"):
        pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, value)

    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "false"


def test_set_progress_bar_sets_the_option(capsys):
    pb.set_progress_bar(False)
    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "false"
    _sql().collect()
    assert BAR_MARKER not in capsys.readouterr().err

    pb.set_progress_bar(True)
    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "true"
    _sql().collect()
    assert BAR_MARKER in capsys.readouterr().err


def test_set_progress_bar_rejects_non_boolean_values():
    pb.set_progress_bar(False)

    with pytest.raises(ValueError, match="datafusion.bio.progress_bar"):
        pb.set_progress_bar("off")

    assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "false"


def test_other_tqdm_progress_bars_are_not_affected(capsys):
    pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)

    for _ in tqdm(range(3), unit="samples"):
        _sql().collect()

    err = capsys.readouterr().err
    assert "samples/s" in err
    assert BAR_MARKER not in err


def _shows_count(stderr, n):
    """Whether a bar reported `n` processed rows.

    Match the count field only: a plain substring would also hit the
    timing-dependent rate, e.g. `20rows` inside `8111.20rows/s`.
    """
    return re.search(rf"(?<![\d.]){n}rows(?!/s)", stderr) is not None


def _run_script(script, **env):
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, **env},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_disabling_after_import_needs_no_environment_variable():
    result = _run_script(
        """
        import polars_bio as pb

        pb.sql("SELECT * FROM generate_series(1, 10)").collect()
        pb.set_option(pb.POLARS_BIO_PROGRESS_BAR, False)
        pb.sql("SELECT * FROM generate_series(1, 20)").collect()
        """
    )

    assert result.stderr.count(BAR_MARKER) >= 1
    assert _shows_count(result.stderr, 10)
    assert not _shows_count(result.stderr, 20)


def test_tqdm_disable_environment_variable_still_wins():
    # TQDM_DISABLE only supplies a default, so the bar must not be created
    # with an explicit `disable=False` while the option is on.
    result = _run_script(
        """
        import polars_bio as pb

        assert pb.get_option(pb.POLARS_BIO_PROGRESS_BAR) == "true"
        pb.sql("SELECT * FROM generate_series(1, 10)").collect()
        """,
        TQDM_DISABLE="1",
    )

    assert BAR_MARKER not in result.stderr
