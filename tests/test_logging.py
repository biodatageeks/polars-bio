import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def _run_logging_script(script):
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.mark.parametrize("configured", [False, True])
def test_import_preserves_application_logging(configured):
    _run_logging_script(
        f"""
        import io
        import logging

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        if {configured}:
            handler = logging.StreamHandler(io.StringIO())
            handler.setFormatter(logging.Formatter("application: %(message)s"))
            root.addHandler(handler)
        handlers = list(root.handlers)
        level = root.level

        import polars_bio

        assert root.handlers == handlers
        assert root.level == level
        assert logging.getLogger("polars_bio").level == logging.WARNING
        """
    )


@pytest.mark.parametrize(
    ("level", "expected"),
    [("debug", 10), ("info", 20), ("warn", 30), ("warning", 30), ("INFO", 20)],
)
def test_set_loglevel_preserves_application_logging(level, expected):
    _run_logging_script(
        f"""
        import io
        import logging
        import polars_bio as pb

        root = logging.getLogger()
        root.setLevel(logging.ERROR)
        handler = logging.StreamHandler(io.StringIO())
        formatter = logging.Formatter("application: %(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)
        handlers = list(root.handlers)

        pb.set_loglevel({level!r})

        assert logging.getLogger("polars_bio").level == {expected}
        assert root.level == logging.ERROR
        assert root.handlers == handlers
        assert handler.formatter is formatter
        assert logging.getLogger("other_library").getEffectiveLevel() == logging.ERROR
        """
    )


@pytest.mark.parametrize("level", ["error", "INVALID", ""])
def test_set_loglevel_rejects_invalid_levels_without_reconfiguring(level):
    _run_logging_script(
        f"""
        import logging
        import polars_bio as pb

        root = logging.getLogger()
        root.setLevel(logging.ERROR)
        handlers = list(root.handlers)
        logger = logging.getLogger("polars_bio")
        previous_level = logger.level
        try:
            pb.set_loglevel({level!r})
        except ValueError as error:
            assert str(error) == {level.lower()!r} + " is not a valid log level"
        else:
            raise AssertionError("Expected ValueError")
        assert logger.level == previous_level
        assert root.level == logging.ERROR
        assert root.handlers == handlers
        """
    )


@pytest.mark.parametrize("configure_first", [False, True])
def test_application_info_logging_is_independent_of_library_level(configure_first):
    result = _run_logging_script(
        f"""
        import io
        import logging
        import polars_bio as pb

        root = logging.getLogger()
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.setFormatter(logging.Formatter("application: %(message)s"))
        def configure_application():
            root.setLevel(logging.INFO)
            root.addHandler(handler)

        if {configure_first}:
            configure_application()
        pb.set_loglevel("warning")
        if not {configure_first}:
            configure_application()

        root.info("Application information")
        logging.getLogger("polars_bio").info("Filtered library information")
        logging.getLogger("polars_bio").warning("Library warning")
        assert output.getvalue().splitlines() == [
            "application: Application information", "application: Library warning"
        ]
        """
    )
    assert result.stderr == ""


def test_application_handler_receives_python_and_rust_logs_once():
    result = _run_logging_script(
        """
        import logging
        import polars_bio as pb

        records = []
        class CaptureHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        root = logging.getLogger()
        handler = CaptureHandler()
        root.addHandler(handler)
        root.setLevel(logging.ERROR)
        pb.set_loglevel("info")
        assert root.level == logging.ERROR
        logging.getLogger("polars_bio").info("Python library message")
        pb.register_fasta("tests/data/io/fasta/test.fasta", "logging_test")

        messages = [record.getMessage() for record in records]
        assert messages.count("Python library message") == 1
        native = [record for record in records if "registered for path:" in record.getMessage()]
        assert len(native) == 1
        assert native[0].name == "polars_bio"
        """
    )
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("level", "configured"), [("info", True), ("warning", True), ("warning", False)]
)
def test_overlap_announcement_respects_library_level(level, configured):
    _run_logging_script(
        f"""
        import logging
        import polars as pl
        import polars_bio as pb

        records = []
        class CaptureHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        root = logging.getLogger()
        handler = CaptureHandler()
        if {configured}:
            root.addHandler(handler)
        root.setLevel(logging.INFO)
        handlers = list(root.handlers)
        pb.set_loglevel({level!r})

        left = pl.DataFrame({{"chrom": ["chr1"], "start": [1], "end": [3]}})
        right = pl.DataFrame({{"chrom": ["chr1"], "start": [2], "end": [4]}})
        left.config_meta.set(coordinate_system_zero_based=True)
        right.config_meta.set(coordinate_system_zero_based=True)
        assert pb.overlap(left, right, algorithm="Coitrees").collect().shape == (1, 6)

        announcement = "Optimizing into IntervalJoinExec using Coitrees algorithm"
        messages = [record for record in records if record.getMessage() == announcement]
        assert len(messages) == (1 if {level!r} == "info" else 0)
        if messages:
            assert messages[0].name == "polars_bio.range_op_io"
        if {configured}:
            root.info("Application information")
            assert sum(record.getMessage() == "Application information" for record in records) == 1
        assert root.level == logging.INFO
        assert root.handlers == handlers
        """
    )


def test_warning_remains_visible_without_application_handlers():
    result = _run_logging_script(
        """
        import logging
        import polars_bio

        logging.getLogger("polars_bio").warning("Library warning without handlers")
        """
    )
    assert result.stderr.count("Library warning without handlers") == 1
