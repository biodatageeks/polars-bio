"""
Test-time compatibility shims.

- Provide a lightweight `pytest.assume()` context manager so tests can
  soft-assert multiple conditions without stopping at the first failure.
- Ensure `pl.testing` attribute is available even if the parent package
  doesn't expose the submodule on import (older Polars behavior).
"""

from __future__ import annotations

import contextlib
import threading

import pytest

# --- pytest.assume shim ----------------------------------------------------
# Collect assumption failures per-test, then fail at teardown with a summary.
_local = threading.local()


@pytest.fixture(autouse=True)
def _assume_collector():
    _local.failures = []  # reset for each test function
    try:
        yield
    finally:
        failures = getattr(_local, "failures", [])
        if failures:
            # Summarize all assumption failures at the end of the test
            pytest.fail("Assumptions failed:\n- " + "\n- ".join(failures))


def _install_pytest_assume():
    if not hasattr(pytest, "assume"):

        @contextlib.contextmanager
        def assume():
            try:
                yield
            except AssertionError as e:  # record, don't stop the test
                failures = getattr(_local, "failures", None)
                if failures is not None:
                    failures.append(str(e))
                else:
                    # Outside of a test context: re-raise
                    raise

        # Attach to pytest namespace
        setattr(pytest, "assume", assume)


_install_pytest_assume()


# --- polars.testing exposure ----------------------------------------------
try:
    import polars as pl
    import polars.testing as _pl_testing  # type: ignore

    # Some versions don't expose `pl.testing` on the parent module by default.
    if not hasattr(pl, "testing"):
        pl.testing = _pl_testing  # type: ignore[attr-defined]
except Exception:
    # If Polars isn't importable here, let the tests surface the real error.
    pass


@pytest.fixture
def typed_tag_alignment_paths(tmp_path):
    """Create a small BAM/SAM fixture with exact scalar and array tag typing."""
    from array import array

    import pysam

    bam_path = tmp_path / "typed_tags.bam"
    sam_path = tmp_path / "typed_tags.sam"
    header = {
        "HD": {"VN": "1.6", "SO": "coordinate"},
        "SQ": [{"SN": "chr1", "LN": 1000}],
    }

    with pysam.AlignmentFile(str(bam_path), "wb", header=header) as bam_out:
        for index, (char_tag, hex_tag) in enumerate((("A", "0A0B"), ("B", "C0FFEE"))):
            record = pysam.AlignedSegment()
            record.query_name = f"typed-read-{index + 1}"
            record.query_sequence = "ACGTACGT"
            record.flag = 0
            record.reference_id = 0
            record.reference_start = 100 + (index * 10)
            record.mapping_quality = 60
            record.cigarstring = "8M"
            record.next_reference_id = -1
            record.next_reference_start = -1
            record.template_length = 0
            record.query_qualities = pysam.qualitystring_to_array("FFFFFFFF")
            record.set_tag("XA", char_tag, value_type="A")
            record.set_tag("XH", hex_tag, value_type="H")
            record.set_tag("XI", 100 + index, value_type="i")
            record.set_tag("XF", 1.5 + index, value_type="f")
            record.set_tag("XZ", f"text-{index}", value_type="Z")
            record.set_tag("ML", array("B", [1 + index, 2 + index, 3 + index]))
            record.set_tag("FZ", array("H", [1000 + index, 2000 + index]))
            record.set_tag("XC", array("b", [-1, 2 + index]))
            record.set_tag("XS", array("h", [-3, 4 + index]))
            record.set_tag("XB", array("i", [5 + index, 6 + index]))
            record.set_tag("XW", array("I", [7 + index, 8 + index]))
            record.set_tag("XY", array("f", [0.25 + index, 0.5 + index]))
            bam_out.write(record)

    with pysam.AlignmentFile(str(bam_path), "rb") as bam_in:
        with pysam.AlignmentFile(str(sam_path), "wh", header=bam_in.header) as sam_out:
            for record in bam_in:
                sam_out.write(record)

    return {"bam": str(bam_path), "sam": str(sam_path)}
